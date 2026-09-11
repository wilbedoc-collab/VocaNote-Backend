#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

import jsonschema

from render_result import render_all
from vocanote_chunking import sha256_obj, stable_json
from vocanote_semantic_executor import extract_json, validate_semantic_output
from vocanote_tombstone import assert_recording_active, guarded_atomic_write_json

SERVER_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
HERMES = os.environ.get('VOCANOTE_HERMES_BIN', '/Users/ahnbot/.openclaw/workspace/hermes_eval/repo/venv/bin/hermes')
HERMES_PROFILE = os.environ.get('VOCANOTE_HERMES_PROFILE', 'vocanoteworker')
REDUCE_PROMPT_VERSION = 'vocanote.semantic_reduce.v1'
FINAL_SCHEMA_VERSION = 'semantic_final_v1'
GROUP_SCHEMA_VERSION = 'semantic_reduce_group_v1'


class ReduceExecutor(Protocol):
    calls: list[str]
    def reduce(self, *, recording_id: str, input_hash: str, items: list[dict[str, Any]], output_kind: str, group_id: str | None = None, source_chunk_ids: list[str] | None = None) -> dict[str, Any]: ...


class ReduceError(RuntimeError):
    pass


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def validate_schema(data: dict[str, Any], schema_name: str) -> None:
    schema = load_json(SERVER_DIR / schema_name)
    jsonschema.validate(instance=data, schema=schema)


def norm_text(s: str) -> str:
    return re.sub(r'\s+', ' ', str(s or '').strip()).lower()


def union_items(outputs: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    by_text: dict[str, dict[str, Any]] = {}
    for out in outputs:
        for item in out.get(field) or []:
            text = str(item.get('text') or '').strip()
            if not text:
                continue
            key = norm_text(text)
            ids = sorted({int(x) for x in item.get('source_segment_ids') or []})
            if key not in by_text:
                by_text[key] = {'text': text, 'source_segment_ids': ids}
            else:
                by_text[key]['source_segment_ids'] = sorted(set(by_text[key]['source_segment_ids']) | set(ids))
    return sorted(by_text.values(), key=lambda x: (x['source_segment_ids'][0] if x['source_segment_ids'] else 10**9, x['text']))


def merge_semantics(*, recording_id: str, input_hash: str, outputs: list[dict[str, Any]], output_kind: str, group_id: str | None = None, source_chunk_ids: list[str] | None = None) -> dict[str, Any]:
    summaries = [str(o.get('summary') or '').strip() for o in outputs if str(o.get('summary') or '').strip()]
    common = {
        'recording_id': recording_id,
        'reduce_input_hash': input_hash,
        'summary': ' / '.join(summaries[:6]) or '의미 요약 결과',
        'topics': union_items(outputs, 'topics')[:20],
        'key_points': union_items(outputs, 'key_points')[:40],
        'decisions': union_items(outputs, 'decisions')[:30],
        'action_items': union_items(outputs, 'action_items')[:30],
        'questions': union_items(outputs, 'questions')[:30],
        'warnings': sorted({w for o in outputs for w in (o.get('warnings') or []) if str(w).strip()}),
    }
    if output_kind == 'group':
        return {'schema_version': GROUP_SCHEMA_VERSION, 'group_id': group_id or 'group_00', 'source_chunk_ids': source_chunk_ids or [], 'source_chunk_count': len(source_chunk_ids or []), **common}
    return {'schema_version': FINAL_SCHEMA_VERSION, 'source_chunk_count': len(source_chunk_ids or outputs), **common}


class FakeReduceExecutor:
    def __init__(self, *, fail_once: set[str] | None = None):
        self.calls: list[str] = []
        self.fail_once = set(fail_once or set())
        self._failed: set[str] = set()

    def reduce(self, *, recording_id: str, input_hash: str, items: list[dict[str, Any]], output_kind: str, group_id: str | None = None, source_chunk_ids: list[str] | None = None) -> dict[str, Any]:
        call_id = group_id or 'final'
        self.calls.append(call_id)
        if call_id in self.fail_once and call_id not in self._failed:
            self._failed.add(call_id)
            raise ReduceError(f'fake reduce injected failure {call_id}')
        return merge_semantics(recording_id=recording_id, input_hash=input_hash, outputs=items, output_kind=output_kind, group_id=group_id, source_chunk_ids=source_chunk_ids)


def _compact_source_items(items: list[dict[str, Any]], *, output_kind: str) -> list[dict[str, Any]]:
    """Keep reduce prompts small enough for Hermes CLI/API while preserving provenance.

    Long semantic chunks can be 15-20KB each. Passing several full outputs via
    `-q` has caused Broken pipe failures. Reduce only needs validated semantic
    claims, not every verbose duplicate.
    """
    limits = {
        'summary_chars': 320 if output_kind == 'group' else 260,
        'topics': 4 if output_kind == 'group' else 8,
        'key_points': 5 if output_kind == 'group' else 10,
        'decisions': 4,
        'action_items': 4,
        'questions': 4,
        'warnings': 2,
    }
    compact = []
    for item in items:
        row = {
            'chunk_id': item.get('chunk_id') or item.get('group_id'),
            'summary': str(item.get('summary') or '')[:limits['summary_chars']],
        }
        for field in ['topics', 'key_points', 'decisions', 'action_items', 'questions']:
            row[field] = [
                {'text': str(x.get('text') or '')[:260], 'source_segment_ids': sorted({int(v) for v in (x.get('source_segment_ids') or [])})[:40]}
                for x in (item.get(field) or [])[:limits.get(field, 8)]
                if str(x.get('text') or '').strip()
            ]
        row['warnings'] = [str(x)[:240] for x in (item.get('warnings') or [])[:limits['warnings']]]
        compact.append(row)
    return compact


def hermes_reduce_prompt(*, recording_id: str, input_hash: str, items: list[dict[str, Any]], output_kind: str, group_id: str | None, source_chunk_ids: list[str]) -> str:
    schema_version = GROUP_SCHEMA_VERSION if output_kind == 'group' else FINAL_SCHEMA_VERSION
    payload = {'recording_id': recording_id, 'reduce_input_hash': input_hash, 'output_kind': output_kind, 'group_id': group_id, 'source_chunk_ids': source_chunk_ids, 'validated_semantic_items': _compact_source_items(items, output_kind=output_kind)}
    group_extra = ', "group_id":"group_01", "source_chunk_ids":["chunk_0001"], "source_chunk_count":1' if output_kind == 'group' else ', "source_chunk_count":1'
    return f"""You are the isolated VocaNote semantic reduce executor.
Use ONLY the JSON payload below. Do not use transcript text outside validated semantic chunk outputs.
Preserve provenance: every item must keep source_segment_ids. If merging duplicates, union/dedupe/sort source_segment_ids.
Preserve high-level meeting identity, not just topic frequency: who/role, organization/project, place/setting, meeting type, observed results, methodological flaws, decisions, next actions, and multi-agenda structure.
Prefer titles and summaries that answer: who met, in what context, and what was discussed.
Output JSON only. No markdown.

Required JSON shape:
{{
  "schema_version":"{schema_version}",
  "recording_id":"...",
  "reduce_input_hash":"...",
  "summary":"...",
  "topics":[{{"text":"...","source_segment_ids":[1]}}],
  "key_points":[], "decisions":[], "action_items":[], "questions":[],
  "warnings":[]{group_extra}
}}

Payload:
""" + json.dumps(payload, ensure_ascii=False)


class HermesReduceExecutor:
    def __init__(self, *, timeout: int | None = None,
                 command_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None):
        self.calls: list[str] = []
        self.timeout = timeout or int(os.environ.get('VOCANOTE_SEMANTIC_REDUCE_HERMES_TIMEOUT', '300'))
        self.command_runner = command_runner

    def reduce(self, *, recording_id: str, input_hash: str, items: list[dict[str, Any]], output_kind: str, group_id: str | None = None, source_chunk_ids: list[str] | None = None) -> dict[str, Any]:
        call_id = group_id or 'final'
        self.calls.append(call_id)
        prompt = hermes_reduce_prompt(recording_id=recording_id, input_hash=input_hash, items=items, output_kind=output_kind, group_id=group_id, source_chunk_ids=source_chunk_ids or [])
        cmd = [HERMES, '--profile', HERMES_PROFILE, 'chat', '-Q', '--ignore-rules', '--source', 'vocanote-semantic-reduce', '--toolsets', 'safe', '--max-turns', '1', '-q', prompt]
        if self.command_runner:
            proc = self.command_runner(cmd, stage=f'semantic_reduce:{call_id}', timeout=self.timeout)
        else:
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=self.timeout)
        raw = (proc.stdout or '') + ('\nSTDERR:\n' + proc.stderr if proc.stderr else '')
        if proc.returncode != 0:
            raise ReduceError(f'Hermes reduce failed rc={proc.returncode}: {raw[-2000:]}')
        data = extract_json(proc.stdout or '')
        if not isinstance(data, dict):
            raise ReduceError('Hermes reduce output is not object')
        return data


def make_reduce_executor(name: str | None = None, **kwargs: Any) -> ReduceExecutor:
    name = (name or os.environ.get('VOCANOTE_SEMANTIC_EXECUTOR') or 'hermes').strip().lower()
    if name == 'fake':
        return FakeReduceExecutor(**kwargs)
    if name == 'hermes':
        return HermesReduceExecutor(**kwargs)
    raise ValueError(f'unknown VOCANOTE_SEMANTIC_EXECUTOR={name!r}')


def _fallback_allowed(exc: Exception) -> bool:
    if os.environ.get('VOCANOTE_REDUCE_DETERMINISTIC_FALLBACK', '1') in {'0', 'false', 'False'}:
        return False
    msg = repr(exc)
    return 'Broken pipe' in msg or 'API call failed after 3 retries' in msg or 'timed out' in msg.lower()


def reduce_with_deterministic_fallback(
    executor: ReduceExecutor,
    *,
    recording_id: str,
    input_hash: str,
    items: list[dict[str, Any]],
    output_kind: str,
    group_id: str | None = None,
    source_chunk_ids: list[str] | None = None,
) -> dict[str, Any]:
    try:
        return executor.reduce(
            recording_id=recording_id,
            input_hash=input_hash,
            items=items,
            output_kind=output_kind,
            group_id=group_id,
            source_chunk_ids=source_chunk_ids,
        )
    except Exception as exc:
        if not _fallback_allowed(exc):
            raise
        out = merge_semantics(
            recording_id=recording_id,
            input_hash=input_hash,
            outputs=items,
            output_kind=output_kind,
            group_id=group_id,
            source_chunk_ids=source_chunk_ids,
        )
        out.setdefault('warnings', [])
        out['warnings'].append(f'deterministic_reduce_fallback:{type(exc).__name__}:{str(exc)[:240]}')
        return out


def output_hashes_and_items(recording_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]], list[str], set[int]]:
    chunk_dir = recording_dir / 'semantic_chunks'
    manifest = load_json(chunk_dir / 'manifest.json')
    items=[]; hashes=[]; valid_ids=set()
    for row in manifest.get('chunks') or []:
        if row.get('status') != 'completed':
            raise ReduceError(f'incomplete semantic chunk {row.get("chunk_id")} status={row.get("status")}')
        chunk = load_json(chunk_dir / row['path'])
        out_path = chunk_dir / row.get('output_path','')
        out = load_json(out_path)
        ok, warnings = validate_semantic_output(out, chunk)
        if not ok:
            raise ReduceError(f'invalid chunk output {row["chunk_id"]}: {warnings[:5]}')
        if row.get('output_hash') != sha256_obj(out):
            raise ReduceError(f'output_hash_mismatch {row["chunk_id"]}')
        hashes.append(row['output_hash'])
        items.append(out)
        valid_ids.update(int(x) for x in chunk['primary']['source_segment_ids'])
    return manifest, items, hashes, valid_ids


def reduce_hash(*, schema_version: str, provider: str, model: str, model_config: dict[str, Any], ordered_hashes: list[str], config: dict[str, Any], group_id: str | None = None) -> str:
    return sha256_obj({'reduce_schema_version': schema_version, 'reduce_prompt_version': REDUCE_PROMPT_VERSION, 'provider': provider, 'model': model, 'model_config': model_config, 'ordered_chunk_output_hashes': ordered_hashes, 'reduce_configuration': config, 'group_id': group_id})


def chars_for_outputs(outputs: list[dict[str, Any]]) -> int:
    return len(stable_json([{k: o.get(k) for k in ['summary','topics','key_points','decisions','action_items','questions']} for o in outputs]))


def group_outputs(outputs: list[dict[str, Any]], hashes: list[str], max_chars: int) -> list[tuple[str, list[dict[str, Any]], list[str]]]:
    groups=[]; cur=[]; curh=[]; idx=1
    for out,h in zip(outputs,hashes):
        if cur and chars_for_outputs(cur+[out]) > max_chars:
            groups.append((f'group_{idx:02d}', cur, curh)); idx+=1; cur=[]; curh=[]
        cur.append(out); curh.append(h)
    if cur: groups.append((f'group_{idx:02d}', cur, curh))
    return groups


def normalize_reduce_output(data: dict[str, Any]) -> list[str]:
    """Normalize common LLM shape drift before strict schema validation.

    Reduce prompts ask for warnings as strings, but real long recordings have
    returned warning objects such as {"text": "...", "source_segment_ids": [...]},
    mirroring semantic item shapes. This is safe to coerce because warnings are
    advisory metadata; provenance-bearing semantic claims remain strictly checked.
    """
    notes: list[str] = []
    normalized_warnings: list[str] = []
    for idx, item in enumerate(data.get('warnings') or []):
        text = ''
        if isinstance(item, str):
            text = item
        elif isinstance(item, dict):
            text = str(item.get('text') or item.get('warning') or item.get('summary') or '').strip()
            ids = item.get('source_segment_ids') or []
            if ids:
                try:
                    clean_ids = sorted({int(x) for x in ids})[:20]
                    if text:
                        text = f"{text} (source_segment_ids={clean_ids})"
                except Exception:
                    pass
            notes.append(f'warnings_object_normalized:{idx}')
        else:
            text = str(item).strip()
            notes.append(f'warnings_non_string_normalized:{idx}')
        if text.strip():
            normalized_warnings.append(text.strip())
    data['warnings'] = normalized_warnings
    return notes


def validate_reduce_output(data: dict[str, Any], *, schema_name: str, recording_id: str, input_hash: str, valid_segment_ids: set[int], source_chunk_count: int | None = None) -> tuple[bool, list[str]]:
    warnings=[]
    warnings.extend(normalize_reduce_output(data))
    try: validate_schema(data, schema_name)
    except Exception as exc: return False, [*warnings, f'schema_validation_failed:{exc}']
    if data.get('recording_id') != recording_id: warnings.append('recording_id_mismatch')
    if data.get('reduce_input_hash') != input_hash: warnings.append('reduce_input_hash_mismatch')
    if source_chunk_count is not None and int(data.get('source_chunk_count') or -1) != source_chunk_count: warnings.append('source_chunk_count_mismatch')
    for field in ['topics','key_points','decisions','action_items','questions']:
        cleaned=[]
        for item in data.get(field) or []:
            ids=sorted({int(x) for x in item.get('source_segment_ids') or []})
            if not ids: warnings.append(f'{field}_missing_source_segment_ids')
            bad=[x for x in ids if x not in valid_segment_ids]
            if bad: warnings.append(f'{field}_unknown_source_segment_ids:{bad[:5]}')
            item['source_segment_ids']=ids
            cleaned.append(item)
        data[field]=cleaned
    fatal=('schema_validation_failed','recording_id_mismatch','reduce_input_hash_mismatch','source_chunk_count_mismatch')
    ok=not any(w.startswith(fatal) or 'missing_source_segment_ids' in w or 'unknown_source_segment_ids' in w for w in warnings)
    return ok,warnings


def _item_texts(semantic_final: dict[str, Any], field: str, limit: int, width: int | None = None) -> list[str]:
    vals = [str(x.get('text') or '').strip() for x in semantic_final.get(field) or [] if str(x.get('text') or '').strip()]
    if width:
        vals = [v[:width].strip() for v in vals]
    return vals[:limit]


def _semantic_corpus(semantic_final: dict[str, Any]) -> str:
    parts = [str(semantic_final.get('summary') or '')]
    for field in ['topics', 'key_points', 'decisions', 'action_items', 'questions']:
        parts.extend(_item_texts(semantic_final, field, 80))
    return '\n'.join(parts)


def _infer_recording_context(semantic_final: dict[str, Any]) -> dict[str, Any]:
    corpus = _semantic_corpus(semantic_final)
    has = lambda *terms: any(t in corpus for t in terms)
    ctx = {
        'situation': '회의록',
        'actors': '',
        'place': '',
        'agenda': [],
        'warnings': [],
    }
    if has('산학', '산단', '교수님', '중소기업 과제', '과제 제안서'):
        ctx['situation'] = '산학연구 대면회의' if has('보여드리', '키워볼까요', '화면', '대면') else '산학연구 회의'
    elif has('상담', '원장님', '환자', '시술'):
        ctx['situation'] = '현장회의' if has('보여드리', '키워볼까요', '여기') else '상담회의'
    if has('리주비놀', 'Re2O', '리투오') or has('마이디'):
        ctx['actors'] = '리주비놀·마이디'
    if has('바노바기'):
        ctx['place'] = '바노바기 피부과'
    agenda_rules = [
        (('AI 검색', 'GPT 추천', 'Gemini', 'GEO', 'AEO', 'candidate pool', '후보군', 'citation'), 'AI 피부과 추천'),
        (('FRAN', 'Face Re-Aging', '얼굴', '노화', '치료 전후', '피부 변화', '미래 변화'), '피부변화 모델'),
        (('자동 레이블링', 'serial', 'longitudinal', '시리얼', '레이블링'), '자동 레이블링'),
        (('더마톡신', '보툴렉스', '오플레스'), '더마톡신 강의'),
        (('학회', '연수평점', '정규연수기관', '의협'), '학회 운영'),
        (('필러', '더채움', '셀레디엠'), '필러 제품'),
    ]
    for terms, label in agenda_rules:
        if has(*terms) and label not in ctx['agenda']:
            ctx['agenda'].append(label)
    if has('두 번째 전사', '오인식', '추첨', '정반전'):
        ctx['warnings'].append('possible_duplicate_or_low_quality_transcript')
    return ctx


def _build_contextual_title(semantic_final: dict[str, Any], topics: list[str], key_points: list[str]) -> tuple[str, str]:
    ctx = _infer_recording_context(semantic_final)
    agenda = ctx['agenda'][:3]
    if ctx['actors'] and ctx['situation'] != '회의록' and agenda:
        return f"{ctx['actors']} {ctx['situation']}-{'·'.join(agenda)}"[:80], 'context_actors_situation_agenda'
    if ctx['place'] and agenda:
        return f"{ctx['place']} {ctx['situation']}-{'·'.join(agenda[:2])}"[:80], 'context_place_situation_agenda'
    base = topics[0] if topics else (key_points[0] if key_points else 'VocaNote 의미 요약')
    return base[:80], 'topic_fallback'


def _build_context_sections(semantic_final: dict[str, Any], key_points: list[str], decisions: list[str], actions: list[str], questions: list[str]) -> list[dict[str, Any]]:
    ctx = _infer_recording_context(semantic_final)
    sections: list[dict[str, Any]] = []
    identity_items = []
    if ctx['situation'] != '회의록':
        identity_items.append(f"회의 성격: {ctx['situation']}")
    if ctx['actors']:
        identity_items.append(f"주요 주체: {ctx['actors']}")
    if ctx['place']:
        identity_items.append(f"장소/맥락: {ctx['place']}")
    if ctx['agenda']:
        identity_items.append('주요 안건: ' + ', '.join(ctx['agenda'][:4]))
    if identity_items:
        sections.append({'heading': '회의 정체', 'items': identity_items})
    observed = [x for x in key_points if any(t in x for t in ['떴', '등장', '확인', '관찰', 'Gemini', 'GPT', '추천순위', '후보군'])]
    hypotheses = [x for x in key_points if any(t in x for t in ['영향', '중요', '요인', '가격', '신뢰도', '후기', '최신', '외부'])]
    if observed:
        sections.append({'heading': '관찰된 결과', 'items': observed[:8]})
    if hypotheses:
        sections.append({'heading': '가설·분석', 'items': hypotheses[:8]})
    remaining_key_points = [x for x in key_points if x not in observed and x not in hypotheses]
    if remaining_key_points:
        sections.append({'heading': '핵심 포인트', 'items': remaining_key_points[:10]})
    if decisions:
        sections.append({'heading': '결정 사항', 'items': decisions[:12]})
    if actions:
        sections.append({'heading': '다음 액션', 'items': actions[:12]})
    if questions:
        sections.append({'heading': '확인 질문', 'items': questions[:10]})
    if not sections:
        sections.append({'heading': '요약', 'items': [semantic_final.get('summary') or '의미 요약 결과']})
    return sections


def semantic_final_to_result(recording_id: str, semantic_final: dict[str, Any], *, language: str='ko') -> dict[str, Any]:
    topics=_item_texts(semantic_final,'topics',8)
    keywords=_item_texts(semantic_final,'topics',8,40)
    key_points=_item_texts(semantic_final,'key_points',12)
    decisions=_item_texts(semantic_final,'decisions',12)
    actions=_item_texts(semantic_final,'action_items',12)
    questions=_item_texts(semantic_final,'questions',12)
    title_text,title_reason=_build_contextual_title(semantic_final, topics, key_points)
    slug=re.sub(r'[^0-9A-Za-z가-힣_-]+','-',title_text).strip('-')[:80] or 'vocanote-summary'
    sections=_build_context_sections(semantic_final, key_points, decisions, actions, questions)
    ctx = _infer_recording_context(semantic_final)
    flow=[]
    if ctx['agenda']:
        flow.append('주요 안건: ' + ', '.join(ctx['agenda'][:4]))
    flow.extend((decisions + actions + key_points)[:10 - len(flow)])
    result={'schema_version':'vocanote.result.v1','recording_id':recording_id,'language':language,'content_type':'meeting','title':{'text':title_text,'filename_slug':slug,'reason':title_reason},'keywords':keywords,'memo_summary':{'topic':title_text,'one_line':semantic_final.get('summary') or title_text,'core_points':key_points[:8] or [semantic_final.get('summary') or title_text],'flow':flow or [semantic_final.get('summary') or title_text]},'structured_note':{'note_type':'meeting','sections':sections},'quality':{'needs_review':False,'uncertain_segments':[],'warnings':(semantic_final.get('warnings') or []) + ctx['warnings']}}
    validate_schema(result,'result.schema.json')
    return result


def _guarded_json(recording_id: str, recording_dir: Path, path: Path, payload: Any, assert_claim: Callable[[],None] | None):
    guarded_atomic_write_json(recording_id=recording_id, recording_dir=recording_dir, target_path=path, payload=payload, assert_claim=assert_claim, create_parent=True)


def run_semantic_reduce(*, recording_dir: Path, recording_id: str, executor: ReduceExecutor | None=None, assert_claim: Callable[[],None] | None=None, stop_after_groups: int | None=None, render_outputs: bool=True) -> dict[str,Any]:
    recording_dir=recording_dir.resolve(); assert_recording_active(recording_id, recording_dir)
    if assert_claim: assert_claim()
    executor=executor or make_reduce_executor()
    manifest, outputs, hashes, valid_ids = output_hashes_and_items(recording_dir)
    settings=manifest.get('settings') or {}
    reduce_max=int(settings.get('SEMANTIC_REDUCE_MAX_CHARS') or os.environ.get('SEMANTIC_REDUCE_MAX_CHARS','12000'))
    group_max=int(settings.get('SEMANTIC_REDUCE_GROUP_MAX_CHARS') or os.environ.get('SEMANTIC_REDUCE_GROUP_MAX_CHARS','24000'))
    provider=manifest.get('provider','openai-codex'); model=manifest.get('model','gpt-5.5'); model_config=manifest.get('model_config') or {}
    reduce_dir=recording_dir/'semantic_reduce'; reduce_dir.mkdir(parents=True, exist_ok=True)
    stats={'mode':'direct','groups':0,'group_reused':0,'group_executed':0,'final_reused':0,'final_executed':0,'map_new_calls':0}
    source_chunk_ids=[o['chunk_id'] for o in outputs]
    config={'SEMANTIC_REDUCE_MAX_CHARS':reduce_max,'SEMANTIC_REDUCE_GROUP_MAX_CHARS':group_max}
    if chars_for_outputs(outputs) <= reduce_max:
        ih=reduce_hash(schema_version=FINAL_SCHEMA_VERSION,provider=provider,model=model,model_config=model_config,ordered_hashes=hashes,config={**config,'mode':'direct'})
        final_path=recording_dir/'semantic_final.json'
        if final_path.exists():
            try:
                final=load_json(final_path); ok,_=validate_reduce_output(final,schema_name='semantic_final.schema.json',recording_id=recording_id,input_hash=ih,valid_segment_ids=valid_ids,source_chunk_count=len(outputs))
                if ok: stats['final_reused']=1
                else: final=None
            except Exception: final=None
        else: final=None
        if not final:
            final=reduce_with_deterministic_fallback(executor, recording_id=recording_id, input_hash=ih, items=outputs, output_kind='final', source_chunk_ids=source_chunk_ids)
            ok,w=validate_reduce_output(final,schema_name='semantic_final.schema.json',recording_id=recording_id,input_hash=ih,valid_segment_ids=valid_ids,source_chunk_count=len(outputs))
            if not ok: raise ReduceError('semantic_final_validation_failed:'+ ';'.join(w[:10]))
            _guarded_json(recording_id,recording_dir,final_path,final,assert_claim); stats['final_executed']=1
    else:
        stats['mode']='group'
        groups=group_outputs(outputs,hashes,group_max); stats['groups']=len(groups)
        existing_reduce_manifest = {}
        reduce_manifest_path = reduce_dir / 'manifest.json'
        if reduce_manifest_path.exists():
            try:
                existing_reduce_manifest = load_json(reduce_manifest_path).get('groups') or {}
            except Exception:
                existing_reduce_manifest = {}
        reduce_manifest_groups = dict(existing_reduce_manifest)
        group_results=[]; group_hashes=[]; done=0
        for gid,gouts,ghashes in groups:
            assert_recording_active(recording_id, recording_dir)
            gih=reduce_hash(schema_version=GROUP_SCHEMA_VERSION,provider=provider,model=model,model_config=model_config,ordered_hashes=ghashes,config={**config,'mode':'group'},group_id=gid)
            gpath=reduce_dir/f'{gid}.json'
            gout=None
            if gpath.exists():
                try:
                    cand=load_json(gpath); ok,_=validate_reduce_output(cand,schema_name='semantic_reduce_group.schema.json',recording_id=recording_id,input_hash=gih,valid_segment_ids=valid_ids,source_chunk_count=len(gouts))
                    if ok and sha256_obj(cand)==reduce_manifest_groups.get(gid,{}).get('output_hash'): gout=cand; stats['group_reused']+=1
                except Exception: gout=None
            if not gout:
                gout=reduce_with_deterministic_fallback(executor, recording_id=recording_id, input_hash=gih, items=gouts, output_kind='group', group_id=gid, source_chunk_ids=[o['chunk_id'] for o in gouts])
                ok,w=validate_reduce_output(gout,schema_name='semantic_reduce_group.schema.json',recording_id=recording_id,input_hash=gih,valid_segment_ids=valid_ids,source_chunk_count=len(gouts))
                if not ok: raise ReduceError(f'group_validation_failed {gid}:'+ ';'.join(w[:10]))
                _guarded_json(recording_id,recording_dir,gpath,gout,assert_claim); stats['group_executed']+=1
            group_results.append(gout); group_hashes.append(sha256_obj(gout))
            reduce_manifest_groups[gid] = {'reduce_input_hash': gout['reduce_input_hash'], 'output_hash': sha256_obj(gout), 'status': 'completed'}
            m={'schema_version':'semantic_reduce_manifest_v1','recording_id':recording_id,'groups':reduce_manifest_groups,'updated_at':now_iso()}
            _guarded_json(recording_id,recording_dir,reduce_dir/'manifest.json',m,assert_claim)
            done+=1
            if stop_after_groups is not None and done>=stop_after_groups:
                stats['aborted']=True; return stats
        fih=reduce_hash(schema_version=FINAL_SCHEMA_VERSION,provider=provider,model=model,model_config=model_config,ordered_hashes=group_hashes,config={**config,'mode':'final_from_groups'})
        final_path=recording_dir/'semantic_final.json'
        final=None
        if final_path.exists() and stats['group_executed'] == 0:
            try:
                cand=load_json(final_path); ok,_=validate_reduce_output(cand,schema_name='semantic_final.schema.json',recording_id=recording_id,input_hash=fih,valid_segment_ids=valid_ids,source_chunk_count=len(outputs))
                if ok: final=cand; stats['final_reused']=1
            except Exception: final=None
        if not final:
            final=reduce_with_deterministic_fallback(executor, recording_id=recording_id, input_hash=fih, items=group_results, output_kind='final', source_chunk_ids=source_chunk_ids)
            final['source_chunk_count']=len(outputs)
            ok,w=validate_reduce_output(final,schema_name='semantic_final.schema.json',recording_id=recording_id,input_hash=fih,valid_segment_ids=valid_ids,source_chunk_count=len(outputs))
            if not ok: raise ReduceError('semantic_final_validation_failed:'+ ';'.join(w[:10]))
            _guarded_json(recording_id,recording_dir,final_path,final,assert_claim); stats['final_executed']=1
    # deterministic adapter and renderer
    final=load_json(recording_dir/'semantic_final.json')
    result=semantic_final_to_result(recording_id, final)
    _guarded_json(recording_id,recording_dir,recording_dir/'result.validated.json',result,assert_claim)
    if render_outputs:
        render_all(recording_dir)
    return stats

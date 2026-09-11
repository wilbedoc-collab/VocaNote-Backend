#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import jsonschema

from phase2b_semantic_executor_test import init_recording, count_completed
from vocanote_chunking import sha256_obj, stable_json
from vocanote_semantic_executor import FakeSemanticExecutor, run_semantic_map
from vocanote_semantic_reduce import FakeReduceExecutor, HermesReduceExecutor, run_semantic_reduce, load_json, semantic_final_to_result
from vocanote_tombstone import create_tombstone

BASE = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
RUN_ROOT = BASE / 'validation_runs'


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def schema_ok(path: Path, schema_name: str) -> bool:
    schema = load_json(BASE / schema_name)
    data = load_json(path)
    jsonschema.validate(instance=data, schema=schema)
    return True


def set_reduce_limits(d: Path, *, reduce_max: int, group_max: int) -> None:
    mp = d / 'semantic_chunks' / 'manifest.json'
    m = load_json(mp)
    m['settings']['SEMANTIC_REDUCE_MAX_CHARS'] = reduce_max
    m['settings']['SEMANTIC_REDUCE_GROUP_MAX_CHARS'] = group_max
    mp.write_text(stable_json(m) + '\n', encoding='utf-8')


def prepare_mapped(parent: Path, *, n: int = 1000, reduce_max: int = 12000, group_max: int = 24000) -> tuple[Path, str]:
    d, rid, _segments, _ctx = init_recording(parent, n=n)
    set_reduce_limits(d, reduce_max=reduce_max, group_max=group_max)
    run_semantic_map(recording_dir=d, recording_id=rid, executor=FakeSemanticExecutor())
    return d, rid


def validate_full_outputs(d: Path) -> bool:
    return (schema_ok(d / 'semantic_final.json', 'semantic_final.schema.json') and schema_ok(d / 'result.validated.json', 'result.schema.json') and (d / 'summary.md').exists() and (d / 'analysis.md').exists() and (d / 'summary.md').stat().st_size > 0 and (d / 'analysis.md').stat().st_size > 0)


def test_direct(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'direct', n=20, reduce_max=999999, group_max=999999)
    ex = FakeReduceExecutor()
    stats = run_semantic_reduce(recording_dir=d, recording_id=rid, executor=ex)
    final = load_json(d / 'semantic_final.json')
    return {'ok': stats['mode'] == 'direct' and stats['final_executed'] == 1 and ex.calls == ['final'] and validate_full_outputs(d), 'mode': stats['mode'], 'calls': ex.calls, 'semantic_final_schema': schema_ok(d/'semantic_final.json','semantic_final.schema.json')}


def test_hierarchical(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'hier', reduce_max=1000, group_max=4500)
    ex = FakeReduceExecutor()
    stats = run_semantic_reduce(recording_dir=d, recording_id=rid, executor=ex)
    groups = len(list((d/'semantic_reduce').glob('group_*.json')))
    return {'ok': stats['mode'] == 'group' and groups > 1 and stats['final_executed'] == 1 and validate_full_outputs(d), 'mode': stats['mode'], 'group_count': groups, 'calls': ex.calls}


def test_group_resume(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'resume', reduce_max=1000, group_max=4500)
    ex1 = FakeReduceExecutor()
    stats1 = run_semantic_reduce(recording_dir=d, recording_id=rid, executor=ex1, stop_after_groups=2)
    ex2 = FakeReduceExecutor()
    stats2 = run_semantic_reduce(recording_dir=d, recording_id=rid, executor=ex2)
    return {'ok': stats1.get('aborted') and stats2['group_reused'] == 2 and stats2['final_executed'] == 1 and validate_full_outputs(d), 'first_calls': ex1.calls, 'second_calls': ex2.calls, 'group_checkpoint_reuse_count': stats2['group_reused'], 'map_new_calls': 0}


def test_changed_chunk_invalidation(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'changed', reduce_max=1000, group_max=4500)
    run_semantic_reduce(recording_dir=d, recording_id=rid, executor=FakeReduceExecutor())
    mp = d / 'semantic_chunks' / 'manifest.json'
    m = load_json(mp)
    row = m['chunks'][9]
    op = d / 'semantic_chunks' / row['output_path']
    out = load_json(op)
    out['key_points'].append({'text': '동일 의미 항목 변경 테스트', 'source_segment_ids': [row['primary_source_segment_ids'][0]]})
    op.write_text(json.dumps(out, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    row['output_hash'] = sha256_obj(out)
    mp.write_text(stable_json(m) + '\n', encoding='utf-8')
    ex = FakeReduceExecutor()
    stats = run_semantic_reduce(recording_dir=d, recording_id=rid, executor=ex)
    group_calls = [c for c in ex.calls if c.startswith('group_')]
    return {'ok': len(group_calls) == 1 and stats['group_reused'] > 0 and stats['final_executed'] == 1, 'affected_group_calls': group_calls, 'group_reuse': stats['group_reused'], 'final_executed': stats['final_executed']}


def test_corrupt_group(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'corrupt_group', reduce_max=1000, group_max=4500)
    run_semantic_reduce(recording_dir=d, recording_id=rid, executor=FakeReduceExecutor())
    gp = sorted((d/'semantic_reduce').glob('group_*.json'))[1]
    gp.write_text('{bad json', encoding='utf-8')
    ex = FakeReduceExecutor()
    stats = run_semantic_reduce(recording_dir=d, recording_id=rid, executor=ex)
    group_calls = [c for c in ex.calls if c.startswith('group_')]
    return {'ok': group_calls == [gp.stem] and stats['final_executed'] == 1, 'reexecuted_group': group_calls, 'final_executed': stats['final_executed']}


def test_provenance_union(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'union', n=50, reduce_max=999999, group_max=999999)
    mp = d/'semantic_chunks'/'manifest.json'
    m=load_json(mp)
    rows=m['chunks'][:2]
    for row in rows:
        op=d/'semantic_chunks'/row['output_path']
        out=load_json(op)
        sid=row['primary_source_segment_ids'][0]
        out['key_points']=[{'text':'중복 의미 항목', 'source_segment_ids':[sid]}]
        op.write_text(json.dumps(out, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
        row['output_hash']=sha256_obj(out)
    mp.write_text(stable_json(m)+'\n', encoding='utf-8')
    run_semantic_reduce(recording_dir=d, recording_id=rid, executor=FakeReduceExecutor())
    final=load_json(d/'semantic_final.json')
    item=[x for x in final['key_points'] if x['text']=='중복 의미 항목'][0]
    expected=sorted([rows[0]['primary_source_segment_ids'][0], rows[1]['primary_source_segment_ids'][0]])
    return {'ok': item['source_segment_ids']==expected, 'union_ids': item['source_segment_ids'], 'expected': expected}


def test_adapter_renderer(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'adapter', n=20, reduce_max=999999, group_max=999999)
    run_semantic_reduce(recording_dir=d, recording_id=rid, executor=FakeReduceExecutor())
    return {'ok': schema_ok(d/'result.validated.json','result.schema.json') and (d/'summary.md').exists() and (d/'analysis.md').exists(), 'result_path': str(d/'result.validated.json'), 'summary_bytes': (d/'summary.md').stat().st_size, 'analysis_bytes': (d/'analysis.md').stat().st_size}


def test_warning_object_normalization(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'warning_object', n=40, reduce_max=999999, group_max=999999)
    class WarningObjectReduce(FakeReduceExecutor):
        def reduce(self, **kwargs):
            out = super().reduce(**kwargs)
            out['warnings'] = [
                '문자열 경고',
                {'text': '객체형 경고도 문자열로 정규화되어야 함', 'source_segment_ids': [1, 2, 2]},
            ]
            return out
    run_semantic_reduce(recording_dir=d, recording_id=rid, executor=WarningObjectReduce())
    final = load_json(d / 'semantic_final.json')
    result = load_json(d / 'result.validated.json')
    normalized = final.get('warnings') or []
    return {
        'ok': validate_full_outputs(d) and all(isinstance(x, str) for x in normalized) and any('객체형 경고' in x for x in normalized) and all(isinstance(x, str) for x in result.get('quality', {}).get('warnings') or []),
        'warnings': normalized,
    }


def test_broken_pipe_deterministic_fallback(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'broken_pipe_fallback', reduce_max=1000, group_max=4500)
    class BrokenFinalReduce(FakeReduceExecutor):
        def reduce(self, **kwargs):
            if kwargs.get('output_kind') == 'final':
                raise RuntimeError('API call failed after 3 retries: [Errno 32] Broken pipe')
            return super().reduce(**kwargs)
    ex = BrokenFinalReduce()
    stats = run_semantic_reduce(recording_dir=d, recording_id=rid, executor=ex)
    final = load_json(d / 'semantic_final.json')
    return {
        'ok': stats['mode'] == 'group' and stats['final_executed'] == 1 and validate_full_outputs(d) and any('deterministic_reduce_fallback' in w for w in final.get('warnings') or []),
        'calls': ex.calls,
        'fallback_warnings': final.get('warnings') or [],
    }


def test_contextual_title_and_sections(run_dir: Path) -> dict[str, Any]:
    rid = 'context-title-test'
    semantic_final = {
        'schema_version': 'semantic_final_v1',
        'recording_id': rid,
        'reduce_input_hash': 'a' * 64,
        'summary': '리주비놀/마이디 측 대표와 대학 연구진이 산학 R&D 대면회의에서 AI 검색 추천, FRAN 피부 변화 모델, 자동 레이블링을 논의했다.',
        'topics': [
            {'text': '리주비놀·마이디 산학연구 회의', 'source_segment_ids': [1]},
            {'text': 'AI 검색과 GPT 추천 후보군 실험', 'source_segment_ids': [2]},
            {'text': 'FRAN 기반 얼굴 피부 변화 모델', 'source_segment_ids': [3]},
            {'text': '자동 레이블링과 longitudinal serial 피부사진 연구', 'source_segment_ids': [4]},
        ],
        'key_points': [
            {'text': 'GPT에서 마이디 피부과가 후보군에 등장하기 시작했지만 Gemini에서는 아직 뜨지 않았다.', 'source_segment_ids': [2]},
            {'text': '가격 정보, 후기, 외부 신뢰 사이트 연동이 AI 추천 랭킹에 영향을 줄 수 있다.', 'source_segment_ids': [5]},
            {'text': '추천 실험은 selection query와 evaluation query를 분리해야 한다.', 'source_segment_ids': [6]},
        ],
        'decisions': [{'text': '작은 API 실험 harness를 에이전트로 자동화해 최종 GEO agent 구성요소로 발전시킨다.', 'source_segment_ids': [7]}],
        'action_items': [{'text': '다음 주 Zoom 미팅에서 자동 레이블링 검토를 이어간다.', 'source_segment_ids': [8]}],
        'questions': [],
        'source_chunk_count': 1,
        'warnings': [],
    }
    result = semantic_final_to_result(rid, semantic_final)
    headings = [s['heading'] for s in result['structured_note']['sections']]
    title = result['title']['text']
    return {
        'ok': '리주비놀·마이디' in title and '산학연구' in title and 'AI 피부과 추천' in title and '회의 정체' in headings and '관찰된 결과' in headings and '가설·분석' in headings and '다음 액션' in headings,
        'title': title,
        'headings': headings,
    }


def test_tombstone(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'tombstone', n=20, reduce_max=999999, group_max=999999)
    class TombstoneReduce(FakeReduceExecutor):
        def reduce(self, **kwargs):
            out=super().reduce(**kwargs)
            create_tombstone(rid, reason='phase2c fault injection', original_path=str(d), request_source='phase2c-test')
            return out
    ex=TombstoneReduce()
    try:
        run_semantic_reduce(recording_dir=d, recording_id=rid, executor=ex)
        ok=False; err=None
    except Exception as exc:
        ok='recording_deleted' in repr(exc) or 'RecordingDeleted' in type(exc).__name__
        err=repr(exc)
    return {'ok': ok and not (d/'semantic_final.json').exists() and not (d/'result.validated.json').exists() and not (d/'summary.md').exists(), 'error': err, 'semantic_final_exists': (d/'semantic_final.json').exists(), 'result_exists': (d/'result.validated.json').exists(), 'summary_exists': (d/'summary.md').exists()}


def test_hermes_small(run_dir: Path) -> dict[str, Any]:
    d, rid = prepare_mapped(run_dir / 'hermes_reduce', n=18, reduce_max=999999, group_max=999999)
    ex=HermesReduceExecutor(timeout=180)
    stats1=run_semantic_reduce(recording_dir=d, recording_id=rid, executor=ex)
    ex2=HermesReduceExecutor(timeout=180)
    stats2=run_semantic_reduce(recording_dir=d, recording_id=rid, executor=ex2)
    final=load_json(d/'semantic_final.json')
    provenance_ok=bool(final.get('key_points') or final.get('topics')) and all(x.get('source_segment_ids') for field in ['topics','key_points','decisions','action_items','questions'] for x in final.get(field,[]))
    return {'ok': stats1['final_executed']==1 and stats2['final_reused']==1 and len(ex2.calls)==0 and validate_full_outputs(d) and provenance_ok, 'chunk_count': load_json(d/'semantic_chunks'/'manifest.json')['chunk_count'], 'hermes_calls': ex.calls, 'cache_reuse': stats2['final_reused'], 'second_calls': len(ex2.calls), 'provenance_ok': provenance_ok}


def main() -> int:
    run_dir = RUN_ROOT / ('phase2c_semantic_reduce_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
    if run_dir.exists(): shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)
    tests={}
    for name, fn in [('direct_reduce',test_direct),('hierarchical_reduce',test_hierarchical),('group_resume',test_group_resume),('changed_chunk_invalidation',test_changed_chunk_invalidation),('corrupt_group_recovery',test_corrupt_group),('provenance_union',test_provenance_union),('result_adapter_renderer',test_adapter_renderer),('warning_object_normalization',test_warning_object_normalization),('broken_pipe_deterministic_fallback',test_broken_pipe_deterministic_fallback),('contextual_title_and_sections',test_contextual_title_and_sections),('tombstone_during_reduce',test_tombstone)]:
        tests[name]=fn(run_dir)
    fake_ok=all(v.get('ok') for v in tests.values())
    if fake_ok:
        try: tests['small_hermes_reduce']=test_hermes_small(run_dir)
        except Exception as exc: tests['small_hermes_reduce']={'ok':False,'error':repr(exc)}
    else:
        tests['small_hermes_reduce']={'ok':False,'error':'skipped because fake tests failed'}
    ok=all(v.get('ok') for v in tests.values())
    report={'ok':ok,'phase':'Phase 2C semantic reduce and adapter','run_dir':str(run_dir),'semantic_reduce_schema_path':str(BASE/'semantic_reduce_group.schema.json'),'semantic_final_schema_path':str(BASE/'semantic_final.schema.json'),'tests':tests}
    report_path=run_dir/'phase2c_semantic_reduce_report.json'
    write_json(report_path, report)
    write_json(RUN_ROOT/'phase2c_latest_report.json', report)
    print(json.dumps({'ok':ok,'report_path':str(report_path),'tests':tests}, ensure_ascii=False, indent=2))
    return 0 if ok else 1

if __name__ == '__main__':
    raise SystemExit(main())

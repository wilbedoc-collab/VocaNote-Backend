#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

import jsonschema

from vocanote_chunking import sha256_obj, stable_json
from vocanote_tombstone import assert_recording_active, check_deleted, guarded_atomic_write_json

SERVER_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
HERMES = os.environ.get('VOCANOTE_HERMES_BIN', '/Users/ahnbot/.openclaw/workspace/hermes_eval/repo/venv/bin/hermes')
HERMES_PROFILE = os.environ.get('VOCANOTE_HERMES_PROFILE', 'vocanoteworker')
SEMANTIC_OUTPUT_SCHEMA_VERSION = 'semantic_chunk_output_v1'


class SemanticExecutor(Protocol):
    calls: list[str]
    def execute(self, chunk: dict[str, Any]) -> dict[str, Any]: ...


class SemanticChunkError(RuntimeError):
    pass


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def validate_schema(data: dict[str, Any], schema_name: str = 'semantic_chunk_output.schema.json') -> None:
    schema = load_json(SERVER_DIR / schema_name)
    jsonschema.validate(instance=data, schema=schema)


def extract_json(text: str) -> Any:
    text = text.strip()
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith('session_id:') and 'tirith security scanner' not in ln]
    cleaned = '\n'.join(lines).strip()
    try:
        return json.loads(cleaned)
    except Exception:
        pass
    import re
    m = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', cleaned, re.S)
    if m:
        return json.loads(m.group(1))
    start = cleaned.find('{')
    end = cleaned.rfind('}')
    if start >= 0 and end > start:
        return json.loads(cleaned[start:end+1])
    raise ValueError('No JSON object found in Hermes output')


def primary_ids(chunk: dict[str, Any]) -> set[int]:
    return {int(x) for x in chunk['primary']['source_segment_ids']}


def overlap_only_ids(chunk: dict[str, Any]) -> set[int]:
    return {int(x) for x in chunk['overlap_context']['source_segment_ids']} - primary_ids(chunk)


def primary_text_chars(chunk: dict[str, Any]) -> int:
    return sum(len(str(s.get('text') or '').strip()) for s in chunk['primary']['segments'])


def _normalize_warning_item(item: Any) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        text = str(item.get('text') or item.get('warning') or item)
        ids = item.get('source_segment_ids')
        return f"{text} source_segment_ids={ids}" if ids else text
    return str(item)


def normalize_semantic_output(output: dict[str, Any]) -> dict[str, Any]:
    """Tolerate Hermes returning structured warning objects; schema stores warnings as strings."""
    output = dict(output)
    output['warnings'] = [_normalize_warning_item(x) for x in (output.get('warnings') or [])]
    return output


def validate_semantic_output(output: dict[str, Any], chunk: dict[str, Any], *, allow_empty_noise: bool = False) -> tuple[bool, list[str]]:
    warnings: list[str] = []
    normalized = normalize_semantic_output(output)
    output.clear(); output.update(normalized)
    try:
        validate_schema(output)
    except Exception as exc:
        return False, [f'schema_validation_failed: {exc}']
    if output.get('chunk_id') != chunk.get('chunk_id'):
        warnings.append(f"chunk_id_mismatch output={output.get('chunk_id')} expected={chunk.get('chunk_id')}")
    if output.get('input_hash') != chunk.get('input_hash'):
        warnings.append('input_hash_mismatch')
    pids = primary_ids(chunk)
    overlap_only = overlap_only_ids(chunk)
    all_items: list[dict[str, Any]] = []
    for field in ['topics', 'key_points', 'decisions', 'action_items', 'questions']:
        for item in output.get(field) or []:
            all_items.append(item)
            ids = item.get('source_segment_ids')
            if not ids:
                warnings.append(f'{field}_missing_source_segment_ids')
                continue
            for sid in ids:
                sid = int(sid)
                if sid not in pids:
                    warnings.append(f'{field}_source_segment_id_not_primary:{sid}')
                if sid in overlap_only:
                    warnings.append(f'{field}_overlap_only_id_used:{sid}')
    all_empty = not str(output.get('summary') or '').strip() and not all_items
    if all_empty and primary_text_chars(chunk) > 40 and not allow_empty_noise:
        warnings.append('empty_semantic_output_for_nonblank_primary')
    fatal_prefixes = ('chunk_id_mismatch', 'input_hash_mismatch', 'schema_validation_failed',)
    fatal_contains = ('missing_source_segment_ids', 'source_segment_id_not_primary', 'overlap_only_id_used', 'empty_semantic_output_for_nonblank_primary')
    ok = not any(w.startswith(fatal_prefixes) or any(x in w for x in fatal_contains) for w in warnings)
    return ok, warnings


class FakeSemanticExecutor:
    def __init__(self, *, fail_once: set[str] | None = None, empty_chunks: set[str] | None = None):
        self.calls: list[str] = []
        self.fail_once = set(fail_once or set())
        self.empty_chunks = set(empty_chunks or set())
        self._failed: set[str] = set()

    def execute(self, chunk: dict[str, Any]) -> dict[str, Any]:
        cid = chunk['chunk_id']
        self.calls.append(cid)
        if cid in self.fail_once and cid not in self._failed:
            self._failed.add(cid)
            raise SemanticChunkError(f'fake injected failure for {cid}')
        ids = chunk['primary']['source_segment_ids']
        if cid in self.empty_chunks:
            return {'schema_version': SEMANTIC_OUTPUT_SCHEMA_VERSION, 'chunk_id': cid, 'input_hash': chunk['input_hash'], 'summary': '', 'topics': [], 'key_points': [], 'decisions': [], 'action_items': [], 'questions': [], 'warnings': ['empty fake chunk']}
        first = ids[0] if ids else -1
        last = ids[-1] if ids else -1
        return {
            'schema_version': SEMANTIC_OUTPUT_SCHEMA_VERSION,
            'chunk_id': cid,
            'input_hash': chunk['input_hash'],
            'summary': f'{cid} primary segments {first}-{last} semantic summary',
            'topics': [{'text': f'topic {first}', 'source_segment_ids': [first]}] if ids else [],
            'key_points': [{'text': f'key point {first}-{last}', 'source_segment_ids': [first, last] if first != last else [first]}] if ids else [],
            'decisions': [],
            'action_items': [],
            'questions': [],
            'warnings': [],
        }


def hermes_semantic_prompt(chunk: dict[str, Any]) -> str:
    payload = {
        'chunk_id': chunk['chunk_id'],
        'input_hash': chunk['input_hash'],
        'PRIMARY SEGMENTS': chunk['primary']['segments'],
        'OVERLAP CONTEXT': chunk['overlap_context']['segments'],
        'primary_source_segment_ids': chunk['primary']['source_segment_ids'],
    }
    return """You are the isolated VocaNote semantic chunk executor.
Use ONLY the JSON payload below.

Task: extract semantic notes from this transcript chunk.

Rules:
- Semantic extraction is allowed ONLY from PRIMARY SEGMENTS.
- OVERLAP CONTEXT is provided only to understand continuity.
- Do not produce an item supported only by OVERLAP CONTEXT.
- Every extracted semantic item must include source_segment_ids.
- source_segment_ids must reference PRIMARY SEGMENTS only.
- Preserve meeting identity signals when present: who/role, organization/project, place/setting, meeting type, screen-sharing/deictic cues, research-project terms, and whether multiple STT-like repetitions appear.
- If a chunk contains observed experiment results, methodological flaws, decisions, or next actions, label them explicitly in key_points/decisions/action_items instead of flattening them into generic topics.
- Do not reduce across chunks. Do not create final result.validated JSON.
- Output JSON only. No markdown.

Required JSON shape:
{
  "schema_version":"semantic_chunk_output_v1",
  "chunk_id":"chunk_0001",
  "input_hash":"...",
  "summary":"...",
  "topics":[{"text":"...","source_segment_ids":[0]}],
  "key_points":[{"text":"...","source_segment_ids":[0]}],
  "decisions":[],
  "action_items":[],
  "questions":[],
  "warnings":[]
}

Payload:
""" + json.dumps(payload, ensure_ascii=False)


class HermesSemanticExecutor:
    def __init__(self, *, timeout: int | None = None,
                 command_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None):
        self.calls: list[str] = []
        self.timeout = timeout or int(os.environ.get('VOCANOTE_SEMANTIC_HERMES_TIMEOUT', '300'))
        self.command_runner = command_runner

    def execute(self, chunk: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(chunk['chunk_id'])
        cmd = [HERMES, '--profile', HERMES_PROFILE, 'chat', '-Q', '--ignore-rules', '--source', 'vocanote-semantic-chunk', '--toolsets', 'safe', '--max-turns', '1', '-q', hermes_semantic_prompt(chunk)]
        if self.command_runner:
            proc = self.command_runner(cmd, stage=f'semantic_map:{chunk["chunk_id"]}', timeout=self.timeout)
        else:
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=self.timeout)
        raw = (proc.stdout or '') + ('\nSTDERR:\n' + proc.stderr if proc.stderr else '')
        if proc.returncode != 0:
            raise SemanticChunkError(f'Hermes failed rc={proc.returncode}: {raw[-2000:]}')
        data = extract_json(proc.stdout or '')
        if not isinstance(data, dict):
            raise SemanticChunkError('Hermes output is not a JSON object')
        return data


def make_executor(name: str | None = None, **kwargs: Any) -> SemanticExecutor:
    name = (name or os.environ.get('VOCANOTE_SEMANTIC_EXECUTOR') or 'hermes').strip().lower()
    if name == 'fake':
        return FakeSemanticExecutor(**kwargs)
    if name == 'hermes':
        return HermesSemanticExecutor(**kwargs)
    raise ValueError(f'unknown VOCANOTE_SEMANTIC_EXECUTOR={name!r}')


def _find_input_path(chunk_dir: Path, row: dict[str, Any]) -> Path:
    return chunk_dir / row['path']


def _output_path(chunk_dir: Path, row: dict[str, Any]) -> Path:
    return chunk_dir / 'outputs' / f"{row['chunk_id']}.semantic.json"


def _validate_reusable_output(chunk_dir: Path, row: dict[str, Any], chunk: dict[str, Any]) -> tuple[bool, str | None]:
    if row.get('status') != 'completed':
        return False, 'manifest_status_not_completed'
    out_path = chunk_dir / str(row.get('output_path') or f"outputs/{row['chunk_id']}.semantic.json")
    if not out_path.exists():
        return False, 'output_file_missing'
    if row.get('input_hash') != chunk.get('input_hash'):
        return False, 'manifest_input_hash_current_mismatch'
    try:
        output = load_json(out_path)
    except Exception as exc:
        return False, f'output_json_invalid:{exc}'
    if output.get('chunk_id') != row.get('chunk_id'):
        return False, 'output_chunk_id_mismatch'
    if output.get('input_hash') != row.get('input_hash'):
        return False, 'output_input_hash_mismatch'
    output_hash = sha256_obj(output)
    if row.get('output_hash') != output_hash:
        return False, 'output_hash_mismatch'
    ok, warnings = validate_semantic_output(output, chunk)
    if not ok:
        return False, 'provenance_validation_failed:' + ';'.join(warnings[:5])
    return True, None


def _write_manifest(recording_id: str, recording_dir: Path, chunk_dir: Path, manifest: dict[str, Any], assert_claim: Callable[[], None] | None) -> None:
    guarded_atomic_write_json(
        recording_id=recording_id,
        recording_dir=recording_dir,
        target_path=chunk_dir / 'manifest.json',
        payload=manifest,
        assert_claim=assert_claim,
        create_parent=True,
    )


def _write_output(recording_id: str, recording_dir: Path, out_path: Path, output: dict[str, Any], assert_claim: Callable[[], None] | None) -> None:
    guarded_atomic_write_json(
        recording_id=recording_id,
        recording_dir=recording_dir,
        target_path=out_path,
        payload=output,
        assert_claim=assert_claim,
        create_parent=True,
    )


def run_semantic_map(
    *,
    recording_dir: Path,
    recording_id: str,
    executor: SemanticExecutor | None = None,
    assert_claim: Callable[[], None] | None = None,
    max_attempts: int | None = None,
    stale_seconds: int | None = None,
    stop_after_completed: int | None = None,
) -> dict[str, Any]:
    """Execute Phase 2B semantic map with guarded writes and resumable checkpoints."""
    recording_dir = recording_dir.resolve()
    assert_recording_active(recording_id, recording_dir)
    if assert_claim:
        assert_claim()
    chunk_dir = recording_dir / 'semantic_chunks'
    manifest_path = chunk_dir / 'manifest.json'
    manifest = load_json(manifest_path)
    executor = executor or make_executor()
    max_attempts = int(max_attempts if max_attempts is not None else os.environ.get('SEMANTIC_CHUNK_MAX_ATTEMPTS', '3'))
    stale_seconds = int(stale_seconds if stale_seconds is not None else os.environ.get('SEMANTIC_PROCESSING_STALE_SECONDS', '900'))
    stats = {'completed': 0, 'reused': 0, 'executed': 0, 'failed_attempts': 0, 'retry_count': 0, 'invalidated': 0, 'aborted': False}
    completed_this_run = 0
    now_ts = time.time()
    for row in manifest.get('chunks') or []:
        assert_recording_active(recording_id, recording_dir)
        if assert_claim:
            assert_claim()
        row.setdefault('status', 'pending')
        row.setdefault('attempts', 0)
        chunk = load_json(_find_input_path(chunk_dir, row))
        reusable, reason = _validate_reusable_output(chunk_dir, row, chunk)
        if reusable:
            stats['reused'] += 1
            stats['completed'] += 1
            continue
        if row.get('status') == 'processing' and row.get('processing_started_at'):
            try:
                started = datetime.fromisoformat(str(row['processing_started_at']).replace('Z', '+00:00')).timestamp()
                if now_ts - started <= stale_seconds:
                    raise SemanticChunkError(f"semantic_chunk_still_processing {row['chunk_id']}")
            except ValueError:
                pass
            row['status'] = 'pending'
            row['last_error'] = 'stale_processing_recovered'
            row['last_error_at'] = now_iso()
        if row.get('status') == 'completed' or reason not in {'manifest_status_not_completed'}:
            stats['invalidated'] += 1
        if int(row.get('attempts') or 0) >= max_attempts:
            row['status'] = 'failed'
            row['last_error'] = row.get('last_error') or 'max_attempts_exceeded'
            row['last_error_at'] = now_iso()
            _write_manifest(recording_id, recording_dir, chunk_dir, manifest, assert_claim)
            raise SemanticChunkError(f"semantic_chunk_max_attempts_exceeded {row['chunk_id']}")
        # mark processing before executor call, but never completed before output validation/write.
        row['status'] = 'processing'
        row['attempts'] = int(row.get('attempts') or 0) + 1
        row['processing_started_at'] = now_iso()
        row['last_error'] = None
        row['last_error_at'] = None
        _write_manifest(recording_id, recording_dir, chunk_dir, manifest, assert_claim)
        try:
            raw_output = executor.execute(chunk)
            ok, warnings = validate_semantic_output(raw_output, chunk)
            if not ok:
                raise SemanticChunkError('semantic_output_validation_failed: ' + '; '.join(warnings[:20]))
            if warnings:
                raw_output.setdefault('warnings', []).extend(warnings)
            out_path = _output_path(chunk_dir, row)
            # validation PASS -> output guarded write -> output hash -> manifest completed write
            _write_output(recording_id, recording_dir, out_path, raw_output, assert_claim)
            saved_output = load_json(out_path)
            output_hash = sha256_obj(saved_output)
            if output_hash != sha256_obj(raw_output):
                raise SemanticChunkError(f"output_hash_verify_failed {row['chunk_id']}")
            row['status'] = 'completed'
            row['output_path'] = str(out_path.relative_to(chunk_dir))
            row['output_hash'] = output_hash
            row['completed_at'] = now_iso()
            row['last_error'] = None
            row['last_error_at'] = None
            _write_manifest(recording_id, recording_dir, chunk_dir, manifest, assert_claim)
            stats['executed'] += 1
            stats['completed'] += 1
            completed_this_run += 1
        except Exception as exc:
            stats['failed_attempts'] += 1
            row['status'] = 'pending' if int(row.get('attempts') or 0) < max_attempts else 'failed'
            row['last_error'] = repr(exc)
            row['last_error_at'] = now_iso()
            _write_manifest(recording_id, recording_dir, chunk_dir, manifest, assert_claim)
            if int(row.get('attempts') or 0) < max_attempts:
                stats['retry_count'] += 1
                continue
            raise
        if stop_after_completed is not None and completed_this_run >= stop_after_completed:
            stats['aborted'] = True
            break
    manifest['phase2b_stats'] = stats
    _write_manifest(recording_id, recording_dir, chunk_dir, manifest, assert_claim)
    return stats

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""VocaNote AI worker daemon.

Pipeline:
  queued job -> STT -> transcript_raw -> Hermes correction -> transcript_clean
  -> Hermes semantic result JSON -> schema validation -> deterministic markdown -> completed

Isolation:
  Hermes is invoked as a fresh one-shot process per stage using profile
  `vocanoteworker`, --ignore-rules, --source vocanote-job, --toolsets safe,
  no --resume/--continue.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import jsonschema

from render_result import render_all
from vocanote_cloud import upload_recording_audio_to_drive
from vocanote_tombstone import RecordingDeleted, assert_recording_active, check_deleted, guarded_atomic_write_json, guarded_atomic_write_text
from vocanote_chunking import (
    CORRECTION_PROMPT_VERSION, build_chunks, chunk_segments_for_prompt,
    deterministic_global_context, merge_correction_chunks, validate_correction_chunk_output,
    write_semantic_chunks,
)
from vocanote_semantic_executor import make_executor, run_semantic_map
from vocanote_semantic_reduce import make_reduce_executor, run_semantic_reduce
from vocanote_process_runner import run_job_process
from vocanote_safe_edit import SafeEditStore
from vocanote_queue import (
    OwnershipLost, add_job_event, assert_owner, claim_next, heartbeat_job,
    increment_attempt_and_set_failure, init_db, mark_completed, recover_stale_jobs, update_job,
    connect, release_claim,
)

SERVER_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
HERMES = os.environ.get('VOCANOTE_HERMES_BIN', '/Users/ahnbot/.openclaw/workspace/hermes_eval/repo/venv/bin/hermes')
PYTHON = os.environ.get('RE2O_PYTHON', '/Users/ahnbot/.openclaw/workspace/hermes_eval/repo/venv/bin/python3')
WHISPER_MODEL = os.environ.get('VOCANOTE_WHISPER_MODEL') or os.environ.get('RE2O_WHISPER_MODEL') or 'large-v3'
STT_ENGINE = os.environ.get('VOCANOTE_STT_ENGINE', 'whisper_cpp').strip().lower()
WHISPER_CPP_BIN = os.environ.get('VOCANOTE_WHISPER_CPP_BIN', '/opt/homebrew/bin/whisper-cli')
WHISPER_CPP_MODEL = os.environ.get('VOCANOTE_WHISPER_CPP_MODEL', '/Users/ahnbot/.cache/whisper.cpp/ggml-large-v3.bin')
STT_TIMEOUT_SECONDS = int(os.environ.get('VOCANOTE_STT_TIMEOUT_SECONDS', '14400'))
HERMES_PROFILE = os.environ.get('VOCANOTE_HERMES_PROFILE', 'vocanoteworker')
HEARTBEAT_INTERVAL = int(os.environ.get('VOCANOTE_HEARTBEAT_INTERVAL', '20'))
LEASE_SECONDS = int(os.environ.get('VOCANOTE_LEASE_SECONDS', '90'))
MAX_ACTIVE_RECORDINGS = int(os.environ.get('VOCANOTE_MAX_ACTIVE_RECORDINGS', '3'))
MAX_CONCURRENT_STT = int(os.environ.get('VOCANOTE_MAX_CONCURRENT_STT', '1'))
MAX_CONCURRENT_CORRECTION = int(os.environ.get('VOCANOTE_MAX_CONCURRENT_CORRECTION', '2'))
CORRECTION_CHUNK_CONCURRENCY = max(1, int(os.environ.get('VOCANOTE_CORRECTION_CHUNK_CONCURRENCY', '2')))
MAX_CONCURRENT_SEMANTIC = int(os.environ.get('VOCANOTE_MAX_CONCURRENT_SEMANTIC', '2'))
MAX_CONCURRENT_REDUCE = int(os.environ.get('VOCANOTE_MAX_CONCURRENT_REDUCE', '1'))
STAGE_LIMITS = {
    'stt': MAX_CONCURRENT_STT,
    'correction': MAX_CONCURRENT_CORRECTION,
    'semantic': MAX_CONCURRENT_SEMANTIC,
    'reduce': MAX_CONCURRENT_REDUCE,
    'render': MAX_ACTIVE_RECORDINGS,
}
STAGE_RUNNING_STATUS = {
    'stt': 'stt_running',
    'correction': 'correction_running',
    'semantic': 'semantic_running',
    'reduce': 'reduce_running',
    'render': 'rendering',
}
STARTUP_ID = uuid.uuid4().hex[:10]


def worker_id() -> str:
    return f"vocanote-worker:{socket.gethostname()}:{os.getpid()}:{STARTUP_ID}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f'.{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def write_json(path: Path, data: Any) -> None:
    atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2))


TWO_STAGE_PIPELINE_VERSION = 'vocanote.two_stage.v1'


def two_stage_enabled() -> bool:
    return os.environ.get('VOCANOTE_TWO_STAGE_ENABLED', '1').strip().lower() not in {'0', 'false', 'no', 'off'}


def no_global_context(raw_segments: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        'schema_version': 'vocanote.global_context.v1',
        'method': 'no_global_context_fast_draft',
        'glossary_candidates': [],
        'note': 'FAST draft correction: no future/full global context. Raw PRIMARY and overlap text remain authoritative.',
    }


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def file_sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def version_paths(recording_dir: Path, version: str) -> dict[str, Path]:
    if version == 'fast':
        base = recording_dir / 'fast'
        return {
            'base': base,
            'correction_dir': base / 'correction' / 'correction_chunks',
            'snapshot_dir': base / 'snapshot',
            'transcript_json': base / 'snapshot' / 'transcript_fast.json',
            'segments_json': base / 'snapshot' / 'segments_fast.json',
            'transcript_txt': base / 'snapshot' / 'transcript_fast.txt',
            'global_context_json': base / 'correction' / 'global_context.fast.json',
        }
    return {
        'base': recording_dir,
        'correction_dir': recording_dir / 'correction_chunks',
        'snapshot_dir': recording_dir,
        'transcript_json': recording_dir / 'transcript_clean.json',
        'segments_json': recording_dir / 'segments_clean.json',
        'transcript_txt': recording_dir / 'transcript_clean.txt',
        'global_context_json': recording_dir / 'global_context.json',
    }


def snapshot_digest(paths: dict[str, Path]) -> str:
    payload = {k: file_sha256(v) for k, v in paths.items() if k.endswith('_json') or k.endswith('_txt')}
    return sha256_text(json.dumps(payload, sort_keys=True))


def job_claim_token(job: dict[str, Any]) -> str:
    token = str(job.get('claim_token') or '')
    if not token:
        raise OwnershipLost(f"missing claim_token job_id={job.get('job_id')}")
    return token


def job_db_path(job: dict[str, Any]) -> Path:
    return Path(job.get('_db_path') or (SERVER_DIR / 'jobs.sqlite3'))


def require_owner(job: dict[str, Any]) -> None:
    check_deleted(job['recording_id'])
    assert_owner(job['job_id'], worker_id=worker_id(), claim_token=job_claim_token(job), db_path=job_db_path(job))


def require_active(job: dict[str, Any]) -> None:
    assert_recording_active(job['recording_id'], Path(job['output_dir']), db_path=job_db_path(job))


def _claim_assertion(job: dict[str, Any]):
    def assertion():
        return require_owner(job)
    setattr(assertion, 'db_path', job_db_path(job))
    setattr(assertion, 'publish_lock', lambda: generation_publish_lock(job))
    setattr(assertion, 'register_publish', lambda path, conn: _register_published_artifact(conn, job, path))
    return assertion


@contextmanager
def generation_publish_lock(job: dict[str, Any]):
    """Serialize a file publish with canonical-generation switches."""
    db_path = job_db_path(job)
    with connect(db_path) as conn:
        conn.execute('BEGIN IMMEDIATE')
        def authoritative() -> bool:
            row = conn.execute('''
                SELECT 1 FROM recording_jobs
                WHERE job_id=? AND claimed_by=? AND claim_token=?
                  AND COALESCE(cancel_requested,0)=0
                  AND generation=COALESCE(
                    (SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),
                    generation
                  )
            ''', (job['job_id'], worker_id(), job_claim_token(job))).fetchone()
            return row is not None
        if not authoritative():
            conn.execute('ROLLBACK')
            raise OwnershipLost(f"publish authority lost: {job['job_id']}")
        try:
            yield conn
            if not authoritative():
                raise OwnershipLost(f"publish authority lost after write: {job['job_id']}")
            conn.execute('COMMIT')
        except Exception:
            try:
                conn.execute('ROLLBACK')
            except Exception:
                pass
            raise


def _register_published_artifact(conn, job: dict[str, Any], path: Path) -> None:
    """Register a published file inside the generation publish transaction."""
    rid = job['recording_id']; generation = int(job.get('generation') or 1)
    current = conn.execute(
        'SELECT current_generation FROM safe_edit_recordings WHERE recording_id=?', (rid,)
    ).fetchone()
    if not current:
        return
    if int(current['current_generation']) != generation:
        raise OwnershipLost(f"artifact registration stale generation: {job['job_id']}")
    resolved = str(Path(path).resolve()); role = artifact_role(Path(path))
    file_row = conn.execute('SELECT file_id FROM safe_edit_files WHERE path=?', (resolved,)).fetchone()
    file_id = file_row['file_id'] if file_row else uuid.uuid4().hex
    timestamp = datetime.now(timezone.utc).isoformat()
    if not file_row:
        conn.execute(
            'INSERT INTO safe_edit_files(file_id,path,created_at) VALUES(?,?,?)',
            (file_id, resolved, timestamp),
        )
    exists = conn.execute('''
        SELECT 1 FROM safe_edit_file_references
        WHERE file_id=? AND recording_id=? AND generation=? AND role=? AND active=1
    ''', (file_id, rid, generation, role)).fetchone()
    if not exists:
        conn.execute('''
            INSERT INTO safe_edit_file_references
            (reference_id,file_id,recording_id,generation,role,active,created_at)
            VALUES(?,?,?,?,?,1,?)
        ''', (uuid.uuid4().hex, file_id, rid, generation, role, timestamp))


def artifact_role(path: Path) -> str:
    rel = str(path).lower(); name = path.name.lower()
    if name == 'status.json': return 'STATUS'
    if name == 'metadata.json': return 'METADATA'
    if 'stt_raw' in name: return 'RAW_STT'
    if 'transcript_raw' in name: return 'TRANSCRIPT_RAW'
    if 'segments_' in name: return 'SEGMENTS'
    if '/fast/' in rel: return 'FAST'
    if 'transcript_clean' in name or 'correction_' in rel: return 'CORRECTION'
    if 'semantic' in rel: return 'SEMANTIC'
    if name == 'summary.md': return 'SUMMARY'
    if name == 'analysis.md' or 'result.' in name: return 'FINAL'
    if 'manifest' in name or 'checkpoint' in rel: return 'CHECKPOINT'
    return 'JOB_ARTIFACT'


def register_artifact_references(job: dict[str, Any], paths: list[Path] | None = None) -> dict[str, Any]:
    require_owner(job)
    d = Path(job['output_dir']); canonical = Path(job['audio_path']).resolve()
    candidates = paths if paths is not None else [p for p in d.rglob('*') if p.is_file() and not p.name.startswith('.')]
    artifacts = [(artifact_role(p), p) for p in candidates if p.exists() and p.resolve() != canonical]
    result = SafeEditStore(job_db_path(job), foundation_enabled=True, destructive_enabled=False).register_artifacts_if_cataloged(
        job['recording_id'], int(job.get('generation') or 1), artifacts
    )
    require_owner(job)
    return result


def guarded_write_json(job: dict[str, Any], path: Path, data: Any) -> None:
    require_active(job)
    guarded_atomic_write_json(
        recording_id=job['recording_id'],
        recording_dir=Path(job['output_dir']),
        target_path=path,
        payload=data,
        assert_claim=_claim_assertion(job),
        create_parent=False,
    )


def guarded_write_text(job: dict[str, Any], path: Path, content: str) -> None:
    require_active(job)
    guarded_atomic_write_text(
        recording_id=job['recording_id'],
        recording_dir=Path(job['output_dir']),
        target_path=path,
        content=content,
        assert_claim=_claim_assertion(job),
        create_parent=False,
    )


def json_valid(path: Path) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    try:
        json.loads(path.read_text(encoding='utf-8'))
        return True
    except Exception:
        return False


def generation_identity(job: dict[str, Any]) -> dict[str, Any]:
    """Bind reusable stage outputs to one canonical generation and audio."""
    audio = Path(job['audio_path']).resolve()
    digest = job.get('_canonical_audio_sha256')
    if not digest:
        digest = file_sha256(audio)
        if not digest:
            raise FileNotFoundError(f'canonical audio missing: {audio}')
        job['_canonical_audio_sha256'] = digest
    return {
        'recording_id': job['recording_id'],
        'generation': int(job.get('generation') or 1),
        'audio_path': str(audio),
        'audio_sha256': digest,
    }


def stage_generation_marker(job: dict[str, Any], stage: str) -> Path:
    return Path(job['output_dir']) / f'.phase88_generation_{stage}.json'


def stage_generation_ok(job: dict[str, Any], stage: str) -> bool:
    marker = stage_generation_marker(job, stage)
    try:
        return marker.exists() and load_json(marker) == generation_identity(job)
    except Exception:
        return False


def mark_stage_generation(job: dict[str, Any], stage: str) -> None:
    guarded_write_json(job, stage_generation_marker(job, stage), generation_identity(job))


def checkpoint_stt_ok(job: dict[str, Any]) -> bool:
    d = Path(job['output_dir'])
    if not stage_generation_ok(job, 'stt'):
        return False
    if not ((d / 'stt_raw.json').exists() and (d / 'transcript_raw.txt').exists() and (d / 'segments_raw.json').exists()):
        return False
    if (d / 'transcript_raw.txt').stat().st_size <= 0:
        return False
    if not (json_valid(d / 'stt_raw.json') and json_valid(d / 'segments_raw.json')):
        return False
    try:
        return load_json(d / 'stt_raw.json').get('recording_id') == job['recording_id'] and load_json(d / 'segments_raw.json').get('recording_id') == job['recording_id']
    except Exception:
        return False


def checkpoint_correction_ok(job: dict[str, Any]) -> bool:
    d = Path(job['output_dir'])
    if not stage_generation_ok(job, 'correction_final'):
        return False
    if not ((d / 'transcript_clean.json').exists() and (d / 'transcript_clean.txt').exists() and (d / 'segments_clean.json').exists()):
        return False
    if not (json_valid(d / 'transcript_clean.json') and json_valid(d / 'segments_clean.json')):
        return False
    try:
        data = load_json(d / 'transcript_clean.json')
        validate_schema(data, 'transcript.schema.json')
        return data.get('recording_id') == job['recording_id'] and load_json(d / 'segments_clean.json').get('recording_id') == job['recording_id']
    except Exception:
        return False


def checkpoint_fast_ok(job: dict[str, Any]) -> bool:
    d = Path(job['output_dir'])
    if not stage_generation_ok(job, 'correction_fast'):
        return False
    paths = version_paths(d, 'fast')
    if not (paths['transcript_json'].exists() and paths['transcript_txt'].exists() and paths['segments_json'].exists()):
        return False
    if not (json_valid(paths['transcript_json']) and json_valid(paths['segments_json'])):
        return False
    try:
        data = load_json(paths['transcript_json'])
        validate_schema(data, 'transcript.schema.json')
        return data.get('recording_id') == job['recording_id'] and load_json(paths['segments_json']).get('recording_id') == job['recording_id']
    except Exception:
        return False


def checkpoint_semantic_ok(job: dict[str, Any]) -> bool:
    d = Path(job['output_dir'])
    if not stage_generation_ok(job, 'semantic'):
        return False
    required = ['semantic_chunks/manifest.json', 'semantic_final.json', 'result.validated.json']
    if any(not (d / name).exists() for name in required):
        return False
    if not json_valid(d / 'semantic_final.json') or not json_valid(d / 'result.validated.json'):
        return False
    try:
        final = load_json(d / 'semantic_final.json')
        result = load_json(d / 'result.validated.json')
        validate_schema(final, 'semantic_final.schema.json')
        validate_schema(result, 'result.schema.json')
        return final.get('recording_id') == job['recording_id'] and result.get('recording_id') == job['recording_id']
    except Exception:
        return False


def checkpoint_render_ok(job: dict[str, Any]) -> bool:
    d = Path(job['output_dir'])
    return stage_generation_ok(job, 'render') and (d / 'summary.md').exists() and (d / 'summary.md').stat().st_size > 0 and (d / 'analysis.md').exists() and (d / 'analysis.md').stat().st_size > 0


def canonical_display_version(recording_dir: Path) -> str:
    status = load_json(recording_dir / 'status.json') if (recording_dir / 'status.json').exists() else {}
    if status.get('human_available'):
        return 'HUMAN'
    if status.get('final_available') or checkpoint_file_set(recording_dir, 'final'):
        return 'FINAL'
    if status.get('fast_available') or checkpoint_file_set(recording_dir, 'fast'):
        return 'FAST'
    return 'PROCESSING'


def checkpoint_file_set(recording_dir: Path, version: str) -> bool:
    paths = version_paths(recording_dir, version)
    return paths['transcript_json'].exists() and paths['segments_json'].exists() and paths['transcript_txt'].exists()


def token_diff_counts(a: str, b: str) -> dict[str, Any]:
    from difflib import SequenceMatcher
    aw = re.findall(r'[0-9A-Za-z가-힣]+|[^0-9A-Za-z가-힣\s]+', a or '')
    bw = re.findall(r'[0-9A-Za-z가-힣]+|[^0-9A-Za-z가-힣\s]+', b or '')
    sm = SequenceMatcher(None, aw, bw)
    insertions = deletions = replacements = 0
    changed_tokens = []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == 'insert': insertions += j2 - j1
        elif tag == 'delete': deletions += i2 - i1
        elif tag == 'replace': replacements += max(i2 - i1, j2 - j1)
        if tag != 'equal' and len(changed_tokens) < 20:
            changed_tokens.append({'op': tag, 'fast': aw[i1:i2], 'final': bw[j1:j2]})
    return {'insertions': insertions, 'deletions': deletions, 'replacements': replacements, 'edit_distance': insertions + deletions + replacements, 'changed_tokens': changed_tokens}


def write_fast_final_diff(job: dict[str, Any]) -> None:
    d = Path(job['output_dir'])
    fast_p = version_paths(d, 'fast')
    final_p = version_paths(d, 'final')
    if not (fast_p['segments_json'].exists() and final_p['segments_json'].exists()):
        return
    raw_by_id = {int(s['index']): s for s in (load_json(d / 'segments_raw.json').get('segments') or [])}
    fast_segments = load_json(fast_p['segments_json']).get('segments') or []
    final_segments = load_json(final_p['segments_json']).get('segments') or []
    final_by_id = {int(s['index']): s for s in final_segments}
    manifest_path = final_p['correction_dir'] / 'manifest.json'
    seg_to_chunk = {}
    if manifest_path.exists():
        for ch in load_json(manifest_path).get('chunks') or []:
            a, b = ch.get('primary_segment_range') or [None, None]
            if a is not None and b is not None:
                for sid in range(int(a), int(b) + 1): seg_to_chunk[sid] = ch.get('chunk_id')
    diffs = []
    mapping_failures = 0
    for fs in fast_segments:
        sid = int(fs['index'])
        fin = final_by_id.get(sid)
        raw = raw_by_id.get(sid, {})
        if not fin:
            mapping_failures += 1
            continue
        ft = str(fs.get('text') or '')
        fint = str(fin.get('text') or '')
        changed = ft.strip() != fint.strip()
        row = {
            'recording_id': job['recording_id'],
            'chunk_id': seg_to_chunk.get(sid),
            'segment_id': sid,
            'start_time': raw.get('start', fs.get('start')),
            'end_time': raw.get('end', fs.get('end')),
            'raw_text': raw.get('text') or fs.get('raw_text') or '',
            'fast_text': ft,
            'final_text': fint,
            'changed': changed,
        }
        if changed:
            row.update(token_diff_counts(ft, fint))
        diffs.append(row)
    payload = {'schema_version': 'vocanote.fast_final_diff.v1', 'recording_id': job['recording_id'], 'pipeline_version': TWO_STAGE_PIPELINE_VERSION, 'changed_segments': sum(1 for r in diffs if r['changed']), 'mapping_failures': mapping_failures, 'diffs': diffs}
    guarded_write_json(job, d / 'fast_final_diff.json', payload)


class StageSlotUnavailable(RuntimeError):
    pass


def next_required_stage(job: dict[str, Any]) -> str | None:
    if not checkpoint_stt_ok(job):
        return 'stt'
    if not checkpoint_correction_ok(job):
        return 'correction'
    if not checkpoint_semantic_ok(job):
        return 'semantic'
    if not checkpoint_render_ok(job):
        return 'render'
    return None


def running_stage_count(conn, stage: str) -> int:
    status = STAGE_RUNNING_STATUS[stage]
    row = conn.execute('''SELECT COUNT(*) AS c FROM recording_jobs
        WHERE status=? AND lease_expires_at IS NOT NULL AND lease_expires_at >= ?
          AND COALESCE(cancel_requested,0)=0
          AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)
    ''', (status, now_iso())).fetchone()
    return int(row['c'] if row else 0)


def active_recording_count(conn) -> int:
    row = conn.execute("""
        SELECT COUNT(*) AS c FROM recording_jobs
        WHERE status IN ('claimed','stt_running','correction_running','semantic_running','reduce_running','validating','rendering')
          AND lease_expires_at IS NOT NULL AND lease_expires_at >= ?
          AND COALESCE(cancel_requested,0)=0
          AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)
    """, (now_iso(),)).fetchone()
    return int(row['c'] if row else 0)


def acquire_stage_slot(job: dict[str, Any], stage: str, *, step: str | None = None) -> bool:
    limit = int(STAGE_LIMITS.get(stage, MAX_ACTIVE_RECORDINGS))
    status = STAGE_RUNNING_STATUS[stage]
    wid = worker_id()
    token = job_claim_token(job)
    ts = now_iso()
    from vocanote_queue import future_iso
    expires = future_iso(LEASE_SECONDS)
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        count = running_stage_count(conn, stage)
        # If this job is already in the stage, do not count it as a new slot.
        own = conn.execute('''SELECT status FROM recording_jobs
            WHERE job_id=? AND claimed_by=? AND claim_token=? AND COALESCE(cancel_requested,0)=0
              AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)
        ''', (job['job_id'], wid, token)).fetchone()
        own_in_stage = bool(own and own['status'] == status)
        if count >= limit and not own_in_stage:
            conn.execute('COMMIT')
            add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='stage_slot_wait', status=job.get('status'), step=stage, details={'stage': stage, 'limit': limit, 'active': count})
            return False
        cur = conn.execute('''
            UPDATE recording_jobs
            SET status=?, step=?, heartbeat_at=?, lease_expires_at=?, updated_at=?
            WHERE job_id=? AND claimed_by=? AND claim_token=?
              AND COALESCE(cancel_requested,0)=0
              AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)
        ''', (status, step or stage, ts, expires, ts, job['job_id'], wid, token))
        if cur.rowcount != 1:
            conn.execute('ROLLBACK')
            raise OwnershipLost(f'ownership_lost operation=acquire_stage_slot job_id={job["job_id"]}')
        conn.execute('''
            INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
            VALUES (?, ?, ?, 'stage_slot_acquired', ?, ?, ?, ?)
        ''', (job['job_id'], job['recording_id'], wid, status, step or stage, ts, json.dumps({'stage': stage, 'limit': limit, 'active_before': count}, ensure_ascii=False)))
        conn.execute('COMMIT')
    job['status'] = status
    job['step'] = step or stage
    job['heartbeat_at'] = ts
    job['lease_expires_at'] = expires
    return True


def yield_claim_for_stage(job: dict[str, Any], *, previous_step: str, reason: str) -> None:
    release_claim(job['job_id'], previous_status='retry_wait', previous_step=previous_step, decrement_attempt=False)
    add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=worker_id(), event='yield_for_stage_slot', status='retry_wait', step=previous_step, details=reason)
    raise StageSlotUnavailable(reason)


def claim_next_eligible(worker_id_value: str, *, lease_seconds: int = LEASE_SECONDS) -> dict[str, Any] | None:
    """Claim the oldest queued/retry job whose next required stage has capacity.

    This keeps a STT-bound recording from blocking another recording whose
    correction/semantic slot is free.
    """
    init_db()
    recover_stale_jobs(worker_id=worker_id_value)
    token = uuid.uuid4().hex
    ts = now_iso()
    from vocanote_queue import future_iso
    expires = future_iso(lease_seconds)
    with connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        if active_recording_count(conn) >= MAX_ACTIVE_RECORDINGS:
            conn.execute('COMMIT')
            return None
        rows = conn.execute('''
            SELECT * FROM recording_jobs
            WHERE status IN ('queued', 'retry_wait')
              AND attempts < max_attempts
              AND COALESCE(cancel_requested,0)=0
              AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)
              AND recording_id NOT IN (SELECT recording_id FROM deleted_recordings)
            ORDER BY created_at ASC
            LIMIT 50
        ''').fetchall()
        chosen = None
        chosen_stage = None
        for row in rows:
            job = dict(row)
            try:
                stage = next_required_stage(job) or 'render'
            except Exception:
                stage = 'stt'
            if running_stage_count(conn, stage) < int(STAGE_LIMITS.get(stage, MAX_ACTIVE_RECORDINGS)):
                chosen = job
                chosen_stage = stage
                break
        if not chosen:
            conn.execute('COMMIT')
            return None
        cur = conn.execute('''
            UPDATE recording_jobs
            SET status='claimed', step='claimed', claimed_at=?, claimed_by=?, claim_token=?, heartbeat_at=?, lease_expires_at=?, updated_at=?
            WHERE job_id=? AND status IN ('queued', 'retry_wait')
              AND COALESCE(cancel_requested,0)=0
              AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)
        ''', (ts, worker_id_value, token, ts, expires, ts, chosen['job_id']))
        if cur.rowcount != 1:
            conn.execute('COMMIT')
            return None
        claimed = conn.execute('SELECT * FROM recording_jobs WHERE job_id=?', (chosen['job_id'],)).fetchone()
        conn.execute('''
            INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
            VALUES (?, ?, ?, 'claim', 'claimed', 'claimed', ?, ?)
        ''', (claimed['job_id'], claimed['recording_id'], worker_id_value, ts, f'claim_token={token}; lease_expires_at={expires}; next_stage={chosen_stage}'))
        conn.execute('COMMIT')
        return dict(claimed)


@contextmanager
def heartbeat_context(job: dict[str, Any]):
    stop = threading.Event()
    wid = worker_id()

    def loop() -> None:
        while not stop.wait(HEARTBEAT_INTERVAL):
            try:
                heartbeat_job(job['job_id'], worker_id=wid, claim_token=job_claim_token(job), lease_seconds=LEASE_SECONDS)
            except Exception:
                # Heartbeat failure should not inject user-visible content; the next DB update will fail if ownership is lost.
                pass

    heartbeat_job(job['job_id'], worker_id=wid, claim_token=job_claim_token(job), lease_seconds=LEASE_SECONDS)
    t = threading.Thread(target=loop, name=f"heartbeat-{job['job_id']}", daemon=True)
    t.start()
    try:
        yield
    finally:
        stop.set()
        t.join(timeout=2)


def content_state_for(status: str, step: str) -> str:
    if status == 'completed' or status == 'final_ready':
        return 'FINAL_READY'
    if status == 'fast_ready':
        return 'FAST_READY'
    if status == 'fast_running':
        return 'FAST_RUNNING'
    if status == 'final_running':
        return 'FINAL_RUNNING'
    if status in {'failed', 'retry_wait'}:
        return 'FAILED' if status == 'failed' else 'RETRY_WAIT'
    if step == 'stt' or status.startswith('stt'):
        return 'STT_RUNNING' if status == 'stt_running' else 'STT_DONE'
    if step == 'correction' or status.startswith('correction'):
        return 'CORRECTION_RUNNING' if status == 'correction_running' else 'CORRECTION_DONE'
    if step == 'semantic' or status.startswith('semantic'):
        return 'SEMANTIC_RUNNING'
    if step == 'reduce' or status == 'reduce_running':
        return 'REDUCE_RUNNING'
    if step in {'render', 'rendering'} or status == 'rendering':
        return 'RENDERING'
    return str(status or 'UNKNOWN').upper()


def set_recording_status(recording_dir: Path, status: str, step: str, **extra: Any) -> None:
    p = recording_dir / 'status.json'
    data = load_json(p) if p.exists() else {}
    rid = str(data.get('recording_id') or data.get('id') or '')
    if not rid and (recording_dir / 'metadata.json').exists():
        meta_for_id = load_json(recording_dir / 'metadata.json')
        rid = str(meta_for_id.get('recording_id') or meta_for_id.get('id') or '')
    if rid:
        check_deleted(rid)
    data.update(extra)
    data['status'] = status  # backward-compatible legacy field
    data['step'] = step      # backward-compatible legacy field
    data['content_state'] = extra.get('content_state') or content_state_for(status, step)
    data['content_step'] = step
    data.setdefault('audio_storage_state', load_json(recording_dir / 'metadata.json').get('audio_storage_state') if (recording_dir / 'metadata.json').exists() else 'LOCAL')
    data['updated_at'] = now_iso()
    steps = data.setdefault('steps', {})
    if step:
        steps[step] = status
    if rid:
        check_deleted(rid)
    write_json(p, data)


def set_job_recording_status(job: dict[str, Any], status: str, step: str, **extra: Any) -> None:
    require_owner(job)
    recording_dir=Path(job['output_dir']);p=recording_dir/'status.json'
    data=load_json(p) if p.exists() else {}
    data.update(extra);data['recording_id']=job['recording_id'];data['generation']=int(job.get('generation') or 1)
    data['status']=status;data['step']=step;data['content_state']=extra.get('content_state') or content_state_for(status,step)
    data['content_step']=step;data.setdefault('audio_storage_state',load_json(recording_dir/'metadata.json').get('audio_storage_state') if (recording_dir/'metadata.json').exists() else 'LOCAL')
    data['updated_at']=now_iso();steps=data.setdefault('steps',{});steps[step]=status
    guarded_write_json(job,p,data)


def render_all_guarded(job: dict[str, Any]) -> None:
    d=Path(job['output_dir']);require_owner(job)
    with tempfile.TemporaryDirectory(prefix='.render-stage-',dir=d) as td:
        stage=Path(td);shutil.copy2(d/'result.validated.json',stage/'result.validated.json')
        render_all(stage)
        require_owner(job)
        guarded_write_text(job,d/'summary.md',(stage/'summary.md').read_text(encoding='utf-8'))
        guarded_write_text(job,d/'analysis.md',(stage/'analysis.md').read_text(encoding='utf-8'))
    register_artifact_references(job)


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env['PATH'] = '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:' + env.get('PATH', '')
    return env


def _command_line(cmd: list[str]) -> str:
    return ' '.join(shlex.quote(str(x)) for x in cmd)


def run_owned_process(job: dict[str, Any], cmd: list[str], *, stage: str,
                      timeout: int, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return run_job_process(
        job=job, worker_id=worker_id(), cmd=cmd, stage=stage,
        timeout=timeout, env=env, db_path=job_db_path(job),
    )


def _whisper_cpp_version(env: dict[str, str], job: dict[str, Any] | None = None) -> str | None:
    try:
        proc = run_owned_process(job, [WHISPER_CPP_BIN, '--version'], stage='stt:whisper_version', timeout=60, env=env) if job else subprocess.run([WHISPER_CPP_BIN, '--version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60, env=env)
        text = ((proc.stdout or '') + '\n' + (proc.stderr or '')).strip()
        m = re.search(r'whisper\.cpp version:\s*[^\n]+', text)
        return m.group(0).strip() if m else (text.splitlines()[-1].strip() if text.splitlines() else None)
    except Exception:
        return None


def _parse_time_l(stderr: str) -> dict[str, Any]:
    data: dict[str, Any] = {}
    m = re.search(r'([0-9.]+) real\s+([0-9.]+) user\s+([0-9.]+) sys', stderr or '')
    if m:
        data.update(real=float(m.group(1)), user=float(m.group(2)), sys=float(m.group(3)))
    m = re.search(r'(\d+)\s+peak memory footprint', stderr or '')
    if m:
        data['peak_ram_bytes'] = int(m.group(1))
    return data


def _audio_duration_sec(audio: Path, env: dict[str, str], job: dict[str, Any] | None = None) -> float | None:
    ffprobe = '/opt/homebrew/bin/ffprobe'
    if not Path(ffprobe).exists():
        ffprobe = 'ffprobe'
    try:
        cmd=[ffprobe, '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=nk=1:nw=1', str(audio)]
        proc = run_owned_process(job, cmd, stage='stt:ffprobe', timeout=60, env=env) if job else subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60, env=env)
        if proc.returncode == 0 and proc.stdout.strip():
            return float(proc.stdout.strip())
    except Exception:
        return None
    return None


def _parse_whisper_cpp_json(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    data = load_json(path)
    raw_segments = data.get('transcription') or data.get('segments') or []
    segments: list[dict[str, Any]] = []
    texts: list[str] = []
    for i, seg in enumerate(raw_segments):
        offsets = seg.get('offsets') or {}
        start = seg.get('start', offsets.get('from', 0.0))
        end = seg.get('end', offsets.get('to', start or 0.0))
        if isinstance(start, int) and start > 1000:
            start = start / 1000
        if isinstance(end, int) and end > 1000:
            end = end / 1000
        text = str(seg.get('text') or '').strip()
        if not text:
            continue
        item = {
            'index': len(segments),
            'speaker': 'S1',
            'start': float(start or 0.0),
            'end': float(end or start or 0.0),
            'text': text,
        }
        segments.append(item)
        texts.append(text)
    transcript = '\n'.join(texts).strip() or str(data.get('text') or '').strip()
    return data, segments, transcript


def _parse_openai_whisper_json(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    data = load_json(path)
    segments: list[dict[str, Any]] = []
    texts: list[str] = []
    for seg in data.get('segments') or []:
        text = str(seg.get('text') or '').strip()
        if not text:
            continue
        start = float(seg.get('start') or 0.0)
        end = float(seg.get('end') or start)
        item = {'index': len(segments), 'speaker': 'S1', 'start': start, 'end': end, 'text': text}
        segments.append(item)
        texts.append(text)
    transcript = '\n'.join(texts).strip() or str(data.get('text') or '').strip()
    return data, segments, transcript


def _write_stt_outputs(job: dict[str, Any], *, recording_dir: Path, engine: str, provider: str, model: str, started: float, proc: subprocess.CompletedProcess[str], cmd: list[str], raw_data: dict[str, Any], segments: list[dict[str, Any]], transcript: str, engine_metadata: dict[str, Any] | None = None, audio_duration_sec: float | None = None, peak_ram_bytes: int | None = None) -> None:
    elapsed = round(time.time() - started, 2)
    if not transcript:
        transcript = '[전사 결과 없음]'
    timing = {
        'audio_duration_sec': audio_duration_sec,
        'stt_time_sec': elapsed,
        'rtf': round(elapsed / audio_duration_sec, 4) if audio_duration_sec else None,
        'peak_ram_bytes': peak_ram_bytes,
    }
    metadata = {
        'engine': engine,
        'provider': provider,
        'model': model,
        'language': 'Korean',
        'task': 'transcribe',
        'command': cmd,
        'command_line': _command_line(cmd),
    }
    if engine_metadata:
        metadata.update(engine_metadata)
    stt_raw = {
        'schema_version': 'vocanote.stt_raw.v1',
        'recording_id': job['recording_id'],
        'provider': provider,
        'engine': engine,
        'model': model,
        'language': 'ko',
        'elapsed_sec': elapsed,
        'timing': timing,
        'engine_metadata': metadata,
        'raw': raw_data,
        'segments': segments,
        'transcript_raw': transcript,
    }
    guarded_write_json(job, recording_dir / 'stt_raw.json', stt_raw)
    guarded_write_text(job, recording_dir / 'transcript_raw.txt', transcript + '\n')
    guarded_write_json(job, recording_dir / 'segments_raw.json', {'schema_version': 'vocanote.segments_raw.v1', 'recording_id': job['recording_id'], 'segments': segments})


def run_stt_openai_whisper(job: dict[str, Any], recording_dir: Path, audio: Path, out_dir: Path, env: dict[str, str], audio_duration: float | None) -> None:
    cmd = [PYTHON, '-m', 'whisper', str(audio), '--model', WHISPER_MODEL, '--language', 'Korean', '--task', 'transcribe', '--output_format', 'json', '--output_dir', str(out_dir), '--fp16', 'False']
    started = time.time()
    proc = run_owned_process(job, cmd, stage='stt:openai_whisper', timeout=STT_TIMEOUT_SECONDS, env=env)
    (out_dir / 'openai_whisper.stdout.log').write_text(proc.stdout or '', encoding='utf-8')
    (out_dir / 'openai_whisper.stderr.log').write_text(proc.stderr or '', encoding='utf-8')
    (out_dir / 'openai_whisper.command.txt').write_text(_command_line(cmd) + '\n', encoding='utf-8')
    if proc.returncode != 0:
        raise RuntimeError(f'openai_whisper failed rc={proc.returncode}: {(proc.stderr or "")[-2000:]}')
    js_path = out_dir / f'{audio.stem}.json'
    if not js_path.exists():
        candidates = sorted(out_dir.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)
        if not candidates:
            details = ((proc.stdout or '') + '\n' + (proc.stderr or ''))[-3000:]
            if 'Failed to load audio' in details or 'Skipping ' in details or 'moov atom not found' in details:
                raise RuntimeError('stt_audio_load_failed: ' + details)
            raise RuntimeError('openai_whisper json output missing: ' + details)
        js_path = candidates[0]
    data, segments, transcript = _parse_openai_whisper_json(js_path)
    _write_stt_outputs(job, recording_dir=recording_dir, engine='openai_whisper', provider='local-whisper', model=WHISPER_MODEL, started=started, proc=proc, cmd=cmd, raw_data=data, segments=segments, transcript=transcript, audio_duration_sec=audio_duration)



def _ensure_whisper_cpp_wav(job: dict[str, Any], audio: Path, out_dir: Path, env: dict[str, str]) -> Path:
    """Convert Android/upload audio to the single WAV input shape used by C4.

    This is not VAD/chunking; it preserves the full recording as one 16 kHz mono
    PCM file because whisper.cpp/miniaudio cannot reliably read Android m4a.
    """
    if audio.suffix.lower() == '.wav':
        return audio
    wav_path = out_dir / 'input_full.wav'
    ffmpeg = '/opt/homebrew/bin/ffmpeg' if Path('/opt/homebrew/bin/ffmpeg').exists() else 'ffmpeg'
    cmd = [ffmpeg, '-y', '-i', str(audio), '-ar', '16000', '-ac', '1', '-c:a', 'pcm_s16le', str(wav_path)]
    proc = run_owned_process(job, cmd, stage='stt:ffmpeg', timeout=STT_TIMEOUT_SECONDS, env=env)
    (out_dir / 'whisper_cpp_ffmpeg.stdout.log').write_text(proc.stdout or '', encoding='utf-8')
    (out_dir / 'whisper_cpp_ffmpeg.stderr.log').write_text(proc.stderr or '', encoding='utf-8')
    (out_dir / 'whisper_cpp_ffmpeg.command.txt').write_text(_command_line(cmd) + '\n', encoding='utf-8')
    if proc.returncode != 0 or not wav_path.exists() or wav_path.stat().st_size <= 0:
        raise RuntimeError(f'stt_audio_load_failed: ffmpeg wav conversion failed rc={proc.returncode}: {(proc.stderr or "")[-2000:]}')
    return wav_path

def run_stt_whisper_cpp(job: dict[str, Any], recording_dir: Path, audio: Path, out_dir: Path, env: dict[str, str], audio_duration: float | None) -> None:
    input_audio = _ensure_whisper_cpp_wav(job, audio, out_dir, env)
    base = out_dir / 'whisper_cpp_result'
    cmd = ['/usr/bin/time', '-l', WHISPER_CPP_BIN, '-m', WHISPER_CPP_MODEL, '-f', str(input_audio), '-l', 'ko', '-ojf', '-of', str(base), '-np', '-t', '10', '-bs', '5', '-bo', '5', '-tp', '0', '-mc', '0']
    started = time.time()
    proc = run_owned_process(job, cmd, stage='stt:whisper_cpp', timeout=STT_TIMEOUT_SECONDS, env=env)
    (out_dir / 'whisper_cpp.stdout.log').write_text(proc.stdout or '', encoding='utf-8')
    (out_dir / 'whisper_cpp.stderr.log').write_text(proc.stderr or '', encoding='utf-8')
    (out_dir / 'whisper_cpp.command.txt').write_text(_command_line(cmd) + '\n', encoding='utf-8')
    if proc.returncode != 0:
        raise RuntimeError(f'whisper_cpp failed rc={proc.returncode}: {(proc.stderr or "")[-2000:]}')
    js_path = base.with_suffix('.json')
    if not js_path.exists():
        details = ((proc.stdout or '') + '\n' + (proc.stderr or ''))[-3000:]
        if 'Failed to load audio' in details or 'Skipping ' in details or 'moov atom not found' in details:
            raise RuntimeError('stt_audio_load_failed: ' + details)
        raise RuntimeError('whisper_cpp json output missing: ' + details)
    data, segments, transcript = _parse_whisper_cpp_json(js_path)
    time_l = _parse_time_l(proc.stderr or '')
    engine_version = _whisper_cpp_version(env, job)
    metadata = {
        'engine_version': engine_version,
        'model_path': WHISPER_CPP_MODEL,
        'model_precision': 'ggml f16',
        'device_backend': 'Metal',
        'metal_enabled': True,
        'gpu_metal_used': 'ggml_metal' in (proc.stderr or '') or 'loaded MTL backend' in (proc.stderr or ''),
        'vad': False,
        'chunking': False,
        'diarization': False,
        'glossary': False,
        'initial_prompt': None,
        'input_audio_path': str(input_audio),
        'input_wav_conversion': str(input_audio) != str(audio),
        'c4_config_id': 'C4_no_context',
        'c4_args': ['-t', '10', '-bs', '5', '-bo', '5', '-tp', '0', '-mc', '0'],
        'process_cpu_sec': round(float(time_l.get('user', 0.0)) + float(time_l.get('sys', 0.0)), 3) if time_l else None,
    }
    _write_stt_outputs(job, recording_dir=recording_dir, engine='whisper_cpp', provider='whisper.cpp', model='large-v3', started=started, proc=proc, cmd=cmd, raw_data=data, segments=segments, transcript=transcript, engine_metadata=metadata, audio_duration_sec=audio_duration, peak_ram_bytes=time_l.get('peak_ram_bytes'))


def run_stt(job: dict[str, Any]) -> None:
    recording_dir = Path(job['output_dir'])
    audio = Path(job['audio_path'])
    require_active(job)
    out_dir = recording_dir / 'stt_work'
    out_dir.mkdir(exist_ok=True)
    env = _subprocess_env()
    audio_duration = _audio_duration_sec(audio, env, job)
    add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=worker_id(), event='stt_engine_selected', status='stt_running', step='stt', details=json.dumps({'engine': STT_ENGINE, 'whisper_model': WHISPER_MODEL, 'whisper_cpp_bin': WHISPER_CPP_BIN, 'whisper_cpp_model': WHISPER_CPP_MODEL}, ensure_ascii=False))
    if STT_ENGINE == 'whisper_cpp':
        run_stt_whisper_cpp(job, recording_dir, audio, out_dir, env, audio_duration)
    elif STT_ENGINE == 'openai_whisper':
        run_stt_openai_whisper(job, recording_dir, audio, out_dir, env, audio_duration)
    else:
        raise RuntimeError(f'unsupported STT engine: {STT_ENGINE!r}; expected whisper_cpp or openai_whisper')

def extract_json(text: str) -> Any:
    text = text.strip()
    # Remove known quiet-mode session lines before JSON.
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith('session_id:') and 'tirith security scanner' not in ln]
    cleaned = '\n'.join(lines).strip()
    try:
        return json.loads(cleaned)
    except Exception:
        pass
    m = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', cleaned, re.S)
    if m:
        return json.loads(m.group(1))
    start = cleaned.find('{')
    end = cleaned.rfind('}')
    if start >= 0 and end > start:
        return json.loads(cleaned[start:end+1])
    raise ValueError('No JSON object found in Hermes output')


def run_hermes_json(job: dict[str, Any], prompt: str, *, stage: str, timeout: int = 900) -> tuple[dict[str, Any], str]:
    cmd = [
        HERMES, '--profile', HERMES_PROFILE,
        'chat', '-Q', '--ignore-rules', '--source', 'vocanote-job', '--toolsets', 'safe', '--max-turns', '1', '-q', prompt,
    ]
    proc = run_owned_process(job, cmd, stage=stage, timeout=timeout)
    raw = (proc.stdout or '') + ('\nSTDERR:\n' + proc.stderr if proc.stderr else '')
    if proc.returncode != 0:
        raise RuntimeError(f'Hermes failed rc={proc.returncode}: {raw[-3000:]}')
    data = extract_json(proc.stdout or '')
    if not isinstance(data, dict):
        raise ValueError('Hermes output is not a JSON object')
    return data, raw


def validate_schema(data: dict[str, Any], schema_name: str) -> None:
    schema = load_json(SERVER_DIR / schema_name)
    jsonschema.validate(instance=data, schema=schema)


def correction_prompt(metadata: dict[str, Any], stt_raw: dict[str, Any]) -> str:
    compact_segments = [
        {k: seg.get(k) for k in ['index', 'speaker', 'start', 'end', 'text']}
        for seg in stt_raw.get('segments', [])
    ]
    payload = {
        'metadata': metadata,
        'segments_raw': compact_segments,
        'transcript_raw': stt_raw.get('transcript_raw', ''),
    }
    return """You are the isolated VocaNote transcript-correction worker.
You must not use any prior conversation, user memory, app-development context, or other recordings.
Use ONLY the JSON payload below.

Task: correct Korean STT errors while preserving meaning.
Rules:
- Do not summarize.
- Do not add facts not spoken.
- Do not rewrite to be more stylish.
- Correct only obvious STT recognition errors, spacing, punctuation, and clear technical terms from context.
- If uncertain, keep the raw meaning and mark uncertain=true.
- Output JSON only. No markdown.

Required JSON shape:
{
  "schema_version":"vocanote.transcript.v1",
  "recording_id":"...",
  "language":"ko",
  "segments":[{"index":0,"speaker":"S1","start":0.0,"end":1.0,"raw_text":"...","corrected_text":"...","uncertain":false}],
  "warnings":[]
}

Payload:
""" + json.dumps(payload, ensure_ascii=False)




def correction_chunk_prompt(metadata: dict[str, Any], chunk: dict[str, Any], primary_segments: list[dict[str, Any]], overlap_segments: list[dict[str, Any]], global_context: dict[str, Any], rolling_context: dict[str, Any] | None = None) -> str:
    payload = {
        'metadata': {k: metadata.get(k) for k in ['recording_id','language','type','meeting_type','recorded_at','duration_sec']},
        'chunk_id': chunk['chunk_id'],
        'input_hash': chunk['input_hash'],
        'primary_segment_range': chunk['primary_segment_range'],
        'overlap_context_range': chunk['overlap_context_range'],
        'global_context': global_context,
        'rolling_context': rolling_context or {},
        'primary_segments': [{k: seg.get(k) for k in ['index', 'speaker', 'start', 'end', 'text']} for seg in primary_segments],
        'overlap_context_segments': [{k: seg.get(k) for k in ['index', 'speaker', 'start', 'end', 'text']} for seg in overlap_segments],
    }
    return """You are the isolated VocaNote chunk transcript-correction worker.
Use ONLY the JSON payload below.

Task: correct Korean STT errors for PRIMARY segments only.
Rules:
- Return output for primary_segments only. Do NOT return overlap_context_segments unless they are also primary.
- segment index/start/end/speaker must be copied exactly.
- raw_text must equal the primary segment text.
- Correct text only. Do not summarize, delete meaning, add facts, or rewrite style.
- Global/Rolling context is reference only for terms/entities. Raw segment text is authoritative.
- If uncertain, preserve raw meaning and set uncertain=true.
- Output JSON only. No markdown.

Required JSON shape:
{
  "schema_version":"vocanote.transcript.v1",
  "recording_id":"...",
  "language":"ko",
  "segments":[{"index":0,"speaker":"S1","start":0.0,"end":1.0,"raw_text":"...","corrected_text":"...","uncertain":false}],
  "warnings":[]
}

Payload:
""" + json.dumps(payload, ensure_ascii=False)


def chunk_file_ok(path: Path, expected_hash: str, raw_primary: list[dict[str, Any]]) -> bool:
    if not path.exists() or not json_valid(path):
        return False
    try:
        data = load_json(path)
        if data.get('input_hash') != expected_hash:
            return False
        result = data.get('output') or {}
        validate_schema(result, 'transcript.schema.json')
        ok, _warnings = validate_correction_chunk_output(raw_primary_segments=raw_primary, output=result)
        return ok
    except Exception:
        return False


def run_correction_chunked(job: dict[str, Any], *, version: str = 'final') -> None:
    d = Path(job['output_dir'])
    metadata = load_json(d / 'metadata.json')
    raw_pack = load_json(d / 'segments_raw.json')
    raw_segments = raw_pack.get('segments') or []
    is_fast = version == 'fast'
    paths = version_paths(d, version)
    global_context = no_global_context(raw_segments) if is_fast else deterministic_global_context(raw_segments)
    paths['correction_dir'].parent.mkdir(parents=True, exist_ok=True)
    paths['snapshot_dir'].mkdir(parents=True, exist_ok=True)
    guarded_write_json(job, paths['global_context_json'], global_context)
    manifest = build_chunks(
        raw_segments,
        target_chars=int(os.environ.get('VOCANOTE_CORRECTION_CHUNK_TARGET_CHARS', '1200')),
        max_duration_sec=int(os.environ.get('VOCANOTE_CORRECTION_CHUNK_MAX_DURATION_SEC', '90')),
        max_segments=int(os.environ.get('VOCANOTE_CORRECTION_CHUNK_MAX_SEGMENTS', '35')),
        overlap_segments=int(os.environ.get('VOCANOTE_CORRECTION_OVERLAP_SEGMENTS', '2')),
        prompt_version=CORRECTION_PROMPT_VERSION,
        model_id=os.environ.get('VOCANOTE_HERMES_MODEL', 'gpt-5.5'),
        provider_id=os.environ.get('VOCANOTE_HERMES_PROVIDER', 'openai-codex'),
        global_context=global_context,
    )
    require_active(job)
    chunk_dir = paths['correction_dir']
    chunk_dir.mkdir(parents=True, exist_ok=True)
    chunk_outputs: dict[int, dict[str, Any]] = {}
    changed = False
    timeline_rows: list[dict[str, Any]] = []
    timeline_lock = threading.Lock()
    active_chunks = 0
    max_active_chunks = 0
    max_chunk_concurrency = min(CORRECTION_CHUNK_CONCURRENCY, 2, max(1, len(manifest['chunks'])))

    def run_one_chunk(seq: int, chunk: dict[str, Any]) -> tuple[int, dict[str, Any], bool]:
        nonlocal active_chunks, max_active_chunks
        require_owner(job)
        primary, overlap = chunk_segments_for_prompt(raw_segments, chunk)
        out_path = chunk_dir / f"{chunk['chunk_id']}.json"
        row = {
            'chunk_id': chunk['chunk_id'],
            'seq': seq,
            'status': 'running',
            'reused_checkpoint': False,
            'started_at': datetime.now(timezone.utc).isoformat(),
            'thread': threading.current_thread().name,
        }
        started = time.time()
        with timeline_lock:
            active_chunks += 1
            max_active_chunks = max(max_active_chunks, active_chunks)
            row['active_at_start'] = active_chunks
        try:
            if chunk_file_ok(out_path, chunk['input_hash'], primary):
                stored = load_json(out_path)
                chunk['status'] = 'passed'
                chunk['attempts'] = int(stored.get('attempts') or 0)
                row['status'] = 'passed'
                row['reused_checkpoint'] = True
                add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=worker_id(), event='correction_chunk_checkpoint_reuse', status='correction_running', step=chunk['chunk_id'], details={'chunk_id': chunk['chunk_id'], 'input_hash': chunk['input_hash'], 'chunk_concurrency': max_chunk_concurrency})
                return seq, stored['output'], False
            prompt = correction_chunk_prompt(metadata, chunk, primary, overlap, global_context)
            guarded_write_text(job, chunk_dir / f"{chunk['chunk_id']}.prompt.txt", prompt)
            chunk['attempts'] = int(chunk.get('attempts') or 0) + 1
            data, raw = run_hermes_json(job, prompt, stage=f'correction:{chunk["chunk_id"]}', timeout=int(os.environ.get('VOCANOTE_CHUNK_HERMES_TIMEOUT', '300')))
            guarded_write_text(job, chunk_dir / f"{chunk['chunk_id']}.raw.txt", raw)
            validate_schema(data, 'transcript.schema.json')
            ok, warnings = validate_correction_chunk_output(raw_primary_segments=primary, output=data)
            if not ok:
                chunk['status'] = 'failed'
                chunk['error'] = '; '.join(warnings[:20])
                guarded_write_json(job, chunk_dir / f"{chunk['chunk_id']}.failed.json", {'chunk': chunk, 'output': data, 'warnings': warnings})
                row['status'] = 'failed'
                row['error'] = chunk['error']
                raise RuntimeError(f"correction_chunk_validation_failed {chunk['chunk_id']}: {chunk['error']}")
            if warnings:
                data.setdefault('warnings', []).extend(warnings)
            chunk['status'] = 'passed'
            stored = {'schema_version': 'vocanote.correction_chunk.v1', 'chunk': chunk, 'input_hash': chunk['input_hash'], 'attempts': chunk['attempts'], 'output': data, 'warnings': warnings}
            guarded_write_json(job, out_path, stored)
            row['status'] = 'passed'
            row['warnings_count'] = len(warnings)
            add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=worker_id(), event='correction_chunk_completed', status='correction_running', step=chunk['chunk_id'], details={'chunk_id': chunk['chunk_id'], 'input_hash': chunk['input_hash'], 'warnings': warnings[:10], 'chunk_concurrency': max_chunk_concurrency})
            return seq, data, True
        except Exception as exc:
            row['status'] = 'error'
            row['error'] = str(exc)[-1000:]
            raise
        finally:
            row['ended_at'] = datetime.now(timezone.utc).isoformat()
            row['elapsed_sec'] = round(time.time() - started, 3)
            with timeline_lock:
                active_chunks -= 1
                row['active_after_end'] = active_chunks
                timeline_rows.append(row)

    if max_chunk_concurrency <= 1:
        for seq, chunk in enumerate(manifest['chunks']):
            seq, output, did_change = run_one_chunk(seq, chunk)
            chunk_outputs[seq] = output
            changed = changed or did_change
    else:
        with ThreadPoolExecutor(max_workers=max_chunk_concurrency, thread_name_prefix='correction-chunk') as executor:
            pending_chunks = iter(enumerate(manifest['chunks']))
            futures = set()
            for _ in range(max_chunk_concurrency):
                try:
                    seq, chunk = next(pending_chunks)
                except StopIteration:
                    break
                futures.add(executor.submit(run_one_chunk, seq, chunk))
            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for fut in done:
                    seq, output, did_change = fut.result()
                    chunk_outputs[seq] = output
                    changed = changed or did_change
                    try:
                        next_seq, next_chunk = next(pending_chunks)
                    except StopIteration:
                        continue
                    futures.add(executor.submit(run_one_chunk, next_seq, next_chunk))
    manifest['chunk_concurrency'] = max_chunk_concurrency
    manifest['max_active_chunks'] = max_active_chunks
    guarded_write_json(job, chunk_dir / 'timeline.json', {'schema_version': 'vocanote.correction_chunk_timeline.v1', 'recording_id': job['recording_id'], 'chunk_concurrency': max_chunk_concurrency, 'max_active_chunks': max_active_chunks, 'chunks': sorted(timeline_rows, key=lambda row: row['seq'])})
    guarded_write_json(job, chunk_dir / 'manifest.json', manifest)
    outputs = [chunk_outputs[idx] for idx in range(len(manifest['chunks']))]
    merged = merge_correction_chunks(job['recording_id'], outputs)
    validate_schema(merged, 'transcript.schema.json')
    merged['snapshot_version'] = version.upper()
    merged['pipeline_version'] = TWO_STAGE_PIPELINE_VERSION
    segments_pack = {'schema_version': f'vocanote.segments_{version}.v1', 'recording_id': job['recording_id'], 'snapshot_version': version.upper(), 'segments': [
        {'index': s['index'], 'speaker': s['speaker'], 'start': s['start'], 'end': s['end'], 'text': s['corrected_text'], 'raw_text': s['raw_text'], 'uncertain': s['uncertain']}
        for s in merged.get('segments', [])
    ]}
    clean_text = '\n'.join(str(s.get('corrected_text') or '').strip() for s in merged.get('segments', []) if str(s.get('corrected_text') or '').strip())
    guarded_write_json(job, paths['transcript_json'], merged)
    guarded_write_json(job, paths['segments_json'], segments_pack)
    guarded_write_text(job, paths['transcript_txt'], clean_text + '\n')
    if is_fast:
        add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=worker_id(), event='fast_snapshot_ready', status='fast_ready', step='fast', details={'snapshot_hash': snapshot_digest(paths), 'chunk_count': len(manifest['chunks'])})

def semantic_prompt(metadata: dict[str, Any], correction: dict[str, Any]) -> str:
    corrected_text = '\n'.join(str(seg.get('corrected_text') or '').strip() for seg in correction.get('segments', []) if str(seg.get('corrected_text') or '').strip())
    payload = {
        'metadata': metadata,
        'corrected_transcript': corrected_text,
        'corrected_segments': correction.get('segments', []),
    }
    return """You are the isolated VocaNote note-intelligence worker.
You must not use any prior conversation, user memory, app-development context, or other recordings.
Use ONLY the JSON payload below.

Task: understand the whole recording and produce structured note JSON.
Rules:
- content_type must be one of: meeting, lecture, idea, conversation, test, other.
- Title must reflect the whole transcript meaning. Never use the first sentence as fallback. Never use frequency words.
- Keywords: 3-6 valuable searchable concepts; exclude filler/frequency words such as 우리, 아주, 맞아요, 그렇죠, 그런데, 제가.
- Memo summary should let the user recall the recording in 30 seconds. Do not copy early transcript sentences as bullets.
- Structured note must match content_type. Do not invent action items/decisions if absent.
- Never include app-development meta, STT-quality workflow, prompt/system/worker comments in content sections.
- Output JSON only. No markdown.

Required JSON shape:
{
  "schema_version":"vocanote.result.v1",
  "recording_id":"...",
  "language":"ko",
  "content_type":"lecture",
  "title":{"text":"...","filename_slug":"...","reason":"..."},
  "keywords":["..."],
  "memo_summary":{"topic":"...","one_line":"...","core_points":["..."],"flow":["..."]},
  "structured_note":{"note_type":"lecture","sections":[{"heading":"핵심 개념","items":["..."]}]},
  "quality":{"needs_review":false,"uncertain_segments":[],"warnings":[]}
}

Payload:
""" + json.dumps(payload, ensure_ascii=False)


def run_correction(job: dict[str, Any], *, version: str = 'final') -> None:
    d = Path(job['output_dir'])
    stt_raw = load_json(d / 'stt_raw.json')
    seg_count = len(stt_raw.get('segments') or [])
    raw_chars = len(str(stt_raw.get('transcript_raw') or ''))
    use_chunked = os.environ.get('VOCANOTE_CHUNKED_AI_ENABLED', '1') == '1' and (seg_count > 60 or raw_chars > 5000)
    if use_chunked:
        add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=worker_id(), event='correction_chunked_enabled', status='correction_running', step='correction', details={'segments': seg_count, 'chars': raw_chars})
        return run_correction_chunked(job, version=version)
    metadata = load_json(d / 'metadata.json')
    prompt = correction_prompt(metadata, stt_raw)
    guarded_write_text(job, d / 'hermes_correction_prompt.txt', prompt)
    data, raw = run_hermes_json(job, prompt, stage=f'correction:{version}')
    guarded_write_text(job, d / 'hermes_correction_output.raw.txt', raw)
    validate_schema(data, 'transcript.schema.json')
    raw_segments = (load_json(d / 'segments_raw.json').get('segments') or []) if (d / 'segments_raw.json').exists() else []
    if raw_segments:
        ok, warnings = validate_correction_chunk_output(raw_primary_segments=raw_segments, output=data)
        if not ok:
            raise RuntimeError('correction_validation_failed: ' + '; '.join(warnings[:20]))
        if warnings:
            data.setdefault('warnings', []).extend(warnings)
    # Preserve raw and clean separately.
    paths = version_paths(d, version)
    paths['snapshot_dir'].mkdir(parents=True, exist_ok=True)
    data['snapshot_version'] = version.upper()
    data['pipeline_version'] = TWO_STAGE_PIPELINE_VERSION
    guarded_write_json(job, paths['transcript_json'], data)
    guarded_write_json(job, paths['segments_json'], {'schema_version': f'vocanote.segments_{version}.v1', 'recording_id': job['recording_id'], 'snapshot_version': version.upper(), 'segments': [
        {'index': s['index'], 'speaker': s['speaker'], 'start': s['start'], 'end': s['end'], 'text': s['corrected_text'], 'raw_text': s['raw_text'], 'uncertain': s['uncertain']}
        for s in data.get('segments', [])
    ]})
    clean_text = '\n'.join(str(s.get('corrected_text') or '').strip() for s in data.get('segments', []) if str(s.get('corrected_text') or '').strip())
    guarded_write_text(job, paths['transcript_txt'], clean_text + '\n')


def normalize_result(data: dict[str, Any]) -> dict[str, Any]:
    """Conservative schema repair for common LLM shape drift.

    Does not invent content. It only coerces harmless structural drift such as
    quality.uncertain_segments=[{"index": N, "reason": "..."}] into
    uncertain_segments=[N] and appends reasons to warnings.
    """
    q = data.setdefault('quality', {})
    warnings = q.setdefault('warnings', [])
    uncertain = q.get('uncertain_segments', [])
    fixed = []
    if isinstance(uncertain, list):
        for item in uncertain:
            if isinstance(item, int):
                fixed.append(item)
            elif isinstance(item, dict) and isinstance(item.get('index'), int):
                fixed.append(item['index'])
                reason = item.get('reason') or item.get('note')
                if reason:
                    warnings.append(f"segment {item['index']}: {reason}")
    q['uncertain_segments'] = sorted(set(fixed))
    # Normalize note_type to content_type if the model drifted.
    if isinstance(data.get('structured_note'), dict):
        nt = data['structured_note'].get('note_type')
        if nt not in {'meeting','lecture','idea','conversation','test','other'}:
            data['structured_note']['note_type'] = data.get('content_type') if data.get('content_type') in {'meeting','lecture','idea','conversation','test','other'} else 'other'
    # Ensure keywords are unique strings and remove known filler words if model ignored prompt.
    fillers = {'우리','아주','맞아요','그렇죠','그런데','제가','저는','음','어'}
    kws=[]
    for x in data.get('keywords') or []:
        k=str(x).strip().lstrip('#')
        if k and k not in fillers and k not in kws:
            kws.append(k)
    data['keywords']=kws[:8]
    return data


def run_semantic(job: dict[str, Any]) -> None:
    d = Path(job['output_dir'])
    recording_id = job['recording_id']
    metadata = load_json(d / 'metadata.json')
    correction = load_json(d / 'transcript_clean.json')
    require_active(job)
    require_owner(job)
    manifest_path = d / 'semantic_chunks' / 'manifest.json'
    if not manifest_path.exists():
        context = deterministic_global_context(correction.get('segments') or [])
        write_semantic_chunks(
            d,
            recording_id=recording_id,
            clean_transcript=correction,
            global_context=context,
            provider='openai-codex',
            model='gpt-5.5',
            model_config={'temperature': 0},
        )
    require_active(job)
    require_owner(job)
    command_runner=lambda cmd,stage,timeout: run_owned_process(job,list(cmd),stage=stage,timeout=timeout)
    semantic_is_hermes=(os.environ.get('VOCANOTE_SEMANTIC_EXECUTOR') or 'hermes').strip().lower()=='hermes'
    map_executor=make_executor(command_runner=command_runner) if semantic_is_hermes else make_executor()
    map_stats = run_semantic_map(recording_dir=d, recording_id=recording_id, executor=map_executor, assert_claim=_claim_assertion(job))
    require_active(job)
    require_owner(job)
    if not acquire_stage_slot(job, 'reduce', step='reduce'):
        yield_claim_for_stage(job, previous_step='semantic_map_done', reason='reduce slot full after semantic map')
    reduce_executor=make_reduce_executor(command_runner=command_runner) if semantic_is_hermes else make_reduce_executor()
    reduce_stats = run_semantic_reduce(recording_dir=d, recording_id=recording_id, executor=reduce_executor, assert_claim=_claim_assertion(job), render_outputs=False)
    data = load_json(d / 'result.validated.json')
    validate_schema(data, 'result.schema.json')
    add_job_event(job_id=job['job_id'], recording_id=recording_id, worker_id=worker_id(), event='semantic_map_reduce_completed', status='semantic_running', step='semantic', details=json.dumps({'map': map_stats, 'reduce': reduce_stats}, ensure_ascii=False)[:2000])
    # Metadata title changes, immutable id and audio filename do not.
    metadata['title'] = data['title']['text']
    metadata['content_type'] = data.get('content_type') or metadata.get('content_type')
    metadata['updated_at'] = now_iso()
    guarded_write_json(job, d / 'metadata.json', metadata)


def process_job(job: dict[str, Any]) -> None:
    d = Path(job['output_dir'])
    wid = worker_id()
    with heartbeat_context(job):
        require_active(job)
        require_owner(job)
        if checkpoint_stt_ok(job):
            add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='checkpoint_skip', status='stt_done', step='stt', details='stt checkpoint valid')
        else:
            set_job_recording_status(job, 'stt_running', 'stt')
            if not acquire_stage_slot(job, 'stt', step='stt'):
                yield_claim_for_stage(job, previous_step='queued', reason='stt slot full before STT')
            run_stt(job)
            mark_stage_generation(job, 'stt')
            register_artifact_references(job)
        require_owner(job)
        set_job_recording_status(job, 'stt_done', 'stt')
        update_job(job['job_id'], status='stt_done', step='stt', worker_id=wid, claim_token=job_claim_token(job))

        require_active(job)
        require_owner(job)
        if two_stage_enabled():
            status_before_fast = load_json(d / 'status.json') if (d / 'status.json').exists() else {}
            timestamps = status_before_fast.setdefault('two_stage_timestamps', {})
            if checkpoint_fast_ok(job):
                add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='checkpoint_skip', status='fast_ready', step='fast', details='FAST checkpoint valid')
            else:
                timestamps.setdefault('fast_started', now_iso())
                set_job_recording_status(job, 'fast_running', 'fast', pipeline_version=TWO_STAGE_PIPELINE_VERSION, available_version='RAW', display_version='PROCESSING', fast_available=False, final_available=False, two_stage_timestamps=timestamps)
                if not acquire_stage_slot(job, 'correction', step='fast'):
                    yield_claim_for_stage(job, previous_step='stt_done', reason='correction slot full before FAST')
                run_correction(job, version='fast')
                mark_stage_generation(job, 'correction_fast')
                register_artifact_references(job)
            require_owner(job)
            timestamps = (load_json(d / 'status.json') if (d / 'status.json').exists() else {}).get('two_stage_timestamps') or {}
            timestamps.setdefault('fast_ready', now_iso())
            set_job_recording_status(job, 'fast_ready', 'fast', pipeline_version=TWO_STAGE_PIPELINE_VERSION, available_version='FAST', display_version='FAST', fast_available=True, final_available=False, two_stage_timestamps=timestamps)
            update_job(job['job_id'], status='correction_done', step='fast_ready', worker_id=wid, claim_token=job_claim_token(job))

            require_active(job)
            require_owner(job)
            timestamps.setdefault('final_started', now_iso())
            set_job_recording_status(job, 'final_running', 'final', pipeline_version=TWO_STAGE_PIPELINE_VERSION, available_version='FAST', display_version='FAST', fast_available=True, final_available=False, two_stage_timestamps=timestamps)
            # FINAL is intentionally independent from FAST and writes to the legacy/root production paths.
            # Do not treat FAST snapshot as checkpoint_correction_ok().
            if checkpoint_correction_ok(job):
                add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='checkpoint_skip', status='final_running', step='final', details='FINAL correction checkpoint valid')
            else:
                if not acquire_stage_slot(job, 'correction', step='final'):
                    yield_claim_for_stage(job, previous_step='fast_ready', reason='correction slot full before FINAL')
                run_correction(job, version='final')
                mark_stage_generation(job, 'correction_final')
                register_artifact_references(job)
            require_owner(job)
            timestamps.setdefault('final_correction_ready', now_iso())
            set_job_recording_status(job, 'correction_done', 'final', pipeline_version=TWO_STAGE_PIPELINE_VERSION, available_version='FAST', display_version='FAST', fast_available=True, final_available=False, two_stage_timestamps=timestamps)
            update_job(job['job_id'], status='correction_done', step='final_correction_done', worker_id=wid, claim_token=job_claim_token(job))
        else:
            if checkpoint_correction_ok(job):
                add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='checkpoint_skip', status='correction_done', step='correction', details='correction checkpoint valid')
            else:
                set_job_recording_status(job, 'correction_running', 'correction')
                if not acquire_stage_slot(job, 'correction', step='correction'):
                    yield_claim_for_stage(job, previous_step='stt_done', reason='correction slot full after STT')
                run_correction(job, version='final')
                mark_stage_generation(job, 'correction_final')
                register_artifact_references(job)
            require_owner(job)
            set_job_recording_status(job, 'correction_done', 'correction')
            update_job(job['job_id'], status='correction_done', step='correction', worker_id=wid, claim_token=job_claim_token(job))

        require_active(job)
        require_owner(job)
        if checkpoint_semantic_ok(job):
            add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='checkpoint_skip', status='semantic_running', step='semantic', details='semantic checkpoint valid')
        else:
            set_job_recording_status(job, 'semantic_running', 'semantic')
            if not acquire_stage_slot(job, 'semantic', step='semantic'):
                yield_claim_for_stage(job, previous_step='correction_done', reason='semantic slot full after correction')
            run_semantic(job)
            mark_stage_generation(job, 'semantic')
            register_artifact_references(job)
        if two_stage_enabled():
            _st = load_json(d / 'status.json') if (d / 'status.json').exists() else {}
            _ts = _st.get('two_stage_timestamps') or {}
            _ts.setdefault('semantic_ready', now_iso())
            set_job_recording_status(job, 'semantic_running', 'semantic', pipeline_version=TWO_STAGE_PIPELINE_VERSION, available_version='FAST', display_version='FAST', fast_available=checkpoint_fast_ok(job), final_available=False, two_stage_timestamps=_ts)

        require_active(job)
        require_owner(job)
        if checkpoint_render_ok(job):
            add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='checkpoint_skip', status='rendering', step='rendering', details='render checkpoint valid')
        else:
            set_job_recording_status(job, 'rendering', 'render')
            if not acquire_stage_slot(job, 'render', step='rendering'):
                yield_claim_for_stage(job, previous_step='semantic_done', reason='render slot full after semantic')
            require_owner(job)
            require_active(job)
            render_all_guarded(job)
            mark_stage_generation(job, 'render')
            require_active(job)
            require_owner(job)
        require_active(job)
        require_owner(job)
        cloud_enabled = os.environ.get('VOCANOTE_CLOUD_ARCHIVE_ENABLED') == '1'
        add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='cloud_archive_flag', status='ai_completed', step='cloud_archive', details=f"VOCANOTE_CLOUD_ARCHIVE_ENABLED={os.environ.get('VOCANOTE_CLOUD_ARCHIVE_ENABLED')!r}")
        if cloud_enabled:
            set_job_recording_status(job, 'cloud_upload_pending', 'cloud_upload', result_json_path=str(d / 'result.validated.json'), error_code=None, error_message=None)
            update_job(job['job_id'], status='cloud_upload_pending', step='cloud_upload', worker_id=wid, claim_token=job_claim_token(job))
            add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='cloud_archive_call', status='cloud_upload_pending', step='cloud_upload', details='calling upload_recording_audio_to_drive')
            set_job_recording_status(job, 'cloud_uploading', 'cloud_upload', result_json_path=str(d / 'result.validated.json'), error_code=None, error_message=None)
            update_job(job['job_id'], status='cloud_uploading', step='cloud_upload', worker_id=wid, claim_token=job_claim_token(job))
            # The production worker never unlinks canonical audio directly. Any
            # physical deletion must be driven later by a row-referenced purge intent.
            upload_recording_audio_to_drive(
                d,
                write_json_fn=lambda path, payload: guarded_write_json(job, path, payload),
                assert_claim=lambda: require_owner(job),
                preserve_local=True,
            )
            require_owner(job)
            add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='cloud_archive_verified', status='cloud_verified', step='cloud_upload', details='Drive upload verified; canonical preserved locally; deletion requires purge intent')
        if two_stage_enabled():
            try:
                write_fast_final_diff(job)
            except Exception as exc:
                add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='fast_final_diff_failed', status='completed', step='diff', details=str(exc)[-1000:])
            _st = load_json(d / 'status.json') if (d / 'status.json').exists() else {}
            _ts = _st.get('two_stage_timestamps') or {}
            _ts.setdefault('final_ready', now_iso())
            set_job_recording_status(job, 'completed', 'completed', pipeline_version=TWO_STAGE_PIPELINE_VERSION, available_version='FINAL', display_version='FINAL', fast_available=checkpoint_fast_ok(job), final_available=True, result_json_path=str(d / 'result.validated.json'), error_code=None, error_message=None, two_stage_timestamps=_ts)
        else:
            set_job_recording_status(job, 'completed', 'completed', result_json_path=str(d / 'result.validated.json'), error_code=None, error_message=None)
        register_artifact_references(job)
        mark_completed(job['job_id'], result_json_path=str(d / 'result.validated.json'), worker_id=wid, claim_token=job_claim_token(job))


def classify_error(exc: Exception) -> tuple[str, bool]:
    msg = repr(exc)
    if isinstance(exc, RecordingDeleted):
        return 'recording_deleted', False
    if isinstance(exc, subprocess.TimeoutExpired):
        return 'timeout', True
    if "No such file or directory: 'ffmpeg'" in msg or 'No such file or directory: \'ffmpeg\'' in msg:
        return 'stt_failed_missing_ffmpeg', True
    if 'stt_audio_load_failed' in msg or 'moov atom not found' in msg or 'Invalid data found when processing input' in msg:
        return 'stt_failed_audio_invalid', False
    if 'cloud' in msg.lower() or 'google' in msg.lower() or 'HttpError' in msg or 'Connection' in msg:
        return 'cloud_upload_failed', True
    if 'Hermes failed' in msg or 'whisper failed' in msg or 'temporarily' in msg.lower():
        return 'temporary_worker_error', True
    if 'ValidationError' in type(exc).__name__ or 'No JSON' in msg or 'JSON' in msg:
        return 'json_invalid', True
    if 'No such file' in msg or 'audio' in msg and 'missing' in msg:
        return 'permanent_file_error', False
    return 'worker_error', True


def run_once(*, dry_run: bool = False) -> int:
    init_db()
    wid = worker_id()
    recovered = recover_stale_jobs(worker_id=wid)
    if recovered:
        print(json.dumps({'ok': True, 'stale_recovered': len(recovered), 'jobs': [{'job_id': r['job_id'], 'recording_id': r['recording_id'], 'new_status': r['new_status']} for r in recovered]}, ensure_ascii=False), flush=True)
    job = claim_next_eligible(wid, lease_seconds=LEASE_SECONDS)
    if not job:
        print(json.dumps({'ok': True, 'claimed': False}, ensure_ascii=False))
        return 0
    print(json.dumps({'ok': True, 'claimed': True, 'job_id': job['job_id'], 'recording_id': job['recording_id'], 'dry_run': dry_run}, ensure_ascii=False), flush=True)
    if dry_run:
        from vocanote_queue import release_claim
        release_claim(job['job_id'], previous_status='queued', previous_step='queued', decrement_attempt=False)
        return 0
    try:
        process_job(job)
        print(json.dumps({'ok': True, 'completed': True, 'job_id': job['job_id'], 'recording_id': job['recording_id']}, ensure_ascii=False), flush=True)
        return 0
    except StageSlotUnavailable as exc:
        print(json.dumps({'ok': True, 'job_id': job['job_id'], 'recording_id': job['recording_id'], 'yielded': True, 'reason': str(exc)}, ensure_ascii=False), flush=True)
        return 0
    except OwnershipLost as exc:
        add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='ownership_lost', status=None, step=None, details=repr(exc))
        print(json.dumps({'ok': False, 'job_id': job['job_id'], 'recording_id': job['recording_id'], 'status': 'ownership_lost', 'error': repr(exc)}, ensure_ascii=False), flush=True)
        return 3
    except Exception as exc:
        code, retryable = classify_error(exc)
        d = Path(job['output_dir'])
        attempts_before = int(job.get('attempts') or 0)
        max_attempts = int(job.get('max_attempts') or 3)
        final = (not retryable) or (attempts_before + 1) >= max_attempts
        step = code
        try:
            if code != 'recording_deleted':
                set_job_recording_status(job, 'failed' if final else 'retry_wait', step, error_code=code, error_message=repr(exc))
        except Exception:
            pass
        try:
            status = increment_attempt_and_set_failure(job['job_id'], final=final, step=step, error_code=code, error_message=repr(exc)[-3000:], worker_id=wid, claim_token=job_claim_token(job))
        except OwnershipLost as own_exc:
            add_job_event(job_id=job['job_id'], recording_id=job['recording_id'], worker_id=wid, event='ownership_lost_during_failure_mark', status=None, step=None, details=repr(own_exc))
            print(json.dumps({'ok': False, 'job_id': job['job_id'], 'recording_id': job['recording_id'], 'status': 'ownership_lost_after_error', 'error_code': code, 'error': repr(exc), 'ownership_error': repr(own_exc)}, ensure_ascii=False), flush=True)
            return 3
        try:
            if code != 'recording_deleted':
                guarded_write_text(job, d / 'error.log', f'{now_iso()} {type(exc).__name__}: {repr(exc)}\n')
        except Exception:
            pass
        print(json.dumps({'ok': False, 'job_id': job['job_id'], 'recording_id': job['recording_id'], 'status': status, 'error_code': code, 'error': repr(exc)}, ensure_ascii=False), flush=True)
        return 2 if final else 1


def daemon_loop(interval: int) -> None:
    init_db()
    print(json.dumps({'ok': True, 'worker_start': worker_id(), 'max_active_recordings': MAX_ACTIVE_RECORDINGS, 'stage_limits': STAGE_LIMITS}, ensure_ascii=False), flush=True)
    with ThreadPoolExecutor(max_workers=max(1, MAX_ACTIVE_RECORDINGS), thread_name_prefix='recording-worker') as pool:
        futures = set()
        while True:
            futures = {f for f in futures if not f.done()}
            while len(futures) < max(1, MAX_ACTIVE_RECORDINGS):
                futures.add(pool.submit(run_once, dry_run=False))
                time.sleep(0.1)
            done, futures = wait(futures, timeout=interval, return_when=FIRST_COMPLETED)
            # Surface exceptions so launchd logs contain failures; run_once normally catches job errors.
            for f in done:
                try:
                    f.result()
                except Exception as exc:
                    print(json.dumps({'ok': False, 'scheduler_exception': repr(exc)}, ensure_ascii=False), flush=True)
            time.sleep(0.1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--interval', type=int, default=10)
    args = parser.parse_args()
    if args.once:
        raise SystemExit(run_once(dry_run=args.dry_run))
    daemon_loop(args.interval)


if __name__ == '__main__':
    main()

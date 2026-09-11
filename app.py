#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import secrets
import shutil
import uuid
from difflib import SequenceMatcher
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import Body, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

from vocanote_queue import DB_PATH, enqueue_job, get_job, init_db, list_jobs, connect, now_iso, recover_stale_jobs
from vocanote_safe_edit import SafeEditError, SafeEditStore
from vocanote_tombstone import (
    check_deleted, delete_recording_to_trash, get_tombstone, init_tombstone_db,
    is_deleted, validate_recording_id, TRASH_DIR, PURGE_MANUAL_ONLY, TRASH_RETENTION_DAYS,
)
from vocanote_cloud import download_cloud_audio
from vocanote_title_phase78 import phase78_display_title

BASE_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱').resolve()
SERVER_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
RECORDINGS_DIR = BASE_DIR / 'recordings'
TOKEN_FILE = SERVER_DIR / '.upload_token'
ALLOWED_EXT = {'.m4a', '.mp3', '.wav', '.webm', '.aac', '.ogg'}
MAX_UPLOAD_BYTES = 1024 * 1024 * 1024  # 1GB
TITLE_SIMILARITY_THRESHOLD = float(os.environ.get('TITLE_SIMILARITY_THRESHOLD', '0.72'))
KST = ZoneInfo('Asia/Seoul')
SAFE_EDIT_FOUNDATION_ENABLED = os.environ.get('VOCANOTE_SAFE_EDIT_FOUNDATION_ENABLED', '1') == '1'
TRIM_SPLIT_ENABLED = os.environ.get('VOCANOTE_TRIM_SPLIT_ENABLED', '0') == '1'

app = FastAPI(title='VocaNote Recording API', version='0.2.0')
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=['GET', 'POST', 'DELETE'],
    allow_headers=['*'],
)


def ensure_dirs() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    SERVER_DIR.mkdir(parents=True, exist_ok=True)
    RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
    if not TOKEN_FILE.exists():
        TOKEN_FILE.write_text(secrets.token_urlsafe(24), encoding='utf-8')
        os.chmod(TOKEN_FILE, 0o600)
    init_db()
    init_tombstone_db()


def token() -> str:
    ensure_dirs()
    return TOKEN_FILE.read_text(encoding='utf-8').strip()


def check_auth(authorization: Optional[str], x_upload_token: Optional[str]) -> None:
    supplied = None
    if authorization and authorization.lower().startswith('bearer '):
        supplied = authorization.split(' ', 1)[1].strip()
    elif x_upload_token:
        supplied = x_upload_token.strip()
    if not supplied or not secrets.compare_digest(supplied, token()):
        raise HTTPException(status_code=401, detail='unauthorized')


def parse_recorded_at(value: Optional[str]) -> datetime:
    if value:
        raw = value.strip()
        # Android currently sends "YYYY-MM-DD HH:MM:SS" without timezone.
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
            try:
                return datetime.strptime(raw[:19], fmt).replace(tzinfo=KST)
            except ValueError:
                pass
        try:
            dt = datetime.fromisoformat(raw)
            return dt if dt.tzinfo else dt.replace(tzinfo=KST)
        except Exception:
            pass
    return datetime.now(KST)


def now_iso() -> str:
    return datetime.now(KST).isoformat(timespec='seconds')


def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding='utf-8'))


# ---------------- New immutable recording layout ----------------

def is_uuid_recording_id(recording_id: str) -> bool:
    try:
        uuid.UUID(recording_id)
        if is_deleted(recording_id):
            return False
        return (RECORDINGS_DIR / recording_id).is_dir()
    except Exception:
        return False


def new_recording_dir(recording_id: str) -> Path:
    return RECORDINGS_DIR / recording_id




def find_recording_by_client_id(client_recording_id: Optional[str]) -> Optional[str]:
    if not client_recording_id:
        return None
    target = client_recording_id.strip()
    if not target:
        return None
    for d in RECORDINGS_DIR.iterdir() if RECORDINGS_DIR.exists() else []:
        if not d.is_dir() or d.name == '.trash' or d.name.startswith('.') or is_deleted(d.name):
            continue
        meta_path = d / 'metadata.json'
        if not meta_path.exists():
            continue
        try:
            meta = read_json(meta_path)
        except Exception:
            continue
        if meta.get('client_recording_id') == target:
            return d.name
    return None

def _is_placeholder_title(value: Optional[str]) -> bool:
    text = (value or '').strip()
    return (not text) or text in {'무제회의', '무제 회의', '무제 녹음', '회의'}


def _fallback_title_from_recording(meta: dict, snippet: str, recording_id: str) -> str:
    explicit = meta.get('title')
    if not _is_placeholder_title(explicit):
        return str(explicit).strip()
    date = str(meta.get('recorded_at') or '').replace('T', ' ').replace('+09:00', '')[:16]
    phrase = re.sub(r'\s+', ' ', (snippet or '').strip())[:28]
    if phrase:
        return f"{date} {phrase}".strip()
    return f"{date} 처리 중 녹음".strip() or recording_id


def two_stage_display_version(d: Path, status: dict | None = None) -> str:
    status = status or read_json(d / 'status.json')
    if status.get('human_available'):
        return 'HUMAN'
    if status.get('final_available') or (d / 'result.validated.json').exists():
        return 'FINAL'
    if status.get('fast_available') or (d / 'fast' / 'snapshot' / 'transcript_fast.txt').exists():
        return 'FAST'
    return 'PROCESSING'


def enrich_two_stage_fields(rec: dict, d: Path, status: dict) -> None:
    display = two_stage_display_version(d, status)
    rec.update({
        'processing_state': status.get('content_state') or status.get('status', 'unknown'),
        'available_version': status.get('available_version') or display,
        'display_version': status.get('display_version') or display,
        'fast_available': bool(status.get('fast_available') or (d / 'fast' / 'snapshot' / 'transcript_fast.txt').exists()),
        'final_available': bool(status.get('final_available') or (d / 'result.validated.json').exists()),
        'pipeline_version': status.get('pipeline_version'),
        'snapshot_version': display,
        'two_stage_timestamps': status.get('two_stage_timestamps') or {},
    })


def public_new_record(recording_id: str) -> dict:
    d = new_recording_dir(recording_id)
    meta = read_json(d / 'metadata.json')
    status = read_json(d / 'status.json')
    display_version = two_stage_display_version(d, status)
    result = read_json(d / 'result.validated.json') if display_version == 'FINAL' else {}
    summary_path = d / 'summary.md'
    snippet = ''
    if summary_path.exists():
        for line in summary_path.read_text('utf-8', errors='ignore').splitlines():
            line = line.strip('#- ` ')
            if line and not line.startswith(('일시', '유형', '메모', '처리')):
                snippet = line[:120]
                break
    if not snippet and display_version == 'FAST' and (d / 'fast' / 'snapshot' / 'transcript_fast.txt').exists():
        snippet = (d / 'fast' / 'snapshot' / 'transcript_fast.txt').read_text('utf-8', errors='ignore').strip().replace('\n', ' ')[:120]
    if not snippet and (d / 'transcript_clean.txt').exists():
        snippet = (d / 'transcript_clean.txt').read_text('utf-8', errors='ignore').strip().replace('\n', ' ')[:120]
    if not snippet and (d / 'transcript_raw.txt').exists():
        snippet = (d / 'transcript_raw.txt').read_text('utf-8', errors='ignore').strip().replace('\n', ' ')[:120]
    rec = {
        'id': recording_id,
        'recording_id': recording_id,
        'title': (result.get('title', {}).get('text') if isinstance(result.get('title'), dict) and not _is_placeholder_title(result.get('title', {}).get('text')) else _fallback_title_from_recording(meta, snippet, recording_id)),
        'type': result.get('content_type') or meta.get('type') or meta.get('meeting_type', ''),
        'recorded_at': meta.get('recorded_at', ''),
        'duration_sec': meta.get('duration_sec'),
        'status': status.get('status', 'unknown'),
        'step': status.get('step'),
        'snippet': snippet,
        'keywords': result.get('keywords') or [],
        'layout': 'recordings.v2',
        'files': sorted([p.name for p in d.iterdir() if p.is_file()]) if d.exists() else [],
    }
    enrich_two_stage_fields(rec, d, status)
    return rec


def read_new_text_file(recording_id: str, kind: str) -> str:
    d = new_recording_dir(recording_id)
    display = two_stage_display_version(d)
    transcript_candidates = ['transcript_clean.txt', 'transcript_raw.txt']
    if display == 'FAST':
        transcript_candidates = ['fast/snapshot/transcript_fast.txt', 'transcript_clean.txt', 'transcript_raw.txt']
    mapping = {
        'transcript': transcript_candidates,
        'transcript_raw': ['transcript_raw.txt'],
        'transcript_fast': ['fast/snapshot/transcript_fast.txt'],
        'transcript_clean': ['transcript_clean.txt'],
        'summary': ['summary.md'],
        'analysis': ['analysis.md'],
        'result': ['result.validated.json', 'result.raw.json'],
    }
    if kind not in mapping:
        raise HTTPException(status_code=400, detail='bad kind')
    parts = []
    for name in mapping[kind]:
        p = d / name
        if p.exists():
            parts.append(p.read_text(encoding='utf-8', errors='ignore'))
    if not parts:
        raise HTTPException(status_code=404, detail='not found')
    return '\n\n'.join(parts)


# ---------------- Legacy flat-file compatibility ----------------

def resolve_legacy_recording_id(recording_id: str) -> str:
    if list(BASE_DIR.glob(recording_id + '_*')):
        return recording_id
    for pattern in ('*_metadata.json', '*_status.json'):
        for p in BASE_DIR.glob(pattern):
            # Do not inspect new recordings subdir here.
            if p.parent != BASE_DIR:
                continue
            try:
                data = json.loads(p.read_text(encoding='utf-8'))
            except Exception:
                continue
            cur = data.get('id') or p.name.rsplit('_', 1)[0]
            old = data.get('old_id')
            old_ids = data.get('old_ids') or []
            if isinstance(old_ids, str):
                old_ids = [old_ids]
            if old == recording_id or recording_id in old_ids or cur == recording_id:
                if list(BASE_DIR.glob(cur + '_*')):
                    return cur
    raise HTTPException(status_code=404, detail='not found')


def public_legacy_record(prefix: str) -> dict:
    files = sorted(BASE_DIR.glob(prefix + '_*'))
    meta_path = BASE_DIR / f'{prefix}_metadata.json'
    status_path = BASE_DIR / f'{prefix}_status.json'
    meta = json.loads(meta_path.read_text(encoding='utf-8')) if meta_path.exists() else {}
    status = json.loads(status_path.read_text(encoding='utf-8')) if status_path.exists() else {}
    summary_path = BASE_DIR / f'{prefix}_summary.md'
    snippet = ''
    if summary_path.exists():
        txt = summary_path.read_text(encoding='utf-8', errors='ignore')
        for line in txt.splitlines():
            line = line.strip('#- ` ')
            if line and not line.startswith(('일시', '유형', '메모', '처리')):
                snippet = line[:120]
                break
    if not snippet:
        transcript_path = BASE_DIR / f'{prefix}_transcript.txt'
        if transcript_path.exists():
            snippet = transcript_path.read_text(encoding='utf-8', errors='ignore').strip().replace('\n', ' ')[:120]
    return {
        'id': prefix,
        'recording_id': prefix,
        'title': meta.get('title') or prefix,
        'type': meta.get('type', ''),
        'recorded_at': meta.get('recorded_at', ''),
        'duration_sec': meta.get('duration_sec'),
        'status': status.get('status', 'unknown'),
        'snippet': snippet,
        'keywords': legacy_keywords_for(prefix, limit=5),
        'layout': 'legacy.flat',
        'files': [p.name for p in files],
    }


def read_legacy_text_file(prefix: str, kind: str) -> str:
    mapping = {
        'transcript': ['transcript.md', 'transcript.txt'],
        'summary': ['summary.md'],
        'analysis': ['analysis.md', 'ir_review.md', 'action_items.md'],
        'ir_review': ['ir_review.md'],
        'action_items': ['action_items.md'],
        'share_kakao': ['share_kakao.txt'],
        'share_email': ['share_email.txt'],
        'share_slack': ['share_slack.txt'],
    }
    if kind not in mapping:
        raise HTTPException(status_code=400, detail='bad kind')
    parts = []
    for suffix in mapping[kind]:
        pp = BASE_DIR / f'{prefix}_{suffix}'
        if pp.exists():
            parts.append(pp.read_text(encoding='utf-8', errors='ignore'))
    if not parts:
        raise HTTPException(status_code=404, detail='not found')
    return '\n\n'.join(parts)


def legacy_keywords_for(prefix: str, limit: int = 10) -> list[str]:
    # Legacy only. New recordings must use result.validated.json keywords.
    text = ''
    for kind in ['summary', 'transcript', 'ir_review', 'action_items']:
        try:
            text += '\n' + read_legacy_text_file(prefix, kind)
        except Exception:
            pass
    words = re.findall(r'[가-힣A-Za-z0-9]{2,}', text)
    stop = {'회의','요약','전사','내용','테스트','확인','일시','유형','메모','핵심','정리','자동','필요','있습니다','합니다','대한','우리','아주','맞아요','그렇죠','그런데'}
    counts = {}
    original = {}
    for w in words:
        wl = w.lower()
        if wl in stop or len(wl) < 2:
            continue
        counts[wl] = counts.get(wl, 0) + 1
        original.setdefault(wl, w)
    return [original[w] for w, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]]


def resolve_any_recording_id(recording_id: str) -> tuple[str, str]:
    if is_deleted(recording_id):
        raise HTTPException(status_code=404, detail='deleted')
    if is_uuid_recording_id(recording_id):
        return 'new', recording_id
    try:
        return 'legacy', resolve_legacy_recording_id(recording_id)
    except HTTPException:
        # New id may exist as a directory even if UUID validation failed in future.
        candidate = (RECORDINGS_DIR / recording_id)
        if candidate.is_dir() and not is_deleted(recording_id) and candidate.name != '.trash' and TRASH_DIR not in candidate.resolve().parents:
            return 'new', recording_id
        raise


def resolve_active_recording(recording_id: str) -> tuple[str, str]:
    """Common API resolver: deleted/.trash recordings are never active resources."""
    return resolve_any_recording_id(recording_id)


def delete_response(ts: dict) -> dict:
    return {
        'ok': True,
        'recording_id': ts.get('recording_id'),
        'delete_state': ts.get('delete_state'),
        'delete_scope': ts.get('delete_scope'),
        'trash_retention_days': TRASH_RETENTION_DAYS,
        'purge_manual_only': PURGE_MANUAL_ONLY,
    }


@app.on_event('startup')
def _startup() -> None:
    ensure_dirs()


@app.get('/health')
def health() -> dict:
    ensure_dirs()
    return {'ok': True, 'base_dir': str(BASE_DIR), 'recordings_dir': str(RECORDINGS_DIR), 'queue_db': str(SERVER_DIR / 'jobs.sqlite3')}


@app.get('/api/token-info')
def token_info() -> dict:
    ensure_dirs()
    return {'configured': TOKEN_FILE.exists(), 'token_file': str(TOKEN_FILE)}


@app.get('/api/jobs')
def api_jobs(
    authorization: Optional[str] = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
    limit: int = 50,
) -> dict:
    check_auth(authorization, x_upload_token)
    return {'items': list_jobs(limit=limit)}


@app.get('/api/purges/android-pending')
def android_pending_purges(
    authorization: Optional[str] = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
) -> dict:
    check_auth(authorization, x_upload_token)
    if not SAFE_EDIT_FOUNDATION_ENABLED:
        raise HTTPException(status_code=404, detail='safe_edit_foundation_disabled')
    store = SafeEditStore(DB_PATH, foundation_enabled=True, destructive_enabled=False)
    store.init_schema()
    return {'items': store.list_android_pending_purges(), 'destructive_edit_enabled': TRIM_SPLIT_ENABLED}


@app.post('/api/purges/{purge_id}/android-ack')
def android_purge_ack(
    purge_id: str,
    authorization: Optional[str] = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
) -> dict:
    check_auth(authorization, x_upload_token)
    if not SAFE_EDIT_FOUNDATION_ENABLED:
        raise HTTPException(status_code=404, detail='safe_edit_foundation_disabled')
    store = SafeEditStore(DB_PATH, foundation_enabled=True, destructive_enabled=False)
    store.init_schema()
    if not store.get_purge(purge_id):
        raise HTTPException(status_code=404, detail='purge_not_found')
    store.ack_android_purge(purge_id)
    return {'ok': True, 'purge': store.get_purge(purge_id)}


def safe_edit_store() -> SafeEditStore:
    store = SafeEditStore(DB_PATH, foundation_enabled=SAFE_EDIT_FOUNDATION_ENABLED, destructive_enabled=TRIM_SPLIT_ENABLED)
    store.init_schema()
    return store


def ensure_lazy_catalog(recording_id: str, store: SafeEditStore) -> dict:
    row = store.get_recording(recording_id)
    if row:
        return row
    kind, resolved = resolve_active_recording(recording_id)
    if kind != 'new':
        raise HTTPException(status_code=409, detail='legacy_recording_edit_not_cataloged')
    rec_dir = RECORDINGS_DIR / resolved
    metadata = json.loads((rec_dir / 'metadata.json').read_text(encoding='utf-8'))
    audio_path = rec_dir / str(metadata.get('audio_file') or 'audio.m4a')
    if not audio_path.is_file():
        raise HTTPException(status_code=409, detail='canonical_audio_missing')
    job = get_job(resolved)
    state = 'FINAL' if job and job.get('status') == 'completed' else 'PROCESSING'
    store.register_recording(resolved, audio_path, state=state, generation=int((job or {}).get('generation') or 1))
    return store.get_recording(resolved) or {}


@app.get('/api/recordings/{recording_id}/edit-state')
def get_edit_state(recording_id: str, authorization: Optional[str] = Header(None), x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token')) -> dict:
    check_auth(authorization, x_upload_token)
    store = safe_edit_store()
    return {'recording': ensure_lazy_catalog(recording_id, store), 'destructive_edit_enabled': TRIM_SPLIT_ENABLED}


@app.post('/api/recordings/{recording_id}/edit-lease/acquire')
def acquire_edit_lease(recording_id: str, payload: dict = Body(...), authorization: Optional[str] = Header(None), x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token')) -> dict:
    check_auth(authorization, x_upload_token)
    store = safe_edit_store(); row = ensure_lazy_catalog(recording_id, store)
    if row.get('state') not in {'EDIT_PENDING','EDIT_RECOVERY_REQUIRED'}:
        store.enter_edit_pending(recording_id, grace_seconds=int(payload.get('grace_seconds') or 5))
    try:
        token = store.acquire_edit_lease(recording_id, str(payload.get('owner') or 'android'), min(300, max(15, int(payload.get('ttl_seconds') or 60))))
    except SafeEditError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {'ok': True, 'lease_token': token, 'recording': store.get_recording(recording_id), 'destructive_edit_enabled': TRIM_SPLIT_ENABLED}


@app.post('/api/recordings/{recording_id}/edit-lease/heartbeat')
def heartbeat_edit_lease_api(recording_id: str, payload: dict = Body(...), authorization: Optional[str] = Header(None), x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token')) -> dict:
    check_auth(authorization, x_upload_token)
    store = safe_edit_store()
    ok = store.heartbeat_edit_lease(recording_id, str(payload.get('lease_token') or ''), min(300, max(15, int(payload.get('ttl_seconds') or 60))))
    if not ok:
        store.recover_expired_leases()
        raise HTTPException(status_code=409, detail='edit_lease_not_active')
    return {'ok': True, 'recording': store.get_recording(recording_id)}


@app.post('/api/recordings/{recording_id}/edit-lease/release')
def release_edit_lease_api(recording_id: str, payload: dict = Body(...), authorization: Optional[str] = Header(None), x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token')) -> dict:
    check_auth(authorization, x_upload_token)
    store = safe_edit_store()
    try:
        store.cancel_edit(recording_id, str(payload.get('lease_token') or ''))
    except SafeEditError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {'ok': True, 'recording': store.get_recording(recording_id), 'destructive_edit_enabled': TRIM_SPLIT_ENABLED}


@app.post('/api/recordings/{recording_id}/edit-confirm')
def confirm_edit_disabled(recording_id: str, authorization: Optional[str] = Header(None), x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token')) -> dict:
    check_auth(authorization, x_upload_token)
    raise HTTPException(status_code=409, detail='destructive_edit_disabled')


RETRYABLE_LIST_OPEN_ERROR_CODES = {
    'temporary_worker_error', 'worker_error', 'json_invalid', 'timeout', 'stale_lease_expired',
    'manual_retry_after_provider_recovery', 'manual_retry_after_semantic_fix',
    'fake_semantic_artifacts_invalidated', 'phase6_semantic_map_reduce_required',
}
NON_RETRYABLE_LIST_OPEN_ERROR_CODES = {
    'stt_failed_audio_invalid', 'permanent_file_error', 'recording_deleted', 'deleted',
}


def _atomic_json_file(path: Path, data: dict) -> None:
    tmp = path.with_name(f'.{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(path)


def _reset_failed_semantic_manifest(recording_dir: Path) -> None:
    mp = recording_dir / 'semantic_chunks' / 'manifest.json'
    if not mp.exists():
        return
    data = read_json(mp)
    changed = False
    for row in data.get('chunks') or []:
        if row.get('status') in {'failed', 'processing'}:
            row['status'] = 'pending'
            row['attempts'] = 0
            row['processing_started_at'] = None
            row['last_error'] = None
            row['last_error_at'] = None
            changed = True
    if changed:
        # The caller holds BEGIN IMMEDIATE and has rechecked generation authority.
        # Keep existing outputs as resumable evidence; the worker's checkpoint/hash
        # validation decides whether they can be reused or must be overwritten.
        _atomic_json_file(mp, data)


def trigger_processing_from_list_open(*, source: str = 'android_list_open') -> dict:
    """Lightweight recovery signal from Android list open.

    Reclaims expired active jobs and safely requeues retryable failed jobs whose audio
    exists. Invalid/tiny audio is never requeued.
    """
    ensure_dirs()
    ts = now_iso()
    recovered = recover_stale_jobs(worker_id=source)
    requeued: list[str] = []
    skipped: list[dict] = []
    with connect() as conn:
        rows = conn.execute('''
            SELECT * FROM recording_jobs
            WHERE status='failed'
              AND recording_id NOT IN (SELECT recording_id FROM deleted_recordings)
            ORDER BY updated_at DESC
            LIMIT 50
        ''').fetchall()
        for row in rows:
            rid = row['recording_id']
            rec_dir = Path(row['output_dir'])
            audio_path = Path(row['audio_path'])
            code = str(row['error_code'] or '')
            if is_deleted(rid) or code in NON_RETRYABLE_LIST_OPEN_ERROR_CODES:
                skipped.append({'recording_id': rid, 'reason': code or 'non_retryable'})
                continue
            if not rec_dir.exists() or not audio_path.exists() or audio_path.stat().st_size < 10000:
                skipped.append({'recording_id': rid, 'reason': 'missing_or_invalid_audio', 'bytes': audio_path.stat().st_size if audio_path.exists() else None})
                continue
            if code and code not in RETRYABLE_LIST_OPEN_ERROR_CODES and not any(s in code for s in ['worker', 'timeout', 'json', 'semantic']):
                skipped.append({'recording_id': rid, 'reason': f'not_auto_retryable:{code}'})
                continue
            try:
                conn.execute('BEGIN IMMEDIATE')
                authoritative = conn.execute('''
                    SELECT 1 FROM recording_jobs j
                    WHERE j.recording_id=? AND j.job_id=? AND j.status='failed'
                      AND j.generation=COALESCE(
                        (SELECT current_generation FROM safe_edit_recordings WHERE recording_id=j.recording_id),
                        j.generation
                      )
                ''', (rid, row['job_id'])).fetchone()
                if not authoritative:
                    conn.execute('ROLLBACK')
                    skipped.append({'recording_id': rid, 'reason': 'stale_generation'})
                    continue
                _reset_failed_semantic_manifest(rec_dir)
                cur = conn.execute('''
                    UPDATE recording_jobs
                    SET status='retry_wait', step='list_open_retry_requested', attempts=0, max_attempts=MAX(max_attempts, 8),
                        claimed_at=NULL, claimed_by=NULL, claim_token=NULL, heartbeat_at=NULL, lease_expires_at=NULL,
                        error_code='list_open_auto_retry', error_message='Android list open requested processing recovery',
                        updated_at=?, created_at=?
                    WHERE recording_id=? AND job_id=? AND status='failed'
                      AND generation=COALESCE(
                        (SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),
                        generation
                      )
                ''', (ts, ts, rid, row['job_id']))
                if cur.rowcount != 1:
                    raise RuntimeError('list_open_generation_authority_lost')
                conn.execute('''
                    INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
                    VALUES (?, ?, ?, 'list_open_auto_retry', 'retry_wait', 'list_open_retry_requested', ?, ?)
                ''', (row['job_id'], rid, source, ts, 'Android opened recording list; retryable failed job requeued'))
                sp = rec_dir / 'status.json'
                if sp.exists():
                    status = read_json(sp)
                    status.update({'status': 'queued', 'step': 'list_open_retry_requested', 'content_state': 'RETRY_WAIT', 'content_step': 'list_open_retry_requested', 'error_code': None, 'error_message': None, 'updated_at': ts})
                    _atomic_json_file(sp, status)
                conn.execute('COMMIT')
            except Exception as exc:
                try:
                    conn.execute('ROLLBACK')
                except Exception:
                    pass
                skipped.append({'recording_id': rid, 'reason': 'semantic_manifest_reset_failed', 'error': repr(exc)[:160]})
                continue
            requeued.append(rid)
    return {'recovered_stale_count': len(recovered), 'requeued_count': len(requeued), 'requeued': requeued, 'skipped': skipped[:10]}


def _weak_disambig_phrase(text: str) -> bool:
    t = re.sub(r'\s+', ' ', (text or '').strip())
    if len(t) < 2:
        return True
    weak = {'설명','내용','이야기','회의','메모','관련','관련 내용','녹음','처리','요약','발화','대화'}
    return t in weak or any(t == w for w in weak)


def _clean_phrase(text: str) -> str:
    t = re.sub(r'[`#*_\[\]{}()<>|]', ' ', str(text or ''))
    t = re.sub(r'\s+', ' ', t).strip(' .,:;·-_/')
    # Keep concise noun-ish phrase; preserve useful slashes like YAP/TAZ.
    parts = t.split()
    if len(parts) > 6:
        t = ' '.join(parts[:6])
    return t[:36].strip(' .,:;·-_')


TITLE_GENERIC_WORDS = {
    '녹음','회의','대화','메모','정리','요약','내용','이야기','관련','설명','소개','확인','처리','전사','기록',
    '연구','논의','사항','주제','부분','문제','결과','오늘','무제','무제회의','무제녹음'
}
TITLE_BAD_PATTERNS = [
    re.compile(r'^\d{4}[.\-/]\d{1,2}[.\-/]\d{1,2}'),
    re.compile(r'^오늘\s*\d{1,2}:\d{2}'),
    re.compile(r'처리\s*중\s*녹음'),
    re.compile(r'^topic\s*\d+$', re.I),
]

# Existing recording display-title overrides requested by user: show
# recording situation / conversation purpose first, then people or detail.
DISPLAY_TITLE_OVERRIDES = {
    'a37d0f2a-f8f8-41e6-a9f6-e92c40cbdd8c': '드라마 녹음 내용-바위에 맞은 장면',
    '8fb17cd2-4fb8-4c2d-9cdb-397d91d61922': '바노바기 피부과 현장회의-더마톡신 강의·학회 운영 논의',
    '0e1e01fa-0549-4445-baa7-765c139ac26d': '드라마 녹음 내용-지은호와 서정',
    '2fc6e321-989c-4188-9c75-14d1bfdf6eba': '드라마 녹음 내용-현수와 은동',
    '0dd5ce2c-fb1b-44b6-8369-47fb92345377': '리주비놀·마이디 산학연구 대면회의-AI 피부과 추천·피부변화 모델',
    '2a097468-13e4-4410-889c-b89c9cfb922d': '회의록-자동 레이저 토닝 사업모델',
    'ba3730e0-e06a-4b81-9b52-d84c97f19737': '기도 내용-하나님을 대면하는 기도',
    '0d380365-b558-4089-9188-aec44a75d552': '녹음 테스트-topic 0',
    '5eeaeef9-9b6a-43a0-b1a1-43b4abe188fa': '숙제 기록-일반과목 문제집',
    '754e940f-6ba3-493c-ae7b-fb9fa9bc99a8': '녹음 테스트-백그라운드 녹음',
    '8d4a534e-2448-458b-a52a-9b9ed8d2040e': '성경 숙제-마태복음 문제 풀이',
    'd8681801-3329-4984-b64f-a37f90a19cf7': '성경 숙제-남은 분량 점검',
    'a41ec2bb-3a5d-4e6b-a2a9-45993e079324': '녹음 테스트-짧은 무음 녹음',
    'e119b1b4-06da-474c-8e4a-43348cabe418': '녹음 테스트-짧은 내용 확인',
    'ed8b2a79-c93d-4a0f-96e3-faf049cf573c': '가족 기도 메모-어머니 위암 항암치료',
    '7b227cee-7c59-46b0-8ce8-6aa0d46a5fea': '녹음 테스트-도메인 프로브',
    '0840622a-12be-440e-99e1-81a1f454c4c0': '녹음 테스트-펜싱 스모크',
    '06dcf0a9-a480-4a41-bed0-4cad044ad30f': '연구 메모-세포 회춘 기계적 힘',
    'd86293b8-1e7b-4814-b8c2-795f27db663e': '연구 메모-세포 회춘 압박 비교',
    'f7707df4-1cd9-46ff-8b9c-db9d9d72753c': '연구 메모-물리적 힘 세포 회춘',
    '7376ef1b-9fdf-49e3-9b73-27e4120532d7': '연구 메모-압박 기반 세포 회춘',
    'b17214d4-71db-46b3-9d0b-1542b2f340c7': '연구 메모-세포 회춘 소개',
    '8f01465e-40d4-438f-9b92-4d973271cf2d': '녹음 테스트-발화 없음',
    'e5ec13b1-88ce-4a7d-b15c-711b7fbb2366': '녹음 테스트-처리 실패 녹음',
    'c4c8d225-580f-4bf7-ad5f-6b9e420211a3': '녹음 테스트-자동 녹음 작동 확인',
    'ebbea272-0d2e-420e-8df4-3042bfda1ab1': '녹음 테스트-자동 녹음 작동 확인 2',
    '5f60cbdb-c85d-47d8-8b60-bfa4254914f5': '연구 메모-세포 회춘 해설',
    '50ddf101-50e6-4552-9744-323b12ae7101': '녹음 테스트-짧은 전사 없음',
    'cfcd572d-ae46-4917-bc24-0de994c9a7a8': '녹음 테스트-전사 결과 없음',
    '260814이렇게직관적으로생각하게되죠': '연구 메모-직관적 사고 설명',
    '260814짧은녹음확인_2': '녹음 테스트-짧은 녹음 확인 2',
    '26081475세기증자의아주늘고지친피부세포가': '연구 메모-75세 기증자 피부세포',
    '260814공간이줄어들때세포가강하게움켜쥐는현상': '연구 메모-세포 공간 감소 현상',
    '260814짧은녹음확인': '녹음 테스트-짧은 녹음 확인',
    '260814자동녹음실행확인': '녹음 테스트-자동 녹음 실행',
    '260814대화없음감지오류확인': '녹음 테스트-대화 없음 감지 오류',
    '260814측면버튼녹음과대화없음감지확인': '녹음 테스트-측면 버튼 녹음과 대화 없음',
    '260814화면꺼짐녹음업로드전사확인': '녹음 테스트-화면 꺼짐 녹음 업로드 전사',
    '260814짧은녹음오작동확인': '녹음 테스트-짧은 녹음 오작동',
    '260813항노화학회평가회참석전메모': '행사 메모-항노화학회 평가회 참석 전',
    '260813화면꺼짐녹음업로드확인': '녹음 테스트-화면 꺼짐 녹음 업로드',
}


def _title_token_clean(raw: str) -> str:
    t = str(raw or '').strip().lstrip('#')
    aliases = {
        'yaptaz': 'YAP/TAZ', 'yap/taz': 'YAP/TAZ', 'ecm': 'ECM', 'tgf-b': 'TGF-β',
        'tgfβ': 'TGF-β', 'tgf-beta': 'TGF-β', 'dej': 'DEJ', 'age': 'AGEs', 'ages': 'AGEs',
    }
    key = re.sub(r'\s+', '', t.lower())
    if key in aliases:
        return aliases[key]
    t = re.sub(r'[`#*_\[\]{}()<>|]', ' ', t)
    t = re.sub(r'\b(에 대한|에 관한|관련한|관련된|통해서|통한)\b', ' ', t)
    t = re.sub(r'(를|을)?\s*위한.*$', '', t)
    t = re.sub(r'(.+?와\s*[^\s의]+)의.*$', r'\1', t)
    t = re.sub(r'\s+', ' ', t).strip(' .,:;·-_')
    if t in TITLE_GENERIC_WORDS:
        return t
    t = re.sub(r'(에|에서|으로|로|을|를|은|는|이|가|의)$', '', t)
    return t.strip()


def _is_bad_title_base(title: str) -> bool:
    t = str(title or '').strip()
    if _is_placeholder_title(t):
        return True
    if len(t) > 22:
        return True
    if any(p.search(t) for p in TITLE_BAD_PATTERNS):
        return True
    compact = re.sub(r'\W+', '', t.lower())
    return compact in TITLE_GENERIC_WORDS or '무제' in t or '처리 중' in t


def _short_keywords(item: dict) -> list[str]:
    out, seen = [], set()
    for raw in item.get('keywords') or []:
        t = _title_token_clean(str(raw))
        if not t:
            continue
        norm = re.sub(r'\W+', '', t.lower())
        if norm in seen or norm in TITLE_GENERIC_WORDS:
            continue
        if any(p.search(t) for p in TITLE_BAD_PATTERNS):
            continue
        # reject sentence-like long tags; pipeline stays unchanged, UI title filters only.
        if len(t) > 12 or len(t.split()) > 2:
            continue
        seen.add(norm)
        out.append(t)
    return out[:4]


def _title_phrase_candidates(item: dict) -> list[str]:
    cands = []
    raw_title = str(item.get('title') or '')
    cands.append(raw_title)
    cands.extend(_candidate_phrases_for_item(item))
    cands.append(str(item.get('snippet') or ''))
    cleaned = []
    seen = set()
    for raw in cands:
        raw_parts = re.split(r'[,，;；·/]|\s+-\s+', str(raw or ''))
        for part in raw_parts:
            t = _title_token_clean(_clean_phrase(part))
            t = re.sub(r'^(물리적|기계적)\s+(힘|압박)\s*(만으로|으로|을 통한)?\s*', '', t)
            t = re.sub(r'(연구|소개|설명|내용|정리|관련)$', '', t).strip(' ·-_')
            if not t or _weak_disambig_phrase(t):
                continue
            if any(p.search(t) for p in TITLE_BAD_PATTERNS):
                continue
            if len(t) < 2:
                continue
            if len(t) > 18:
                words = t.split()
                if len(words) >= 2:
                    t = ' '.join(words[:3]).strip()
                if len(t) > 18:
                    continue
            norm = re.sub(r'\W+', '', t.lower())
            if norm in seen or norm in TITLE_GENERIC_WORDS:
                continue
            seen.add(norm); cleaned.append(t)
    return cleaned


def short_display_title_for_item(item: dict) -> tuple[str, str]:
    status = str(item.get('status') or '').lower()
    raw_title = str(item.get('title') or '').strip()
    kws = _short_keywords(item)
    phrases = _title_phrase_candidates(item)

    # Prefer compact entity/topic title when model title is placeholder, raw-date, or too verbose.
    if _is_bad_title_base(raw_title):
        if len(kws) >= 2:
            title = f'{kws[0]} · {kws[1]}'
            return title[:18], 'keywords'
        if kws:
            return kws[0], 'keyword'
        if phrases:
            return phrases[0], 'phrase'
        if status not in {'completed', 'complete', 'done', '완료'}:
            return '제목 생성 중…', 'processing'
        return '녹음 메모', 'fallback'

    title = _title_token_clean(raw_title)
    if len(title) <= 18:
        return title, 'original_short'
    if kws:
        return kws[0], 'keyword_from_long_original'
    if phrases:
        return phrases[0], 'phrase_from_long_original'
    return title[:18].rstrip(' ·-_'), 'truncated'


def _phase78_title_enabled() -> bool:
    return os.environ.get('VOCANOTE_PHASE78_TITLE_ENABLED', '1').strip().lower() not in {'0', 'false', 'no', 'off'}


def apply_short_display_titles(items: list[dict]) -> list[dict]:
    for item in items:
        rid = str(item.get('id') or item.get('recording_id') or '')
        if _phase78_title_enabled() and is_uuid_recording_id(rid):
            d = new_recording_dir(rid)
            result = read_json(d / 'result.validated.json')
            display_title, quality = phase78_display_title(d, result)
            if display_title:
                item['short_title'] = display_title
                item['display_title'] = display_title
                item['title_quality'] = quality | {'reason': 'phase84_title_logic'}
                continue
        override = DISPLAY_TITLE_OVERRIDES.get(rid)
        if override:
            short, reason = override, 'manual_situation_purpose_override'
        else:
            short, reason = short_display_title_for_item(item)
        item['short_title'] = short
        item['display_title'] = short
        item['title_quality'] = {
            'display_algorithm': 'v3_situation_purpose_override' if override else 'v2_compact_topic',
            'reason': reason,
            'source_title': item.get('title') or '',
        }
    return items


def _candidate_phrases_for_item(item: dict) -> list[str]:
    rid = str(item.get('id') or item.get('recording_id') or '')
    candidates: list[str] = []
    d = new_recording_dir(rid) if is_uuid_recording_id(rid) else None
    if d and d.exists():
        sem = read_json(d / 'semantic_final.json')
        for key in ('key_points', 'topics', 'decisions'):
            val = sem.get(key)
            if isinstance(val, list):
                for x in val:
                    if isinstance(x, dict):
                        candidates.append(x.get('text') or x.get('summary') or x.get('topic') or x.get('decision') or '')
                    else:
                        candidates.append(str(x))
        result = read_json(d / 'result.validated.json')
        for x in result.get('keywords') or []:
            candidates.append(str(x))
        memo = result.get('memo_summary') or {}
        if isinstance(memo, dict):
            for key in ('topic', 'one_line'):
                candidates.append(str(memo.get(key) or ''))
            for key in ('core_points', 'flow'):
                for x in memo.get(key) or []:
                    candidates.append(str(x))
        note = result.get('structured_note') or {}
        if isinstance(note, dict):
            for section in note.get('sections') or []:
                if isinstance(section, dict):
                    candidates.append(str(section.get('heading') or ''))
                    for x in section.get('items') or []:
                        candidates.append(str(x))
    for x in item.get('keywords') or []:
        candidates.append(str(x))
    candidates.append(str(item.get('snippet') or ''))
    out: list[str] = []
    seen = set()
    for raw in candidates:
        phrase = _clean_phrase(raw)
        if not phrase or _weak_disambig_phrase(phrase):
            continue
        norm = re.sub(r'\W+', '', phrase.lower())
        if norm and norm not in seen:
            seen.add(norm)
            out.append(phrase)
    return out[:20]


def _norm_title(value: str) -> str:
    t = re.sub(r'\s+', '', str(value or '').lower())
    t = re.sub(r'[\W_]+', '', t)
    return t


def _title_similar(a: str, b: str) -> bool:
    na, nb = _norm_title(a), _norm_title(b)
    if not na or not nb:
        return False
    if na in nb or nb in na:
        return True
    return SequenceMatcher(None, na, nb).ratio() >= TITLE_SIMILARITY_THRESHOLD


def _collision_groups(items: list[dict]) -> list[list[int]]:
    n = len(items)
    parent = list(range(n))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a,b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra
    for i in range(n):
        for j in range(i + 1, n):
            if (items[i].get('title_quality') or {}).get('reason') in {'manual_situation_purpose_override', 'phase78_title_logic', 'phase84_title_logic'}:
                continue
            if (items[j].get('title_quality') or {}).get('reason') in {'manual_situation_purpose_override', 'phase78_title_logic', 'phase84_title_logic'}:
                continue
            if _title_similar(str(items[i].get('title') or ''), str(items[j].get('title') or '')):
                union(i, j)
    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)
    return [g for g in groups.values() if len(g) >= 2]


def apply_title_disambiguation(items: list[dict]) -> list[dict]:
    for group in _collision_groups(items):
        candidate_lists = {idx: _candidate_phrases_for_item(items[idx]) for idx in group}
        freq = Counter()
        for idx, cands in candidate_lists.items():
            norms = {re.sub(r'\W+', '', c.lower()) for c in cands if c}
            freq.update(norms)
        chosen: dict[int, str] = {}
        for idx in group:
            cands = candidate_lists.get(idx) or []
            scored = []
            for pos, cand in enumerate(cands):
                norm = re.sub(r'\W+', '', cand.lower())
                title_norm = _norm_title(str(items[idx].get('title') or ''))
                if not norm or norm in title_norm:
                    continue
                scored.append((freq[norm], pos, cand))
            scored.sort(key=lambda x: (x[0], x[1]))
            if scored:
                chosen[idx] = scored[0][2]
            elif cands:
                chosen[idx] = cands[0]
            else:
                chosen[idx] = ''
        used = set()
        for idx in group:
            base = str(items[idx].get('display_title') or items[idx].get('short_title') or items[idx].get('title') or '').strip()
            dis = _title_token_clean(_clean_phrase(chosen.get(idx) or ''))
            if len(dis) > 12:
                dis = dis[:12].rstrip(' ·-_')
            if not dis:
                dis = compact_recorded_at_for_title(str(items[idx].get('recorded_at') or ''))
            display = f'{base} · {dis}' if dis else base
            if len(display) > 24:
                date_hint = compact_recorded_at_for_title(str(items[idx].get('recorded_at') or ''))
                display = f'{base} · {date_hint}' if date_hint and len(f'{base} · {date_hint}') <= 24 else base
            # fallback if still duplicated in this group
            extra_i = 1
            cands = candidate_lists.get(idx) or []
            while display in used and extra_i < len(cands):
                extra = _clean_phrase(cands[extra_i])
                if extra and extra not in display:
                    display = f'{base} · {dis} · {extra}' if dis else f'{base} · {extra}'
                extra_i += 1
            if display in used:
                date = compact_recorded_at_for_title(str(items[idx].get('recorded_at') or ''))
                display = f'{display} · {date}' if date else f'{display} · {idx + 1}'
            items[idx]['display_title'] = display
            items[idx]['title_disambiguation'] = display.replace(base, '', 1).strip(' ·')
            used.add(display)
    return items


def compact_recorded_at_for_title(value: str) -> str:
    raw = str(value or '').replace('T', ' ').replace('+09:00', '')
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})\s*(\d{2}:\d{2})?', raw)
    if not m:
        return ''
    return f"{int(m.group(2))}/{int(m.group(3))}" + (f" {m.group(4)}" if m.group(4) else '')


@app.get('/api/recordings')
def list_recordings(
    authorization: Optional[str] = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
    x_vocanote_list_opened: Optional[str] = Header(None, alias='X-VocaNote-List-Opened'),
) -> dict:
    check_auth(authorization, x_upload_token)
    recovery = None
    if str(x_vocanote_list_opened or '').lower() in {'1', 'true', 'yes'}:
        recovery = trigger_processing_from_list_open()
    items = []
    for d in RECORDINGS_DIR.iterdir() if RECORDINGS_DIR.exists() else []:
        if d.name == '.trash' or d.name.startswith('.') or is_deleted(d.name):
            continue
        if d.is_dir() and (d / 'metadata.json').exists():
            try:
                items.append(public_new_record(d.name))
            except Exception:
                pass
    prefixes = set()
    for p in BASE_DIR.glob('*_*'):
        if p.parent != BASE_DIR:
            continue
        name = p.name
        for suffix in ['_original', '_transcript', '_summary', '_ir_review', '_action_items', '_share_kakao', '_share_email', '_share_slack', '_segments', '_metadata', '_status']:
            i = name.find(suffix)
            if i > 0:
                prefixes.add(name[:i])
                break
    items.extend(public_legacy_record(prefix) for prefix in prefixes)
    items.sort(key=lambda item: item.get('recorded_at') or item.get('id') or '', reverse=True)
    apply_short_display_titles(items)
    apply_title_disambiguation(items)
    response = {'items': items}
    if recovery is not None:
        response['processing_signal'] = recovery
    return response


@app.post('/api/recordings')
def upload_recording(
    audio: UploadFile = File(...),
    title: str = Form('무제회의'),
    meeting_type: str = Form('meeting'),
    memo: str = Form(''),
    recorded_at: Optional[str] = Form(None),
    duration_sec: Optional[int] = Form(None),
    language: str = Form('ko'),
    client_recording_id: Optional[str] = Form(None),
    authorization: Optional[str] = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
) -> dict:
    check_auth(authorization, x_upload_token)
    ensure_dirs()
    existing_id = find_recording_by_client_id(client_recording_id)
    if existing_id:
        rec = public_new_record(existing_id)
        return {'ok': True, 'id': existing_id, 'recording_id': existing_id, 'job_id': f'vocanote_{existing_id}', 'idempotent': True, 'recording': rec}
    recording_id = str(uuid.uuid4())
    job_id = f'vocanote_{recording_id}'
    rec_dir = RECORDINGS_DIR / recording_id
    rec_dir.mkdir(parents=True, exist_ok=False)
    original_name = audio.filename or 'recording.m4a'
    ext = Path(original_name).suffix.lower() or '.m4a'
    if ext not in ALLOWED_EXT:
        shutil.rmtree(rec_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail=f'unsupported audio type: {ext}')
    audio_name = f'audio{ext}'
    audio_path = rec_dir / audio_name
    written = 0
    with audio_path.open('wb') as f:
        while True:
            chunk = audio.file.read(1024 * 1024)
            if not chunk:
                break
            written += len(chunk)
            if written > MAX_UPLOAD_BYTES:
                f.close()
                shutil.rmtree(rec_dir, ignore_errors=True)
                raise HTTPException(status_code=413, detail='file too large')
            f.write(chunk)
    dt = parse_recorded_at(recorded_at)
    uploaded_at = now_iso()
    metadata = {
        'schema_version': 'vocanote.metadata.v2',
        'recording_id': recording_id,
        'id': recording_id,
        'recorded_at': dt.isoformat(timespec='seconds'),
        'uploaded_at': uploaded_at,
        'title': None if title in {'무제회의', '무제 회의', '회의', ''} else title,
        'language': language or 'ko',
        'type': meeting_type,
        'meeting_type': meeting_type,
        'memo': memo,
        'duration_sec': duration_sec or 0,
        'client_recording_id': client_recording_id.strip() if client_recording_id else None,
        'audio_file': audio_name,
        'original_filename': original_name,
        'bytes': written,
        'layout': 'recordings.v2',
    }
    status = {
        'schema_version': 'vocanote.status.v2',
        'recording_id': recording_id,
        'id': recording_id,
        'status': 'queued',
        'step': 'queued',
        'updated_at': uploaded_at,
        'steps': {'upload': 'done', 'queue': 'queued', 'stt': 'waiting', 'correction': 'waiting', 'semantic': 'waiting', 'render': 'waiting'},
    }
    metadata_path = rec_dir / 'metadata.json'
    status_path = rec_dir / 'status.json'
    write_json(metadata_path, metadata)
    write_json(status_path, status)
    enqueue_job(job_id=job_id, recording_id=recording_id, audio_path=str(audio_path), metadata_path=str(metadata_path), output_dir=str(rec_dir), generation=1, catalog_canonical=True)
    rec = public_new_record(recording_id)
    return {'ok': True, 'id': recording_id, 'recording_id': recording_id, 'job_id': job_id, 'recording': rec}


def delete_legacy_recording_to_trash(prefix: str) -> dict:
    if not prefix or '/' in prefix or '\\' in prefix or prefix.startswith('.'):
        raise ValueError('invalid legacy recording id')
    files = []
    for p in BASE_DIR.glob(prefix + '_*'):
        if p.parent == BASE_DIR and p.is_file():
            files.append(p)
    if not files:
        raise FileNotFoundError(prefix)
    dest_dir = BASE_DIR / '.trash' / f'legacy_{prefix}_{datetime.now(KST).strftime("%Y%m%d_%H%M%S")}'
    dest_dir.mkdir(parents=True, exist_ok=False)
    moved = []
    for p in files:
        dest = dest_dir / p.name
        os.replace(p, dest)
        moved.append(str(dest))
    return {
        'ok': True,
        'recording_id': prefix,
        'delete_state': 'deleted',
        'delete_scope': 'legacy_flat',
        'trash_path': str(dest_dir),
        'moved_count': len(moved),
        'purge_manual_only': True,
    }


@app.delete('/api/recordings/{recording_id}')
def delete_recording(
    recording_id: str,
    authorization: str | None = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
) -> dict:
    check_auth(authorization, x_upload_token)
    ensure_dirs()
    try:
        rid = validate_recording_id(recording_id)
    except ValueError:
        try:
            return delete_legacy_recording_to_trash(recording_id)
        except ValueError:
            raise HTTPException(status_code=400, detail='invalid_recording_id')
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail='not_found')
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f'delete_failed: {type(exc).__name__}')
    ts = get_tombstone(rid)
    if ts and ts.get('delete_state') == 'deleted':
        return delete_response(ts)
    rec_dir = new_recording_dir(rid)
    if not rec_dir.exists():
        raise HTTPException(status_code=404, detail='not_found')
    try:
        final = delete_recording_to_trash(rid, recording_dir=rec_dir, delete_scope='local_server', request_source='api')
        return delete_response(final)
    except ValueError:
        raise HTTPException(status_code=400, detail='invalid_recording_id')
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail='not_found')
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'delete_failed: {type(exc).__name__}')


@app.get('/api/recordings/{recording_id}')
def get_recording(
    recording_id: str,
    authorization: Optional[str] = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
) -> dict:
    check_auth(authorization, x_upload_token)
    layout, rid = resolve_active_recording(recording_id)
    rec = public_new_record(rid) if layout == 'new' else public_legacy_record(rid)
    apply_short_display_titles([rec])
    if layout == 'new':
        rec['job'] = get_job(rid)
    return rec


@app.get('/api/recordings/{recording_id}/detail')
def get_recording_detail(
    recording_id: str,
    authorization: Optional[str] = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
) -> dict:
    check_auth(authorization, x_upload_token)
    layout, rid = resolve_active_recording(recording_id)
    if layout == 'new':
        rec = public_new_record(rid)
        apply_short_display_titles([rec])
        for kind in ['transcript', 'summary', 'analysis', 'result']:
            try:
                rec[kind] = read_new_text_file(rid, kind)
            except HTTPException:
                rec[kind] = ''
        d = new_recording_dir(rid)
        if two_stage_display_version(d) == 'FAST' and (d / 'fast' / 'snapshot' / 'segments_fast.json').exists():
            p = d / 'fast' / 'snapshot' / 'segments_fast.json'
        else:
            p = d / 'segments_clean.json'
        if not p.exists():
            p = d / 'segments_raw.json'
        rec['segments'] = read_json(p).get('segments', []) if p.exists() else []
        rec['job'] = get_job(rid)
        return rec
    rec = public_legacy_record(rid)
    rec['keywords'] = legacy_keywords_for(rid)
    apply_short_display_titles([rec])
    for kind in ['transcript', 'summary', 'analysis']:
        try:
            rec[kind] = read_legacy_text_file(rid, kind)
        except HTTPException:
            rec[kind] = ''
    seg_path = BASE_DIR / f'{rid}_segments.json'
    rec['segments'] = json.loads(seg_path.read_text(encoding='utf-8')).get('segments', []) if seg_path.exists() else []
    return rec


@app.get('/api/recordings/{recording_id}/segments')
def get_segments(
    recording_id: str,
    authorization: Optional[str] = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
) -> dict:
    check_auth(authorization, x_upload_token)
    layout, rid = resolve_active_recording(recording_id)
    if layout == 'new':
        d = new_recording_dir(rid)
        if two_stage_display_version(d) == 'FAST' and (d / 'fast' / 'snapshot' / 'segments_fast.json').exists():
            p = d / 'fast' / 'snapshot' / 'segments_fast.json'
        else:
            p = d / 'segments_clean.json'
        if not p.exists():
            p = d / 'segments_raw.json'
        if not p.exists():
            raise HTTPException(status_code=404, detail='not found')
        return read_json(p)
    p = BASE_DIR / f'{rid}_segments.json'
    if not p.exists():
        raise HTTPException(status_code=404, detail='not found')
    return json.loads(p.read_text(encoding='utf-8'))


@app.get('/api/recordings/{recording_id}/text/{kind}')
def get_text(
    recording_id: str,
    kind: str,
    authorization: Optional[str] = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
):
    check_auth(authorization, x_upload_token)
    layout, rid = resolve_active_recording(recording_id)
    return PlainTextResponse(read_new_text_file(rid, kind) if layout == 'new' else read_legacy_text_file(rid, kind))


@app.get('/api/recordings/{recording_id}/status')
def get_recording_status(
    recording_id: str,
    authorization: str | None = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
) -> dict:
    check_auth(authorization, x_upload_token)
    layout, rid = resolve_active_recording(recording_id)
    if layout == 'new':
        d = new_recording_dir(rid)
        p = d / 'status.json'
        if not p.exists():
            raise HTTPException(status_code=404, detail='not found')
        return read_json(p)
    p = BASE_DIR / f'{rid}_status.json'
    if not p.exists():
        raise HTTPException(status_code=404, detail='not found')
    return json.loads(p.read_text(encoding='utf-8'))


@app.get('/api/recordings/{recording_id}/download/{kind}')
def download_file(
    recording_id: str,
    kind: str,
    authorization: Optional[str] = Header(None),
    x_upload_token: Optional[str] = Header(None, alias='X-Upload-Token'),
):
    check_auth(authorization, x_upload_token)
    layout, rid = resolve_active_recording(recording_id)
    if layout == 'new':
        d = new_recording_dir(rid)
        if kind == 'audio':
            meta = read_json(d / 'metadata.json')
            p = d / (meta.get('audio_file') or 'audio.m4a')
            if not p.exists() and meta.get('cloud_file_id'):
                p = download_cloud_audio(d)
        else:
            candidates = list(d.glob(f'{kind}.*')) + [d / kind]
            p = next((x for x in candidates if x.exists()), None)
        if not p or not p.exists():
            raise HTTPException(status_code=404, detail='not found')
        return FileResponse(p, filename=p.name)
    candidates = []
    if kind == 'audio':
        candidates = list(BASE_DIR.glob(f'{rid}_original.*'))
    else:
        candidates = list(BASE_DIR.glob(f'{rid}_{kind}.*'))
    if not candidates:
        raise HTTPException(status_code=404, detail='not found')
    return FileResponse(candidates[0], filename=candidates[0].name)


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=int(os.environ.get('PORT', '8793')))

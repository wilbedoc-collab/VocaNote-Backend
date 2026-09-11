#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from vocanote_queue import DB_PATH, connect, init_db, now_iso

BASE_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱').resolve()
RECORDINGS_DIR = BASE_DIR / 'recordings'
TRASH_DIR = RECORDINGS_DIR / '.trash'

DELETE_STATES = {'delete_requested', 'deleting', 'deleted', 'delete_failed'}
ACTIVE_DELETE_STATES = DELETE_STATES
DELETE_SCOPES = {'local_server', 'local_only', 'local_server_cloud'}


class RecordingDeleted(RuntimeError):
    pass


def init_tombstone_db(db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    with connect(db_path) as conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS deleted_recordings (
            recording_id TEXT PRIMARY KEY,
            delete_state TEXT NOT NULL,
            delete_scope TEXT NOT NULL,
            requested_at TEXT NOT NULL,
            deleted_at TEXT,
            reason TEXT,
            original_path TEXT,
            trash_path TEXT,
            request_source TEXT,
            last_error TEXT,
            updated_at TEXT NOT NULL
        )
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_deleted_state ON deleted_recordings(delete_state)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_deleted_updated ON deleted_recordings(updated_at)')


def validate_recording_id(recording_id: str) -> str:
    rid = (recording_id or '').strip()
    try:
        uuid.UUID(rid)
    except Exception as exc:
        raise ValueError(f'malformed recording_id: {recording_id!r}') from exc
    return rid


def get_tombstone(recording_id: str, db_path: Path = DB_PATH) -> dict[str, Any] | None:
    init_tombstone_db(db_path)
    rid = (recording_id or '').strip()
    with connect(db_path) as conn:
        row = conn.execute('SELECT * FROM deleted_recordings WHERE recording_id=?', (rid,)).fetchone()
        return dict(row) if row else None


def is_deleted(recording_id: str, db_path: Path = DB_PATH) -> bool:
    ts = get_tombstone(recording_id, db_path=db_path)
    return bool(ts and ts.get('delete_state') in ACTIVE_DELETE_STATES)


def check_deleted(recording_id: str, db_path: Path = DB_PATH) -> None:
    ts = get_tombstone(recording_id, db_path=db_path)
    if ts and ts.get('delete_state') in ACTIVE_DELETE_STATES:
        raise RecordingDeleted(f"recording_deleted recording_id={recording_id} state={ts.get('delete_state')}")


def _assert_recording_dir_active(rid: str, recording_dir: Path | None) -> None:
    if recording_dir is not None:
        rd = recording_dir.resolve()
        if TRASH_DIR in rd.parents or rd == TRASH_DIR:
            raise RecordingDeleted(f'recording_in_trash recording_id={rid} path={rd}')
        if not rd.exists():
            raise FileNotFoundError(f'active recording dir missing: {rd}')
        meta = rd / 'metadata.json'
        if not meta.exists():
            raise FileNotFoundError(f'active recording metadata missing: {meta}')
        try:
            data = json.loads(meta.read_text(encoding='utf-8'))
            if str(data.get('recording_id') or data.get('id') or '') != rid:
                raise RuntimeError(f'metadata recording_id mismatch: {rid} != {data.get("recording_id") or data.get("id")}')
        except json.JSONDecodeError:
            raise RuntimeError(f'metadata json invalid: {meta}')


def assert_recording_active(recording_id: str, recording_dir: Path | None = None, db_path: Path = DB_PATH) -> None:
    rid = validate_recording_id(recording_id)
    check_deleted(rid, db_path=db_path)
    _assert_recording_dir_active(rid, recording_dir)


def create_tombstone(
    recording_id: str,
    *,
    delete_state: str = 'delete_requested',
    delete_scope: str = 'local_server',
    reason: str | None = None,
    original_path: str | None = None,
    trash_path: str | None = None,
    request_source: str = 'server',
    last_error: str | None = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    rid = validate_recording_id(recording_id)
    if delete_state not in DELETE_STATES:
        raise ValueError(f'bad delete_state: {delete_state}')
    if delete_scope not in DELETE_SCOPES:
        raise ValueError(f'bad delete_scope: {delete_scope}')
    init_tombstone_db(db_path)
    ts = now_iso()
    deleted_at = ts if delete_state == 'deleted' else None
    with connect(db_path) as conn:
        conn.execute('''
        INSERT INTO deleted_recordings(recording_id, delete_state, delete_scope, requested_at, deleted_at, reason, original_path, trash_path, request_source, last_error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(recording_id) DO UPDATE SET
            delete_state=excluded.delete_state,
            delete_scope=excluded.delete_scope,
            deleted_at=COALESCE(excluded.deleted_at, deleted_recordings.deleted_at),
            reason=COALESCE(excluded.reason, deleted_recordings.reason),
            original_path=COALESCE(excluded.original_path, deleted_recordings.original_path),
            trash_path=COALESCE(excluded.trash_path, deleted_recordings.trash_path),
            request_source=COALESCE(excluded.request_source, deleted_recordings.request_source),
            last_error=excluded.last_error,
            updated_at=excluded.updated_at
        ''', (rid, delete_state, delete_scope, ts, deleted_at, reason, original_path, trash_path, request_source, last_error, ts))
    return get_tombstone(rid, db_path=db_path) or {}


def update_tombstone_state(recording_id: str, delete_state: str, *, trash_path: str | None = None, last_error: str | None = None, db_path: Path = DB_PATH) -> dict[str, Any]:
    rid = validate_recording_id(recording_id)
    if delete_state not in DELETE_STATES:
        raise ValueError(f'bad delete_state: {delete_state}')
    init_tombstone_db(db_path)
    ts = now_iso()
    deleted_at = ts if delete_state == 'deleted' else None
    with connect(db_path) as conn:
        conn.execute('''
            UPDATE deleted_recordings
            SET delete_state=?, deleted_at=COALESCE(?, deleted_at), trash_path=COALESCE(?, trash_path), last_error=?, updated_at=?
            WHERE recording_id=?
        ''', (delete_state, deleted_at, trash_path, last_error, ts, rid))
    return get_tombstone(rid, db_path=db_path) or {}


def guarded_atomic_write_text(
    *,
    recording_id: str,
    recording_dir: Path,
    target_path: Path,
    content: str,
    assert_claim=None,
    create_parent: bool = False,
) -> None:
    rid = validate_recording_id(recording_id)
    rd = recording_dir.resolve()
    target = target_path.resolve()
    claim_db_path = Path(getattr(assert_claim, 'db_path', DB_PATH)) if assert_claim else DB_PATH
    assert_recording_active(rid, rd, db_path=claim_db_path)
    if assert_claim:
        assert_claim()
    if not target.is_relative_to(rd):
        raise RuntimeError(f'target outside recording dir: {target}')
    if not target.parent.exists():
        if create_parent:
            assert_recording_active(rid, rd, db_path=claim_db_path)
            target.parent.mkdir(parents=True, exist_ok=True)
        else:
            raise FileNotFoundError(f'parent missing for guarded write: {target.parent}')
    tmp = target.with_name(f'.{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    publish_lock = getattr(assert_claim, 'publish_lock', None) if assert_claim else None
    backup = target.with_name(f'.{target.name}.{os.getpid()}.{uuid.uuid4().hex}.bak')
    published = False
    try:
        with (publish_lock() if publish_lock else nullcontext()) as publish_context:
            if publish_context is not None:
                tombstone = publish_context.execute(
                    'SELECT delete_state FROM deleted_recordings WHERE recording_id=?', (rid,)
                ).fetchone()
                if tombstone and tombstone['delete_state'] in ACTIVE_DELETE_STATES:
                    raise RecordingDeleted(f"recording_deleted recording_id={rid} state={tombstone['delete_state']}")
                _assert_recording_dir_active(rid, rd)
            else:
                assert_recording_active(rid, rd, db_path=claim_db_path)
            # A publish lock is responsible for checking claim/generation authority
            # on the same transaction that excludes generation switches. Calling
            # the standalone assertion here would open a second SQLite connection
            # while BEGIN IMMEDIATE is held and self-deadlock.
            if assert_claim and not publish_lock:
                assert_claim()
            register_publish = getattr(assert_claim, 'register_publish', None) if assert_claim else None
            if register_publish:
                register_publish(target, publish_context)
            if target.exists():
                os.link(target, backup)
            os.replace(tmp, target)
            published = True
        backup.unlink(missing_ok=True)
    except Exception:
        if published:
            if backup.exists():
                os.replace(backup, target)
            else:
                target.unlink(missing_ok=True)
        tmp.unlink(missing_ok=True)
        backup.unlink(missing_ok=True)
        raise
    if not target.exists() or target.stat().st_size < 0:
        raise RuntimeError(f'guarded write failed: {target}')


def guarded_atomic_write_json(**kwargs: Any) -> None:
    payload = kwargs.pop('payload')
    guarded_atomic_write_text(content=json.dumps(payload, ensure_ascii=False, indent=2), **kwargs)


TRASH_RETENTION_DAYS = None
PURGE_MANUAL_ONLY = True

def trash_destination(recording_id: str, *, deleted_at: str | None = None) -> Path:
    rid = validate_recording_id(recording_id)
    safe_ts = (deleted_at or now_iso()).replace(':', '').replace('+', 'Z').replace('-', '').replace('T', '_')
    return TRASH_DIR / f'{rid}_{safe_ts}'


def cancel_recording_jobs(recording_id: str, db_path: Path = DB_PATH) -> list[dict[str, Any]]:
    rid = validate_recording_id(recording_id)
    init_tombstone_db(db_path)
    ts = now_iso()
    cancelled: list[dict[str, Any]] = []
    with connect(db_path) as conn:
        rows = conn.execute('SELECT * FROM recording_jobs WHERE recording_id=? AND status != ?', (rid, 'cancelled')).fetchall()
        for row in rows:
            conn.execute("""
                UPDATE recording_jobs
                SET status='cancelled', step='deleted', claimed_at=NULL, claimed_by=NULL, claim_token=NULL,
                    heartbeat_at=NULL, lease_expires_at=NULL, error_code='deleted', error_message='recording deleted', updated_at=?
                WHERE job_id=?
            """, (ts, row['job_id']))
            conn.execute("""
                INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
                VALUES (?, ?, NULL, 'cancelled_by_delete', 'cancelled', 'deleted', ?, 'error_code=deleted')
            """, (row['job_id'], rid, ts))
            cancelled.append(dict(row))
    return cancelled


def active_recording_dir_for_delete(recording_id: str, recording_dir: Path | None = None) -> Path:
    rid = validate_recording_id(recording_id)
    rd = (recording_dir or (RECORDINGS_DIR / rid)).resolve()
    assert_recording_active(rid, rd)
    return rd


def delete_recording_to_trash(recording_id: str, *, recording_dir: Path | None = None, delete_scope: str = 'local_server', request_source: str = 'api') -> dict[str, Any]:
    """Idempotently tombstone, cancel jobs, and atomically move an active recording dir to .trash."""
    rid = validate_recording_id(recording_id)
    ts = get_tombstone(rid)
    if ts and ts.get('delete_state') == 'deleted':
        return ts
    original_path = str((recording_dir or (RECORDINGS_DIR / rid)).resolve())
    try:
        rd = active_recording_dir_for_delete(rid, recording_dir=recording_dir)
        create_tombstone(rid, delete_state='delete_requested', delete_scope=delete_scope, reason='api delete requested', original_path=str(rd), request_source=request_source)
        cancel_recording_jobs(rid)
        update_tombstone_state(rid, 'deleting')
        TRASH_DIR.mkdir(parents=True, exist_ok=True)
        dest = trash_destination(rid)
        while dest.exists():
            dest = dest.with_name(dest.name + '_' + uuid.uuid4().hex[:6])
        os.replace(rd, dest)
        final = update_tombstone_state(rid, 'deleted', trash_path=str(dest))
        return final
    except Exception as exc:
        if get_tombstone(rid):
            create_tombstone(rid, delete_state='delete_failed', delete_scope=delete_scope, reason='api delete failed', original_path=original_path, request_source=request_source, last_error=repr(exc))
        raise

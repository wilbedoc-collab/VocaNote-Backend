#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import socket
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

SERVER_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
DB_PATH = SERVER_DIR / 'jobs.sqlite3'

JOB_STATUSES = {
    'queued', 'claimed',
    'stt_running', 'stt_done',
    'correction_running', 'correction_done',
    'semantic_running', 'reduce_running', 'validating', 'rendering',
    'completed', 'retry_wait', 'failed', 'cancelled',
}
ACTIVE_LEASE_STATUSES = {
    'claimed', 'stt_running', 'correction_running', 'semantic_running', 'reduce_running', 'validating', 'rendering'
}


class OwnershipLost(RuntimeError):
    """Raised when a worker tries to mutate a job it no longer owns."""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def future_iso(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec='seconds')


@contextmanager
def connect(db_path: Path = DB_PATH):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA foreign_keys=ON')
        yield conn
    finally:
        conn.close()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = {row['name'] for row in conn.execute(f'PRAGMA table_info({table})')}
    if column not in cols:
        conn.execute(f'ALTER TABLE {table} ADD COLUMN {ddl}')


def init_db(db_path: Path = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS recording_jobs (
            job_id TEXT PRIMARY KEY,
            recording_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            step TEXT,
            audio_path TEXT NOT NULL,
            metadata_path TEXT NOT NULL,
            output_dir TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            claimed_at TEXT,
            claimed_by TEXT,
            error_code TEXT,
            error_message TEXT,
            result_json_path TEXT
        )
        ''')
        _ensure_column(conn, 'recording_jobs', 'heartbeat_at', 'heartbeat_at TEXT')
        _ensure_column(conn, 'recording_jobs', 'lease_expires_at', 'lease_expires_at TEXT')
        _ensure_column(conn, 'recording_jobs', 'claim_token', 'claim_token TEXT')
        _ensure_column(conn, 'recording_jobs', 'generation', 'generation INTEGER NOT NULL DEFAULT 1')
        _ensure_column(conn, 'recording_jobs', 'cancel_requested', 'cancel_requested INTEGER NOT NULL DEFAULT 0')
        _ensure_column(conn, 'recording_jobs', 'process_pid', 'process_pid INTEGER')
        _ensure_column(conn, 'recording_jobs', 'process_group_id', 'process_group_id INTEGER')
        _ensure_column(conn, 'recording_jobs', 'process_stage', 'process_stage TEXT')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON recording_jobs(status, created_at)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_recording_id ON recording_jobs(recording_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_lease ON recording_jobs(status, lease_expires_at)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_jobs_owner ON recording_jobs(job_id, claimed_by, claim_token)')
        conn.execute('''
        CREATE TABLE IF NOT EXISTS job_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            recording_id TEXT,
            worker_id TEXT,
            event TEXT NOT NULL,
            status TEXT,
            step TEXT,
            timestamp TEXT NOT NULL,
            details TEXT
        )
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_job_events_job_time ON job_events(job_id, timestamp)')
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
    # Phase88A is additive: schema foundation only, no canonical switch.
    from vocanote_safe_edit import SafeEditStore
    SafeEditStore(db_path, foundation_enabled=True, destructive_enabled=False).init_schema()


def add_job_event(*, job_id: str | None, recording_id: str | None = None, worker_id: str | None = None,
                  event: str, status: str | None = None, step: str | None = None,
                  details: str | dict[str, Any] | None = None, db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    if isinstance(details, dict):
        import json
        details_text = json.dumps(details, ensure_ascii=False, sort_keys=True)
    else:
        details_text = details
    with connect(db_path) as conn:
        conn.execute('''
            INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (job_id, recording_id, worker_id, event, status, step, now_iso(), details_text))


def _owner_clause(worker_id: str | None, claim_token: str | None) -> tuple[str, list[Any]]:
    if worker_id and claim_token:
        return ' AND claimed_by=? AND claim_token=?', [worker_id, claim_token]
    return '', []


def _raise_if_unowned(cur: sqlite3.Cursor, job_id: str, operation: str) -> None:
    if cur.rowcount != 1:
        raise OwnershipLost(f'ownership_lost operation={operation} job_id={job_id}')


def assert_owner(job_id: str, *, worker_id: str, claim_token: str, db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    with connect(db_path) as conn:
        row = conn.execute('''
            SELECT j.status FROM recording_jobs j
            WHERE j.job_id=? AND j.claimed_by=? AND j.claim_token=?
              AND COALESCE(j.cancel_requested,0)=0
              AND j.generation=COALESCE(
                  (SELECT r.current_generation FROM safe_edit_recordings r WHERE r.recording_id=j.recording_id),
                  j.generation
              )
        ''', (job_id, worker_id, claim_token)).fetchone()
        if row is None:
            raise OwnershipLost(f'ownership_lost operation=assert_owner job_id={job_id}')


def enqueue_job(*, job_id: str, recording_id: str, audio_path: str, metadata_path: str, output_dir: str, max_attempts: int = 3, generation: int = 1, catalog_canonical: bool = False, db_path: Path = DB_PATH) -> None:
    init_db(db_path)
    ts = now_iso()
    with connect(db_path) as conn:
        conn.execute('BEGIN IMMEDIATE')
        if catalog_canonical:
            canonical = str(Path(audio_path).resolve())
            file_row = conn.execute('SELECT file_id FROM safe_edit_files WHERE path=?', (canonical,)).fetchone()
            file_id = file_row['file_id'] if file_row else uuid.uuid4().hex
            if not file_row:
                conn.execute('INSERT INTO safe_edit_files(file_id,path,created_at) VALUES(?,?,?)', (file_id, canonical, ts))
            conn.execute('''INSERT INTO safe_edit_recordings(
                recording_id,current_generation,canonical_file_id,state,visible,created_at,updated_at
            ) VALUES(?,?,?,'PROCESSING',1,?,?)
            ON CONFLICT(recording_id) DO NOTHING''', (recording_id, generation, file_id, ts, ts))
            conn.execute('''INSERT OR IGNORE INTO safe_edit_generations(
                recording_id,generation,audio_file_id,state,superseded,created_at
            ) VALUES(?,?,?,'PROCESSING',0,?)''', (recording_id, generation, file_id, ts))
            conn.execute('''INSERT OR IGNORE INTO safe_edit_file_references(
                reference_id,file_id,recording_id,generation,role,active,created_at
            ) VALUES(?,?,?,?, 'CANONICAL_AUDIO',1,?)''', (uuid.uuid4().hex, file_id, recording_id, generation, ts))
        conn.execute('''
        INSERT INTO recording_jobs (
            job_id, recording_id, status, step, audio_path, metadata_path, output_dir,
            attempts, max_attempts, created_at, updated_at, generation, cancel_requested
        ) VALUES (?, ?, 'queued', 'queued', ?, ?, ?, 0, ?, ?, ?, ?, 0)
        ON CONFLICT(recording_id) DO NOTHING
        ''', (job_id, recording_id, audio_path, metadata_path, output_dir, max_attempts, ts, ts, generation))
        conn.execute('COMMIT')
    add_job_event(job_id=job_id, recording_id=recording_id, event='enqueue', status='queued', step='queued', db_path=db_path)


def recover_stale_jobs(*, worker_id: str | None = None, now: str | None = None, db_path: Path = DB_PATH) -> list[dict[str, Any]]:
    init_db(db_path)
    ts = now or now_iso()
    recovered: list[dict[str, Any]] = []
    with connect(db_path) as conn:
        conn.execute('BEGIN IMMEDIATE')
        conn.execute(f'''
            UPDATE recording_jobs
            SET status='cancelled', step='stale_generation', claimed_at=NULL, claimed_by=NULL,
                claim_token=NULL, heartbeat_at=NULL, lease_expires_at=NULL, updated_at=?
            WHERE status IN ({','.join('?' for _ in ACTIVE_LEASE_STATUSES)})
              AND (COALESCE(cancel_requested,0)=1 OR generation<>COALESCE(
                  (SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation
              ))
        ''', (ts, *sorted(ACTIVE_LEASE_STATUSES)))
        rows = conn.execute(f'''
            SELECT * FROM recording_jobs
            WHERE status IN ({','.join('?' for _ in ACTIVE_LEASE_STATUSES)})
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at < ?
              AND COALESCE(cancel_requested,0)=0
              AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)
            ORDER BY lease_expires_at ASC
        ''', (*sorted(ACTIVE_LEASE_STATUSES), ts)).fetchall()
        for row in rows:
            next_attempts = int(row['attempts'] or 0) + 1
            final = next_attempts >= int(row['max_attempts'] or 3)
            new_status = 'failed' if final else 'retry_wait'
            new_step = 'stale_failed' if final else 'stale_recovered'
            err_code = 'stale_lease_expired'
            err_msg = f"lease expired at {row['lease_expires_at']} while status={row['status']} owner={row['claimed_by']} token={row['claim_token']}"
            conn.execute('''
                UPDATE recording_jobs
                SET status=?, step=?, attempts=?, claimed_at=NULL, claimed_by=NULL, claim_token=NULL,
                    heartbeat_at=NULL, lease_expires_at=NULL, error_code=?, error_message=?, updated_at=?
                WHERE job_id=?
            ''', (new_status, new_step, next_attempts, err_code, err_msg, ts, row['job_id']))
            conn.execute('''
                INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
                VALUES (?, ?, ?, 'stale_recovery', ?, ?, ?, ?)
            ''', (row['job_id'], row['recording_id'], worker_id, new_status, new_step, ts, err_msg))
            recovered.append({**dict(row), 'new_status': new_status, 'new_step': new_step, 'attempts_after': next_attempts})
        conn.execute('COMMIT')
    return recovered


def claim_next(worker_id: Optional[str] = None, db_path: Path = DB_PATH, lease_seconds: int = 90) -> Optional[dict[str, Any]]:
    init_db(db_path)
    worker_id = worker_id or f'{socket.gethostname()}:{os.getpid()}'
    token = uuid.uuid4().hex
    ts = now_iso()
    expires = future_iso(lease_seconds)
    with connect(db_path) as conn:
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute('''
            SELECT job_id FROM recording_jobs
            WHERE status IN ('queued', 'retry_wait')
              AND attempts < max_attempts
              AND COALESCE(cancel_requested,0)=0
              AND generation=COALESCE(
                  (SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),
                  generation
              )
              AND recording_id NOT IN (SELECT recording_id FROM deleted_recordings)
            ORDER BY created_at ASC
            LIMIT 1
        ''').fetchone()
        if row is None:
            conn.execute('COMMIT')
            return None
        conn.execute('''
            UPDATE recording_jobs
            SET status='claimed', step='claimed', claimed_at=?, claimed_by=?, claim_token=?, heartbeat_at=?, lease_expires_at=?, updated_at=?
            WHERE job_id=? AND status IN ('queued', 'retry_wait')
              AND COALESCE(cancel_requested,0)=0
              AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)
        ''', (ts, worker_id, token, ts, expires, ts, row['job_id']))
        claimed = conn.execute('SELECT * FROM recording_jobs WHERE job_id=?', (row['job_id'],)).fetchone()
        if claimed:
            conn.execute('''
                INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
                VALUES (?, ?, ?, 'claim', 'claimed', 'claimed', ?, ?)
            ''', (claimed['job_id'], claimed['recording_id'], worker_id, ts, f'claim_token={token}; lease_expires_at={expires}'))
        conn.execute('COMMIT')
        return dict(claimed) if claimed else None


def heartbeat_job(job_id: str, *, worker_id: str, claim_token: str, lease_seconds: int = 90, db_path: Path = DB_PATH) -> bool:
    ts = now_iso()
    expires = future_iso(lease_seconds)
    with connect(db_path) as conn:
        cur = conn.execute(f'''
            UPDATE recording_jobs
            SET heartbeat_at=?, lease_expires_at=?, updated_at=?
            WHERE job_id=? AND claimed_by=? AND claim_token=? AND status IN ({','.join('?' for _ in ACTIVE_LEASE_STATUSES)})
              AND COALESCE(cancel_requested,0)=0
              AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)
        ''', (ts, expires, ts, job_id, worker_id, claim_token, *sorted(ACTIVE_LEASE_STATUSES)))
        ok = cur.rowcount == 1
        if ok:
            row = conn.execute('SELECT recording_id,status,step FROM recording_jobs WHERE job_id=?', (job_id,)).fetchone()
            conn.execute('''
                INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
                VALUES (?, ?, ?, 'heartbeat', ?, ?, ?, ?)
            ''', (job_id, row['recording_id'] if row else None, worker_id, row['status'] if row else None, row['step'] if row else None, ts, f'claim_token={claim_token}; lease_expires_at={expires}'))
    return ok


def release_claim(job_id: str, *, previous_status: str = 'queued', previous_step: str = 'queued', decrement_attempt: bool = False, db_path: Path = DB_PATH) -> None:
    with connect(db_path) as conn:
        if decrement_attempt:
            conn.execute(
                "UPDATE recording_jobs SET status=?, step=?, claimed_at=NULL, claimed_by=NULL, claim_token=NULL, heartbeat_at=NULL, lease_expires_at=NULL, attempts=MAX(attempts-1, 0), updated_at=? WHERE job_id=?",
                (previous_status, previous_step, now_iso(), job_id),
            )
        else:
            conn.execute(
                "UPDATE recording_jobs SET status=?, step=?, claimed_at=NULL, claimed_by=NULL, claim_token=NULL, heartbeat_at=NULL, lease_expires_at=NULL, updated_at=? WHERE job_id=?",
                (previous_status, previous_step, now_iso(), job_id),
            )


def update_job(job_id: str, *, status: Optional[str] = None, step: Optional[str] = None, error_code: Optional[str] = None, error_message: Optional[str] = None, result_json_path: Optional[str] = None, worker_id: str | None = None, claim_token: str | None = None, db_path: Path = DB_PATH) -> None:
    if status is not None and status not in JOB_STATUSES:
        raise ValueError(f'bad job status: {status}')
    fields = ['updated_at=?']
    values: list[Any] = [now_iso()]
    for key, value in [('status', status), ('step', step), ('error_code', error_code), ('error_message', error_message), ('result_json_path', result_json_path)]:
        if value is not None:
            fields.append(f'{key}=?')
            values.append(value)
    clause, owner_values = _owner_clause(worker_id, claim_token)
    values.extend([job_id, *owner_values])
    with connect(db_path) as conn:
        fence = " AND COALESCE(cancel_requested,0)=0 AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)" if clause else ""
        cur = conn.execute(f"UPDATE recording_jobs SET {', '.join(fields)} WHERE job_id=?{clause}{fence}", values)
        if clause:
            _raise_if_unowned(cur, job_id, 'update_job')
        row = conn.execute('SELECT recording_id,status,step FROM recording_jobs WHERE job_id=?', (job_id,)).fetchone()
        if row and (status is not None or step is not None):
            conn.execute('''
                INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
                VALUES (?, ?, ?, 'status_update', ?, ?, ?, ?)
            ''', (job_id, row['recording_id'], worker_id, row['status'], row['step'], now_iso(), f'claim_token={claim_token}' if claim_token else None))


def increment_attempt_and_set_failure(job_id: str, *, final: bool, step: str, error_code: str, error_message: str, worker_id: str | None = None, claim_token: str | None = None, db_path: Path = DB_PATH) -> str:
    status = 'failed' if final else 'retry_wait'
    clause, owner_values = _owner_clause(worker_id, claim_token)
    with connect(db_path) as conn:
        fence = " AND COALESCE(cancel_requested,0)=0 AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)" if clause else ""
        cur = conn.execute(f'''
            UPDATE recording_jobs
            SET status=?, step=?, attempts=attempts+1, claimed_at=NULL, claimed_by=NULL, claim_token=NULL,
                heartbeat_at=NULL, lease_expires_at=NULL, error_code=?, error_message=?, updated_at=?
            WHERE job_id=?{clause}{fence}
        ''', (status, step, error_code, error_message, now_iso(), job_id, *owner_values))
        if clause:
            _raise_if_unowned(cur, job_id, 'increment_attempt_and_set_failure')
        row = conn.execute('SELECT recording_id,attempts FROM recording_jobs WHERE job_id=?', (job_id,)).fetchone()
        conn.execute('''
            INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
            VALUES (?, ?, ?, 'failure', ?, ?, ?, ?)
        ''', (job_id, row['recording_id'] if row else None, worker_id, status, step, now_iso(), f'{error_code}; attempts={row["attempts"] if row else None}; claim_token={claim_token}'))
    return status


def clear_job_error(job_id: str, db_path: Path = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("UPDATE recording_jobs SET error_code=NULL, error_message=NULL, updated_at=? WHERE job_id=?", (now_iso(), job_id))


def mark_completed(job_id: str, *, result_json_path: str, worker_id: str | None = None, claim_token: str | None = None, db_path: Path = DB_PATH) -> None:
    clause, owner_values = _owner_clause(worker_id, claim_token)
    with connect(db_path) as conn:
        fence = " AND COALESCE(cancel_requested,0)=0 AND generation=COALESCE((SELECT current_generation FROM safe_edit_recordings WHERE recording_id=recording_jobs.recording_id),generation)" if clause else ""
        cur = conn.execute(f'''
            UPDATE recording_jobs
            SET status='completed', step='completed', result_json_path=?, error_code=NULL, error_message=NULL,
                heartbeat_at=NULL, lease_expires_at=NULL, updated_at=?
            WHERE job_id=?{clause}{fence}
        ''', (result_json_path, now_iso(), job_id, *owner_values))
        if clause:
            _raise_if_unowned(cur, job_id, 'mark_completed')
        row = conn.execute('SELECT recording_id FROM recording_jobs WHERE job_id=?', (job_id,)).fetchone()
        conn.execute('''
            INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
            VALUES (?, ?, ?, 'completed', 'completed', 'completed', ?, ?)
        ''', (job_id, row['recording_id'] if row else None, worker_id, now_iso(), f'{result_json_path}; claim_token={claim_token}'))


def get_job(recording_id: str, db_path: Path = DB_PATH) -> Optional[dict[str, Any]]:
    init_db(db_path)
    with connect(db_path) as conn:
        row = conn.execute('SELECT * FROM recording_jobs WHERE recording_id=? OR job_id=?', (recording_id, recording_id)).fetchone()
        return dict(row) if row else None


def list_jobs(limit: int = 50, db_path: Path = DB_PATH) -> list[dict[str, Any]]:
    init_db(db_path)
    with connect(db_path) as conn:
        rows = conn.execute('''
            SELECT * FROM recording_jobs
            WHERE recording_id NOT IN (SELECT recording_id FROM deleted_recordings)
            ORDER BY created_at DESC LIMIT ?
        ''', (limit,)).fetchall()
        return [dict(r) for r in rows]


def list_events(job_id: str, db_path: Path = DB_PATH) -> list[dict[str, Any]]:
    init_db(db_path)
    with connect(db_path) as conn:
        rows = conn.execute('SELECT * FROM job_events WHERE job_id=? ORDER BY event_id ASC', (job_id,)).fetchall()
        return [dict(r) for r in rows]


if __name__ == '__main__':
    init_db()
    print(DB_PATH)

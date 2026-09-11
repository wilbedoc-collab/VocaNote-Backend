#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""VocaNote Phase86 reconciliation/watchdog.

Idempotent server-side reconciliation with heartbeat/stale false-positive guards.
This module never recomputes an artifact that already passes the worker checkpoint.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from vocanote_safe_edit import SafeEditStore

from vocanote_queue import connect, init_db, add_job_event

BASE_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱').resolve()
SERVER_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
RECORDINGS_DIR = BASE_DIR / 'recordings'
TRASH_DIR = RECORDINGS_DIR / '.trash'
DB_PATH = SERVER_DIR / 'jobs.sqlite3'
ACTIVE_STATUSES = {'claimed','stt_running','correction_running','semantic_running','reduce_running','validating','rendering'}
TERMINAL_STATUSES = {'completed','cancelled','failed'}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding='utf-8'))


def atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f'.{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp, path)


def adb_state() -> dict[str, Any]:
    try:
        out = subprocess.run(['adb','devices'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10)
        devices=[]
        for line in out.stdout.splitlines()[1:]:
            parts=line.split()
            if len(parts)>=2 and parts[1]=='device':
                devices.append(parts[0])
        return {'checked': True, 'devices_output': out.stdout.strip(), 'connected_devices': devices, 'state': 'CONNECTED' if devices else 'PENDING_NO_DEVICE'}
    except Exception as exc:
        return {'checked': True, 'devices_output': '', 'connected_devices': [], 'state': 'PENDING_ADB_UNAVAILABLE', 'error': repr(exc)}


def ffprobe_ok(audio: Path) -> dict[str, Any]:
    if not audio.exists():
        return {'exists': False, 'playable': False, 'duration_sec': None, 'error': 'missing'}
    try:
        cp = subprocess.run(['ffprobe','-v','error','-show_entries','format=duration:stream=codec_type,codec_name','-of','json',str(audio)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
        if cp.returncode != 0:
            return {'exists': True, 'playable': False, 'duration_sec': None, 'error': cp.stderr.strip()[-500:]}
        data=json.loads(cp.stdout or '{}')
        dur=data.get('format',{}).get('duration')
        playable=any(s.get('codec_type')=='audio' for s in data.get('streams') or [])
        return {'exists': True, 'playable': bool(playable), 'duration_sec': float(dur) if dur not in (None,'','N/A') else None, 'error': None}
    except Exception as exc:
        return {'exists': True, 'playable': False, 'duration_sec': None, 'error': repr(exc)}


def get_job(rid: str) -> dict[str, Any] | None:
    with connect(DB_PATH) as conn:
        row = conn.execute('SELECT * FROM recording_jobs WHERE recording_id=?', (rid,)).fetchone()
        return dict(row) if row else None


def active_worker_alive(job: dict[str, Any]) -> bool:
    claimed_by = str(job.get('claimed_by') or '')
    # vocanote-worker:host:pid:startup
    parts = claimed_by.split(':')
    if len(parts) >= 3 and parts[2].isdigit():
        pid = parts[2]
        try:
            cp = subprocess.run(['ps','-p',pid,'-o','pid='], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=5)
            return cp.returncode == 0 and pid in cp.stdout
        except Exception:
            return False
    return False


def stale_decision(job: dict[str, Any], *, stale_grace_sec: int = 300) -> dict[str, Any]:
    status = job.get('status')
    if status not in ACTIVE_STATUSES:
        return {'active': False, 'stale': False, 'reason': 'not_active_status'}
    now = time.time()
    fmt_values = [job.get('heartbeat_at'), job.get('updated_at'), job.get('lease_expires_at')]
    newest = None
    for val in fmt_values:
        if not val: continue
        try:
            ts = datetime.fromisoformat(str(val).replace('Z','+00:00')).timestamp()
            newest = max(newest or ts, ts)
        except Exception:
            pass
    worker_alive = active_worker_alive(job)
    if worker_alive:
        return {'active': True, 'stale': False, 'reason': 'worker_process_alive'}
    if newest is not None and now - newest < stale_grace_sec:
        return {'active': True, 'stale': False, 'reason': f'within_stale_grace_{round(now-newest,1)}s'}
    return {'active': True, 'stale': True, 'reason': 'no_alive_worker_and_heartbeat_old'}


def artifact_stage(recording_dir: Path, rid: str) -> dict[str, Any]:
    meta = load_json(recording_dir/'metadata.json')
    audio = recording_dir / str(meta.get('audio_file') or 'audio.m4a')
    audio_probe = ffprobe_ok(audio)
    stt = (recording_dir/'stt_raw.json').exists() and (recording_dir/'segments_raw.json').exists() and (recording_dir/'transcript_raw.txt').exists()
    corrected = (recording_dir/'transcript_clean.json').exists() and (recording_dir/'segments_clean.json').exists() and (recording_dir/'transcript_clean.txt').exists()
    semantic = (recording_dir/'semantic_final.json').exists() and (recording_dir/'result.validated.json').exists()
    rendered = semantic and (recording_dir/'summary.md').exists() and (recording_dir/'analysis.md').exists()
    if rendered:
        last = 'final'; next_stage = None
    elif semantic:
        last = 'semantic'; next_stage = 'render'
    elif corrected:
        last = 'correction'; next_stage = 'semantic'
    elif stt:
        last = 'stt'; next_stage = 'correction'
    elif audio_probe.get('playable'):
        last = 'upload'; next_stage = 'stt'
    else:
        last = 'none'; next_stage = 'upload'
    return {'recording_id': rid, 'audio': str(audio), 'audio_probe': audio_probe, 'has_stt': stt, 'has_corrected': corrected, 'has_semantic': semantic, 'has_rendered': rendered, 'last_good_stage': last, 'next_stage': next_stage}


def ensure_job_for_resume(rid: str, *, dry_run: bool = False) -> dict[str, Any]:
    d = RECORDINGS_DIR / rid
    stage = artifact_stage(d, rid)
    job = get_job(rid)
    decision = stale_decision(job) if job else {'active': False, 'stale': False, 'reason': 'no_job'}
    if decision.get('active') and not decision.get('stale'):
        return {'recording_id': rid, 'action': 'skip_active', 'stage': stage, 'job': job, 'stale_decision': decision}
    if stage['next_stage'] is None:
        return {'recording_id': rid, 'action': 'already_final', 'stage': stage, 'job': job, 'stale_decision': decision}
    if stage['last_good_stage'] == 'none':
        return {'recording_id': rid, 'action': 'unrecoverable_no_audio', 'stage': stage, 'job': job, 'stale_decision': decision}
    if dry_run:
        return {'recording_id': rid, 'action': 'would_enqueue_resume', 'stage': stage, 'job': job, 'stale_decision': decision}
    meta = d / 'metadata.json'
    audio = Path(stage['audio'])
    ts = now_iso()
    with connect(DB_PATH) as conn:
        conn.execute('BEGIN IMMEDIATE')
        existing = conn.execute('SELECT * FROM recording_jobs WHERE recording_id=?', (rid,)).fetchone()
        if existing:
            ex = dict(existing)
            dec = stale_decision(ex)
            if dec.get('active') and not dec.get('stale'):
                conn.execute('COMMIT')
                return {'recording_id': rid, 'action': 'skip_active_race', 'stage': stage, 'job': ex, 'stale_decision': dec}
            conn.execute('''UPDATE recording_jobs SET status='retry_wait', step=?, attempts=MIN(attempts, max_attempts-1), claimed_at=NULL, claimed_by=NULL, claim_token=NULL, heartbeat_at=NULL, lease_expires_at=NULL, error_code=NULL, error_message=NULL, audio_path=?, metadata_path=?, output_dir=?, updated_at=? WHERE recording_id=?''', (stage['next_stage'], str(audio), str(meta), str(d), ts, rid))
            job_id = ex['job_id']
        else:
            job_id = f'vocanote_{rid}'
            conn.execute('''INSERT INTO recording_jobs(job_id,recording_id,status,step,audio_path,metadata_path,output_dir,attempts,max_attempts,created_at,updated_at) VALUES(?,?, 'retry_wait', ?, ?, ?, ?, 0, 3, ?, ?)''', (job_id, rid, stage['next_stage'], str(audio), str(meta), str(d), ts, ts))
        conn.execute('''INSERT INTO job_events(job_id,recording_id,worker_id,event,status,step,timestamp,details) VALUES(?,?,?, 'phase86_reconcile_resume', 'retry_wait', ?, ?, ?)''', (job_id, rid, 'phase86-reconciler', stage['next_stage'], ts, json.dumps({'last_good_stage': stage['last_good_stage'], 'next_stage': stage['next_stage'], 'stale_decision': decision}, ensure_ascii=False)))
        conn.execute('COMMIT')
    return {'recording_id': rid, 'action': 'enqueued_resume', 'stage': stage, 'job_id': job_id, 'stale_decision': decision}


def find_external_references(rid: str) -> list[dict[str, str]]:
    refs=[]
    target=str(RECORDINGS_DIR/rid)
    for table in ['recording_jobs','job_events','deleted_recordings']:
        with connect(DB_PATH) as conn:
            if table == 'job_events':
                rows = conn.execute("SELECT event_id,recording_id,details FROM job_events WHERE details LIKE ? AND COALESCE(recording_id,'')<>? LIMIT 20", (f'%{rid}%', rid)).fetchall()
                for r in rows: refs.append({'table':table, 'key':str(r['event_id']), 'recording_id':str(r['recording_id']), 'details':str(r['details'])[:300]})
            elif table == 'recording_jobs':
                rows = conn.execute("SELECT job_id,recording_id,audio_path,metadata_path,output_dir FROM recording_jobs WHERE (audio_path LIKE ? OR metadata_path LIKE ? OR output_dir LIKE ?) AND recording_id<>? LIMIT 20", (f'%{rid}%', f'%{rid}%', f'%{rid}%', rid)).fetchall()
                for r in rows: refs.append({'table':table, 'key':str(r['job_id']), 'recording_id':str(r['recording_id'])})
            else:
                rows = conn.execute("SELECT recording_id,original_path,trash_path FROM deleted_recordings WHERE (original_path LIKE ? OR trash_path LIKE ?) AND recording_id<>? LIMIT 20", (f'%{rid}%', f'%{rid}%', rid)).fetchall()
                for r in rows: refs.append({'table':table, 'key':str(r['recording_id']), 'recording_id':str(r['recording_id'])})
    return refs


def safe_delete_candidate(rid: str, *, dry_run: bool = False) -> dict[str, Any]:
    d = RECORDINGS_DIR / rid
    graph = {'recording_id': rid, 'dir_exists': d.exists(), 'files': sorted([str(p.relative_to(d)) for p in d.rglob('*') if p.is_file()]) if d.exists() else [], 'refs': find_external_references(rid), 'job': get_job(rid), 'stage': artifact_stage(d, rid) if d.exists() else None}
    if not d.exists():
        return {'recording_id': rid, 'deleted': False, 'skipped': 'dir_missing', 'graph': graph}
    if graph['refs']:
        return {'recording_id': rid, 'deleted': False, 'skipped': 'external_references_found', 'graph': graph}
    # only allow known safe patterns
    files=set(graph['files'])
    corrupt = 'audio.m4a' in files and not (graph['stage'] or {}).get('audio_probe',{}).get('playable')
    incomplete = files.issubset({'status.json','error.log'})
    if not (corrupt or incomplete):
        return {'recording_id': rid, 'deleted': False, 'skipped': 'not_safe_pattern', 'graph': graph}
    if dry_run:
        return {'recording_id': rid, 'deleted': False, 'would_delete': True, 'graph': graph}
    ts = now_iso()
    dest = TRASH_DIR / f'{rid}_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")}_phase86'
    TRASH_DIR.mkdir(parents=True, exist_ok=True)
    with connect(DB_PATH) as conn:
        conn.execute('BEGIN IMMEDIATE')
        # recheck no active non-stale job under lock
        job = conn.execute('SELECT * FROM recording_jobs WHERE recording_id=?', (rid,)).fetchone()
        if job:
            dec = stale_decision(dict(job))
            if dec.get('active') and not dec.get('stale'):
                conn.execute('ROLLBACK')
                return {'recording_id': rid, 'deleted': False, 'skipped': 'active_job', 'graph': graph, 'stale_decision': dec}
        conn.execute('''INSERT INTO deleted_recordings(recording_id,delete_state,delete_scope,requested_at,deleted_at,reason,original_path,trash_path,request_source,last_error,updated_at) VALUES(?, 'deleting', 'local_server', ?, NULL, 'phase86 safe delete', ?, NULL, 'phase86', NULL, ?) ON CONFLICT(recording_id) DO UPDATE SET delete_state='deleting', delete_scope='local_server', reason='phase86 safe delete', original_path=excluded.original_path, request_source='phase86', updated_at=excluded.updated_at''', (rid, ts, str(d), ts))
        conn.execute("UPDATE recording_jobs SET status='cancelled', step='phase86_safe_delete', claimed_at=NULL, claimed_by=NULL, claim_token=NULL, heartbeat_at=NULL, lease_expires_at=NULL, error_code='deleted', error_message='phase86 safe delete', updated_at=? WHERE recording_id=?", (ts, rid))
        conn.execute('COMMIT')
    try:
        os.replace(d, dest)
        with connect(DB_PATH) as conn:
            conn.execute("UPDATE deleted_recordings SET delete_state='deleted', deleted_at=?, trash_path=?, updated_at=? WHERE recording_id=?", (now_iso(), str(dest), now_iso(), rid))
            conn.execute('''INSERT INTO job_events(job_id,recording_id,worker_id,event,status,step,timestamp,details) VALUES(?,?,?, 'phase86_safe_deleted', 'cancelled', 'phase86_safe_delete', ?, ?)''', (f'vocanote_{rid}', rid, 'phase86-reconciler', now_iso(), str(dest)))
        return {'recording_id': rid, 'deleted': True, 'trash_path': str(dest), 'graph': graph}
    except Exception as exc:
        with connect(DB_PATH) as conn:
            conn.execute("UPDATE deleted_recordings SET delete_state='delete_failed', last_error=?, updated_at=? WHERE recording_id=?", (repr(exc), now_iso(), rid))
        raise


def candidate_recording_ids() -> list[str]:
    if not RECORDINGS_DIR.exists():
        return []
    ids = []
    for d in RECORDINGS_DIR.iterdir():
        if not d.is_dir() or d.name.startswith('.') or d.name == '.trash':
            continue
        if (d / 'metadata.json').exists():
            ids.append(d.name)
    return sorted(ids)


def scan_resume(*, dry_run: bool = False) -> list[dict[str, Any]]:
    results = []
    for rid in candidate_recording_ids():
        try:
            job = get_job(rid)
            if job and job.get('status') in TERMINAL_STATUSES:
                if job.get('status') != 'completed':
                    results.append({'recording_id': rid, 'action': 'skip_terminal', 'job_status': job.get('status'), 'job_step': job.get('step')})
                continue
            res = ensure_job_for_resume(rid, dry_run=dry_run)
            if res.get('action') not in {'already_final', 'skip_active'}:
                results.append(res)
        except Exception as exc:
            results.append({'recording_id': rid, 'action': 'scan_error', 'error': repr(exc)})
    return results


def phase88_purge_retry(*, dry_run: bool = False) -> dict[str, Any]:
    store = SafeEditStore(DB_PATH, foundation_enabled=True, destructive_enabled=False)
    store.init_schema()
    pending = [p for p in store.list_purge_intents() if p['server_status'] != 'SERVER_PURGED']
    if dry_run:
        return {'pending': len(pending), 'purged': 0, 'failed': 0, 'dry_run': True}
    result = store.purge_server(outstanding_only=True)
    orphan = store.cleanup_orphan_finalized_candidates()
    return {'pending_before': len(pending), **result, 'orphan_candidates': orphan, 'dry_run': False}


def main() -> None:
    ap=argparse.ArgumentParser()
    ap.add_argument('--adb-state', action='store_true')
    ap.add_argument('--resume', nargs='*')
    ap.add_argument('--safe-delete', nargs='*')
    ap.add_argument('--scan', action='store_true')
    ap.add_argument('--interval', type=int, default=0)
    ap.add_argument('--dry-run', action='store_true')
    args=ap.parse_args()
    init_db(DB_PATH)
    if args.interval and args.interval > 0:
        while True:
            out={'generated_at': now_iso(), 'dry_run': args.dry_run, 'interval': args.interval, 'scan': scan_resume(dry_run=args.dry_run), 'phase88_purge': phase88_purge_retry(dry_run=args.dry_run)}
            if out['scan'] or out['phase88_purge'].get('purged') or out['phase88_purge'].get('failed'):
                print(json.dumps(out, ensure_ascii=False), flush=True)
            time.sleep(args.interval)
    out={'generated_at': now_iso(), 'dry_run': args.dry_run}
    if args.adb_state:
        out['android']=adb_state()
    if args.resume is not None:
        out['resume']=[ensure_job_for_resume(rid, dry_run=args.dry_run) for rid in args.resume]
    if args.safe_delete is not None:
        out['safe_delete']=[safe_delete_candidate(rid, dry_run=args.dry_run) for rid in args.safe_delete]
    if args.scan:
        out['scan']=scan_resume(dry_run=args.dry_run)
        out['phase88_purge']=phase88_purge_retry(dry_run=args.dry_run)
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()

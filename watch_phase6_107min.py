#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from vocanote_queue import get_job, recover_stale_jobs, connect, now_iso

RID = '2fc6e321-989c-4188-9c75-14d1bfdf6eba'
SERVER_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server')
REC_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱/recordings') / RID
TIMEOUT_SECONDS = int(os.environ.get('PHASE6_WATCH_TIMEOUT_SECONDS', str(24 * 60 * 60)))
POLL_SECONDS = int(os.environ.get('PHASE6_WATCH_POLL_SECONDS', '60'))
STALE_SECONDS = int(os.environ.get('PHASE6_WATCH_STALE_SECONDS', '900'))
RETRYABLE = {'retry_wait', 'queued', 'failed'}


def load_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def active_worker_or_hermes() -> bool:
    try:
        out = subprocess.run(
            "ps aux | grep -E 'vocanote_worker|hermes --profile vocanoteworker|whisper' | grep -v grep || true",
            shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=10,
        ).stdout.strip()
        return bool(out)
    except Exception:
        return True


def file_info(rel: str):
    p = REC_DIR / rel
    return {'exists': p.exists(), 'bytes': p.stat().st_size if p.exists() else 0}


def semantic_counts():
    mp = REC_DIR / 'semantic_chunks' / 'manifest.json'
    if not mp.exists():
        return {'exists': False, 'chunk_count': 0, 'completed': 0, 'failed': 0, 'pending': 0, 'processing': 0}
    m = load_json(mp)
    rows = m.get('chunks') or []
    chunk_count = int(m.get('chunk_count') or len(rows))
    output_files = list((REC_DIR / 'semantic_chunks' / 'outputs').glob('*.semantic.json'))
    completed = sum(1 for r in rows if r.get('status') == 'completed')
    failed = sum(1 for r in rows if r.get('status') == 'failed')
    pending = sum(1 for r in rows if r.get('status') == 'pending')
    processing = sum(1 for r in rows if r.get('status') == 'processing')
    status_sum = completed + failed + pending + processing
    unknown = max(0, len(rows) - status_sum)
    sum_ok = status_sum == chunk_count and unknown == 0
    return {
        'exists': True,
        'chunk_count': chunk_count,
        'completed': completed,
        'failed': failed,
        'pending': pending,
        'processing': processing,
        'unknown': unknown,
        'status_sum': status_sum,
        'status_sum_ok': sum_ok,
        'output_files': len(output_files),
        'long_recording_chunked': chunk_count >= 2,
    }


def phase6_check():
    job = get_job(RID) or {}
    meta = load_json(REC_DIR / 'metadata.json') if (REC_DIR / 'metadata.json').exists() else {}
    duration = int(meta.get('duration_sec') or 0)
    sem = semantic_counts()
    required = {
        'audio.m4a': file_info('audio.m4a'),
        'segments_raw.json': file_info('segments_raw.json'),
        'segments_clean.json': file_info('segments_clean.json'),
        'transcript_clean.json': file_info('transcript_clean.json'),
        'semantic_chunks/manifest.json': file_info('semantic_chunks/manifest.json'),
        'semantic_final.json': file_info('semantic_final.json'),
        'result.validated.json': file_info('result.validated.json'),
        'summary.md': file_info('summary.md'),
        'analysis.md': file_info('analysis.md'),
    }
    pass_ok = (
        duration >= 3600 and
        job.get('status') == 'completed' and job.get('step') == 'completed' and
        required['audio.m4a']['bytes'] > 10000 and
        all(v['exists'] and v['bytes'] > 0 for k, v in required.items()) and
        sem['exists'] and sem['long_recording_chunked'] and sem['status_sum_ok'] and
        sem['chunk_count'] > 0 and sem['completed'] == sem['chunk_count'] and sem['output_files'] == sem['chunk_count'] and sem['failed'] == 0 and
        (REC_DIR / 'semantic_final.json').exists()
    )
    return {
        'ok': pass_ok,
        'recording_id': RID,
        'checked_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'job': {k: job.get(k) for k in ['status','step','attempts','max_attempts','claimed_by','heartbeat_at','lease_expires_at','error_code','error_message']},
        'duration_sec': duration,
        'semantic': sem,
        'files': required,
    }


def reset_target_retry(reason: str):
    ts = now_iso()
    with connect() as conn:
        row = conn.execute('SELECT * FROM recording_jobs WHERE recording_id=?', (RID,)).fetchone()
        if not row:
            return False
        conn.execute('''
            UPDATE recording_jobs
            SET status='retry_wait', step='phase6_watchdog_retry', attempts=0, max_attempts=MAX(max_attempts, 8),
                claimed_at=NULL, claimed_by=NULL, claim_token=NULL, heartbeat_at=NULL, lease_expires_at=NULL,
                error_code='phase6_watchdog_retry', error_message=?, updated_at=?
            WHERE recording_id=?
        ''', (reason, ts, RID))
        conn.execute('''
            INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
            VALUES (?, ?, 'phase6-watchdog', 'phase6_watchdog_retry', 'retry_wait', 'phase6_watchdog_retry', ?, ?)
        ''', (row['job_id'], RID, ts, reason))
    return True


def main() -> int:
    start = time.time()
    last_status = None
    while time.time() - start < TIMEOUT_SECONDS:
        chk = phase6_check()
        job = chk['job']
        status = job.get('status')
        step = job.get('step')
        sem = chk['semantic']
        current = (status, step, sem.get('completed'), sem.get('processing'), sem.get('pending'), sem.get('failed'))
        if current != last_status:
            progress_path = SERVER_DIR / 'validation_runs' / 'phase6_107_watchdog_progress.json'
            progress_path.parent.mkdir(parents=True, exist_ok=True)
            progress_path.write_text(json.dumps(chk, ensure_ascii=False, indent=2), encoding='utf-8')
            last_status = current
        if chk['ok']:
            out = SERVER_DIR / 'validation_runs' / 'phase6_107_final_pass_report.json'
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(chk | {'verdict': 'PASS'}, ensure_ascii=False, indent=2), encoding='utf-8')
            print('Phase 6 PASS')
            print(json.dumps({'recording_id': RID, 'duration_sec': chk['duration_sec'], 'semantic': sem, 'report': str(out)}, ensure_ascii=False, indent=2))
            return 0
        # Recover only if target itself is failed/stale. Do not disturb normal queue work.
        if status == 'failed':
            code = str(job.get('error_code') or '')
            if any(x in code for x in ['worker','timeout','json','semantic','manual_retry','watchdog']) or code in {'temporary_worker_error'}:
                reset_target_retry(f'retryable target failure recovered: {code}')
        elif status in {'stt_running','correction_running','semantic_running','validating','rendering'}:
            hb = job.get('heartbeat_at')
            try:
                if hb:
                    # supports ISO with timezone or naive; stale check best-effort
                    s = hb.replace('Z', '+00:00')
                    dt = datetime.fromisoformat(s)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    age = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()
                    if age > STALE_SECONDS:
                        recover_stale_jobs(worker_id='phase6-watchdog')
            except Exception:
                pass
        elif status in {'retry_wait','queued'}:
            # If nothing at all is working, nudge only this target; otherwise keep queue untouched.
            if not active_worker_or_hermes():
                reset_target_retry('target waiting while no worker/hermes process detected')
        time.sleep(POLL_SECONDS)
    chk = phase6_check()
    out = SERVER_DIR / 'validation_runs' / 'phase6_107_final_timeout_report.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(chk | {'verdict': 'TIMEOUT_NOT_PASS'}, ensure_ascii=False, indent=2), encoding='utf-8')
    print('Phase 6 NOT PASS: watchdog timeout')
    print(json.dumps({'recording_id': RID, 'job': chk['job'], 'semantic': chk['semantic'], 'report': str(out)}, ensure_ascii=False, indent=2))
    return 1


if __name__ == '__main__':
    raise SystemExit(main())

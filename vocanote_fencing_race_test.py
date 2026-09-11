#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from vocanote_queue import (
    DB_PATH,
    OwnershipLost,
    assert_owner,
    claim_next,
    connect,
    enqueue_job,
    heartbeat_job,
    increment_attempt_and_set_failure,
    init_db,
    mark_completed,
    recover_stale_jobs,
    update_job,
)


def iso(dt):
    return dt.isoformat(timespec='seconds')


def main() -> None:
    run_dir = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱/validation_runs') / ('fencing_race_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
    run_dir.mkdir(parents=True, exist_ok=True)
    rec_dir = run_dir / 'recording'
    rec_dir.mkdir()
    rid = str(uuid.uuid4())
    job_id = 'fence_' + rid
    (rec_dir / 'audio.m4a').write_bytes(b'fence-test-audio-placeholder')
    (rec_dir / 'metadata.json').write_text(json.dumps({'recording_id': rid}, ensure_ascii=False), encoding='utf-8')
    enqueue_job(job_id=job_id, recording_id=rid, audio_path=str(rec_dir/'audio.m4a'), metadata_path=str(rec_dir/'metadata.json'), output_dir=str(rec_dir), max_attempts=3)

    worker_a = 'fence-worker-A'
    worker_b = 'fence-worker-B'
    job_a = claim_next(worker_id=worker_a, lease_seconds=3)
    assert job_a and job_a['recording_id'] == rid
    token_a = job_a['claim_token']
    update_job(job_id, status='semantic_running', step='semantic', worker_id=worker_a, claim_token=token_a)

    expired = iso(datetime.now(timezone.utc) - timedelta(seconds=1))
    with connect() as conn:
        conn.execute('UPDATE recording_jobs SET heartbeat_at=?, lease_expires_at=? WHERE job_id=?', (expired, expired, job_id))

    recovered = recover_stale_jobs(worker_id='fence-recoverer')
    job_b = claim_next(worker_id=worker_b, lease_seconds=90)
    assert job_b and job_b['recording_id'] == rid
    token_b = job_b['claim_token']

    stale_results = {}
    stale_results['heartbeat_a'] = heartbeat_job(job_id, worker_id=worker_a, claim_token=token_a, lease_seconds=90)
    for op, fn in {
        'status_update_a': lambda: update_job(job_id, status='rendering', step='rendering', worker_id=worker_a, claim_token=token_a),
        'failure_update_a': lambda: increment_attempt_and_set_failure(job_id, final=False, step='temporary_worker_error', error_code='temporary_worker_error', error_message='stale should fail', worker_id=worker_a, claim_token=token_a),
        'completed_update_a': lambda: mark_completed(job_id, result_json_path=str(rec_dir/'stale_result.validated.json'), worker_id=worker_a, claim_token=token_a),
        'assert_owner_a': lambda: assert_owner(job_id, worker_id=worker_a, claim_token=token_a),
    }.items():
        try:
            fn()
            stale_results[op] = 'UNEXPECTED_OK'
        except OwnershipLost as e:
            stale_results[op] = 'OwnershipLost'

    # Current owner B must still be able to mutate and complete once.
    assert_owner(job_id, worker_id=worker_b, claim_token=token_b)
    update_job(job_id, status='rendering', step='rendering', worker_id=worker_b, claim_token=token_b)
    (rec_dir / 'result.validated.json').write_text('{"fence":true}\n', encoding='utf-8')
    mark_completed(job_id, result_json_path=str(rec_dir/'result.validated.json'), worker_id=worker_b, claim_token=token_b)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    final_job = dict(conn.execute('select * from recording_jobs where job_id=?', (job_id,)).fetchone())
    events = [dict(r) for r in conn.execute('select event,worker_id,status,step,details from job_events where job_id=? order by event_id', (job_id,))]
    completed_events = [e for e in events if e['event'] == 'completed']

    result = {
        'run_dir': str(run_dir),
        'recording_id': rid,
        'job_id': job_id,
        'worker_a': worker_a,
        'token_a': token_a,
        'worker_b': worker_b,
        'token_b': token_b,
        'recovered_count': len([r for r in recovered if r['job_id'] == job_id]),
        'stale_results': stale_results,
        'final_job': {k: final_job[k] for k in ['status','step','claimed_by','claim_token','attempts','result_json_path','error_code']},
        'completed_event_count': len(completed_events),
        'events': events,
    }
    result['pass'] = (
        result['recovered_count'] == 1
        and stale_results.get('heartbeat_a') is False
        and all(v == 'OwnershipLost' for k, v in stale_results.items() if k != 'heartbeat_a')
        and final_job['status'] == 'completed'
        and final_job['claimed_by'] == worker_b
        and final_job['claim_token'] == token_b
        and len(completed_events) == 1
    )
    (run_dir / 'fencing_race_report.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    lines = [
        '# Fencing Race Report', '',
        f"- PASS: {result['pass']}",
        f"- recording_id: `{rid}`",
        f"- A token: `{token_a}`",
        f"- B token: `{token_b}`",
        f"- stale recovery count: {result['recovered_count']}",
        f"- stale mutation results: `{stale_results}`",
        f"- final owner: `{final_job['claimed_by']}`",
        f"- final claim_token is B: {final_job['claim_token'] == token_b}",
        f"- completed event count: {len(completed_events)}",
    ]
    (run_dir / 'fencing_race_report.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(json.dumps({'ok': True, 'pass': result['pass'], 'run_dir': str(run_dir)}, ensure_ascii=False))


if __name__ == '__main__':
    main()

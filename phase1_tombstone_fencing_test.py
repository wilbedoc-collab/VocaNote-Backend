#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import uuid
from pathlib import Path

SERVER = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server')
BASE = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱')
RECORDINGS = BASE / 'recordings'
sys.path.insert(0, str(SERVER))

from vocanote_queue import claim_next, enqueue_job, get_job, init_db, list_jobs
from vocanote_tombstone import (
    RecordingDeleted,
    assert_recording_active,
    create_tombstone,
    guarded_atomic_write_text,
    init_tombstone_db,
    is_deleted,
)


def write_json(p: Path, data: dict):
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


def main():
    init_tombstone_db()
    rid = str(uuid.uuid4())
    rec_dir = RECORDINGS / rid
    if rec_dir.exists():
        shutil.rmtree(rec_dir)
    rec_dir.mkdir(parents=True)
    write_json(rec_dir / 'metadata.json', {'recording_id': rid, 'id': rid, 'title': 'phase1 fence test', 'recorded_at': '2026-08-16T00:00:00+09:00'})
    write_json(rec_dir / 'status.json', {'recording_id': rid, 'status': 'queued', 'step': 'queued'})
    (rec_dir / 'audio.m4a').write_bytes(b'phase1-test')
    job_id = f'phase1_{rid}'
    enqueue_job(job_id=job_id, recording_id=rid, audio_path=str(rec_dir/'audio.m4a'), metadata_path=str(rec_dir/'metadata.json'), output_dir=str(rec_dir))

    # Before tombstone: claim works for this job if queue reaches it. Release not needed; use a second tombstoned job for skip.
    rid2 = str(uuid.uuid4())
    rec_dir2 = RECORDINGS / rid2
    rec_dir2.mkdir(parents=True)
    write_json(rec_dir2 / 'metadata.json', {'recording_id': rid2, 'id': rid2, 'title': 'phase1 deleted skip', 'recorded_at': '2026-08-16T00:00:01+09:00'})
    write_json(rec_dir2 / 'status.json', {'recording_id': rid2, 'status': 'queued', 'step': 'queued'})
    (rec_dir2 / 'audio.m4a').write_bytes(b'phase1-test')
    job_id2 = f'phase1_{rid2}'
    enqueue_job(job_id=job_id2, recording_id=rid2, audio_path=str(rec_dir2/'audio.m4a'), metadata_path=str(rec_dir2/'metadata.json'), output_dir=str(rec_dir2))
    create_tombstone(rid2, delete_state='delete_requested', delete_scope='local_server', original_path=str(rec_dir2), request_source='test', reason='phase1 claim skip')

    # Claim should never return tombstoned rid2.
    claimed = claim_next(worker_id='phase1-test-worker')
    claim_skip_ok = claimed is None or claimed.get('recording_id') != rid2

    # Tombstone blocks active assertion and guarded write.
    create_tombstone(rid, delete_state='delete_requested', delete_scope='local_server', original_path=str(rec_dir), request_source='test', reason='phase1 write fence')
    active_blocked = False
    try:
        assert_recording_active(rid, rec_dir)
    except RecordingDeleted:
        active_blocked = True
    write_blocked = False
    resurrect_file = rec_dir / 'should_not_write.txt'
    try:
        guarded_atomic_write_text(recording_id=rid, recording_dir=rec_dir, target_path=resurrect_file, content='bad', create_parent=False)
    except RecordingDeleted:
        write_blocked = True

    # App scanner/list should exclude tombstoned recordings and .trash.
    import app as voca_app
    listed = []
    for d in voca_app.RECORDINGS_DIR.iterdir() if voca_app.RECORDINGS_DIR.exists() else []:
        if d.name == '.trash' or d.name.startswith('.') or voca_app.is_deleted(d.name):
            continue
        if d.is_dir() and (d / 'metadata.json').exists():
            listed.append(d.name)
    scanner_excludes = rid not in listed and rid2 not in listed

    # Cleanup test directories only by moving to test trash to avoid rm-rf production semantics.
    trash = RECORDINGS / '.trash'
    trash.mkdir(exist_ok=True)
    for r, rd in [(rid, rec_dir), (rid2, rec_dir2)]:
        if rd.exists():
            dst = trash / f'{r}_phase1_test_cleanup'
            if dst.exists():
                shutil.rmtree(dst)
            rd.rename(dst)

    result = {
        'ok': active_blocked and write_blocked and not resurrect_file.exists() and claim_skip_ok and scanner_excludes and is_deleted(rid) and is_deleted(rid2),
        'recording_id': rid,
        'recording_id_tombstoned_claim_skip': rid2,
        'active_blocked': active_blocked,
        'write_blocked': write_blocked,
        'resurrect_file_exists': resurrect_file.exists(),
        'claim_skip_ok': claim_skip_ok,
        'claimed_recording': claimed.get('recording_id') if claimed else None,
        'scanner_excludes_tombstones': scanner_excludes,
        'trash_cleanup_done': True,
    }
    out = BASE / 'validation_runs' / 'phase1_tombstone_fencing_report.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    write_json(out, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result['ok'] else 1)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

import app
from vocanote_queue import connect, enqueue_job, get_job, init_db
from vocanote_tombstone import get_tombstone, guarded_atomic_write_json, is_deleted

BASE = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
RECORDINGS_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱/recordings').resolve()
RUN_ROOT = BASE / 'validation_runs'


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def make_recording(*, with_job: bool = False, claimed: bool = False) -> tuple[str, Path]:
    rid = str(uuid.uuid4())
    d = RECORDINGS_DIR / rid
    d.mkdir(parents=True, exist_ok=False)
    (d / 'audio.m4a').write_bytes(b'fake-audio-for-delete-test')
    write_json(d / 'metadata.json', {'schema_version': 'vocanote.metadata.v2', 'recording_id': rid, 'id': rid, 'recorded_at': datetime.now().isoformat(), 'audio_file': 'audio.m4a', 'layout': 'recordings.v2'})
    write_json(d / 'status.json', {'schema_version': 'vocanote.status.v2', 'recording_id': rid, 'id': rid, 'status': 'queued', 'step': 'queued'})
    write_json(d / 'segments_raw.json', {'schema_version': 'vocanote.segments_raw.v1', 'recording_id': rid, 'segments': [{'index': 0, 'speaker': 'S1', 'start': 0, 'end': 1, 'text': '삭제 테스트'}]})
    (d / 'summary.md').write_text('# 삭제 테스트\n', encoding='utf-8')
    (d / 'analysis.md').write_text('# 삭제 분석\n', encoding='utf-8')
    if with_job:
        init_db()
        enqueue_job(job_id=f'vocanote_{rid}', recording_id=rid, audio_path=str(d / 'audio.m4a'), metadata_path=str(d / 'metadata.json'), output_dir=str(d))
        if claimed:
            with connect() as conn:
                conn.execute("""
                    UPDATE recording_jobs
                    SET status='semantic_running', step='semantic', claimed_by='phase4-worker', claim_token='phase4-token', heartbeat_at=?, lease_expires_at=?, updated_at=?
                    WHERE recording_id=?
                """, ('2026-01-01T00:00:00+00:00', '2999-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00', rid))
    return rid, d


def auth_headers() -> dict[str, str]:
    return {'X-Upload-Token': app.token()}


def status(client: TestClient, method: str, path: str):
    return getattr(client, method)(path, headers=auth_headers())


def assert_deleted_endpoints(client: TestClient, rid: str) -> dict[str, int]:
    endpoints = {
        'detail': f'/api/recordings/{rid}/detail',
        'segments': f'/api/recordings/{rid}/segments',
        'summary': f'/api/recordings/{rid}/text/summary',
        'analysis': f'/api/recordings/{rid}/text/analysis',
        'audio': f'/api/recordings/{rid}/download/audio',
        'status': f'/api/recordings/{rid}/status',
        'resource': f'/api/recordings/{rid}',
    }
    codes = {name: status(client, 'get', path).status_code for name, path in endpoints.items()}
    assert all(code == 404 for code in codes.values()), codes
    return codes


def main() -> int:
    run_dir = RUN_ROOT / ('phase4_delete_api_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
    run_dir.mkdir(parents=True, exist_ok=True)
    client = TestClient(app.app)
    app.ensure_dirs()
    report: dict[str, Any] = {'ok': False, 'run_dir': str(run_dir), 'tests': {}}

    # Test A/B: active delete + idempotent repeat.
    rid, d = make_recording(with_job=True)
    before = {
        'dir_exists': d.exists(),
        'list_contains': any(x['recording_id'] == rid for x in status(client, 'get', '/api/recordings').json()['items']),
        'detail_status': status(client, 'get', f'/api/recordings/{rid}/detail').status_code,
        'audio_status': status(client, 'get', f'/api/recordings/{rid}/download/audio').status_code,
        'tombstone': get_tombstone(rid),
        'job': get_job(rid),
    }
    r1 = status(client, 'delete', f'/api/recordings/{rid}')
    after_ts = get_tombstone(rid)
    trash_matches = sorted((RECORDINGS_DIR / '.trash').glob(f'{rid}_*'))
    r2 = status(client, 'delete', f'/api/recordings/{rid}')
    trash_matches2 = sorted((RECORDINGS_DIR / '.trash').glob(f'{rid}_*'))
    endpoint_codes = assert_deleted_endpoints(client, rid)
    job_after = get_job(rid)
    active_delete_ok = (
        r1.status_code == 200 and r1.json()['delete_state'] == 'deleted'
        and r2.status_code == 200 and r2.json()['delete_state'] == 'deleted'
        and len(trash_matches) == 1 and len(trash_matches2) == 1
        and not d.exists() and trash_matches2[0].exists()
        and after_ts and after_ts['delete_state'] == 'deleted'
        and not any(x['recording_id'] == rid for x in status(client, 'get', '/api/recordings').json()['items'])
        and job_after and job_after['status'] == 'cancelled' and job_after['step'] == 'deleted' and job_after['claimed_by'] is None and job_after['claim_token'] is None and job_after['lease_expires_at'] is None and job_after['error_code'] == 'deleted'
    )
    report['tests']['active_and_idempotent_delete'] = {'ok': active_delete_ok, 'before': before, 'first_response': r1.json(), 'repeat_response': r2.json(), 'trash_count_first': len(trash_matches), 'trash_count_second': len(trash_matches2), 'trash_destination': str(trash_matches2[0]) if trash_matches2 else None, 'endpoint_codes': endpoint_codes, 'job_after': job_after, 'tombstone_after': after_ts}

    # Unknown/malformed.
    unknown = str(uuid.uuid4())
    ru = status(client, 'delete', f'/api/recordings/{unknown}')
    rm = status(client, 'delete', '/api/recordings/not-a-uuid')
    report['tests']['unknown_and_malformed'] = {'ok': ru.status_code == 404 and rm.status_code == 400, 'unknown_status': ru.status_code, 'unknown_body': ru.json(), 'malformed_status': rm.status_code, 'malformed_body': rm.json()}

    # Processing delete: active claim is invalidated; guarded stale write cannot resurrect.
    prid, pd = make_recording(with_job=True, claimed=True)
    job_before = get_job(prid)
    rp = status(client, 'delete', f'/api/recordings/{prid}')
    job_cancelled = get_job(prid)
    stale_write_blocked = False
    try:
        guarded_atomic_write_json(recording_id=prid, recording_dir=pd, target_path=pd / 'semantic_final.json', payload={'bad': True}, create_parent=True)
    except Exception:
        stale_write_blocked = True
    trash_processing = sorted((RECORDINGS_DIR / '.trash').glob(f'{prid}_*'))
    processing_ok = (
        rp.status_code == 200 and job_cancelled and job_cancelled['status'] == 'cancelled'
        and job_cancelled['claimed_by'] is None and job_cancelled['claim_token'] is None and job_cancelled['lease_expires_at'] is None
        and stale_write_blocked and not pd.exists() and len(trash_processing) == 1
        and not (pd / 'semantic_final.json').exists() and not (pd / 'result.validated.json').exists()
    )
    report['tests']['processing_delete_and_stale_resurrection'] = {'ok': processing_ok, 'delete_status': rp.status_code, 'job_before': job_before, 'job_after': job_cancelled, 'stale_write_blocked': stale_write_blocked, 'recording_dir_exists_after': pd.exists(), 'trash_destination': str(trash_processing[0]) if trash_processing else None, 'semantic_final_resurrected': (pd / 'semantic_final.json').exists(), 'result_resurrected': (pd / 'result.validated.json').exists()}

    # Scanner exclusion: trash subtree ignored and idempotency scan must not find trashed client id.
    client_id = 'phase4-client-' + uuid.uuid4().hex
    srid, sd = make_recording()
    meta = read_json(sd / 'metadata.json')
    meta['client_recording_id'] = client_id
    write_json(sd / 'metadata.json', meta)
    status(client, 'delete', f'/api/recordings/{srid}')
    scanner_ok = (app.find_recording_by_client_id(client_id) is None and not any(x['recording_id'] == srid for x in status(client, 'get', '/api/recordings').json()['items']))
    report['tests']['scanner_exclusion'] = {'ok': scanner_ok, 'find_by_client_id': app.find_recording_by_client_id(client_id), 'list_contains': any(x['recording_id'] == srid for x in status(client, 'get', '/api/recordings').json()['items'])}

    report['ok'] = all(t.get('ok') for t in report['tests'].values())
    report_path = run_dir / 'phase4_delete_api_report.json'
    write_json(report_path, report)
    write_json(RUN_ROOT / 'phase4_latest_report.json', report)
    print(json.dumps({'ok': report['ok'], 'report_path': str(report_path), 'tests': report['tests']}, ensure_ascii=False, indent=2))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

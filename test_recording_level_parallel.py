#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import vocanote_worker as vw
from vocanote_queue import enqueue_job, connect, now_iso, get_job, recover_stale_jobs
from vocanote_tombstone import delete_recording_to_trash, RecordingDeleted

ROOT = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱/recordings')
REPORT = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server/validation_runs/recording_parallel_test_report.json')
TEST_PREFIX = 'parallel-test-'
PHASE6 = '2fc6e321-989c-4188-9c75-14d1bfdf6eba'

lock = threading.Lock()
observed_statuses: list[dict] = []
semantic_active = 0
semantic_active_max = 0
fake_sleep = 3.0
created: list[str] = []
TEST_IDS: set[str] = set()


def snap(label: str):
    with connect() as conn:
        if TEST_IDS:
            placeholders = ','.join('?' for _ in TEST_IDS)
            rows = conn.execute(f"""
                SELECT recording_id,status,step,claimed_by,claim_token
                FROM recording_jobs
                WHERE recording_id IN ({placeholders})
                ORDER BY recording_id
            """, tuple(TEST_IDS)).fetchall()
        else:
            rows = []
    with lock:
        observed_statuses.append({'label': label, 'ts': time.time(), 'rows': [dict(r) for r in rows]})


def rid(name: str) -> str:
    return str(uuid.uuid4())


def make_job(name: str, *, stt=False, correction=False, semantic=False) -> str:
    r = rid(name)
    created.append(r)
    TEST_IDS.add(r)
    d = ROOT / r
    d.mkdir(parents=True, exist_ok=False)
    (d / 'audio.m4a').write_bytes(b'fake audio bytes for scheduler test' * 1000)
    meta = {'schema_version':'vocanote.metadata.v2','recording_id':r,'id':r,'recorded_at':now_iso(),'uploaded_at':now_iso(),'title':name,'language':'ko','type':'test','meeting_type':'test','duration_sec':10,'audio_file':'audio.m4a','layout':'recordings.v2'}
    status = {'schema_version':'vocanote.status.v2','recording_id':r,'id':r,'status':'queued','step':'queued','updated_at':now_iso()}
    (d / 'metadata.json').write_text(json.dumps(meta, ensure_ascii=False), encoding='utf-8')
    (d / 'status.json').write_text(json.dumps(status, ensure_ascii=False), encoding='utf-8')
    enqueue_job(job_id='vocanote_'+r, recording_id=r, audio_path=str(d/'audio.m4a'), metadata_path=str(d/'metadata.json'), output_dir=str(d), max_attempts=8)
    if stt:
        fake_stt_artifacts(r)
    if correction:
        fake_correction_artifacts(r)
    if semantic:
        fake_semantic_artifacts(r)
    return r


def fake_stt_artifacts(r: str):
    d = ROOT / r
    segs = [{'index':0,'speaker':'S1','start':0.0,'end':1.0,'text':'테스트'}]
    (d/'stt_raw.json').write_text(json.dumps({'schema_version':'vocanote.stt_raw.v1','recording_id':r,'segments':segs,'transcript_raw':'테스트'}, ensure_ascii=False), encoding='utf-8')
    (d/'segments_raw.json').write_text(json.dumps({'schema_version':'vocanote.segments_raw.v1','recording_id':r,'segments':segs}, ensure_ascii=False), encoding='utf-8')
    (d/'transcript_raw.txt').write_text('테스트\n', encoding='utf-8')


def fake_correction_artifacts(r: str):
    d = ROOT / r
    segs = [{'index':0,'speaker':'S1','start':0.0,'end':1.0,'raw_text':'테스트','corrected_text':'테스트','uncertain':False}]
    (d/'transcript_clean.json').write_text(json.dumps({'schema_version':'vocanote.transcript.v1','recording_id':r,'language':'ko','segments':segs,'warnings':[]}, ensure_ascii=False), encoding='utf-8')
    (d/'segments_clean.json').write_text(json.dumps({'schema_version':'vocanote.segments_clean.v1','recording_id':r,'segments':[{'index':0,'speaker':'S1','start':0.0,'end':1.0,'text':'테스트','raw_text':'테스트','uncertain':False}]}, ensure_ascii=False), encoding='utf-8')
    (d/'transcript_clean.txt').write_text('테스트\n', encoding='utf-8')


def fake_semantic_artifacts(r: str):
    d = ROOT / r
    (d/'semantic_chunks').mkdir(exist_ok=True)
    (d/'semantic_chunks'/'manifest.json').write_text(json.dumps({'chunk_count':1,'chunks':[{'chunk_id':'chunk_0001','status':'completed'}]}, ensure_ascii=False), encoding='utf-8')
    (d/'semantic_final.json').write_text(json.dumps({'schema_version':'vocanote.semantic_final.v1','recording_id':r,'summary':'ok','topics':[],'key_points':[],'decisions':[],'action_items':[],'questions':[],'warnings':[]}, ensure_ascii=False), encoding='utf-8')
    result = {'schema_version':'vocanote.result.v1','recording_id':r,'language':'ko','content_type':'test','title':{'text':'test','filename_slug':'test','reason':'test'},'keywords':['test'],'memo_summary':{'topic':'test','one_line':'test','core_points':['test'],'flow':['test']},'structured_note':{'note_type':'test','sections':[{'heading':'test','items':['test']}]},'quality':{'needs_review':False,'uncertain_segments':[],'warnings':[]}}
    (d/'result.validated.json').write_text(json.dumps(result, ensure_ascii=False), encoding='utf-8')


def patch_worker_for_fast_tests():
    vw.MAX_ACTIVE_RECORDINGS = 3
    vw.MAX_CONCURRENT_STT = 1
    vw.MAX_CONCURRENT_CORRECTION = 2
    vw.MAX_CONCURRENT_SEMANTIC = 2
    vw.MAX_CONCURRENT_REDUCE = 1
    vw.STAGE_LIMITS.update({'stt':1,'correction':2,'semantic':2,'reduce':1,'render':3})
    vw.checkpoint_stt_ok = lambda job: (Path(job['output_dir'])/'stt_raw.json').exists() and (Path(job['output_dir'])/'segments_raw.json').exists()
    vw.checkpoint_correction_ok = lambda job: (Path(job['output_dir'])/'transcript_clean.json').exists() and (Path(job['output_dir'])/'segments_clean.json').exists()
    vw.checkpoint_semantic_ok = lambda job: (Path(job['output_dir'])/'semantic_final.json').exists() and (Path(job['output_dir'])/'result.validated.json').exists()
    vw.checkpoint_render_ok = lambda job: (Path(job['output_dir'])/'summary.md').exists() and (Path(job['output_dir'])/'analysis.md').exists()

    def slow_loop(job, stage):
        for _ in range(int(fake_sleep * 10)):
            vw.require_active(job)
            vw.require_owner(job)
            time.sleep(0.1)

    def run_stt(job):
        slow_loop(job, 'stt')
        fake_stt_artifacts(job['recording_id'])
    def run_correction(job):
        slow_loop(job, 'correction')
        fake_correction_artifacts(job['recording_id'])
    def run_semantic(job):
        global semantic_active, semantic_active_max
        with lock:
            semantic_active += 1
            semantic_active_max = max(semantic_active_max, semantic_active)
        try:
            slow_loop(job, 'semantic')
            if not vw.acquire_stage_slot(job, 'reduce', step='reduce'):
                vw.yield_claim_for_stage(job, previous_step='semantic_map_done', reason='reduce slot full test')
            slow_loop(job, 'reduce')
            fake_semantic_artifacts(job['recording_id'])
        finally:
            with lock:
                semantic_active -= 1
    def render(d: Path):
        (d/'summary.md').write_text('summary\n', encoding='utf-8')
        (d/'analysis.md').write_text('analysis\n', encoding='utf-8')
    vw.run_stt = run_stt
    vw.run_correction = run_correction
    vw.run_semantic = run_semantic
    vw.render_all = render

    def test_claim_next_eligible(worker_id_value: str, *, lease_seconds: int = vw.LEASE_SECONDS):
        if not TEST_IDS:
            return None
        token = uuid.uuid4().hex
        ts = now_iso()
        from vocanote_queue import future_iso
        expires = future_iso(lease_seconds)
        with connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            active = conn.execute("""
                SELECT COUNT(*) AS c FROM recording_jobs
                WHERE recording_id IN (%s)
                  AND status IN ('claimed','stt_running','correction_running','semantic_running','reduce_running','validating','rendering')
                  AND lease_expires_at IS NOT NULL AND lease_expires_at >= ?
            """ % ','.join('?' for _ in TEST_IDS), (*tuple(TEST_IDS), ts)).fetchone()['c']
            if int(active) >= vw.MAX_ACTIVE_RECORDINGS:
                conn.execute('COMMIT')
                return None
            rows = conn.execute("""
                SELECT * FROM recording_jobs
                WHERE recording_id IN (%s) AND status IN ('queued','retry_wait') AND attempts < max_attempts
                ORDER BY created_at ASC
            """ % ','.join('?' for _ in TEST_IDS), tuple(TEST_IDS)).fetchall()
            chosen = None
            for row in rows:
                job = dict(row)
                stage = vw.next_required_stage(job) or 'render'
                status = vw.STAGE_RUNNING_STATUS[stage]
                count = conn.execute("SELECT COUNT(*) AS c FROM recording_jobs WHERE recording_id IN (%s) AND status=? AND lease_expires_at IS NOT NULL AND lease_expires_at >= ?" % ','.join('?' for _ in TEST_IDS), (*tuple(TEST_IDS), status, ts)).fetchone()['c']
                if int(count) < int(vw.STAGE_LIMITS.get(stage, vw.MAX_ACTIVE_RECORDINGS)):
                    chosen = job
                    break
            if not chosen:
                conn.execute('COMMIT')
                return None
            cur = conn.execute("""
                UPDATE recording_jobs
                SET status='claimed', step='claimed', claimed_at=?, claimed_by=?, claim_token=?, heartbeat_at=?, lease_expires_at=?, updated_at=?
                WHERE job_id=? AND status IN ('queued','retry_wait')
            """, (ts, worker_id_value, token, ts, expires, ts, chosen['job_id']))
            if cur.rowcount != 1:
                conn.execute('COMMIT')
                return None
            claimed = conn.execute('SELECT * FROM recording_jobs WHERE job_id=?', (chosen['job_id'],)).fetchone()
            conn.execute("""INSERT INTO job_events(job_id, recording_id, worker_id, event, status, step, timestamp, details)
                         VALUES (?, ?, ?, 'claim', 'claimed', 'claimed', ?, ?)""", (claimed['job_id'], claimed['recording_id'], worker_id_value, ts, f'test claim_token={token}'))
            conn.execute('COMMIT')
            return dict(claimed)
    vw.claim_next_eligible = test_claim_next_eligible


def run_threads(n: int, timeout: float = 30):
    with ThreadPoolExecutor(max_workers=n) as ex:
        futs = [ex.submit(vw.run_once, dry_run=False) for _ in range(n)]
        end = time.time() + timeout
        while time.time() < end and any(not f.done() for f in futs):
            snap('during')
            time.sleep(0.25)
        return [f.result(timeout=5) for f in futs]


def test_three_recording_parallel():
    a = make_job('A-stt')
    b = make_job('B-correction', stt=True)
    c = make_job('C-semantic', stt=True, correction=True)
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [ex.submit(vw.run_once, dry_run=False) for _ in range(3)]
        time.sleep(1.0)
        snap('three_active')
        results = [f.result(timeout=30) for f in futs]
    active_snap = next(x for x in observed_statuses if x['label']=='three_active')
    statuses = {r['recording_id']: r['status'] for r in active_snap['rows'] if r['recording_id'] in {a,b,c}}
    return {'ids': {'A':a,'B':b,'C':c}, 'snapshot_statuses': statuses, 'results': results,
            'pass': set(statuses.values()) >= {'stt_running','correction_running','semantic_running'}}


def test_duplicate_claim():
    d = make_job('D-duplicate')
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [ex.submit(vw.run_once, dry_run=False) for _ in range(3)]
        time.sleep(0.8)
        with connect() as conn:
            owners = conn.execute('SELECT COUNT(DISTINCT claimed_by || claim_token) AS c FROM recording_jobs WHERE recording_id=? AND status IN (\'claimed\',\'stt_running\',\'correction_running\',\'semantic_running\',\'reduce_running\',\'rendering\')', (d,)).fetchone()['c']
        results = [f.result(timeout=30) for f in futs]
    with connect() as conn:
        claims = conn.execute('SELECT COUNT(*) AS c FROM job_events WHERE recording_id=? AND event=\'claim\'', (d,)).fetchone()['c']
    return {'id': d, 'active_owner_count_observed': owners, 'claim_events': claims, 'results': results, 'pass': owners == 1 and claims == 1}


def test_semantic_concurrency_limit():
    global semantic_active_max
    semantic_active_max = 0
    ids = [make_job(f'S{i}-semantic-limit', stt=True, correction=True) for i in range(3)]
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [ex.submit(vw.run_once, dry_run=False) for _ in range(3)]
        time.sleep(1.0)
        with connect() as conn:
            placeholders = ','.join('?' for _ in ids)
            active = [dict(r) for r in conn.execute(f"SELECT recording_id,status FROM recording_jobs WHERE recording_id IN ({placeholders}) AND status='semantic_running'", tuple(ids)).fetchall()]
        results = [f.result(timeout=40) for f in futs]
    return {'ids': ids, 'active_semantic_snapshot': active, 'semantic_active_max': semantic_active_max, 'results': results, 'pass': semantic_active_max <= 2 and len(active) <= 2}


def test_delete_one_while_others_run():
    a = make_job('DA-stt')
    b = make_job('DB-delete', stt=True, correction=True)
    c = make_job('DC-correction', stt=True)
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [ex.submit(vw.run_once, dry_run=False) for _ in range(3)]
        time.sleep(0.9)
        delete_recording_to_trash(b, recording_dir=ROOT/b, delete_scope='local_server', request_source='parallel-test')
        results = [f.result(timeout=40) for f in futs]
    # Drain surviving A/C so test proves B cancellation does not block others.
    drain = []
    for _ in range(6):
        ja, jc = get_job(a), get_job(c)
        if ja.get('status') == 'completed' and jc.get('status') == 'completed':
            break
        drain.append(vw.run_once(dry_run=False))
    ja, jb, jc = get_job(a), get_job(b), get_job(c)
    resurrection = (ROOT/b).exists() and any((ROOT/b).glob('*.json'))
    return {'ids': {'A':a,'B':b,'C':c}, 'results': results, 'job_statuses': {'A':ja.get('status'), 'B':jb.get('status'), 'C':jc.get('status')}, 'b_dir_exists': (ROOT/b).exists(), 'resurrection': resurrection, 'pass': ja.get('status')=='completed' and jc.get('status')=='completed' and not resurrection}


def test_crash_recovery_one_worker():
    e = make_job('E-crash')
    f = make_job('F-continues')
    # Claim E with a fake worker and short lease, then never heartbeat: simulated crash.
    crashed = vw.claim_next_eligible('parallel-test-crashed-worker', lease_seconds=1)
    # Let F proceed in another worker while E lease expires.
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(vw.run_once, dry_run=False)
        time.sleep(2.0)
        recovered = recover_stale_jobs(worker_id='parallel-test-recover')
        result = fut.result(timeout=30)
    je, jf = get_job(e), get_job(f)
    for _ in range(6):
        if jf.get('status') == 'completed':
            break
        vw.run_once(dry_run=False)
        jf = get_job(f)
    crashed_rid = crashed and crashed.get('recording_id')
    recovered_rids = {r['recording_id'] for r in recovered}
    other = f if crashed_rid == e else e
    only_crashed_recovered = crashed_rid in recovered_rids and all(r['recording_id']==crashed_rid for r in recovered)
    return {'ids': {'E':e,'F':f}, 'crashed_claim': crashed_rid, 'recovered': [{'rid':r['recording_id'],'new_status':r['new_status']} for r in recovered], 'f_result': result, 'job_statuses': {'E':je.get('status'), 'F':jf.get('status')}, 'pass': only_crashed_recovered and get_job(other).get('status')=='completed' and get_job(crashed_rid).get('status')=='completed'}


def cleanup():
    with connect() as conn:
        for r in list(created):
            try:
                conn.execute('DELETE FROM recording_jobs WHERE recording_id=?', (r,))
                conn.execute('DELETE FROM job_events WHERE recording_id=?', (r,))
                conn.execute('DELETE FROM deleted_recordings WHERE recording_id=?', (r,))
            except Exception:
                pass
            shutil.rmtree(ROOT/r, ignore_errors=True)


def run_isolated(name: str, fn):
    TEST_IDS.clear()
    created.clear()
    try:
        return fn()
    finally:
        cleanup()
        TEST_IDS.clear()
        created.clear()


def main() -> int:
    phase6_before = {}
    p6dir = ROOT / PHASE6
    for rel in ['semantic_chunks/manifest.json','semantic_final.json','result.validated.json','summary.md','analysis.md']:
        p = p6dir/rel
        phase6_before[rel] = {'exists': p.exists(), 'bytes': p.stat().st_size if p.exists() else None, 'mtime': p.stat().st_mtime if p.exists() else None}
    patch_worker_for_fast_tests()
    tests = {}
    try:
        tests['three_recording_parallel'] = run_isolated('three_recording_parallel', test_three_recording_parallel)
        tests['duplicate_claim'] = run_isolated('duplicate_claim', test_duplicate_claim)
        tests['semantic_concurrency'] = run_isolated('semantic_concurrency', test_semantic_concurrency_limit)
        tests['delete_one_while_others_run'] = run_isolated('delete_one_while_others_run', test_delete_one_while_others_run)
        tests['worker_one_crash'] = run_isolated('worker_one_crash', test_crash_recovery_one_worker)
    finally:
        cleanup()
    phase6_after = {}
    for rel in phase6_before:
        p = p6dir/rel
        phase6_after[rel] = {'exists': p.exists(), 'bytes': p.stat().st_size if p.exists() else None, 'mtime': p.stat().st_mtime if p.exists() else None}
    phase6_preserved = phase6_before == phase6_after
    report = {
        'ok': all(t.get('pass') for t in tests.values()) and phase6_preserved,
        'limits': {'max_active_recordings': 3, 'stt':1, 'correction':2, 'semantic':2, 'reduce':1},
        'tests': tests,
        'observed_statuses': observed_statuses[-20:],
        'phase6_107_checkpoint_preserved': phase6_preserved,
        'phase6_before': phase6_before,
        'phase6_after': phase6_after,
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'ok': report['ok'], 'report': str(REPORT), 'test_passes': {k:v.get('pass') for k,v in tests.items()}, 'phase6_107_checkpoint_preserved': phase6_preserved}, ensure_ascii=False, indent=2))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

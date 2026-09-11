#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import difflib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

SERVER_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
BASE_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱').resolve()
RECORDINGS_DIR = BASE_DIR / 'recordings'
RUNS_DIR = BASE_DIR / 'validation_runs'
PYTHON = '/Users/ahnbot/.openclaw/workspace/hermes_eval/repo/venv/bin/python'
API = 'http://127.0.0.1:8793'


def run(cmd: list[str], timeout: int = 120, env: dict[str, str] | None = None) -> dict[str, Any]:
    start = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout, env=env)
    return {'cmd': sanitize_obj(cmd), 'returncode': p.returncode, 'stdout': redact_secret_text(p.stdout), 'stderr': redact_secret_text(p.stderr), 'elapsed_sec': round(time.time() - start, 3)}


def token() -> str:
    return (SERVER_DIR / '.upload_token').read_text('utf-8').strip()


def redact_secret_text(text: str) -> str:
    try:
        t = token()
        if t:
            text = text.replace(t, '***TOKEN_REDACTED***')
    except Exception:
        pass
    return text


def sanitize_obj(obj: Any) -> Any:
    if isinstance(obj, str):
        return redact_secret_text(obj)
    if isinstance(obj, list):
        return [sanitize_obj(x) for x in obj]
    if isinstance(obj, dict):
        return {k: sanitize_obj(v) for k, v in obj.items()}
    return obj


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


def api_get(path: str, timeout: int = 30) -> dict[str, Any]:
    r = run(['curl', '-sS', '-m', str(timeout), '-w', '\nHTTP_CODE:%{http_code}\n', '-H', f'X-Upload-Token: {token()}', API + path], timeout=timeout + 5)
    text = r['stdout']
    code = None
    body = text
    marker = '\nHTTP_CODE:'
    if marker in text:
        body, tail = text.rsplit(marker, 1)
        try:
            code = int(tail.strip())
        except Exception:
            code = None
    return {'http_code': code, 'body': body, 'raw': r}


def upload_audio(audio: Path, *, recorded_at: str, memo: str, duration_sec: int = 10) -> dict[str, Any]:
    cmd = [
        'curl', '-sS', '-m', '90', '-X', 'POST', API + '/api/recordings',
        '-H', f'X-Upload-Token: {token()}',
        '-F', f'audio=@{audio};type=audio/mp4;filename={audio.name}',
        '-F', 'title=무제회의',
        '-F', 'meeting_type=test',
        '-F', f'memo={memo}',
        '-F', f'recorded_at={recorded_at}',
        '-F', f'duration_sec={duration_sec}',
        '-F', 'language=ko',
    ]
    r = run(cmd, timeout=100)
    try:
        data = json.loads(r['stdout'])
    except Exception:
        data = {'parse_error': r['stdout'][-500:]}
    return {'upload': r, 'response': data}


def poll_recording(rid: str, max_wait_sec: int = 600, interval: int = 10) -> dict[str, Any]:
    d = RECORDINGS_DIR / rid
    history = []
    start = time.time()
    while time.time() - start <= max_wait_sec:
        if not (d / 'status.json').exists():
            status = {'status': 'missing', 'step': 'missing'}
        else:
            status = read_json(d / 'status.json')
        item = {'t': round(time.time() - start, 1), 'status': status.get('status'), 'step': status.get('step')}
        history.append(item)
        if status.get('status') in {'completed', 'failed', 'cancelled'}:
            break
        time.sleep(interval)
    return {'history': history, 'elapsed_sec': round(time.time() - start, 2), 'final': history[-1] if history else None}


def job_row(rid: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(SERVER_DIR / 'jobs.sqlite3')
    conn.row_factory = sqlite3.Row
    row = conn.execute('select * from recording_jobs where recording_id=?', (rid,)).fetchone()
    return dict(row) if row else None


def summarize_recording(rid: str) -> dict[str, Any]:
    d = RECORDINGS_DIR / rid
    out: dict[str, Any] = {'recording_id': rid, 'dir': str(d), 'exists': d.exists(), 'job': job_row(rid)}
    if not d.exists():
        return out
    for name in ['metadata.json','status.json','stt_raw.json','transcript_raw.txt','segments_raw.json','transcript_clean.json','transcript_clean.txt','segments_clean.json','result.raw.json','result.validated.json','summary.md','analysis.md','error.log']:
        p = d / name
        out[name] = {'exists': p.exists(), 'bytes': p.stat().st_size if p.exists() else 0}
    if (d / 'metadata.json').exists():
        meta = read_json(d / 'metadata.json')
        out['metadata_title'] = meta.get('title')
        out['metadata_recording_id'] = meta.get('recording_id')
        out['audio_file'] = meta.get('audio_file')
        out['audio_size'] = (d / str(meta.get('audio_file','audio.m4a'))).stat().st_size if (d / str(meta.get('audio_file','audio.m4a'))).exists() else None
    if (d / 'result.validated.json').exists():
        result = read_json(d / 'result.validated.json')
        out['result_title'] = result.get('title', {}).get('text')
        out['content_type'] = result.get('content_type')
        out['keywords'] = result.get('keywords')
        out['quality'] = result.get('quality')
    for endpoint in ['', '/detail', '/segments', '/text/summary', '/text/analysis']:
        got = api_get(f'/api/recordings/{rid}{endpoint}', timeout=15)
        out[f'api{endpoint or "/"}'] = {'http_code': got['http_code'], 'bytes': len(got['body'])}
    return out


def sample_valid_audio(prefer_short: bool = True) -> Path:
    candidates = sorted(BASE_DIR.glob('*_original.m4a'), key=lambda p: p.stat().st_size if prefer_short else -p.stat().st_size)
    for p in candidates:
        probe = run(['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'json', str(p)], timeout=20)
        if probe['returncode'] == 0:
            return p
    raise RuntimeError('no valid audio found')


def service_status() -> dict[str, Any]:
    uid = os.getuid()
    return {
        'api': run(['launchctl', 'print', f'gui/{uid}/com.re2o.vocanote-api'], timeout=30),
        'worker': run(['launchctl', 'print', f'gui/{uid}/com.re2o.vocanote-worker'], timeout=30),
        'health': run(['curl', '-sS', '-m', '10', API + '/health'], timeout=15),
        'processes': run(['bash', '-lc', "ps -ef | grep -E 'meeting_recorder_server/app.py|vocanote_worker.py|process_recordings.py' | grep -v grep || true"], timeout=15),
    }


def scenario_e2e(run_dir: Path) -> dict[str, Any]:
    src = sample_valid_audio(prefer_short=False)
    tmp = run_dir / 'e2e_upload.m4a'
    shutil.copy2(src, tmp)
    start = time.time()
    up = upload_audio(tmp, recorded_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), memo='validation_e2e', duration_sec=60)
    rid = up.get('response', {}).get('recording_id')
    if not rid:
        return {'name': 'server_e2e', 'pass': False, 'upload': up}
    polled = poll_recording(rid, max_wait_sec=600, interval=10)
    summary = summarize_recording(rid)
    passed = polled['final']['status'] == 'completed' and all(summary.get(f'api{x}', {}).get('http_code') == 200 for x in ['/', '/detail', '/segments', '/text/summary', '/text/analysis'])
    return {'name': 'server_e2e', 'pass': passed, 'source_audio': str(src), 'recording_id': rid, 'upload': up, 'poll': polled, 'summary': summary, 'total_elapsed_sec': round(time.time()-start, 2)}


def scenario_consecutive(run_dir: Path) -> dict[str, Any]:
    src = sample_valid_audio(prefer_short=True)
    results = []
    ids = []
    for i in range(2):
        tmp = run_dir / f'consecutive_{i}.m4a'
        shutil.copy2(src, tmp)
        up = upload_audio(tmp, recorded_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), memo=f'validation_consecutive_{i}', duration_sec=10)
        rid = up.get('response', {}).get('recording_id')
        ids.append(rid)
        results.append({'upload': up, 'recording_id': rid})
    for r in results:
        if r['recording_id']:
            r['poll'] = poll_recording(r['recording_id'], max_wait_sec=600, interval=10)
            r['summary'] = summarize_recording(r['recording_id'])
    passed = len(set(ids)) == 2 and all(r.get('poll', {}).get('final', {}).get('status') == 'completed' for r in results)
    if passed:
        # Check content/result files do not point to the same output dir or same recording_id.
        passed = all(r['summary']['metadata_recording_id'] == r['recording_id'] for r in results)
    return {'name': 'consecutive_uploads', 'pass': passed, 'source_audio': str(src), 'results': results}


def scenario_corrupt_audio(run_dir: Path) -> dict[str, Any]:
    corrupt = run_dir / 'corrupt.m4a'
    corrupt.write_bytes(b'not a valid m4a')
    up = upload_audio(corrupt, recorded_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), memo='validation_corrupt_audio', duration_sec=1)
    rid = up.get('response', {}).get('recording_id')
    if not rid:
        return {'name': 'corrupt_audio', 'pass': False, 'upload': up}
    polled = poll_recording(rid, max_wait_sec=240, interval=10)
    summary = summarize_recording(rid)
    passed = polled['final']['status'] == 'failed' and summary.get('audio_size', 0) and summary.get('job', {}).get('error_code') == 'stt_failed_audio_invalid'
    return {'name': 'corrupt_audio', 'pass': passed, 'recording_id': rid, 'poll': polled, 'summary': summary}


def score_keywords(keywords: list[str]) -> dict[str, Any]:
    filler = {'우리','아주','맞아요','그렇죠','그런데','제가','저는','음','어'}
    bad = [k for k in keywords if str(k).strip().lstrip('#') in filler]
    return {'count': len(keywords), 'filler_hits': bad, 'pass': len(bad) == 0 and len(keywords) <= 8}


def text_similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a, b).ratio()


def create_gold_skeleton(run_dir: Path) -> dict[str, Any]:
    cases = []
    src_cases_path = SERVER_DIR / 'stt_benchmark_cases.json'
    if src_cases_path.exists():
        data = read_json(src_cases_path)
        for case in data.get('cases', []):
            cases.append({
                'case_id': case.get('case_id'),
                'audio_file': case.get('audio_file'),
                'content_type': case.get('content_type'),
                'gold_transcript': '',
                'gold_transcript_status': 'NEEDS_HUMAN_TRANSCRIPTION',
                'must_correct': [{'wrong': x, 'correct': ''} for x in case.get('bad_patterns', [])],
                'critical_facts': [],
                'forbidden_hallucinations': [],
                'good_title_examples': [],
                'expected_keywords': case.get('must_preserve', []),
                'critical_terms': case.get('must_preserve', []),
            })
    out = {'schema_version': 'vocanote.gold_dataset.v1', 'created_at': datetime.now().isoformat(timespec='seconds'), 'cases': cases}
    path = run_dir / 'gold_dataset_skeleton.json'
    write_json(path, out)
    return {'path': str(path), 'case_count': len(cases), 'note': 'Gold transcripts are intentionally blank and require human transcription before quality scores are final.'}


def security_probe(run_dir: Path) -> dict[str, Any]:
    probes = {}
    probes['listen'] = run(['bash', '-lc', 'lsof -nP -iTCP:8793 -sTCP:LISTEN || true'], timeout=20)
    probes['health_no_auth'] = run(['curl', '-sS', '-m', '10', '-w', '\nHTTP_CODE:%{http_code}\n', API + '/health'], timeout=15)
    probes['list_no_auth'] = run(['curl', '-sS', '-m', '10', '-w', '\nHTTP_CODE:%{http_code}\n', API + '/api/recordings'], timeout=15)
    probes['list_auth'] = run(['curl', '-sS', '-m', '10', '-w', '\nHTTP_CODE:%{http_code}\n', '-H', f'X-Upload-Token: {token()}', API + '/api/recordings'], timeout=15)
    probes['traversal'] = run(['curl', '-sS', '-m', '10', '-w', '\nHTTP_CODE:%{http_code}\n', '-H', f'X-Upload-Token: {token()}', API + '/api/recordings/../../../../etc/passwd'], timeout=15)
    return probes


def run_server_suite(run_dir: Path) -> dict[str, Any]:
    report = {'run_id': run_dir.name, 'started_at': datetime.now().isoformat(timespec='seconds'), 'service_status_before': service_status(), 'scenarios': []}
    for fn in [scenario_e2e, scenario_consecutive, scenario_corrupt_audio]:
        item = fn(run_dir)
        report['scenarios'].append(item)
        write_json(run_dir / f"scenario_{item['name']}.json", item)
    report['gold_skeleton'] = create_gold_skeleton(run_dir)
    report['security'] = security_probe(run_dir)
    report['finished_at'] = datetime.now().isoformat(timespec='seconds')
    write_json(run_dir / 'report.json', report)
    return report


def render_markdown(report: dict[str, Any], run_dir: Path) -> str:
    lines = ['# VocaNote Validation Report', '', f"Run: `{report.get('run_id')}`", '']
    lines += ['## Automated Server/Worker Scenarios', '', '| Scenario | PASS | Key result |', '|---|---:|---|']
    for s in report.get('scenarios', []):
        key = s.get('recording_id') or ','.join(str(r.get('recording_id')) for r in s.get('results', []))
        lines.append(f"| {s.get('name')} | {s.get('pass')} | `{key}` |")
    lines += ['', '## Gold Dataset', '', f"Skeleton cases: {report.get('gold_skeleton', {}).get('case_count')}", f"Path: `{report.get('gold_skeleton', {}).get('path')}`", '']
    lines += ['## Security Probe Summary', '']
    sec = report.get('security', {})
    for k, v in sec.items():
        out = (v.get('stdout') or '').replace('\n', ' ')[:220]
        lines.append(f"- `{k}` rc={v.get('returncode')} output=`{out}`")
    lines += ['', '## Physical Tests Pending', '', '- Android real-device E2E: requires phone operation.', '- Mac mini reboot test: requires reboot approval and post-boot observation.', '- Wi-Fi/network interruption tests: require phone/network manipulation.', '']
    md = '\n'.join(lines) + '\n'
    (run_dir / 'report.md').write_text(md, encoding='utf-8')
    return md


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-dir', default='')
    parser.add_argument('--server-suite', action='store_true')
    parser.add_argument('--gold-skeleton', action='store_true')
    parser.add_argument('--security', action='store_true')
    args = parser.parse_args()
    run_dir = Path(args.run_dir) if args.run_dir else RUNS_DIR / datetime.now().strftime('%Y%m%d_%H%M%S')
    run_dir.mkdir(parents=True, exist_ok=True)
    if args.server_suite:
        report = run_server_suite(run_dir)
        render_markdown(report, run_dir)
        print(json.dumps({'ok': True, 'run_dir': str(run_dir), 'report_json': str(run_dir/'report.json'), 'report_md': str(run_dir/'report.md')}, ensure_ascii=False))
    elif args.gold_skeleton:
        print(json.dumps(create_gold_skeleton(run_dir), ensure_ascii=False))
    elif args.security:
        out = security_probe(run_dir)
        write_json(run_dir / 'security_probe.json', out)
        print(json.dumps({'ok': True, 'path': str(run_dir/'security_probe.json')}, ensure_ascii=False))
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

SERVER_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server')
CASES = SERVER_DIR / 'stt_benchmark_cases.json'
OUT = SERVER_DIR / 'stt_benchmark_runs' / datetime.now().strftime('%Y%m%d_%H%M%S')
PYTHON = '/Users/ahnbot/.openclaw/workspace/hermes_eval/repo/venv/bin/python3'
MODELS = ['base', 'small']


def run_case(model: str, audio: Path) -> tuple[float, str, list[dict]]:
    out_dir = OUT / model
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [PYTHON, '-m', 'whisper', str(audio), '--model', model, '--language', 'Korean', '--task', 'transcribe', '--output_format', 'json', '--output_dir', str(out_dir), '--fp16', 'False']
    start = time.time()
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=3600)
    elapsed = time.time() - start
    js = out_dir / f'{audio.stem}.json'
    data = json.loads(js.read_text('utf-8'))
    text = '\n'.join(str(s.get('text') or '').strip() for s in data.get('segments') or [] if str(s.get('text') or '').strip()) or str(data.get('text') or '')
    return elapsed, text, data.get('segments') or []


def main() -> None:
    cases = json.loads(CASES.read_text('utf-8'))['cases']
    results = {'run_dir': str(OUT), 'models': MODELS, 'cases': []}
    OUT.mkdir(parents=True, exist_ok=True)
    for case in cases:
        audio = Path(case['audio_file'])
        row = {'case_id': case['case_id'], 'audio_file': str(audio), 'models': {}}
        for model in MODELS:
            elapsed, text, segments = run_case(model, audio)
            must = case.get('must_preserve') or []
            bad = case.get('bad_patterns') or []
            row['models'][model] = {
                'elapsed_sec': round(elapsed, 2),
                'chars': len(text),
                'segment_count': len(segments),
                'must_hits': [x for x in must if x in text],
                'must_misses': [x for x in must if x not in text],
                'bad_hits': [x for x in bad if x in text],
                'text_preview': text[:600],
            }
            (OUT / f"{case['case_id']}.{model}.txt").write_text(text + '\n', encoding='utf-8')
        results['cases'].append(row)
    (OUT / 'results.json').write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
    # compact markdown report
    lines = [f"# STT benchmark {OUT.name}", '', '| case | model | sec | must hit/miss | bad hits |', '|---|---:|---:|---|---|']
    for case in results['cases']:
        for model, m in case['models'].items():
            lines.append(f"| {case['case_id']} | {model} | {m['elapsed_sec']} | {len(m['must_hits'])}/{len(m['must_hits'])+len(m['must_misses'])} miss:{', '.join(m['must_misses'])} | {', '.join(m['bad_hits'])} |")
    (OUT / 'report.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    latest = SERVER_DIR / 'stt_benchmark_runs' / 'latest'
    try:
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(OUT, target_is_directory=True)
    except Exception:
        pass
    print(json.dumps({'ok': True, 'run_dir': str(OUT), 'report': str(OUT / 'report.md')}, ensure_ascii=False))


if __name__ == '__main__':
    main()

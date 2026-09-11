#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

SERVER = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server')
sys.path.insert(0, str(SERVER))
from vocanote_chunking import (  # noqa: E402
    CORRECTION_PROMPT_VERSION,
    build_chunks,
    chunk_segments_for_prompt,
    deterministic_global_context,
    merge_correction_chunks,
    validate_correction_chunk_output,
)
from vocanote_worker import run_hermes_json, validate_schema  # noqa: E402

RID = '0d380365-b558-4089-9188-aec44a75d552'
D = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱/recordings') / RID
REPORT = D / 'phase1_correction_e2e_report.json'


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f'.{path.name}.{os.getpid()}.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp, path)


def prompt(metadata, chunk, primary, overlap, global_context):
    payload = {
        'metadata': {k: metadata.get(k) for k in ['recording_id','language','type','meeting_type','recorded_at','duration_sec']},
        'chunk_id': chunk['chunk_id'],
        'input_hash': chunk['input_hash'],
        'primary_segment_range': chunk['primary_segment_range'],
        'overlap_context_range': chunk['overlap_context_range'],
        'global_context': global_context,
        'rolling_context': {},
        'primary_segments': [{k: seg.get(k) for k in ['index', 'speaker', 'start', 'end', 'text']} for seg in primary],
        'overlap_context_segments': [{k: seg.get(k) for k in ['index', 'speaker', 'start', 'end', 'text']} for seg in overlap],
    }
    return """You are the isolated VocaNote chunk transcript-correction worker.
Use ONLY the JSON payload below.

Task: correct Korean STT errors for PRIMARY segments only.
Rules:
- Return output for primary_segments only. Do NOT return overlap_context_segments unless they are also primary.
- segment index/start/end/speaker must be copied exactly.
- raw_text must equal the primary segment text.
- Correct text only. Do not summarize, delete meaning, add facts, or rewrite style.
- Global/Rolling context is reference only for terms/entities. Raw segment text is authoritative.
- If uncertain, preserve raw meaning and set uncertain=true.
- Output JSON only. No markdown.

Required JSON shape:
{
  "schema_version":"vocanote.transcript.v1",
  "recording_id":"...",
  "language":"ko",
  "segments":[{"index":0,"speaker":"S1","start":0.0,"end":1.0,"raw_text":"...","corrected_text":"...","uncertain":false}],
  "warnings":[]
}

Payload:
""" + json.dumps(payload, ensure_ascii=False)


def load_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def run(reuse_only: bool = False):
    metadata = load_json(D / 'metadata.json')
    raw_segments = load_json(D / 'segments_raw.json')['segments']
    global_context = deterministic_global_context(raw_segments)
    write_json(D / 'global_context.json', global_context)
    manifest = build_chunks(
        raw_segments,
        target_chars=1200,
        max_duration_sec=60,
        max_segments=35,
        overlap_segments=2,
        prompt_version=CORRECTION_PROMPT_VERSION,
        model_id='gpt-5.5',
        provider_id='openai-codex',
        global_context=global_context,
    )
    cdir = D / 'correction_chunks'
    cdir.mkdir(exist_ok=True)
    outputs = []
    chunk_reports = []
    hermes_calls = 0
    for chunk in manifest['chunks']:
        cpath = cdir / f"{chunk['chunk_id']}.json"
        primary, overlap = chunk_segments_for_prompt(raw_segments, chunk)
        reused = False
        elapsed = 0.0
        if cpath.exists():
            stored = load_json(cpath)
            if stored.get('input_hash') == chunk['input_hash']:
                out = stored.get('output') or {}
                validate_schema(out, 'transcript.schema.json')
                ok, warnings = validate_correction_chunk_output(raw_primary_segments=primary, output=out)
                if ok:
                    reused = True
                    outputs.append(out)
                    chunk['status'] = 'passed'
                    chunk['attempts'] = stored.get('attempts', 1)
                    chunk_reports.append({**chunk, 'reused': True, 'elapsed_sec': 0.0, 'warnings': warnings, 'hermes_called': False})
                    continue
        if reuse_only:
            raise RuntimeError(f"checkpoint missing/not reusable: {chunk['chunk_id']}")
        p = prompt(metadata, chunk, primary, overlap, global_context)
        (cdir / f"{chunk['chunk_id']}.prompt.txt").write_text(p, encoding='utf-8')
        started = time.time()
        hermes_calls += 1
        try:
            out, raw = run_hermes_json(p, timeout=300)
        except Exception as e:
            elapsed = time.time() - started
            chunk['status'] = 'failed'
            chunk['attempts'] = 1
            chunk['error'] = repr(e)
            chunk_reports.append({**chunk, 'reused': False, 'elapsed_sec': round(elapsed, 2), 'hermes_called': True, 'error': repr(e)})
            write_json(REPORT, {'ok': False, 'stage': 'chunk', 'chunk_reports': chunk_reports, 'hermes_calls': hermes_calls})
            raise
        elapsed = time.time() - started
        (cdir / f"{chunk['chunk_id']}.raw.txt").write_text(raw, encoding='utf-8')
        validate_schema(out, 'transcript.schema.json')
        ok, warnings = validate_correction_chunk_output(raw_primary_segments=primary, output=out)
        if not ok:
            raise RuntimeError(f"validation failed {chunk['chunk_id']}: {warnings}")
        chunk['status'] = 'passed'
        chunk['attempts'] = 1
        stored = {'schema_version': 'vocanote.correction_chunk.v1', 'input_hash': chunk['input_hash'], 'attempts': 1, 'chunk': chunk, 'output': out, 'warnings': warnings}
        write_json(cpath, stored)
        outputs.append(out)
        chunk_reports.append({**chunk, 'reused': reused, 'elapsed_sec': round(elapsed, 2), 'warnings': warnings, 'hermes_called': True})
    write_json(cdir / 'manifest.json', manifest)
    merged = merge_correction_chunks(RID, outputs)
    validate_schema(merged, 'transcript.schema.json')
    clean_segments = merged['segments']
    raw_indices = [int(s['index']) for s in raw_segments]
    clean_indices = [int(s['index']) for s in clean_segments]
    validation = {
        'raw_segment_count': len(raw_segments),
        'clean_segment_count': len(clean_segments),
        'same_count': len(raw_segments) == len(clean_segments),
        'same_order': raw_indices == clean_indices,
        'duplicate_segments': len(clean_indices) != len(set(clean_indices)),
        'missing_corrected_text': [int(s['index']) for s in clean_segments if str(s.get('corrected_text') or '').strip() == ''],
        'overlap_duplicate_present': len(clean_indices) != len(set(clean_indices)),
    }
    validation['ok'] = validation['same_count'] and validation['same_order'] and not validation['duplicate_segments'] and not validation['missing_corrected_text']
    if not validation['ok']:
        raise RuntimeError(f'merge validation failed: {validation}')
    write_json(D / 'transcript_clean.json', merged)
    write_json(D / 'segments_clean.json', {'schema_version': 'vocanote.segments_clean.v1', 'recording_id': RID, 'segments': [
        {'index': s['index'], 'speaker': s['speaker'], 'start': s['start'], 'end': s['end'], 'text': s['corrected_text'], 'raw_text': s['raw_text'], 'uncertain': s['uncertain']}
        for s in clean_segments
    ]})
    (D / 'transcript_clean.txt').write_text('\n'.join(str(s.get('corrected_text') or '').strip() for s in clean_segments) + '\n', encoding='utf-8')
    report = {
        'ok': True,
        'recording_id': RID,
        'reuse_only': reuse_only,
        'hermes_calls': hermes_calls,
        'chunk_count': len(manifest['chunks']),
        'chunk_reports': chunk_reports,
        'validation': validation,
        'outputs': {
            'transcript_clean_json': str(D / 'transcript_clean.json'),
            'transcript_clean_txt': str(D / 'transcript_clean.txt'),
            'segments_clean_json': str(D / 'segments_clean.json'),
            'manifest': str(cdir / 'manifest.json'),
        }
    }
    write_json(REPORT if not reuse_only else D / 'phase1_correction_reuse_report.json', report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    run(reuse_only='--reuse-only' in sys.argv)

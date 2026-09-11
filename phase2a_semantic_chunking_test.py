#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import jsonschema

from vocanote_chunking import build_semantic_chunks, write_semantic_chunks, stable_json

BASE = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def make_segments(n: int = 1000) -> list[dict[str, Any]]:
    segments = []
    for i in range(n):
        topic = ['피부장벽', '콜라겐', '염증', '수면', '운동'][i % 5]
        text = f"세그먼트 {i:04d} 내용입니다. {topic} 관련 핵심 발화이며 검증용 문장입니다."
        segments.append({
            'index': i,
            'speaker': 'S1' if i % 3 else 'S2',
            'start': round(i * 2.4, 3),
            'end': round(i * 2.4 + 1.8, 3),
            'raw_text': text,
            'corrected_text': text,
            'uncertain': False,
        })
    return segments


def validate_artifacts(run_dir: Path, manifest: dict[str, Any]) -> tuple[bool, list[str]]:
    errors = []
    manifest_schema = load_json(BASE / 'semantic_manifest.schema.json')
    chunk_schema = load_json(BASE / 'semantic_chunk.schema.json')
    try:
        jsonschema.validate(instance=manifest, schema=manifest_schema)
    except Exception as exc:
        errors.append(f'manifest_schema_error={exc}')
    for row in manifest['chunks']:
        path = run_dir / 'semantic_chunks' / row['path']
        try:
            chunk = load_json(path)
            jsonschema.validate(instance=chunk, schema=chunk_schema)
        except Exception as exc:
            errors.append(f"chunk_schema_error {row['chunk_id']}={exc}")
    return not errors, errors


def main() -> int:
    started = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_dir = BASE / 'validation_runs' / f'phase2a_semantic_chunking_{started}'
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)

    recording_id = 'phase2a-synthetic-1000'
    segments = make_segments(1000)
    clean_transcript = {
        'schema_version': 'vocanote.transcript.v1',
        'recording_id': recording_id,
        'language': 'ko',
        'segments': segments,
        'warnings': [],
    }
    global_context = {
        'schema_version': 'vocanote.global_context.v1',
        'method': 'synthetic',
        'glossary_candidates': [{'term': '피부장벽', 'count': 200}],
        'note': 'phase2a deterministic test context',
    }
    model_config = {'temperature': 0, 'max_output_tokens': 4096}
    target_chars = 1000
    max_chars = 1200

    # Build and write Phase 2A artifacts. This must not call Hermes.
    manifest_written = write_semantic_chunks(
        run_dir,
        recording_id=recording_id,
        clean_transcript=clean_transcript,
        global_context=global_context,
        target_chars=target_chars,
        max_chars=max_chars,
        reduce_max_chars=12000,
        reduce_group_max_chars=24000,
        overlap_segments=2,
        provider='openai-codex',
        model='gpt-5.5',
        model_config=model_config,
    )
    manifest_path = run_dir / 'semantic_chunks' / 'manifest.json'
    manifest = load_json(manifest_path)

    manifest2, chunks2 = build_semantic_chunks(
        recording_id=recording_id,
        clean_segments=segments,
        global_context=global_context,
        target_chars=target_chars,
        max_chars=max_chars,
        reduce_max_chars=12000,
        reduce_group_max_chars=24000,
        overlap_segments=2,
        provider='openai-codex',
        model='gpt-5.5',
        model_config=model_config,
    )
    manifest3, chunks3 = build_semantic_chunks(
        recording_id=recording_id,
        clean_segments=segments,
        global_context=global_context,
        target_chars=target_chars,
        max_chars=max_chars,
        reduce_max_chars=12000,
        reduce_group_max_chars=24000,
        overlap_segments=2,
        provider='openai-codex',
        model='gpt-5.5',
        model_config=model_config,
    )

    primary_ids = []
    overlap_context_only = True
    max_primary_chars = 0
    primary_ranges = []
    overlap_ranges = []
    for row in manifest['chunks']:
        chunk = load_json(run_dir / 'semantic_chunks' / row['path'])
        primary_ids.extend(chunk['primary']['source_segment_ids'])
        overlap_context_only = overlap_context_only and chunk['overlap_context']['context_only'] and chunk['semantic_extraction_scope'] == 'PRIMARY_ONLY'
        max_primary_chars = max(max_primary_chars, chunk['primary']['text_chars'])
        primary_ranges.append([chunk['primary']['range']['start_segment_id'], chunk['primary']['range']['end_segment_id']])
        overlap_ranges.append([chunk['overlap_context']['range']['start_segment_id'], chunk['overlap_context']['range']['end_segment_id']])

    primary_duplicate_free = len(primary_ids) == len(set(primary_ids))
    all_primary_once = primary_ids == list(range(1000))
    hard_max_ok = max_primary_chars <= max_chars
    hash_determinism = [c['input_hash'] for c in chunks2] == [c['input_hash'] for c in chunks3]

    # Mutate a segment well inside one chunk, outside adjacent overlap windows.
    base_hashes = [c['input_hash'] for c in chunks2]
    chosen_chunk_idx = min(3, len(chunks2) - 1)
    chosen_ids = chunks2[chosen_chunk_idx]['primary']['source_segment_ids']
    chosen_segment_id = chosen_ids[len(chosen_ids)//2]
    mutated_segments = [dict(s) for s in segments]
    mutated_segments[chosen_segment_id] = dict(mutated_segments[chosen_segment_id])
    mutated_segments[chosen_segment_id]['corrected_text'] += ' X'
    _m_manifest, mutated_chunks = build_semantic_chunks(
        recording_id=recording_id,
        clean_segments=mutated_segments,
        global_context=global_context,
        target_chars=target_chars,
        max_chars=max_chars,
        reduce_max_chars=12000,
        reduce_group_max_chars=24000,
        overlap_segments=2,
        provider='openai-codex',
        model='gpt-5.5',
        model_config=model_config,
    )
    changed_by_mutation = [i for i, (a, b) in enumerate(zip(base_hashes, [c['input_hash'] for c in mutated_chunks])) if a != b]
    single_segment_mutation_ok = changed_by_mutation == [chosen_chunk_idx]

    changed_context = dict(global_context)
    changed_context['note'] = 'phase2a changed global context'
    _g_manifest, global_changed_chunks = build_semantic_chunks(
        recording_id=recording_id,
        clean_segments=segments,
        global_context=changed_context,
        target_chars=target_chars,
        max_chars=max_chars,
        reduce_max_chars=12000,
        reduce_group_max_chars=24000,
        overlap_segments=2,
        provider='openai-codex',
        model='gpt-5.5',
        model_config=model_config,
    )
    global_changed = [i for i, (a, b) in enumerate(zip(base_hashes, [c['input_hash'] for c in global_changed_chunks])) if a != b]
    global_context_invalidation_ok = len(global_changed) == len(base_hashes)

    schema_ok, schema_errors = validate_artifacts(run_dir, manifest)

    checks = {
        'segment_count_1000': len(segments) == 1000,
        'primary_duplicate_free': primary_duplicate_free,
        'overlap_context_only': overlap_context_only,
        'all_primary_segments_exactly_once': all_primary_once,
        'chunk_size_hard_max_ok': hard_max_ok,
        'input_hash_deterministic': hash_determinism,
        'primary_text_mutation_changed_only_target_chunk': single_segment_mutation_ok,
        'other_chunk_hashes_not_unnecessarily_changed': single_segment_mutation_ok,
        'global_context_hash_invalidation': global_context_invalidation_ok,
        'manifest_schema_validation': schema_ok,
    }
    report = {
        'ok': all(checks.values()),
        'phase': 'Phase 2A semantic chunking',
        'run_dir': str(run_dir),
        'manifest_path': str(manifest_path),
        'chunk_schema_path': str(BASE / 'semantic_chunk.schema.json'),
        'manifest_schema_path': str(BASE / 'semantic_manifest.schema.json'),
        'synthetic_segment_count': len(segments),
        'chunk_count': manifest['chunk_count'],
        'primary_ranges': primary_ranges,
        'overlap_ranges': overlap_ranges,
        'max_chunk_chars': max_primary_chars,
        'hash_determinism_result': hash_determinism,
        'single_segment_mutation': {
            'chosen_segment_id': chosen_segment_id,
            'chosen_chunk_id': chunks2[chosen_chunk_idx]['chunk_id'],
            'changed_chunk_indexes': changed_by_mutation,
            'ok': single_segment_mutation_ok,
        },
        'global_context_invalidation': {
            'changed_chunk_count': len(global_changed),
            'total_chunk_count': len(base_hashes),
            'ok': global_context_invalidation_ok,
        },
        'schema_errors': schema_errors,
        'checks': checks,
        'manifest_validation_result': schema_ok,
    }
    report_path = run_dir / 'phase2a_semantic_chunking_report.json'
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    latest = BASE / 'validation_runs' / 'phase2a_latest_report.json'
    latest.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({
        'ok': report['ok'],
        'report_path': str(report_path),
        'manifest_path': str(manifest_path),
        'chunk_count': manifest['chunk_count'],
        'max_chunk_chars': max_primary_chars,
        'checks': checks,
    }, ensure_ascii=False, indent=2))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import jsonschema

from vocanote_chunking import build_semantic_chunks, sha256_obj, stable_json, write_semantic_chunks
from vocanote_semantic_executor import FakeSemanticExecutor, HermesSemanticExecutor, run_semantic_map, SemanticChunkError
from vocanote_tombstone import create_tombstone

BASE = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server').resolve()
RUN_ROOT = BASE / 'validation_runs'


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def make_segments(n: int = 1000) -> list[dict[str, Any]]:
    segs = []
    for i in range(n):
        topic = ['피부장벽', '콜라겐', '염증', '수면', '운동'][i % 5]
        text = f"세그먼트 {i:04d} 내용입니다. {topic} 관련 핵심 발화이며 검증용 문장입니다."
        segs.append({'index': i, 'speaker': 'S1', 'start': round(i * 2.0, 3), 'end': round(i * 2.0 + 1.5, 3), 'raw_text': text, 'corrected_text': text, 'uncertain': False})
    return segs


def init_recording(parent: Path, *, n: int = 1000, target_chars: int = 1000, max_chars: int = 1200, global_note: str = 'phase2b deterministic context') -> tuple[Path, str, list[dict[str, Any]], dict[str, Any]]:
    rid = str(uuid.uuid4())
    d = parent / rid
    d.mkdir(parents=True)
    write_json(d / 'metadata.json', {'recording_id': rid, 'title': 'Phase 2B synthetic', 'language': 'ko'})
    segments = make_segments(n)
    clean = {'schema_version': 'vocanote.transcript.v1', 'recording_id': rid, 'language': 'ko', 'segments': segments, 'warnings': []}
    context = {'schema_version': 'vocanote.global_context.v1', 'method': 'synthetic', 'note': global_note, 'glossary_candidates': [{'term': '피부장벽', 'count': 10}]}
    write_semantic_chunks(d, recording_id=rid, clean_transcript=clean, global_context=context, target_chars=target_chars, max_chars=max_chars, reduce_max_chars=12000, reduce_group_max_chars=24000, overlap_segments=2, provider='openai-codex', model='gpt-5.5', model_config={'temperature': 0})
    return d, rid, segments, context


def refresh_changed_chunks(recording_dir: Path, rid: str, segments: list[dict[str, Any]], context: dict[str, Any], *, target_chars: int = 1000, max_chars: int = 1200) -> list[str]:
    manifest_path = recording_dir / 'semantic_chunks' / 'manifest.json'
    old_manifest = load_json(manifest_path)
    new_manifest, new_chunks = build_semantic_chunks(recording_id=rid, clean_segments=segments, global_context=context, target_chars=target_chars, max_chars=max_chars, reduce_max_chars=12000, reduce_group_max_chars=24000, overlap_segments=2, provider='openai-codex', model='gpt-5.5', model_config={'temperature': 0})
    changed = []
    old_rows = {r['chunk_id']: r for r in old_manifest['chunks']}
    merged_rows = []
    for row, chunk in zip(new_manifest['chunks'], new_chunks):
        old = old_rows.get(row['chunk_id'], {})
        if old.get('input_hash') == row['input_hash']:
            keep = {**row}
            for k in ['status', 'attempts', 'output_path', 'output_hash', 'completed_at', 'last_error', 'last_error_at']:
                if k in old:
                    keep[k] = old[k]
            merged_rows.append(keep)
        else:
            changed.append(row['chunk_id'])
            merged_rows.append({**row, 'status': old.get('status', 'pending'), 'attempts': old.get('attempts', 0), 'output_path': old.get('output_path'), 'output_hash': old.get('output_hash')})
        (recording_dir / 'semantic_chunks' / f"{chunk['chunk_id']}.json").write_text(stable_json(chunk) + '\n', encoding='utf-8')
    old_manifest.update(new_manifest)
    old_manifest['chunks'] = merged_rows
    (recording_dir / 'semantic_chunks' / 'manifest.json').write_text(stable_json(old_manifest) + '\n', encoding='utf-8')
    return changed


def count_completed(d: Path) -> int:
    return sum(1 for r in load_json(d / 'semantic_chunks' / 'manifest.json')['chunks'] if r.get('status') == 'completed')


def validate_manifest_and_outputs(d: Path) -> bool:
    manifest_schema = load_json(BASE / 'semantic_manifest.schema.json')
    output_schema = load_json(BASE / 'semantic_chunk_output.schema.json')
    manifest = load_json(d / 'semantic_chunks' / 'manifest.json')
    jsonschema.validate(instance=manifest, schema=manifest_schema)
    for row in manifest['chunks']:
        if row.get('status') == 'completed':
            out = load_json(d / 'semantic_chunks' / row['output_path'])
            jsonschema.validate(instance=out, schema=output_schema)
    return True


def test_normal(run_dir: Path) -> dict[str, Any]:
    d, rid, _segments, _ctx = init_recording(run_dir / 'normal')
    ex = FakeSemanticExecutor()
    stats = run_semantic_map(recording_dir=d, recording_id=rid, executor=ex)
    return {'ok': stats['completed'] == 44 and len(ex.calls) == 44 and count_completed(d) == 44 and validate_manifest_and_outputs(d), 'chunk_count': 44, 'completed': count_completed(d), 'executor_calls': len(ex.calls)}


def test_resume(run_dir: Path) -> dict[str, Any]:
    d, rid, _segments, _ctx = init_recording(run_dir / 'resume')
    ex1 = FakeSemanticExecutor()
    stats1 = run_semantic_map(recording_dir=d, recording_id=rid, executor=ex1, stop_after_completed=17)
    first_completed = count_completed(d)
    ex2 = FakeSemanticExecutor()
    stats2 = run_semantic_map(recording_dir=d, recording_id=rid, executor=ex2)
    return {'ok': first_completed == 17 and len(ex2.calls) == 27 and ex2.calls[0] == 'chunk_0018' and count_completed(d) == 44, 'first_run_completed': first_completed, 'rerun_new_executor_calls': len(ex2.calls), 'rerun_first_called': ex2.calls[0] if ex2.calls else None, 'reused': stats2['reused']}


def test_retry(run_dir: Path) -> dict[str, Any]:
    d, rid, _segments, _ctx = init_recording(run_dir / 'retry')
    ex = FakeSemanticExecutor(fail_once={'chunk_0007'})
    stats1 = run_semantic_map(recording_dir=d, recording_id=rid, executor=ex)
    pending_after_first = [r['chunk_id'] for r in load_json(d / 'semantic_chunks' / 'manifest.json')['chunks'] if r.get('status') != 'completed']
    ex2 = FakeSemanticExecutor()
    stats2 = run_semantic_map(recording_dir=d, recording_id=rid, executor=ex2)
    return {'ok': pending_after_first == ['chunk_0007'] and ex2.calls == ['chunk_0007'] and count_completed(d) == 44, 'first_failed_attempts': stats1['failed_attempts'], 'retry_count': stats1['retry_count'] + stats2['retry_count'], 'second_calls': ex2.calls}


def test_corruption(run_dir: Path) -> dict[str, Any]:
    d, rid, _segments, _ctx = init_recording(run_dir / 'corrupt')
    run_semantic_map(recording_dir=d, recording_id=rid, executor=FakeSemanticExecutor())
    manifest = load_json(d / 'semantic_chunks' / 'manifest.json')
    row = manifest['chunks'][7]
    (d / 'semantic_chunks' / row['output_path']).write_text('{bad json', encoding='utf-8')
    ex = FakeSemanticExecutor()
    stats = run_semantic_map(recording_dir=d, recording_id=rid, executor=ex)
    return {'ok': ex.calls == ['chunk_0008'] and count_completed(d) == 44, 'reexecuted': ex.calls, 'invalidated': stats['invalidated']}


def test_primary_mutation(run_dir: Path) -> dict[str, Any]:
    d, rid, segments, ctx = init_recording(run_dir / 'mutation')
    run_semantic_map(recording_dir=d, recording_id=rid, executor=FakeSemanticExecutor())
    segments[80] = dict(segments[80])
    segments[80]['corrected_text'] += ' X'
    changed = refresh_changed_chunks(d, rid, segments, ctx)
    ex = FakeSemanticExecutor()
    stats = run_semantic_map(recording_dir=d, recording_id=rid, executor=ex)
    return {'ok': changed == ['chunk_0004'] and ex.calls == ['chunk_0004'] and count_completed(d) == 44, 'changed_chunks': changed, 'executor_calls': ex.calls, 'invalidated': stats['invalidated']}


def test_global_context(run_dir: Path) -> dict[str, Any]:
    d, rid, segments, ctx = init_recording(run_dir / 'global')
    run_semantic_map(recording_dir=d, recording_id=rid, executor=FakeSemanticExecutor())
    ctx = dict(ctx)
    ctx['note'] = 'changed global context'
    changed = refresh_changed_chunks(d, rid, segments, ctx)
    ex = FakeSemanticExecutor()
    stats = run_semantic_map(recording_dir=d, recording_id=rid, executor=ex)
    return {'ok': len(changed) == 44 and len(ex.calls) == 44 and count_completed(d) == 44, 'changed_chunks': len(changed), 'executor_calls': len(ex.calls), 'invalidated': stats['invalidated']}


def test_tombstone(run_dir: Path) -> dict[str, Any]:
    d, rid, _segments, _ctx = init_recording(run_dir / 'tombstone')
    class TombstoneExecutor(FakeSemanticExecutor):
        def execute(self, chunk: dict[str, Any]) -> dict[str, Any]:
            out = super().execute(chunk)
            create_tombstone(rid, reason='phase2b fault injection', original_path=str(d), request_source='phase2b-test')
            return out
    ex = TombstoneExecutor()
    try:
        run_semantic_map(recording_dir=d, recording_id=rid, executor=ex)
        ok = False
        err = None
    except Exception as exc:
        ok = 'recording_deleted' in repr(exc) or 'RecordingDeleted' in type(exc).__name__
        err = repr(exc)
    resurrect_outputs = list((d / 'semantic_chunks' / 'outputs').glob('*.json')) if (d / 'semantic_chunks' / 'outputs').exists() else []
    return {'ok': ok and not resurrect_outputs, 'error': err, 'output_files_after_tombstone': len(resurrect_outputs), 'executor_calls': len(ex.calls)}


def test_hermes_small(run_dir: Path) -> dict[str, Any]:
    d, rid, _segments, _ctx = init_recording(run_dir / 'hermes', n=18, target_chars=260, max_chars=400)
    chunk_count = load_json(d / 'semantic_chunks' / 'manifest.json')['chunk_count']
    ex = HermesSemanticExecutor(timeout=180)
    stats1 = run_semantic_map(recording_dir=d, recording_id=rid, executor=ex)
    ex2 = HermesSemanticExecutor(timeout=180)
    stats2 = run_semantic_map(recording_dir=d, recording_id=rid, executor=ex2)
    return {'ok': count_completed(d) == chunk_count and len(ex.calls) == chunk_count and len(ex2.calls) == 0, 'chunk_count': chunk_count, 'success_count': count_completed(d), 'first_calls': len(ex.calls), 'cache_reuse': stats2['reused'], 'second_calls': len(ex2.calls)}


def main() -> int:
    run_dir = RUN_ROOT / ('phase2b_semantic_executor_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)
    tests = {}
    for name, fn in [
        ('normal_map', test_normal),
        ('resume', test_resume),
        ('retry', test_retry),
        ('corruption', test_corruption),
        ('single_primary_mutation', test_primary_mutation),
        ('global_context_invalidation', test_global_context),
        ('tombstone_fencing', test_tombstone),
    ]:
        tests[name] = fn(run_dir)
    # Fake tests must pass before Hermes integration.
    fake_ok = all(v.get('ok') for v in tests.values())
    if fake_ok:
        try:
            tests['small_hermes_integration'] = test_hermes_small(run_dir)
        except Exception as exc:
            tests['small_hermes_integration'] = {'ok': False, 'error': repr(exc)}
    else:
        tests['small_hermes_integration'] = {'ok': False, 'error': 'skipped because fake tests failed'}
    ok = all(v.get('ok') for v in tests.values())
    report = {
        'ok': ok,
        'phase': 'Phase 2B semantic executor',
        'run_dir': str(run_dir),
        'semantic_output_schema_path': str(BASE / 'semantic_chunk_output.schema.json'),
        'executor_interface_file': str(BASE / 'vocanote_semantic_executor.py'),
        'synthetic_chunk_count': 44,
        'tests': tests,
    }
    report_path = run_dir / 'phase2b_semantic_executor_report.json'
    write_json(report_path, report)
    write_json(RUN_ROOT / 'phase2b_latest_report.json', report)
    print(json.dumps({'ok': ok, 'report_path': str(report_path), 'tests': tests}, ensure_ascii=False, indent=2))
    return 0 if ok else 1


if __name__ == '__main__':
    raise SystemExit(main())

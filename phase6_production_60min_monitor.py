#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SERVER_ROOT = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/meeting_recorder_server')
RECORDINGS_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱/recordings')
RUN_ROOT = SERVER_ROOT / 'validation_runs'
STARTED_AT = datetime.now(timezone.utc)
START_EPOCH = time.time()
TIMEOUT_SECONDS = int(os.environ.get('PHASE6_MONITOR_TIMEOUT_SECONDS', str(6 * 60 * 60)))
POLL_SECONDS = int(os.environ.get('PHASE6_MONITOR_POLL_SECONDS', '30'))
TARGET_RECORDING_ID = os.environ.get('PHASE6_RECORDING_ID', '').strip()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')


def is_candidate_dir(d: Path) -> bool:
    if not d.is_dir() or d.name == '.trash' or d.name.startswith('.'):
        return False
    meta = d / 'metadata.json'
    if not meta.exists():
        return False
    # Candidate must be created/modified after monitor start. This avoids old recordings.
    return max(d.stat().st_mtime, meta.stat().st_mtime) >= START_EPOCH - 5


def find_new_recording() -> Path | None:
    candidates = [d for d in RECORDINGS_DIR.iterdir() if is_candidate_dir(d)] if RECORDINGS_DIR.exists() else []
    if not candidates:
        return None
    return max(candidates, key=lambda p: max(p.stat().st_mtime, (p / 'metadata.json').stat().st_mtime))


def count_segments(path: Path) -> int | None:
    data = load_json(path)
    if not data:
        return None
    segs = data.get('segments')
    return len(segs) if isinstance(segs, list) else None


def segment_ids_ok(path: Path) -> dict[str, Any]:
    data = load_json(path) or {}
    segs = data.get('segments') or []
    ids = []
    for i, s in enumerate(segs):
        if isinstance(s, dict):
            ids.append(s.get('id', s.get('index', i)))
    return {
        'count': len(ids),
        'duplicate_count': len(ids) - len(set(ids)),
        'missing_like_count': 0 if len(ids) == len(segs) else len(segs) - len(ids),
    }


def list_files(d: Path, pattern: str) -> list[str]:
    return sorted(str(p.relative_to(d)) for p in d.glob(pattern) if p.exists())


def collect_recording_report(d: Path, detected_at: str, completed_at: str | None = None) -> dict[str, Any]:
    rid = d.name
    meta = load_json(d / 'metadata.json') or {}
    status = load_json(d / 'status.json') or {}
    semantic_manifest = load_json(d / 'semantic_chunks' / 'manifest.json') or {}
    reduce_manifest = load_json(d / 'semantic_reduce' / 'manifest.json') or {}
    semantic_final = load_json(d / 'semantic_final.json') or {}
    result = load_json(d / 'result.validated.json') or {}
    audio_candidates = [d / 'audio.m4a', d / 'audio.mp4', d / 'audio.wav']
    audio = next((p for p in audio_candidates if p.exists()), None)
    chunks = semantic_manifest.get('chunks') or []
    groups = reduce_manifest.get('groups') or {}
    output_files = list_files(d, 'semantic_chunks/outputs/*.json') + list_files(d, 'semantic_chunks/output*.json')
    final_items = []
    for key in ['key_points', 'decisions', 'action_items', 'questions', 'topics']:
        val = semantic_final.get(key) or []
        if isinstance(val, list):
            final_items.extend([(key, x) for x in val[:5] if isinstance(x, dict)])
    provenance_samples = []
    clean_data = load_json(d / 'segments_clean.json') or load_json(d / 'transcript_clean.json') or {}
    clean_segments = clean_data.get('segments') or []
    by_id = {}
    for i, seg in enumerate(clean_segments):
        if isinstance(seg, dict):
            sid = seg.get('id', seg.get('index', i))
            by_id[sid] = seg
    for key, item in final_items[:8]:
        sids = item.get('source_segment_ids') or []
        provenance_samples.append({
            'type': key,
            'text': item.get('text', ''),
            'source_segment_ids': sids,
            'all_ids_exist': all(sid in by_id for sid in sids),
            'source_text_sample': [by_id.get(sid, {}).get('text', '')[:160] for sid in sids[:3]],
        })
    return {
        'recording_id': rid,
        'production_like': True,
        'android_version': 'actual Android device - user/ADB confirmation pending',
        'app_version': '0.2.6',
        'cloud_archive': 'OFF / NOT TESTED',
        'semantic_executor': os.environ.get('VOCANOTE_SEMANTIC_EXECUTOR', 'hermes/unset_check_required'),
        'fault_injection_enabled': os.environ.get('VOCANOTE_FAULT_INJECTION_ENABLED', 'unset'),
        'test_mode': os.environ.get('VOCANOTE_TEST_MODE', 'unset'),
        'detected_at': detected_at,
        'completed_at': completed_at,
        'metadata_recorded_at': meta.get('recorded_at'),
        'status': status,
        'audio_file': str(audio) if audio else None,
        'android_audio_file_size': audio.stat().st_size if audio else None,
        'raw_segment_count': count_segments(d / 'segments_raw.json'),
        'clean_segment_count': count_segments(d / 'segments_clean.json') or count_segments(d / 'transcript_clean.json'),
        'raw_segment_ids': segment_ids_ok(d / 'segments_raw.json') if (d / 'segments_raw.json').exists() else None,
        'clean_segment_ids': segment_ids_ok(d / 'segments_clean.json') if (d / 'segments_clean.json').exists() else None,
        'correction_chunk_count': len(list((d / 'correction_chunks').glob('*.json'))) if (d / 'correction_chunks').exists() else None,
        'semantic_chunk_count': len(chunks) if isinstance(chunks, list) else None,
        'semantic_completed_count': sum(1 for c in chunks if isinstance(c, dict) and c.get('status') == 'completed') if isinstance(chunks, list) else None,
        'semantic_group_count': len(groups) if isinstance(groups, dict) else None,
        'semantic_output_files': len(output_files),
        'semantic_final_exists': (d / 'semantic_final.json').exists(),
        'result_validated_exists': (d / 'result.validated.json').exists(),
        'summary_md_exists': (d / 'summary.md').exists(),
        'analysis_md_exists': (d / 'analysis.md').exists(),
        'provenance_samples': provenance_samples,
        'schema_validation': 'pending post-completion validation script',
        'renderer_validation': {'summary_md': (d / 'summary.md').exists(), 'analysis_md': (d / 'analysis.md').exists()},
    }


def main() -> int:
    run_dir = RUN_ROOT / ('phase6_production_60min_e2e_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
    run_dir.mkdir(parents=True, exist_ok=True)
    progress_path = run_dir / 'phase6_progress.json'
    report_path = run_dir / 'production_60min_e2e_report.json'
    latest_path = RUN_ROOT / 'phase6_latest_report.json'
    initial = {
        'ok': False,
        'state': 'waiting_for_android_recording_upload',
        'monitor_started_at': STARTED_AT.isoformat(),
        'production_like': True,
        'requirements': {
            'fake_semantic_executor': 'forbidden',
            'synthetic_transcript': 'forbidden',
            'fault_injection': 'forbidden',
            'test_mode': 'forbidden',
            'worker_once': 'forbidden',
        },
        'env': {
            'VOCANOTE_SEMANTIC_EXECUTOR': os.environ.get('VOCANOTE_SEMANTIC_EXECUTOR', 'unset'),
            'VOCANOTE_FAULT_INJECTION_ENABLED': os.environ.get('VOCANOTE_FAULT_INJECTION_ENABLED', 'unset'),
            'VOCANOTE_TEST_MODE': os.environ.get('VOCANOTE_TEST_MODE', 'unset'),
        },
        'cloud_archive': 'OFF / NOT TESTED',
    }
    write_json(progress_path, initial)

    detected = None
    detected_at = None
    if TARGET_RECORDING_ID:
        target_dir = RECORDINGS_DIR / TARGET_RECORDING_ID
        if target_dir.exists():
            detected = target_dir
            detected_at = now_iso()
        else:
            final = initial | {'state': 'target_recording_not_found', 'ok': False, 'recording_id': TARGET_RECORDING_ID, 'ended_at': now_iso()}
            write_json(report_path, final); write_json(latest_path, final)
            print(json.dumps({'ok': False, 'state': final['state'], 'recording_id': TARGET_RECORDING_ID, 'report_path': str(report_path)}, ensure_ascii=False, indent=2))
            return 2
    while detected is None and time.time() - START_EPOCH < TIMEOUT_SECONDS:
        d = find_new_recording()
        if d:
            detected = d
            detected_at = now_iso()
            break
        time.sleep(POLL_SECONDS)
    if not detected:
        final = initial | {'state': 'timeout_no_new_android_recording_detected', 'ok': False, 'ended_at': now_iso()}
        write_json(report_path, final); write_json(latest_path, final)
        print('PHASE6_MONITOR_TIMEOUT no new recording detected')
        return 2

    while time.time() - START_EPOCH < TIMEOUT_SECONDS:
        rep = collect_recording_report(detected, detected_at or now_iso())
        status = rep.get('status') or {}
        completed = bool(rep['semantic_final_exists'] and rep['result_validated_exists'] and rep['summary_md_exists'] and rep['analysis_md_exists']) or status.get('status') == 'completed'
        rep.update({'ok': False, 'state': 'processing' if not completed else 'completed_candidate', 'run_dir': str(run_dir)})
        write_json(progress_path, rep)
        if completed:
            rep = collect_recording_report(detected, detected_at or now_iso(), completed_at=now_iso())
            pass_checks = bool(rep['semantic_final_exists'] and rep['result_validated_exists'] and rep['summary_md_exists'] and rep['analysis_md_exists'])
            rep.update({'ok': pass_checks, 'state': 'completed' if pass_checks else 'completed_with_missing_artifacts', 'run_dir': str(run_dir)})
            write_json(report_path, rep); write_json(latest_path, rep)
            print(json.dumps({'ok': rep['ok'], 'recording_id': rep['recording_id'], 'report_path': str(report_path)}, ensure_ascii=False, indent=2))
            return 0 if rep['ok'] else 1
        time.sleep(POLL_SECONDS)

    final = collect_recording_report(detected, detected_at or now_iso())
    final.update({'ok': False, 'state': 'timeout_processing_not_complete', 'run_dir': str(run_dir), 'ended_at': now_iso()})
    write_json(report_path, final); write_json(latest_path, final)
    print(json.dumps({'ok': False, 'state': final['state'], 'recording_id': final.get('recording_id'), 'report_path': str(report_path)}, ensure_ascii=False, indent=2))
    return 3


if __name__ == '__main__':
    raise SystemExit(main())

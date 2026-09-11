#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CHUNK_SCHEMA_VERSION = 'vocanote.chunk_manifest.v1'
CORRECTION_PROMPT_VERSION = 'vocanote.correction_prompt.chunked.v1'
SEMANTIC_PROMPT_VERSION = 'vocanote.semantic_prompt.chunked.v1'
SEMANTIC_CHUNK_SCHEMA_VERSION = 'vocanote.semantic_chunk.v1'
SEMANTIC_MANIFEST_SCHEMA_VERSION = 'vocanote.semantic_manifest.v1'
DEFAULT_SEMANTIC_CHUNK_TARGET_CHARS = 6000
DEFAULT_SEMANTIC_CHUNK_MAX_CHARS = 8000
DEFAULT_SEMANTIC_REDUCE_MAX_CHARS = 12000
DEFAULT_SEMANTIC_REDUCE_GROUP_MAX_CHARS = 24000
DEFAULT_TARGET_CHARS = 1200
DEFAULT_MAX_DURATION_SEC = 60
DEFAULT_MAX_SEGMENTS = 35
DEFAULT_OVERLAP_SEGMENTS = 2


def stable_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def sha256_obj(data: Any) -> str:
    return hashlib.sha256(stable_json(data).encode('utf-8')).hexdigest()


def seg_text(seg: dict[str, Any]) -> str:
    return str(seg.get('text') or seg.get('corrected_text') or seg.get('raw_text') or '').strip()


def segment_fingerprint(seg: dict[str, Any]) -> dict[str, Any]:
    return {
        'index': int(seg['index']),
        'speaker': str(seg.get('speaker') or 'S1'),
        'start': float(seg.get('start') or 0.0),
        'end': float(seg.get('end') or 0.0),
        'text': seg_text(seg),
    }


def context_hash(context: dict[str, Any] | None) -> str:
    return sha256_obj(context or {})


def build_chunks(
    segments: list[dict[str, Any]],
    *,
    target_chars: int = DEFAULT_TARGET_CHARS,
    max_duration_sec: int = DEFAULT_MAX_DURATION_SEC,
    max_segments: int = DEFAULT_MAX_SEGMENTS,
    overlap_segments: int = DEFAULT_OVERLAP_SEGMENTS,
    prompt_version: str = CORRECTION_PROMPT_VERSION,
    model_id: str,
    provider_id: str,
    global_context: dict[str, Any] | None = None,
    rolling_contexts: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Create deterministic primary/overlap chunks.

    primary_segment_range is the only range whose output may be persisted.
    overlap_context_range is context-only and must be ignored on merge.
    """
    chunks: list[dict[str, Any]] = []
    n = len(segments)
    i = 0
    chunk_no = 1
    ghash = context_hash(global_context)
    while i < n:
        start_i = i
        chars = 0
        j = i
        first_start = float(segments[start_i].get('start') or 0.0)
        while j < n:
            candidate_end = float(segments[j].get('end') or segments[j].get('start') or first_start)
            duration = candidate_end - first_start
            if j > start_i and chars >= target_chars:
                break
            if j > start_i and duration >= max_duration_sec:
                break
            if j > start_i and (j - start_i) >= max_segments:
                break
            chars += len(seg_text(segments[j])) + 1
            j += 1
        # keep at least one segment
        end_i = max(j - 1, start_i)
        primary = segments[start_i:end_i + 1]
        overlap_start = max(0, start_i - overlap_segments)
        overlap_end = min(n - 1, end_i + overlap_segments)
        chunk_id = f'chunk_{chunk_no:04d}'
        rolling = (rolling_contexts or {}).get(chunk_id, {})
        payload = {
            'chunk_schema_version': CHUNK_SCHEMA_VERSION,
            'prompt_version': prompt_version,
            'model_id': model_id,
            'provider_id': provider_id,
            'global_context_hash': ghash,
            'rolling_context_hash': context_hash(rolling),
            'primary_segments': [segment_fingerprint(s) for s in primary],
            'overlap_segments': [segment_fingerprint(s) for s in segments[overlap_start:overlap_end + 1]],
        }
        chunks.append({
            'chunk_id': chunk_id,
            'status': 'pending',
            'attempts': 0,
            'input_hash': sha256_obj(payload),
            'prompt_version': prompt_version,
            'model_id': model_id,
            'provider_id': provider_id,
            'global_context_hash': ghash,
            'rolling_context_hash': context_hash(rolling),
            'primary_segment_range': [int(primary[0]['index']), int(primary[-1]['index'])],
            'overlap_context_range': [int(segments[overlap_start]['index']), int(segments[overlap_end]['index'])],
            'primary_count': len(primary),
            'primary_text_chars': sum(len(seg_text(s)) for s in primary),
            'error': None,
        })
        i = end_i + 1
        chunk_no += 1
    return {
        'schema_version': CHUNK_SCHEMA_VERSION,
        'target_chars': target_chars,
        'max_duration_sec': max_duration_sec,
        'max_segments': max_segments,
        'overlap_segments': overlap_segments,
        'prompt_version': prompt_version,
        'model_id': model_id,
        'provider_id': provider_id,
        'global_context_hash': ghash,
        'total_segments': n,
        'chunks': chunks,
    }


def chunk_segments_for_prompt(segments: list[dict[str, Any]], chunk: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_index = {int(s['index']): s for s in segments}
    p0, p1 = chunk['primary_segment_range']
    o0, o1 = chunk['overlap_context_range']
    primary = [by_index[i] for i in range(p0, p1 + 1) if i in by_index]
    overlap = [by_index[i] for i in range(o0, o1 + 1) if i in by_index]
    return primary, overlap


def validate_correction_chunk_output(
    *,
    raw_primary_segments: list[dict[str, Any]],
    output: dict[str, Any],
    max_length_ratio: float = 3.0,
    min_length_ratio: float = 0.2,
) -> tuple[bool, list[str]]:
    warnings: list[str] = []
    out_segments = output.get('segments') or []
    if len(out_segments) != len(raw_primary_segments):
        warnings.append(f'segment_count_mismatch raw={len(raw_primary_segments)} out={len(out_segments)}')
        return False, warnings
    raw_by_pos = list(raw_primary_segments)
    seen: set[int] = set()
    for raw, out in zip(raw_by_pos, out_segments):
        raw_idx = int(raw['index'])
        out_idx = int(out.get('index', -1))
        if out_idx != raw_idx:
            warnings.append(f'index_mismatch raw={raw_idx} out={out_idx}')
        if out_idx in seen:
            warnings.append(f'duplicate_index {out_idx}')
        seen.add(out_idx)
        for key in ['start', 'end']:
            raw_val = raw.get(key)
            out_val = out.get(key)
            rv = float(raw_val if raw_val is not None else 0.0)
            ov = float(out_val if out_val is not None else -1.0)
            if abs(rv - ov) > 0.001:
                warnings.append(f'{key}_changed index={raw_idx} raw={rv} out={ov}')
        corrected = str(out.get('corrected_text') or '').strip()
        raw_text = seg_text(raw)
        if raw_text and not corrected:
            warnings.append(f'empty_corrected_text index={raw_idx}')
        if raw_text and corrected:
            ratio = len(corrected) / max(len(raw_text), 1)
            if ratio > max_length_ratio or ratio < min_length_ratio:
                warnings.append(f'length_ratio_outlier index={raw_idx} ratio={ratio:.2f}')
    ok = not any(w.startswith(('segment_count_mismatch','index_mismatch','duplicate_index','start_changed','end_changed','empty_corrected_text')) for w in warnings)
    return ok, warnings


def merge_correction_chunks(recording_id: str, chunk_outputs: list[dict[str, Any]]) -> dict[str, Any]:
    merged: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen: set[int] = set()
    for out in chunk_outputs:
        warnings.extend(str(w) for w in out.get('warnings') or [])
        for seg in out.get('segments') or []:
            idx = int(seg['index'])
            if idx in seen:
                warnings.append(f'duplicate_segment_dropped index={idx}')
                continue
            seen.add(idx)
            merged.append(seg)
    merged.sort(key=lambda s: int(s['index']))
    return {
        'schema_version': 'vocanote.transcript.v1',
        'recording_id': recording_id,
        'language': 'ko',
        'segments': merged,
        'warnings': warnings,
    }


def deterministic_global_context(segments: list[dict[str, Any]], max_terms: int = 20) -> dict[str, Any]:
    """Small deterministic context seed; later phases may replace with chunk/reduce LLM context."""
    text = '\n'.join(seg_text(s) for s in segments)
    candidates = re.findall(r'[A-Za-z][A-Za-z0-9._-]{1,}|[가-힣A-Za-z0-9]{2,}', text)
    stop = {'그리고','그런데','그러면','이제','지금','제가','우리','그쵸','아니','예를','들어서','하는','있는','없는','것은','거예요','됩니다','입니다'}
    freq: dict[str, int] = {}
    for c in candidates:
        c = c.strip()
        if c in stop or len(c) < 2:
            continue
        freq[c] = freq.get(c, 0) + 1
    terms = sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))[:max_terms]
    return {
        'schema_version': 'vocanote.global_context.v1',
        'method': 'deterministic_seed',
        'glossary_candidates': [{'term': k, 'count': v} for k, v in terms],
        'note': 'Reference only. Raw segment text is authoritative.',
    }



def segment_id(seg: dict[str, Any]) -> int:
    """Immutable segment id for provenance; current transcript stores it as index."""
    return int(seg.get('segment_id', seg.get('index')))


def semantic_segment_fingerprint(seg: dict[str, Any]) -> dict[str, Any]:
    return {
        'segment_id': segment_id(seg),
        'speaker': str(seg.get('speaker') or 'S1'),
        'start': float(seg.get('start') or 0.0),
        'end': float(seg.get('end') or 0.0),
        'text': seg_text(seg),
    }


def semantic_settings_from_env() -> dict[str, int]:
    """Define all required Phase 2 semantic settings; Phase 2A uses the first two."""
    return {
        'SEMANTIC_CHUNK_TARGET_CHARS': int(os.environ.get('SEMANTIC_CHUNK_TARGET_CHARS', str(DEFAULT_SEMANTIC_CHUNK_TARGET_CHARS))),
        'SEMANTIC_CHUNK_MAX_CHARS': int(os.environ.get('SEMANTIC_CHUNK_MAX_CHARS', str(DEFAULT_SEMANTIC_CHUNK_MAX_CHARS))),
        'SEMANTIC_REDUCE_MAX_CHARS': int(os.environ.get('SEMANTIC_REDUCE_MAX_CHARS', str(DEFAULT_SEMANTIC_REDUCE_MAX_CHARS))),
        'SEMANTIC_REDUCE_GROUP_MAX_CHARS': int(os.environ.get('SEMANTIC_REDUCE_GROUP_MAX_CHARS', str(DEFAULT_SEMANTIC_REDUCE_GROUP_MAX_CHARS))),
    }


def _range_for(items: list[dict[str, Any]]) -> dict[str, int]:
    if not items:
        return {'start_segment_id': -1, 'end_segment_id': -1}
    return {'start_segment_id': segment_id(items[0]), 'end_segment_id': segment_id(items[-1])}


def _semantic_hash_payload(*, primary: list[dict[str, Any]], overlap: list[dict[str, Any]], global_context_hash: str, semantic_prompt_version: str, provider: str, model: str, model_config: dict[str, Any]) -> dict[str, Any]:
    return {
        'semantic_chunk_schema_version': SEMANTIC_CHUNK_SCHEMA_VERSION,
        'semantic_prompt_version': semantic_prompt_version,
        'provider': provider,
        'model': model,
        'model_config': model_config,
        'PRIMARY': [semantic_segment_fingerprint(s) for s in primary],
        'OVERLAP': [semantic_segment_fingerprint(s) for s in overlap],
        'global_context_hash': global_context_hash,
    }


def build_semantic_chunks(
    *,
    recording_id: str,
    clean_segments: list[dict[str, Any]],
    global_context: dict[str, Any] | None = None,
    target_chars: int | None = None,
    max_chars: int | None = None,
    reduce_max_chars: int | None = None,
    reduce_group_max_chars: int | None = None,
    overlap_segments: int = DEFAULT_OVERLAP_SEGMENTS,
    semantic_prompt_version: str = SEMANTIC_PROMPT_VERSION,
    provider: str = 'openai-codex',
    model: str = 'gpt-5.5',
    model_config: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Phase 2A deterministic clean-transcript -> semantic chunks.

    No Hermes call is made here. Only PRIMARY segments may be semantically extracted.
    OVERLAP is persisted as context_only and must never be used as extraction output scope.
    """
    settings = semantic_settings_from_env()
    if target_chars is not None:
        settings['SEMANTIC_CHUNK_TARGET_CHARS'] = int(target_chars)
    if max_chars is not None:
        settings['SEMANTIC_CHUNK_MAX_CHARS'] = int(max_chars)
    if reduce_max_chars is not None:
        settings['SEMANTIC_REDUCE_MAX_CHARS'] = int(reduce_max_chars)
    if reduce_group_max_chars is not None:
        settings['SEMANTIC_REDUCE_GROUP_MAX_CHARS'] = int(reduce_group_max_chars)
    target = settings['SEMANTIC_CHUNK_TARGET_CHARS']
    hard_max = settings['SEMANTIC_CHUNK_MAX_CHARS']
    if target > hard_max:
        raise ValueError(f'SEMANTIC_CHUNK_TARGET_CHARS {target} exceeds SEMANTIC_CHUNK_MAX_CHARS {hard_max}')
    model_config = model_config or {}
    ghash = context_hash(global_context)
    chunks: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    n = len(clean_segments)
    i = 0
    chunk_no = 1
    while i < n:
        start_i = i
        j = i
        chars = 0
        last_safe_j = i
        last_safe_chars = 0
        while j < n:
            next_chars = chars + len(seg_text(clean_segments[j])) + (1 if chars else 0)
            if j > start_i and next_chars > target:
                break
            if next_chars <= hard_max:
                last_safe_j = j + 1
                last_safe_chars = next_chars
            elif j == start_i:
                raise ValueError(f'single_segment_exceeds_semantic_hard_max segment_id={segment_id(clean_segments[j])} chars={next_chars} max={hard_max}')
            else:
                break
            chars = next_chars
            j += 1
        end_exclusive = max(last_safe_j, start_i + 1)
        primary = clean_segments[start_i:end_exclusive]
        primary_chars = sum(len(seg_text(s)) for s in primary)
        if primary_chars > hard_max:
            raise ValueError(f'semantic_chunk_hard_max_exceeded chunk={chunk_no} chars={primary_chars} max={hard_max}')
        overlap_start = max(0, start_i - overlap_segments)
        overlap_end = min(n, end_exclusive + overlap_segments)
        overlap = clean_segments[overlap_start:overlap_end]
        chunk_id = f'chunk_{chunk_no:04d}'
        primary_ids = [segment_id(s) for s in primary]
        overlap_ids = [segment_id(s) for s in overlap]
        input_hash = sha256_obj(_semantic_hash_payload(
            primary=primary,
            overlap=overlap,
            global_context_hash=ghash,
            semantic_prompt_version=semantic_prompt_version,
            provider=provider,
            model=model,
            model_config=model_config,
        ))
        chunk = {
            'schema_version': SEMANTIC_CHUNK_SCHEMA_VERSION,
            'chunk_id': chunk_id,
            'recording_id': recording_id,
            'semantic_prompt_version': semantic_prompt_version,
            'provider': provider,
            'model': model,
            'model_config': model_config,
            'input_hash': input_hash,
            'global_context_hash': ghash,
            'semantic_extraction_scope': 'PRIMARY_ONLY',
            'primary': {
                'range': _range_for(primary),
                'source_segment_ids': primary_ids,
                'text_chars': primary_chars,
                'segments': [semantic_segment_fingerprint(s) for s in primary],
            },
            'overlap_context': {
                'range': _range_for(overlap),
                'source_segment_ids': overlap_ids,
                'text_chars': sum(len(seg_text(s)) for s in overlap),
                'segments': [semantic_segment_fingerprint(s) for s in overlap],
                'context_only': True,
            },
            'provenance': {
                'source_segment_ids': primary_ids,
                'primary_source_segment_ids': primary_ids,
                'overlap_context_segment_ids': overlap_ids,
            },
        }
        chunks.append(chunk)
        manifest_rows.append({
            'chunk_id': chunk_id,
            'path': f'{chunk_id}.json',
            'input_hash': input_hash,
            'primary_range': chunk['primary']['range'],
            'overlap_context_range': chunk['overlap_context']['range'],
            'primary_source_segment_ids': primary_ids,
            'overlap_context_segment_ids': overlap_ids,
            'primary_text_chars': primary_chars,
        })
        i = end_exclusive
        chunk_no += 1
    manifest = {
        'schema_version': SEMANTIC_MANIFEST_SCHEMA_VERSION,
        'recording_id': recording_id,
        'created_at': 'deterministic-placeholder',
        'semantic_prompt_version': semantic_prompt_version,
        'provider': provider,
        'model': model,
        'model_config': model_config,
        'settings': {**settings, 'overlap_segments': overlap_segments},
        'global_context_hash': ghash,
        'total_segments': n,
        'chunk_count': len(chunks),
        'semantic_extraction_scope': 'PRIMARY_ONLY',
        'chunks': manifest_rows,
    }
    return manifest, chunks


def write_semantic_chunks(recording_dir: Path, *, recording_id: str, clean_transcript: dict[str, Any], global_context: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
    """Write semantic_chunks/*.json and semantic_chunks/manifest.json for Phase 2A."""
    chunk_dir = recording_dir / 'semantic_chunks'
    chunk_dir.mkdir(parents=True, exist_ok=True)
    manifest, chunks = build_semantic_chunks(
        recording_id=recording_id,
        clean_segments=list(clean_transcript.get('segments') or []),
        global_context=global_context,
        **kwargs,
    )
    # Real manifest timestamps must not affect deterministic hashes.
    from datetime import datetime, timezone
    manifest['created_at'] = datetime.now(timezone.utc).isoformat(timespec='seconds')
    for chunk in chunks:
        (chunk_dir / f"{chunk['chunk_id']}.json").write_text(stable_json(chunk) + '\n', encoding='utf-8')
    (chunk_dir / 'manifest.json').write_text(stable_json(manifest) + '\n', encoding='utf-8')
    return manifest

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

from vocanote_tombstone import guarded_atomic_write_text


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f'.{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def bullet(items: list[Any]) -> str:
    if not items:
        return '- 내용 없음\n'
    return ''.join(f'- {str(x).strip()}\n' for x in items if str(x).strip()) or '- 내용 없음\n'


def render_summary(result: dict[str, Any], metadata: dict[str, Any]) -> str:
    title = result['title']['text']
    memo = result['memo_summary']
    quality = result.get('quality', {})
    lines = [
        f"# {title}",
        '',
        '## 한눈에 보기',
        '',
        f"- 주제: {memo.get('topic','')}",
        f"- 유형: {result.get('content_type','')}",
        f"- 요약: {memo.get('one_line','')}",
        '',
        '## 핵심 키워드',
        '',
        ', '.join(result.get('keywords') or []) or '키워드 없음',
        '',
        '## 핵심 포인트',
        '',
        bullet(memo.get('core_points') or []),
        '## 흐름',
        '',
        bullet(memo.get('flow') or []),
        '## 품질 메모',
        '',
        f"- 검토 필요: {'예' if quality.get('needs_review') else '아니오'}",
    ]
    warnings = quality.get('warnings') or []
    if warnings:
        lines += ['', '### 경고', '', bullet(warnings)]
    return '\n'.join(lines).rstrip() + '\n'


def render_analysis(result: dict[str, Any], metadata: dict[str, Any]) -> str:
    title = result['title']['text']
    note = result['structured_note']
    lines = [f"# {title} — 정리", '', f"- 녹음 ID: `{result['recording_id']}`", f"- 노트 유형: {note.get('note_type','')}", '']
    for section in note.get('sections') or []:
        heading = str(section.get('heading') or '섹션').strip()
        lines += [f"## {heading}", '', bullet(section.get('items') or []), '']
    return '\n'.join(lines).rstrip() + '\n'


def render_all(recording_dir: Path) -> None:
    result = load_json(recording_dir / 'result.validated.json')
    metadata = load_json(recording_dir / 'metadata.json')
    recording_id = str(metadata.get('recording_id') or metadata.get('id') or result.get('recording_id') or '')
    guarded_atomic_write_text(recording_id=recording_id, recording_dir=recording_dir, target_path=recording_dir / 'summary.md', content=render_summary(result, metadata), create_parent=False)
    guarded_atomic_write_text(recording_id=recording_id, recording_dir=recording_dir, target_path=recording_dir / 'analysis.md', content=render_analysis(result, metadata), create_parent=False)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('recording_dir')
    args = parser.parse_args()
    render_all(Path(args.recording_dir))

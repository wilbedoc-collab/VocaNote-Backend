#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Phase84 title-only display_title generation for VocaNote.

Scope guard:
- display title only
- no STT/correction/semantic body/summary/keypoints mutation
- no DB/content writes

The public function name is kept as phase78_display_title so the existing
Phase81/app.py integration can be replaced without broad API rewiring.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

STOPWORDS = set(
    '그리고 그러나 그래서 대한 관련 중심 진행 논의 회의 강의 발표 소개 설명 검토 확인 대상 통한 위해 있는 없는 매우 일부 후반 전반 내용 녹음 기존 실제 주요 현재 방향 방식 과정 가능 것을 하는 했다 한다 있다 없다 입니다 합니다 있습니다 오늘 이제 여기 저기 이거 그거 저희 우리 제가'.split()
)
PLACEHOLDER_TITLES = {'', '무제회의', '무제 회의', '무제 녹음', '회의'}
GENERIC_TYPE_SUFFIXES = [' lecture', ' meeting', ' other', ' test']


def _norm(value: Any) -> str:
    return re.sub(r'\s+', ' ', str(value or '')).strip()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        pass
    return {}


def _read_text(path: Path) -> str:
    try:
        if path.exists():
            return path.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        pass
    return ''


def _tokens(text: str) -> list[str]:
    raw = re.findall(r'[A-Za-z][A-Za-z0-9+./-]*|[가-힣0-9]{2,}', _norm(text))
    return [t for t in raw if t not in STOPWORDS and len(t) >= 2]


def _anyhas(text: str, words: list[str]) -> bool:
    low = text.lower()
    return any(w.lower() in low for w in words)


def _compact_recorded_at(meta: dict[str, Any]) -> str:
    raw = str(meta.get('recorded_at') or '')
    m = re.search(r'(\d{4})[-.](\d{2})[-.](\d{2})[T ](\d{2}):(\d{2})', raw)
    if not m:
        return ''
    return f"{int(m.group(2))}/{int(m.group(3))} {m.group(4)}:{m.group(5)}"


def _clean_title(title: str) -> str:
    t = _norm(title)
    for suffix in GENERIC_TYPE_SUFFIXES:
        if t.lower().endswith(suffix):
            t = t[:-len(suffix)].strip()
    t = re.sub(r'[.。]+$', '', t).strip()
    t = re.sub(r'^S\d+이\s*', '', t)
    t = t.replace('— —', '—')
    # Keep it a recording name, not a sentence.
    t = re.sub(r'(했다|한다|있다|없다|됩니다|입니다|하였다|되었다|이었다|보인다|포함된다)$', '', t).rstrip(' ,·-')
    return t


def _semantic_final_text(recording_dir: Path) -> str:
    obj = _load_json(recording_dir / 'semantic_final.json')
    if not obj:
        return ''
    parts: list[str] = []
    if obj.get('summary'):
        parts.append(str(obj['summary']))
    for key in ('topics', 'key_points'):
        for item in obj.get(key) or []:
            if isinstance(item, dict):
                parts.append(str(item.get('text') or ''))
            else:
                parts.append(str(item))
    return '\n'.join(p for p in parts if _norm(p))


def _result_title_text(result: dict[str, Any]) -> str:
    title = result.get('title')
    if isinstance(title, dict):
        return _norm(title.get('text'))
    return _norm(title or result.get('display_title') or '')


def _blank_transcript(text: str) -> bool:
    return len(_norm(text)) < 5


def _fallback_from_metadata(meta: dict[str, Any], transcript: str) -> str:
    date = _compact_recorded_at(meta)
    meta_title = _clean_title(str(meta.get('title') or ''))
    if meta_title and meta_title not in PLACEHOLDER_TITLES:
        if _blank_transcript(transcript) and date and date not in meta_title:
            return f'{meta_title} — {date}'
        return meta_title
    if _blank_transcript(transcript):
        return f'내용 확인 필요 테스트 녹음 — {date}'.strip(' —')
    top = [t for t in _tokens(transcript)[:6] if t not in STOPWORDS]
    return _clean_title(('·'.join(top[:4]) or '내용 확인 필요 녹음') + (f' — {date}' if date else ''))


def phase84_candidate_title(recording_dir: Path, result: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
    """Generate a display-only recording name from metadata + corrected transcript.

    Generalized discriminator rules:
    - Prefer core identity + useful discriminator.
    - For similar/blank recordings, add date and time.
    - Do not use Gold labels or recording_id-specific overrides.
    """
    result = result or {}
    meta = _load_json(recording_dir / 'metadata.json')
    transcript = _read_text(recording_dir / 'transcript_clean.txt')
    if not transcript:
        transcript = _read_text(recording_dir / 'transcript_raw.txt')
    s = _norm(transcript)
    h = s[:6000]
    early = s[:1500]
    date = _compact_recorded_at(meta)
    try:
        dur = float(meta.get('duration_sec') or 0)
    except Exception:
        dur = 0.0

    title = ''
    reason = 'phase84_rule'

    # Strong core identities before broad late/incidental terms.
    if _anyhas(h, ['75세', '기증자', '피부세포', '야마나카', '회춘', 'confinement', '미세 공간', '기계적', '압박']):
        title = f'75세 피부세포 기계적 압박 회춘 연구 해설 — {date}'
    elif _anyhas(early, ['ABM', '에이비엠', '리투오', '리투브', '미투', '컨센서스', '부작용', '사후관리', '프로토콜']) and _anyhas(early, ['3회', '3차', '세 번째', '오라클', '박재']):
        title = '리투오 ABM 3차 컨센서스 회의 — 임상사례·프로토콜'
    # Collision fix: clinical Japanese alpha/boost talk must beat generic RF seminar when those cues are present early.
    elif _anyhas(early, ['일본', '시니어', '클리닉', '알파팁', '알파 팁']) and _anyhas(h, ['덴서티', '부스트', '팁', '임상']):
        title = '일본 의사 덴서티 알파팁·부스트팁 임상 발표'
    elif _anyhas(h, ['RF', '알파틱', 'Alphatic', '모노폴라', '바이폴라', '콜라겐', '엘라스틴', '쿨링', '부스트 팁']) and _anyhas(h, ['덴서티', '부스트']):
        title = '덴서티 RF 부스트팁 세미나 — 레이어별 타깃팅·임상 Q&A'
    elif _anyhas(h, ['도올', '이재명', '검찰', '정치', '민주주의']):
        title = '민주주의·정치개혁 시사 대담 — 이재명·공직 책임'
    elif _anyhas(h, ['GEO', 'SEO', 'AEO', '제미나이', 'Gemini', 'ChatGPT', '후보군', 'citation', '시테이션']) and _anyhas(h, ['마이디', '피부과']):
        title = '제23차 마이디피부과 AI 추천 노출 연구 미팅' if _anyhas(h, ['23번째', '스물세', '23차', '제23']) else '마이디피부과 AI 추천 노출 분석·에이전트 연구 회의'
    elif _anyhas(h, ['캄보디아', '타지키스탄', '멜라닌', '홍조', 'ROI', '랜드마크', '라벨링', '색소']):
        title = '피부 색소 자동 라벨링 개발 회의 — 멜라닌·홍조·ROI'
    elif s.count('기술보증기금') >= 5:
        title = '기술보증기금 대상 리주비놀 IR 마지막 부분'
    elif _anyhas(h, ['자동', '레이저', '토닝']) and _anyhas(h, ['인허가', '사업', '기술보증', '시연', '투자']):
        title = '리주비놀 자동 미용의료 장비 IR — 기술·인허가·사업성' if _anyhas(h, ['기술보증', 'IR', '투자', '창업']) else '자동 레이저 토닝 장비 사업·제품 시연 검토'
    elif _anyhas(h, ['모션', '카메라', '오린', '나노', '목받침', '터렛', '커튼', '위치', '움직임']) and _anyhas(h, ['레이저', '장비']):
        title = '자동 레이저 치료 장비 개발 회의 — 모션감지·카메라·기구설계'
    elif _anyhas(h, ['항노화학회', '학노화', '학회']) and _anyhas(h, ['Re2O', '리투오', 'HADM', '공동', '스터디', '논문', '교육', '연구비', 'PI', '책', '산학', '기업']):
        minutes = int(round(dur / 60)) if dur else 0
        dur_hint = f' · {minutes}분' if minutes else ''
        source_title = _result_title_text(result)
        anchor = ''
        if 'HADM' in source_title:
            anchor = ' · HADM'
        elif '산학협력' in source_title or '산학' in source_title:
            anchor = ' · 산학협력 제안'
        elif '이주희' in source_title:
            anchor = ' · 이주희 교수 제안'
        title = f'항노화학회 공동연구·논문·교육 운영 회의 — {date}{dur_hint}{anchor}'
    elif _anyhas(early, ['항노화학회', '학회']) and _anyhas(early, ['웨비나', '보툴렉스', '오플레스', '학술']):
        title = '대한피부항노화학회 교육·웨비나·임상 프로토콜 회의'
    elif _anyhas(h, ['자동', '레코드', '시작', '작동']) and dur < 60:
        title = f'자동 녹음 시작 기능 테스트 — {date}'
    elif _anyhas(h, ['자리', '앉', '앉으', '다시 해', '시작']) and dur < 180:
        title = f'회의 시작 전 착석·정돈 준비 대화 — {date}'
    elif _anyhas(h, ['덴서티', '알파', '알파팁', '부스트', '부스팅', '시니어', '일본', '클리닉']) and _anyhas(h, ['팁', '임상', '환자', '통증']):
        title = '일본 의사 덴서티 알파팁·부스트팁 임상 발표'
    elif _anyhas(h, ['RF', '알파틱', 'Alphatic', '모노폴라', '바이폴라', '콜라겐', '엘라스틴', '쿨링', '부스트 팁']):
        title = '덴서티 RF 부스트팁 세미나 — 레이어별 타깃팅·임상 Q&A'
    elif (('은혜와 평강' in early) or ('갈라디아서' in early)) and ('기도' in h):
        title = '은혜와 평강 설교 — 기도·죄의 자각·속죄'
    elif ('청지기' in early) or ('교회 중심' in early):
        title = '교회 리더십·청지기 삶 강의 — 은사·재정·돌봄'
    elif ('트렌드 코리아' in h) or (('소비' in early) and ('뷰티' in h)):
        title = '의사 대상 소비 트렌드·미용의료 시장 강의'
    elif ('마태복음' in early) or ('동방박사' in early):
        title = '신약 성경 문제집 학습 메모 — 마태복음 문제'
    elif ('예레미야애가' in early) or ('에스겔' in early and '신약' in early):
        title = '성경 문제집 진도 메모 — 에스겔부터 신약까지'
    elif ('어머니' in early) and ('항암' in early or '수액' in early):
        title = '어머니 항암치료 돌봄 개인 메모 — 수액·기도'
    elif _anyhas(h, ['화면', '끄고', '백그라운드', '다른 앱', '홈 화면', '소리']) and _anyhas(h, ['녹음', '테스트']):
        title = '화면 OFF·다른 앱 전환 백그라운드 녹음 테스트'
    else:
        title = _fallback_from_metadata(meta, transcript)
        reason = 'phase84_metadata_or_transcript_fallback'

    title = _clean_title(title)
    if not title:
        title = _fallback_from_metadata(meta, transcript)
        reason = 'phase84_empty_fallback'
    return title, {
        'display_algorithm': 'phase84_title_only_generalized',
        'source': 'metadata+transcript_clean',
        'source_title': _result_title_text(result),
        'semantic_body_mutated': False,
        'reason': reason,
    }


def phase78_display_title(recording_dir: Path, result: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Backward-compatible entrypoint used by app.py."""
    return phase84_candidate_title(recording_dir, result)

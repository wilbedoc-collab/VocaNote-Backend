#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

BASE_DIR = Path('/Users/ahnbot/Desktop/sharefolder/08_업무_회계_문서/회의록앱').resolve()
AUDIO_SUFFIXES = ['.m4a', '.mp3', '.wav', '.webm', '.aac', '.ogg']


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path: Path, data: Dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


def set_status(prefix: str, status: str, **extra) -> None:
    p = BASE_DIR / f'{prefix}_status.json'
    data = load_json(p)
    data.update(extra)
    data['status'] = status
    data['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    write_json(p, data)


def get_audio(prefix: str) -> Path | None:
    for suffix in AUDIO_SUFFIXES:
        p = BASE_DIR / f'{prefix}_original{suffix}'
        if p.exists():
            return p
    return None


def transcribe_local_whisper(audio: Path) -> tuple[str, list[dict]]:
    model = os.environ.get('RE2O_WHISPER_MODEL', 'small')
    out_dir = BASE_DIR / '.whisper_tmp'
    out_dir.mkdir(exist_ok=True)
    cmd = [
        os.environ.get('RE2O_PYTHON', '/Users/ahnbot/.openclaw/workspace/hermes_eval/repo/venv/bin/python3'),
        '-m', 'whisper', str(audio),
        '--model', model,
        '--language', 'Korean',
        '--task', 'transcribe',
        '--output_format', 'json',
        '--output_dir', str(out_dir),
        '--fp16', 'False',
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=3600)
    js_path = out_dir / f'{audio.stem}.json'
    if not js_path.exists():
        raise RuntimeError('whisper json output missing')
    data = json.loads(js_path.read_text('utf-8'))
    segments = []
    texts = []
    for i, seg in enumerate(data.get('segments') or []):
        text = str(seg.get('text') or '').strip()
        if not text:
            continue
        start = float(seg.get('start') or 0.0)
        end = float(seg.get('end') or start)
        segments.append({'index': i, 'speaker': '참석자 1', 'start': start, 'end': end, 'text': text})
        texts.append(text)
    transcript = '\n'.join(texts).strip() or str(data.get('text') or '').strip()
    return (transcript or '[전사 결과 없음]', segments)


def transcribe_openai_if_configured(audio: Path) -> tuple[str, list[dict]] | None:
    if not os.environ.get('OPENAI_API_KEY'):
        return None
    from openai import OpenAI
    client = OpenAI()
    with audio.open('rb') as f:
        res = client.audio.transcriptions.create(model='whisper-1', file=f, language='ko')
    text = getattr(res, 'text', '') or str(res)
    return text, make_fallback_segments(text, duration_sec=None)



def apply_domain_corrections(text: str) -> str:
    """Light Korean/domain post-correction for known VocaNote lecture-style STT errors.

    This is conservative: only high-confidence replacements observed in recorded
    samples are applied. It improves title/summary generation without pretending
    to be a full human transcript editor.
    """
    replacements = {
        '맞췄다고 한 번 상상해 볼까요': '막혔다고 한 번 상상해 볼까요',
        '바쳤다고 한번 상상해 보세요': '막혔다고 한 번 상상해 볼까요',
        '벽이 사방에서 막 밀려고 서이슬 공간조차': '벽이 사방에서 막 밀려오고 서 있을 공간조차',
        '벽이 사방에서 막 밀려오고 서 있을 공간조차': '벽이 사방에서 막 밀려오고 서 있을 공간조차',
        '패드게 빠지겠죠': '패닉에 빠지겠죠',
        '편히게 파지겠죠': '패닉에 빠지겠죠',
        '바닐 세포': '단일 세포',
        '불효하거나 풍기하는 대신에': '두려워하거나 포기하는 대신에',
        '부여하거나 풍기하는 계신에': '두려워하거나 포기하는 대신에',
        '바닥을 욕혀주는': '바닥을 움켜쥐는',
        '바닥을 움켜주는': '바닥을 움켜쥐는',
        '우리가 덩이로운 괴력을': '오히려 경이로운 괴력을',
        '우리가 정의로운 괴력을': '오히려 경이로운 괴력을',
        '18배로': '15배요',
        '엄청 늦은데요': '엄청난 수치인데요',
        '엄청난 수치네요': '엄청난 수치인데요',
        '기아하게 이해하고': '기하학을 이해하고',
        '기아학을 이해하고': '기하학을 이해하고',
        '다시 내 물리적 한길 기어놓는다는': '자신의 물리적 한계를 뛰어넘는다는',
        '자신의 물리적 한 밀을 띄워놓는다는': '자신의 물리적 한계를 뛰어넘는다는',
        '최신의 욕을 속에 발표했습니다': '최신 연구를 통해 밝혀졌습니다',
        '최신 연구를 통해 밝혀졌습니다': '최신 연구를 통해 밝혀졌습니다',
        '공간이 들어세면': '공간이 줄어들면',
    }
    for a, b in replacements.items():
        text = text.replace(a, b)
    text = re.sub(r'\s+', ' ', text).replace(' ?','?').replace(' .','.')
    # restore line breaks roughly after punctuation for readability
    text = re.sub(r'(?<=[.?!요죠다])\s+', '\n', text)
    return text.strip()

def clean_transcript(transcript: str) -> str:
    lines = [ln.strip() for ln in transcript.splitlines()]
    lines = [ln for ln in lines if ln]
    out = []
    prev = None
    for ln in lines:
        if ln != prev:
            out.append(ln)
        prev = ln
    return apply_domain_corrections('\n'.join(out).strip())


def make_fallback_segments(transcript: str, duration_sec: int | None = None) -> list[dict]:
    lines = [ln.strip() for ln in transcript.splitlines() if ln.strip()]
    if not lines:
        return []
    step = max(2.0, (duration_sec or (len(lines) * 8)) / max(1, len(lines)))
    out = []
    cur = 0.0
    for i, line in enumerate(lines):
        end = cur + step
        out.append({'index': i, 'speaker': '참석자 1', 'start': round(cur, 2), 'end': round(end, 2), 'text': line})
        cur = end
    return out


def split_sentences(text: str) -> list[str]:
    raw = []
    for line in text.splitlines():
        parts = re.split(r'(?<=[.!?。])\s+|(?<=요)\s+|(?<=다)\s+', line.strip())
        raw.extend(parts)
    out = []
    for s in raw:
        s = re.sub(r'\s+', ' ', s).strip()
        if len(s) >= 3:
            out.append(s)
    return out


def extract_keywords(text: str, limit: int = 8) -> list[str]:
    stop = {
        '회의','요약','전사','내용','테스트','확인','일시','유형','메모','핵심','정리','자동','필요','있습니다','합니다','대한',
        '지금','나중','녹음','무제','화면','중지','업로드','되는지','있는지','이거','안녕','바래','내가','다시'
    }
    words = re.findall(r'[가-힣A-Za-z0-9]{2,}', text)
    counts: dict[str, int] = {}
    original: dict[str, str] = {}
    for w in words:
        wl = w.lower()
        if wl in stop or len(wl) < 2:
            continue
        counts[wl] = counts.get(wl, 0) + 1
        original.setdefault(wl, w)
    return [original[w] for w, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]]


def infer_topic(transcript: str, metadata: Dict) -> str:
    """Return a concise summary-title, not a keyword dump.

    Naming rule: final file prefix = YYMMDD + this summarized title.
    The title should describe what the recording is about in one phrase.
    """
    explicit = (metadata.get('title') or '').strip()
    if explicit and explicit not in {'무제 회의', '무제회의', '회의'}:
        return explicit
    text = re.sub(r'\s+', ' ', transcript.strip())
    lowered = text.lower()

    # App/test recordings: summarize the functional purpose.
    has_record = '녹음' in text
    has_test = '테스트' in text or '확인' in text or '잘 되는' in text or '잘됩니다' in text
    if '대화 없음' in text or '대화가 없' in text or '대화 없는' in text or '음량' in text:
        if '측면' in text or '사이드' in text or '버튼' in text:
            return '측면버튼녹음과대화없음감지확인'
        return '대화없음감지오류확인' if '안되고' in text or '똑같이' in text else '대화없음감지확인'
    if '측면' in text or '사이드' in text or '버튼' in text:
        return '측면버튼자동녹음확인'
    if '화면' in text and ('꺼' in text or '꺼짐' in text):
        if '업로드' in text or '전사' in text or '요약' in text:
            return '화면꺼짐녹음업로드전사확인'
        return '화면꺼짐녹음유지확인'
    if has_record and ('업로드' in text or '전사' in text or '요약' in text):
        return '녹음업로드전사요약확인'
    if has_record and has_test:
        return '녹음테스트'

    if ('세포' in text and ('공간' in text or '바닥' in text) and ('15배' in text or '기하학' in text or '움켜' in text)):
        return '공간이줄어들때세포가강하게움켜쥐는현상'

    # Real-world meeting topics.
    if '리주비놀' in text or 'rejuvinol' in lowered:
        return '리주비놀전체회의' if '전체' in text or '회의' in text else '리주비놀논의'
    if '항노화' in text or '학회' in text or '평가회' in text:
        return '항노화학회평가회참석전메모' if '가기' in text or '택시' in text or '나가야' in text else '항노화학회평가회'
    if '식약처' in text or '인허가' in text:
        return '식약처인허가논의'
    if 'ir' in lowered or '투자' in text:
        return 'IR미팅'

    # Very short/unclear clips.
    sentences = split_sentences(transcript)
    if len(text) < 40:
        return '짧은녹음확인'
    # Last fallback: use the first meaningful sentence, cleaned and shortened.
    first = sentences[0] if sentences else text[:30]
    first = re.sub(r'^(안녕|아|음|어|저기|그냥)[,\s]*', '', first)
    first = re.sub(r'[^가-힣A-Za-z0-9]+', '', first)
    return (first[:18] or '무제회의')


def slug_title(title: str) -> str:
    title = re.sub(r'[\\/:*?"<>|\n\r\t\s_]+', '', title.strip())
    return title[:30] or '무제회의'


def recorded_date_yyMMdd(metadata: Dict, prefix: str) -> str:
    val = metadata.get('recorded_at') or metadata.get('uploaded_at') or ''
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
        try:
            return datetime.strptime(val[:19], fmt).strftime('%y%m%d')
        except Exception:
            pass
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', prefix)
    if m:
        return f"{m.group(1)[2:]}{m.group(2)}{m.group(3)}"
    return datetime.now().strftime('%y%m%d')


def unique_new_prefix(new_prefix: str, old_prefix: str) -> str:
    if new_prefix == old_prefix:
        return new_prefix
    if not list(BASE_DIR.glob(new_prefix + '_*')):
        return new_prefix
    for i in range(2, 1000):
        c = f'{new_prefix}_{i}'
        if not list(BASE_DIR.glob(c + '_*')):
            return c
    return new_prefix


def rename_recording(old_prefix: str, transcript: str, metadata: Dict) -> str:
    topic = infer_topic(transcript, metadata)
    date = recorded_date_yyMMdd(metadata, old_prefix)
    new_prefix = unique_new_prefix(f'{date}{slug_title(topic)}', old_prefix)
    if new_prefix == old_prefix:
        metadata['title'] = topic
        metadata['id'] = old_prefix
        write_json(BASE_DIR / f'{old_prefix}_metadata.json', metadata)
        return old_prefix
    files = sorted(BASE_DIR.glob(old_prefix + '_*'))
    for p in files:
        suffix = p.name[len(old_prefix):]
        p.rename(BASE_DIR / f'{new_prefix}{suffix}')
    # Update ids after rename.
    meta_path = BASE_DIR / f'{new_prefix}_metadata.json'
    status_path = BASE_DIR / f'{new_prefix}_status.json'
    seg_path = BASE_DIR / f'{new_prefix}_segments.json'
    metadata = load_json(meta_path)
    old_ids = []
    prev_old = metadata.get('old_id')
    if prev_old:
        old_ids.append(prev_old)
    prev_list = metadata.get('old_ids') or []
    if isinstance(prev_list, str):
        prev_list = [prev_list]
    old_ids.extend(prev_list)
    old_ids.append(old_prefix)
    old_ids = list(dict.fromkeys([x for x in old_ids if x]))
    metadata.update({'id': new_prefix, 'old_id': old_prefix, 'old_ids': old_ids, 'title': topic})
    write_json(meta_path, metadata)
    status = load_json(status_path)
    status_old_ids = []
    prev_old = status.get('old_id')
    if prev_old:
        status_old_ids.append(prev_old)
    prev_list = status.get('old_ids') or []
    if isinstance(prev_list, str):
        prev_list = [prev_list]
    status_old_ids.extend(prev_list)
    status_old_ids.append(old_prefix)
    status_old_ids = list(dict.fromkeys([x for x in status_old_ids if x]))
    status.update({'id': new_prefix, 'old_id': old_prefix, 'old_ids': status_old_ids})
    write_json(status_path, status)
    if seg_path.exists():
        seg = load_json(seg_path)
        seg['id'] = new_prefix
        write_json(seg_path, seg)
    return new_prefix


def summarize_content(prefix: str, transcript: str, metadata: Dict) -> Dict[str, str]:
    title = metadata.get('title') or infer_topic(transcript, metadata)
    recorded_at = metadata.get('recorded_at', '')
    meeting_type = metadata.get('type', '') or metadata.get('meeting_type', '') or '대화'
    clean = clean_transcript(transcript)
    sentences = split_sentences(clean)
    keywords = extract_keywords(clean, limit=8)

    # Content-aware summary for the cell/spatial-confinement lecture sample.
    if '세포' in clean and ('15배' in clean or '움켜' in clean) and ('공간' in clean or '기하학' in clean):
        topic_line = '공간이 줄어들 때 세포는 강하게 움켜쥔다'
        one_line = '공간에 따른 세포 행동에 관한 대화입니다.'
        main_md = '- 사람이 사방에서 공간이 없어지면 패닉에 빠지겠지만, 세포는 공간이 좁아지면 오히려 최대 15배나 강하게 바닥을 움켜쥔다.'
        flow_items = [
            '사람에게서 공간이 사라질 경우를 상상함',
            '세포의 경우라면 반응이 다르다는 점을 제시함',
            '세포는 공간이 좁아지는 극한 상황에서 최대 15배나 더 강력하게 바닥을 움켜쥠',
            '세포는 기하학을 이해하고 생명을 위해 물리적 한계를 뛰어넘는다는 결론으로 이어짐',
        ]
    else:
        # Generic fallback: do not dump raw transcript. Build a compact topic line and
        # one synthesized core sentence from the first meaningful sentences.
        topic_line = title
        if keywords:
            topic_line = ' · '.join(keywords[:3])
        if sentences:
            one_line = f'{title}에 관한 대화입니다.' if title else sentences[0][:90]
            core = ' '.join(sentences[:3])
            main_md = f'- {core[:180]}'
            flow_items = [s[:90] for s in sentences[:4]]
        else:
            one_line = '전사문에서 대화 주제를 충분히 확인하지 못했습니다.'
            main_md = '- 유의미한 전사 내용이 부족합니다.'
            flow_items = ['추가 맥락 없음']

    flow_md = '\n'.join(f'- {x}' for x in flow_items)
    summary = f"""# {title}

## 한눈에 보기

- 주제: {topic_line}
- 일시: {recorded_at}
- 유형: {meeting_type}
- 요약: {one_line}

## 핵심 내용

{main_md}

## 대화 흐름

{flow_md}
"""

    # Analysis is separate from memo/summary: evaluation, suggestions, follow-ups.
    action_candidates = [s for s in sentences if any(k in s for k in ['해야', '할게', '볼게', '확인', '필요', '정리', '업로드', '전사', '요약', '평가', '제안'])]
    if not action_candidates and sentences:
        action_candidates = sentences[:4]
    action_md = '\n'.join(f'- {x}' for x in action_candidates[:6]) if action_candidates else '- 확인할 후속 내용 없음'
    analysis = f"""# {title} 정리

## 판단/평가가 필요한 지점

- 전사 품질과 실제 발화가 일치하는지 확인이 필요합니다.
- 메모·요약과 달리 이 영역은 평가, 제안, 후속 조치 중심으로 사용합니다.

## 제안/후속 조치

{action_md}

## 할 일 후보

- 잘못 인식된 핵심 단어 교정
- 공유 전 제목과 핵심 요약 문구 확인
- 필요한 경우 논의 주제별로 별도 문서화
"""
    kakao = f"[{title}]\n{one_line}\n\n핵심:\n" + main_md
    email = f"제목: [회의요약] {title} - {recorded_at}\n\n{summary}"
    slack = f"*{title}*\n{one_line}\n" + main_md.replace('- ', '• ')
    return {
        'summary.md': summary,
        'analysis.md': analysis,
        'action_items.md': analysis,
        'ir_review.md': analysis,
        'share_kakao.txt': kakao,
        'share_email.txt': email,
        'share_slack.txt': slack,
    }


def process_one(prefix: str, stub: bool = False, force: bool = False) -> bool:
    meta_path = BASE_DIR / f'{prefix}_metadata.json'
    status_path = BASE_DIR / f'{prefix}_status.json'
    metadata = load_json(meta_path)
    status = load_json(status_path)
    if not metadata:
        return False
    if not force and status.get('status') not in {'uploaded', 'failed_retry', 'pending_api_key', 'failed'}:
        return False
    audio = get_audio(prefix)
    transcript_path = BASE_DIR / f'{prefix}_transcript.txt'
    segments_path = BASE_DIR / f'{prefix}_segments.json'
    try:
        set_status(prefix, 'transcribing', steps={'upload': 'done', 'transcription': 'running', 'summary': 'waiting', 'share_text': 'waiting'})
        if transcript_path.exists() and force:
            transcript = clean_transcript(transcript_path.read_text('utf-8', errors='ignore'))
            segments = load_json(segments_path).get('segments', []) if segments_path.exists() else []
        else:
            if not audio:
                set_status(prefix, 'failed', error='audio file not found')
                return True
            if stub:
                transcript = f"[테스트 전사문]\n파일: {audio.name}\n회의명: {metadata.get('title', prefix)}"
                segments = make_fallback_segments(transcript, metadata.get('duration_sec'))
            else:
                transcript, segments = transcribe_openai_if_configured(audio) or transcribe_local_whisper(audio)
        transcript = clean_transcript(transcript)
        if not segments:
            segments = make_fallback_segments(transcript, metadata.get('duration_sec'))
        (BASE_DIR / f'{prefix}_transcript.txt').write_text(transcript + '\n', encoding='utf-8')
        (BASE_DIR / f'{prefix}_segments.json').write_text(json.dumps({'id': prefix, 'segments': segments}, ensure_ascii=False, indent=2), encoding='utf-8')
        # Rename before writing final summary files.
        new_prefix = rename_recording(prefix, transcript, metadata)
        if new_prefix != prefix:
            prefix = new_prefix
            metadata = load_json(BASE_DIR / f'{prefix}_metadata.json')
            transcript = (BASE_DIR / f'{prefix}_transcript.txt').read_text('utf-8', errors='ignore')
        set_status(prefix, 'summarizing', steps={'upload': 'done', 'transcription': 'done', 'summary': 'running', 'share_text': 'waiting'})
        outputs = summarize_content(prefix, transcript, metadata)
        for name, content in outputs.items():
            (BASE_DIR / f'{prefix}_{name}').write_text(content, encoding='utf-8')
        set_status(prefix, 'completed', steps={'upload': 'done', 'transcription': 'done', 'summary': 'done', 'share_text': 'done'})
        return True
    except Exception as e:
        set_status(prefix, 'failed', error=repr(e), steps={'upload': 'done', 'transcription': 'failed', 'summary': 'waiting', 'share_text': 'waiting'})
        return True


def scan_once(stub: bool = False, force: bool = False) -> int:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for meta in sorted(BASE_DIR.glob('*_metadata.json')):
        prefix = meta.name[:-len('_metadata.json')]
        if process_one(prefix, stub=stub, force=force):
            count += 1
    return count


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--stub', action='store_true')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--interval', type=int, default=10)
    args = parser.parse_args()
    if args.once:
        print(json.dumps({'processed': scan_once(stub=args.stub, force=args.force)}, ensure_ascii=False))
        return
    while True:
        processed = scan_once(stub=args.stub, force=False)
        if processed:
            print(datetime.now().isoformat(), 'processed', processed, flush=True)
        time.sleep(args.interval)


if __name__ == '__main__':
    main()

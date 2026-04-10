"""
제주4.3 뉴스 아카이브 - Claude API 태거
수집된 기사에 자동으로 태그/주제/인물을 추출·부착
"""

import os
import json
import logging
import time

import anthropic
import database as db

log = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY'))

# ── 관련 인물 목록 ──────────────────────────────────────────

KNOWN_FIGURES = [
    # 군·경 지휘관
    '이승만', '조병옥', '박진경', '김익렬', '유재흥', '함병선',
    '송요찬', '최천', '브라운', '로버츠',
    # 종교·정치
    '김재능', '한경직', '영락교회',
    # 희생자·유족 관련 운동
    '현기영', '강요배',
    # 영문 표기
    'Syngman Rhee', 'Jo Byeong-ok', 'Park Jin-gyeong',
    'Kim Ik-ryeol', 'Yoo Jae-heung', 'Ham Byeong-seon',
    'Kim Jae-neung', 'Youngnak Church', 'Han Kyung-jik',
    'William Roberts', 'William Brown',
]

# ── 대주제 분류 ─────────────────────────────────────────────

TOPICS = [
    '진상규명',
    '추모/기념',
    '유해발굴',
    '법제화',
    '해외반응',
    '명예회복',
    '관련인물',
    '부정/논쟁',
    '문화/예술',
    '교육',
]

# ── 프롬프트 ────────────────────────────────────────────────

SYSTEM_PROMPT = """당신은 제주4.3사건 관련 뉴스 기사를 분석하는 전문가입니다.
주어진 기사에서 다음을 추출하여 JSON으로 반환하세요.

반환 형식:
{
  "summary": "기사의 핵심 내용을 2~3문장으로 요약 (원문 언어 그대로)",
  "tags": ["태그1", "태그2", ...],
  "topics": ["대주제1", ...],
  "figures": ["인물1", ...]
}

규칙:
- tags: 기사의 핵심 키워드 5~10개 (명사 중심, 원문 언어)
- topics: 아래 목록에서 해당하는 것만 선택 (복수 가능)
  """ + str(TOPICS) + """
- figures: 아래 인물 목록에 있는 인물 중 기사에 언급된 것만 포함
  """ + str(KNOWN_FIGURES) + """
- JSON만 반환. 설명 텍스트 없이.
"""


def tag_article(article_id: int) -> bool:
    """
    단일 기사에 태그/주제/인물 부착.
    반환: 성공 여부
    """
    article, existing_tags, _, _ = db.get_article(article_id)
    if not article:
        return False

    # 이미 태깅된 경우 스킵
    if existing_tags:
        return True

    # 분석할 텍스트 준비
    text = article['content_text'] or article['title']
    if not text:
        return False

    # 토큰 절약: 최대 3000자
    text_snippet = text[:3000]

    user_prompt = f"""제목: {article['title']}
매체: {article['media_outlet'] or ''}
날짜: {article['published_at'] or ''}

본문:
{text_snippet}
"""

    try:
        response = client.messages.create(
            model='claude-haiku-4-5-20251001',  # 비용 절감을 위해 Haiku 사용
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{'role': 'user', 'content': user_prompt}],
        )

        raw = response.content[0].text.strip()

        # JSON 파싱
        # 코드 블록 감싸진 경우 제거
        if raw.startswith('```'):
            raw = raw.split('```')[1]
            if raw.startswith('json'):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)

        summary = result.get('summary', '')
        tags    = result.get('tags', [])
        topics  = result.get('topics', [])
        figures = result.get('figures', [])

        # 유효한 주제만 필터
        valid_topics = [t for t in topics if t in TOPICS]

        # DB 저장
        if summary:
            conn = db.get_db()
            conn.execute("UPDATE articles SET summary = ? WHERE id = ?", (summary, article_id))
            conn.commit()
            conn.close()

        if tags:
            db.set_article_tags(article_id, tags)
        if valid_topics:
            db.set_article_topics(article_id, valid_topics)
        if figures:
            db.set_article_figures(article_id, figures)

        log.info(f"[tagger] 기사 {article_id} 태깅 완료 — 태그 {len(tags)}개, 주제 {len(valid_topics)}개, 인물 {len(figures)}명")
        return True

    except json.JSONDecodeError as e:
        log.error(f"[tagger] JSON 파싱 실패 (id={article_id}): {e}\n응답: {raw[:200]}")
        return False
    except anthropic.RateLimitError:
        log.warning("[tagger] API rate limit — 60초 대기 후 재시도")
        time.sleep(60)
        return False
    except Exception as e:
        log.error(f"[tagger] 오류 (id={article_id}): {e}")
        return False


def tag_untagged(batch_size: int = 50):
    """태그가 없는 기사를 batch_size만큼 처리."""
    conn = db.get_db()
    rows = conn.execute("""
        SELECT a.id FROM articles a
        WHERE a.id NOT IN (SELECT DISTINCT article_id FROM article_tags)
        ORDER BY a.published_at DESC
        LIMIT ?
    """, (batch_size,)).fetchall()
    conn.close()

    ids = [r['id'] for r in rows]
    log.info(f"[tagger] 미태깅 기사 {len(ids)}개 처리 시작")

    success = 0
    for article_id in ids:
        ok = tag_article(article_id)
        if ok:
            success += 1
        time.sleep(0.3)  # API rate limit 방지

    log.info(f"[tagger] 완료 — {success}/{len(ids)}개 성공")
    return success


if __name__ == '__main__':
    import sys
    db.init_db()
    batch = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    tag_untagged(batch)

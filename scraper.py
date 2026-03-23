"""
제주4.3 뉴스 아카이브 - 스크래퍼
RSS 수집 + 전문 추출 (trafilatura)
"""

import os
import time
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

import feedparser
import trafilatura
import requests

import database as db

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── 수집 대상 RSS 피드 ──────────────────────────────────────

RSS_SOURCES = [
    # ── 국내 ──
    {
        'name': '한겨레',
        'url': 'https://www.hani.co.kr/rss/',
        'language': 'ko',
    },
    {
        'name': '경향신문',
        'url': 'https://www.khan.co.kr/rss/rssdata/total_news.xml',
        'language': 'ko',
    },
    {
        'name': '오마이뉴스',
        'url': 'https://www.ohmynews.com/NWS_Web/Rss/allArticle.aspx',
        'language': 'ko',
    },
    {
        'name': '연합뉴스',
        'url': 'https://www.yna.co.kr/rss/news.xml',
        'language': 'ko',
    },
    {
        'name': '제주도민일보',
        'url': 'https://www.jejudomin.co.kr/rss/allArticle.xml',
        'language': 'ko',
    },
    {
        'name': '제주일보',
        'url': 'https://www.jejunews.com/rss/allArticle.xml',
        'language': 'ko',
    },
    {
        'name': '제주의소리',
        'url': 'https://www.jejusori.net/rss/allArticle.xml',
        'language': 'ko',
    },
    {
        'name': '헤드라인제주',
        'url': 'https://www.headlinejeju.co.kr/rss/allArticle.xml',
        'language': 'ko',
    },
    {
        'name': '제민일보',
        'url': 'https://www.jemin.com/rss/allArticle.xml',
        'language': 'ko',
    },
    {
        'name': '한겨레21',
        'url': 'https://h21.hani.co.kr/rss/',
        'language': 'ko',
    },
    {
        'name': '미디어오늘',
        'url': 'https://www.mediatoday.co.kr/rss/allArticle.xml',
        'language': 'ko',
    },
    # ── 영문 (해외 주요 언론) ──
    {
        'name': 'New York Times',
        'url': 'https://rss.nytimes.com/services/xml/rss/nyt/World.xml',
        'language': 'en',
    },
    {
        'name': 'Washington Post',
        'url': 'https://feeds.washingtonpost.com/rss/world',
        'language': 'en',
    },
    {
        'name': 'The Guardian',
        'url': 'https://www.theguardian.com/world/rss',
        'language': 'en',
    },
    {
        'name': 'Reuters',
        'url': 'https://feeds.reuters.com/reuters/world',
        'language': 'en',
    },
    {
        'name': 'AP News',
        'url': 'https://rsshub.app/apnews/topics/world-news',
        'language': 'en',
    },
    {
        'name': 'BBC News',
        'url': 'https://feeds.bbci.co.uk/news/world/rss.xml',
        'language': 'en',
    },
    {
        'name': 'Time',
        'url': 'https://time.com/feed/',
        'language': 'en',
    },
]

# ── 필터링 키워드 ───────────────────────────────────────────

# 기사 포함 조건: 아래 키워드 중 하나 이상 포함
KEYWORDS_KO = [
    '제주4.3', '제주 4·3', '제주4·3', '4·3사건', '4.3사건',
    '이승만', '조병옥', '박진경', '김익렬', '유재흥', '함병선',
    '김재능', '영락교회',
    '한국전쟁전후민간인학살', '유해발굴', '암매장', '백조일손',
    '제주공항 유해', '민간인학살 유해',
]

KEYWORDS_EN = [
    'Jeju April 3', 'Jeju 4.3', 'Jeju uprising', 'Jeju massacre',
    'Jeju incident', 'April Third',
    'Syngman Rhee', 'Jo Byeong-ok', 'Park Jin-gyeong',
    'Kim Ik-ryeol', 'Youngnak Church',
    'Jeju massacre remains', 'Korean War civilian massacre',
    'mass grave Korea', 'Jeju excavation',
]

ALL_KEYWORDS = KEYWORDS_KO + KEYWORDS_EN


def _contains_keyword(text: str) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    for kw in ALL_KEYWORDS:
        if kw.lower() in text_lower:
            return True
    return False


# ── 날짜 파싱 ───────────────────────────────────────────────

def _parse_date(entry) -> str | None:
    """feedparser entry에서 ISO 날짜 문자열 추출."""
    for field in ('published_parsed', 'updated_parsed'):
        t = getattr(entry, field, None)
        if t:
            try:
                dt = datetime(*t[:6], tzinfo=timezone.utc)
                return dt.strftime('%Y-%m-%d')
            except Exception:
                pass
    # 문자열 필드 fallback
    for field in ('published', 'updated'):
        raw = getattr(entry, field, None)
        if raw:
            # 앞 10자 (YYYY-MM-DD)
            try:
                return raw[:10]
            except Exception:
                pass
    return None


# ── 전문 추출 ───────────────────────────────────────────────

def _fetch_full_text(url: str) -> tuple[str | None, str]:
    """
    trafilatura로 전문 추출 시도.
    반환: (content_text or None, fetch_method)
    """
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(
                downloaded,
                include_comments=False,
                include_tables=False,
                no_fallback=False,
            )
            if text and len(text) > 100:
                return text, 'full_text'
    except Exception as e:
        log.debug(f"trafilatura failed for {url}: {e}")
    return None, 'external_link'


# ── 언어 감지 ───────────────────────────────────────────────

def _detect_language(text: str, default: str) -> str:
    """간단한 한글 비율로 언어 감지."""
    if not text:
        return default
    ko_chars = sum(1 for c in text if '\uAC00' <= c <= '\uD7A3')
    ratio = ko_chars / max(len(text), 1)
    if ratio > 0.1:
        return 'ko'
    # ASCII 비율이 높으면 영어
    ascii_chars = sum(1 for c in text if c.isascii() and c.isalpha())
    if ascii_chars / max(len(text), 1) > 0.5:
        return 'en'
    return default


# ── RSS 피드 수집 ───────────────────────────────────────────

def scrape_feed(source: dict) -> tuple[int, int]:
    """
    단일 RSS 피드 수집.
    반환: (new_count, error_count)
    """
    name = source['name']
    url  = source['url']
    lang = source.get('language', 'ko')
    new_count = 0
    err_count = 0

    log.info(f"[{name}] 피드 수집 시작: {url}")

    try:
        feed = feedparser.parse(url, request_headers={
            'User-Agent': 'Mozilla/5.0 (compatible; Jeju43NewsArchive/1.0)'
        })
    except Exception as e:
        log.error(f"[{name}] 피드 파싱 실패: {e}")
        return 0, 1

    entries = feed.get('entries', [])
    log.info(f"[{name}] {len(entries)}개 항목 발견")

    for entry in entries:
        try:
            title   = entry.get('title', '').strip()
            link    = entry.get('link', '').strip()
            summary = entry.get('summary', '') or entry.get('description', '') or ''

            if not title or not link:
                continue

            # 키워드 필터: 제목 또는 요약에 관련 키워드가 있어야 수집
            if not _contains_keyword(title) and not _contains_keyword(summary):
                continue

            if db.url_exists(link):
                continue

            published_at = _parse_date(entry)

            # 전문 추출 시도
            content_text, fetch_method = _fetch_full_text(link)

            # 언어 감지 (전문 있으면 전문으로, 없으면 요약으로)
            detected_lang = _detect_language(content_text or title, lang)

            article_data = {
                'url':          link,
                'title':        title,
                'author':       entry.get('author', None),
                'media_outlet': name,
                'language':     detected_lang,
                'published_at': published_at,
                'content_text': content_text,
                'fetch_method': fetch_method,
                'summary':      None,  # tagger.py에서 채움
            }

            article_id = db.insert_article(article_data)
            if article_id:
                db.assign_cluster(article_id)
                new_count += 1
                log.info(f"[{name}] 새 기사: {title[:60]}")

            # 과도한 요청 방지
            time.sleep(0.5)

        except Exception as e:
            log.error(f"[{name}] 항목 처리 오류: {e}")
            err_count += 1
            continue

    db.log_scrape(name, new_count, err_count)
    log.info(f"[{name}] 완료 — 신규 {new_count}개, 오류 {err_count}개")
    return new_count, err_count


# ── 전체 실행 ───────────────────────────────────────────────

def run_all(sources=None):
    """모든 RSS 피드 순차 수집."""
    sources = sources or RSS_SOURCES
    total_new = 0
    total_err = 0

    for source in sources:
        new, err = scrape_feed(source)
        total_new += new
        total_err += err
        time.sleep(1)  # 피드 간 딜레이

    log.info(f"전체 완료 — 총 신규 {total_new}개, 오류 {total_err}개")
    return total_new, total_err


if __name__ == '__main__':
    db.init_db()
    run_all()

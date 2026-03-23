"""
네이버 뉴스 검색 API를 이용한 과거 기사 백필 스크립트
사용: python backfill_naver.py [--pages N] [--query "검색어"]

- 기본 쿼리: "제주4.3"
- 1회 최대 100건, start 최대 1000 → 쿼리당 최대 1000건
- 여러 쿼리로 실행하면 더 많은 기사 수집 가능
"""

import argparse
import html
import logging
import os
import re
import time

import requests
import trafilatura
import database as db

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

NAVER_CLIENT_ID     = os.environ.get('NAVER_CLIENT_ID',     'OOtJxU7WSarNKC1rZSrq')
NAVER_CLIENT_SECRET = os.environ.get('NAVER_CLIENT_SECRET', 'Jqa0WtHEDD')

NAVER_NEWS_URL = 'https://openapi.naver.com/v1/search/news.json'

QUERIES = [
    '제주4.3',
    '제주 4·3',
    '제주4.3사건',
    '제주4.3희생자',
    '제주4.3추모',
    '제주4.3유해발굴',
]


def clean_html(text: str) -> str:
    """HTML 태그 및 엔티티 제거."""
    text = re.sub(r'<[^>]+>', '', text)
    return html.unescape(text).strip()


def fetch_naver_news(query: str, display: int = 100, start: int = 1, sort: str = 'date') -> list:
    """네이버 뉴스 검색 API 호출."""
    headers = {
        'X-Naver-Client-Id':     NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET,
    }
    params = {
        'query':   query,
        'display': display,
        'start':   start,
        'sort':    sort,
    }
    try:
        resp = requests.get(NAVER_NEWS_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get('items', [])
    except Exception as e:
        log.error(f"API 오류 (query={query}, start={start}): {e}")
        return []


def extract_outlet(link: str, description: str) -> str:
    """URL에서 언론사명 추출."""
    domain_map = {
        'hani.co.kr':        '한겨레',
        'khan.co.kr':        '경향신문',
        'ohmynews.com':      '오마이뉴스',
        'yna.co.kr':         '연합뉴스',
        'jejunews.com':      '제주일보',
        'jejudomin.co.kr':   '제주도민일보',
        'jejusori.net':      '제주의소리',
        'headlinejeju.co.kr':'헤드라인제주',
        'jemin.com':         '제민일보',
        'mediatoday.co.kr':  '미디어오늘',
        'nytimes.com':       'New York Times',
        'washingtonpost.com':'Washington Post',
        'theguardian.com':   'The Guardian',
        'reuters.com':       'Reuters',
        'apnews.com':        'AP News',
        'bbc.com':           'BBC',
        'bbc.co.uk':         'BBC',
        'time.com':          'Time',
        'yonhapnewstv.co.kr':'연합뉴스TV',
        'news1.kr':          '뉴스1',
        'newsis.com':        '뉴시스',
        'edaily.co.kr':      '이데일리',
        'joins.com':         '중앙일보',
        'joongang.co.kr':    '중앙일보',
        'chosun.com':        '조선일보',
        'donga.com':         '동아일보',
        'pressian.com':      '프레시안',
    }
    for domain, name in domain_map.items():
        if domain in link:
            return name
    # URL에서 도메인 추출
    m = re.search(r'https?://(?:www\.)?([^/]+)', link)
    return m.group(1) if m else '기타'


# 제주4.3 관련 필수 키워드 (제목 또는 설명에 반드시 포함되어야 함)
# 주의: '4.3'만으로는 부족 ("기온 4.3도", "매출 4.3%")이 혼입되므로
#       반드시 '제주'가 붙거나 중점(·) 표기를 요구함
REQUIRED_KEYWORDS = [
    # 제주 + 4.3 조합 (띄어쓰기/붙여쓰기, 점/중점 모두)
    '제주4.3', '제주 4.3', '제주4·3', '제주 4·3',
    # 4·3 단독 (중점 표기 → 숫자 4.3과 명확히 구분됨)
    '4·3사건', '4·3평화', '4·3희생', '4·3추모',
    '4·3유해', '4·3진상', '4·3특별법', '4·3위원회',
    '4·3보고서', '4·3공원', '4·3교육',
    # 영문
    'jeju 4.3', 'jeju april 3', 'jeju massacre', 'april third',
    # 기타
    '제주항쟁', '제주봉기',
]


def is_relevant(title: str, description: str) -> bool:
    """제주4.3 관련 기사인지 제목+설명으로 판단."""
    text = (title + ' ' + description).lower()
    return any(kw.lower() in text for kw in REQUIRED_KEYWORDS)


def fetch_content(url: str) -> str:
    """기사 본문 추출 (trafilatura)."""
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded, include_comments=False, include_tables=False)
            return text or ''
    except Exception:
        pass
    return ''


def backfill(queries: list, max_pages: int = 10, fetch_content_flag: bool = False):
    """
    쿼리 목록으로 네이버 뉴스를 수집해 DB에 저장.
    max_pages: 쿼리당 최대 페이지 수 (1페이지 = 100건)
    fetch_content_flag: True이면 원문 본문 크롤링 (느림)
    """
    db.init_db()
    total_new = 0
    total_skip = 0

    for query in queries:
        log.info(f"[{query}] 수집 시작")
        for page in range(max_pages):
            start = page * 100 + 1
            if start > 1000:
                break

            items = fetch_naver_news(query, display=100, start=start)
            if not items:
                log.info(f"[{query}] start={start} 결과 없음, 종료")
                break

            new_count = 0
            for item in items:
                title   = clean_html(item.get('title', ''))
                link    = item.get('originallink') or item.get('link', '')
                desc    = clean_html(item.get('description', ''))
                pub_raw = item.get('pubDate', '')

                if not title or not link:
                    continue

                # 관련성 필터: 제목+설명에 제주4.3 키워드 없으면 스킵
                if not is_relevant(title, desc):
                    total_skip += 1
                    continue

                # 날짜 파싱 (RFC 2822 → ISO)
                published_at = None
                try:
                    from email.utils import parsedate_to_datetime
                    published_at = parsedate_to_datetime(pub_raw).strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    published_at = pub_raw

                outlet = extract_outlet(link, desc)
                lang   = 'en' if any(d in link for d in ['nytimes', 'washingtonpost', 'theguardian', 'reuters', 'apnews', 'bbc', 'time.com']) else 'ko'

                # 본문: 설명문 사용 (크롤링 옵션 있을 때만 원문)
                content = desc
                if fetch_content_flag and len(desc) < 100:
                    content = fetch_content(link)
                    time.sleep(0.5)

                conn = db.get_db()
                try:
                    conn.execute("""
                        INSERT INTO articles
                            (url, title, media_outlet, language, published_at, content_text, fetch_method)
                        VALUES (?, ?, ?, ?, ?, ?, 'naver_backfill')
                    """, (link, title, outlet, lang, published_at, content))
                    conn.commit()
                    article_id = conn.execute("SELECT id FROM articles WHERE url=?", (link,)).fetchone()['id']
                    conn.close()
                    db.assign_cluster(article_id)
                    new_count += 1
                    total_new += 1
                except Exception:
                    conn.close()
                    total_skip += 1

            log.info(f"[{query}] page {page+1}: {new_count}건 추가 (중복 제외)")
            if len(items) < 100:
                break
            time.sleep(0.3)

        log.info(f"[{query}] 완료")
        time.sleep(1)

    log.info(f"백필 완료 — 신규: {total_new}건, 중복 스킵: {total_skip}건")
    return total_new


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='네이버 뉴스 백필')
    parser.add_argument('--pages',   type=int, default=10,     help='쿼리당 최대 페이지 수 (기본 10 = 최대 1000건)')
    parser.add_argument('--query',   type=str, default=None,   help='단일 검색어 (기본: 전체 쿼리 목록)')
    parser.add_argument('--content', action='store_true',       help='원문 본문 크롤링 여부 (느림)')
    args = parser.parse_args()

    queries = [args.query] if args.query else QUERIES
    total = backfill(queries, max_pages=args.pages, fetch_content_flag=args.content)
    print(f"\n총 {total}건 수집 완료")

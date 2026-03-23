"""
제주4.3 뉴스 아카이브 - 데이터베이스 모듈
"""

import re
import sqlite3
import os

DB_PATH = os.environ.get(
    'DB_PATH',
    os.path.join(os.path.dirname(__file__), 'data', 'news.db')
)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = get_db()
    c = conn.cursor()

    c.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            url             TEXT    UNIQUE NOT NULL,
            title           TEXT    NOT NULL,
            author          TEXT,
            media_outlet    TEXT,
            language        TEXT    DEFAULT 'ko',
            published_at    TEXT,
            content_text    TEXT,
            fetch_method    TEXT    DEFAULT 'external_link',
            summary         TEXT,
            cluster_id      INTEGER REFERENCES clusters(id) ON DELETE SET NULL,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 자동 태그 (Claude API 생성)
        CREATE TABLE IF NOT EXISTS article_tags (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id  INTEGER NOT NULL,
            tag         TEXT    NOT NULL,
            FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
            UNIQUE(article_id, tag)
        );

        -- 대주제 분류 (타임라인용)
        -- 진상규명 | 추모/기념 | 유해발굴 | 법제화 | 해외반응 | 명예회복 | 관련인물 | 부정/논쟁
        CREATE TABLE IF NOT EXISTS article_topics (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id  INTEGER NOT NULL,
            topic       TEXT    NOT NULL,
            FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
            UNIQUE(article_id, topic)
        );

        -- 언급된 관련 인물
        CREATE TABLE IF NOT EXISTS article_figures (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id  INTEGER NOT NULL,
            figure_name TEXT    NOT NULL,
            FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
            UNIQUE(article_id, figure_name)
        );

        -- 사건 클러스터 (같은 사건 보도 묶음)
        CREATE TABLE IF NOT EXISTS clusters (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_title     TEXT    NOT NULL,   -- 대표 제목
            event_date      TEXT,               -- 대표 날짜
            article_count   INTEGER DEFAULT 1,
            outlet_count    INTEGER DEFAULT 1,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 수집 로그
        CREATE TABLE IF NOT EXISTS scrape_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source      TEXT,
            new_articles INTEGER DEFAULT 0,
            errors      INTEGER  DEFAULT 0,
            notes       TEXT
        );
    """)

    # FTS 가상 테이블 (전문 검색)
    c.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
            title, content_text, summary, author, media_outlet,
            content='articles',
            content_rowid='id',
            tokenize='unicode61'
        );
    """)

    c.executescript("""
        CREATE TRIGGER IF NOT EXISTS articles_ai AFTER INSERT ON articles BEGIN
            INSERT INTO articles_fts(rowid, title, content_text, summary, author, media_outlet)
            VALUES (new.id, new.title, new.content_text, new.summary, new.author, new.media_outlet);
        END;

        CREATE TRIGGER IF NOT EXISTS articles_ad AFTER DELETE ON articles BEGIN
            INSERT INTO articles_fts(articles_fts, rowid, title, content_text, summary, author, media_outlet)
            VALUES('delete', old.id, old.title, old.content_text, old.summary, old.author, old.media_outlet);
        END;

        CREATE TRIGGER IF NOT EXISTS articles_au AFTER UPDATE ON articles BEGIN
            INSERT INTO articles_fts(articles_fts, rowid, title, content_text, summary, author, media_outlet)
            VALUES('delete', old.id, old.title, old.content_text, old.summary, old.author, old.media_outlet);
            INSERT INTO articles_fts(rowid, title, content_text, summary, author, media_outlet)
            VALUES (new.id, new.title, new.content_text, new.summary, new.author, new.media_outlet);
        END;
    """)

    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")


# ── 기사 조회 ────────────────────────────────────────────

def get_articles(page=1, per_page=20,
                 language=None, media_outlet=None,
                 topic=None, tag=None, figure=None,
                 year_from=None, year_to=None,
                 sort='published_at', sort_dir='desc'):
    conn = get_db()
    offset = (page - 1) * per_page

    allowed_sorts = {'published_at', 'created_at', 'title', 'media_outlet'}
    if sort not in allowed_sorts:
        sort = 'published_at'
    direction = 'DESC' if sort_dir == 'desc' else 'ASC'

    conds, params = [], []

    if language:
        conds.append("a.language = ?")
        params.append(language)
    if media_outlet:
        conds.append("a.media_outlet = ?")
        params.append(media_outlet)
    if topic:
        conds.append("a.id IN (SELECT article_id FROM article_topics WHERE topic = ?)")
        params.append(topic)
    if tag:
        conds.append("a.id IN (SELECT article_id FROM article_tags WHERE tag = ?)")
        params.append(tag)
    if figure:
        conds.append("a.id IN (SELECT article_id FROM article_figures WHERE figure_name = ?)")
        params.append(figure)
    if year_from:
        conds.append("substr(a.published_at, 1, 4) >= ?")
        params.append(str(year_from))
    if year_to:
        conds.append("substr(a.published_at, 1, 4) <= ?")
        params.append(str(year_to))

    where = ("WHERE " + " AND ".join(conds)) if conds else ""

    rows = conn.execute(f"""
        SELECT a.*,
               GROUP_CONCAT(DISTINCT t.tag)         AS tags,
               GROUP_CONCAT(DISTINCT tp.topic)      AS topics,
               GROUP_CONCAT(DISTINCT f.figure_name) AS figures
        FROM articles a
        LEFT JOIN article_tags    t  ON t.article_id  = a.id
        LEFT JOIN article_topics  tp ON tp.article_id = a.id
        LEFT JOIN article_figures f  ON f.article_id  = a.id
        {where}
        GROUP BY a.id
        ORDER BY a.{sort} {direction}, a.id DESC
        LIMIT ? OFFSET ?
    """, params + [per_page, offset]).fetchall()

    total = conn.execute(
        f"SELECT COUNT(*) FROM articles a {where}", params
    ).fetchone()[0]

    conn.close()
    return rows, total


def _fts_escape(query: str) -> str:
    """FTS5 쿼리에서 특수문자를 큰따옴표로 감싸 리터럴 검색."""
    return f'"{query.replace(chr(34), "")}"'


def search_articles(query, page=1, per_page=20):
    conn = get_db()
    offset = (page - 1) * per_page
    fts_query = _fts_escape(query)

    rows = conn.execute("""
        SELECT a.*,
               GROUP_CONCAT(DISTINCT t.tag)         AS tags,
               GROUP_CONCAT(DISTINCT tp.topic)      AS topics,
               GROUP_CONCAT(DISTINCT f.figure_name) AS figures
        FROM articles a
        LEFT JOIN article_tags    t  ON t.article_id  = a.id
        LEFT JOIN article_topics  tp ON tp.article_id = a.id
        LEFT JOIN article_figures f  ON f.article_id  = a.id
        WHERE a.id IN (SELECT rowid FROM articles_fts WHERE articles_fts MATCH ?)
        GROUP BY a.id
        ORDER BY a.published_at DESC, a.id DESC
        LIMIT ? OFFSET ?
    """, [fts_query, per_page, offset]).fetchall()

    total = conn.execute("""
        SELECT COUNT(*) FROM articles
        WHERE id IN (SELECT rowid FROM articles_fts WHERE articles_fts MATCH ?)
    """, [fts_query]).fetchone()[0]

    conn.close()
    return rows, total


def get_article(article_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM articles WHERE id = ?", (article_id,)).fetchone()
    if not row:
        conn.close()
        return None, [], [], []

    tags    = [r['tag']         for r in conn.execute("SELECT tag FROM article_tags WHERE article_id = ?",    (article_id,)).fetchall()]
    topics  = [r['topic']       for r in conn.execute("SELECT topic FROM article_topics WHERE article_id = ?", (article_id,)).fetchall()]
    figures = [r['figure_name'] for r in conn.execute("SELECT figure_name FROM article_figures WHERE article_id = ?", (article_id,)).fetchall()]

    conn.close()
    return row, tags, topics, figures


# ── 클러스터링 ────────────────────────────────────────────

_STOP = {'의','에','을','를','이','가','은','는','과','와','로','으로',
         '에서','으로부터','까지','부터','도','만','에게','에서의',
         '그','이','저','것','수','등','및','또는','관련','대한',
         'the','a','an','and','or','of','in','for','to','on','with'}

def _tokenize(title: str) -> set:
    title = re.sub(r'[^\w\s가-힣]', ' ', title.lower())
    return {w for w in title.split() if w and w not in _STOP and len(w) > 1}


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def assign_cluster(article_id: int, window_days: int = 7, threshold: float = 0.35) -> int:
    """
    기사와 가장 유사한 최근 클러스터를 찾아 배정.
    적합한 클러스터가 없으면 새로 생성.
    반환: cluster_id
    """
    conn = get_db()
    article = conn.execute(
        "SELECT title, published_at FROM articles WHERE id = ?", (article_id,)
    ).fetchone()
    if not article:
        conn.close()
        return 0

    tokens = _tokenize(article['title'])
    pub_date = (article['published_at'] or '')[:10]

    # 최근 window_days일 내의 다른 기사들과 비교
    candidates = conn.execute("""
        SELECT a.id, a.title, a.cluster_id
        FROM articles a
        WHERE a.id != ?
          AND a.published_at >= date(?, ?)
          AND a.cluster_id IS NOT NULL
        ORDER BY a.published_at DESC
        LIMIT 200
    """, (article_id, pub_date or 'now', f'-{window_days} days')).fetchall()

    best_cluster_id = None
    best_score = 0.0
    for cand in candidates:
        score = _jaccard(tokens, _tokenize(cand['title']))
        if score > best_score:
            best_score = score
            best_cluster_id = cand['cluster_id']

    if best_score >= threshold and best_cluster_id:
        # 기존 클러스터에 배정
        conn.execute("UPDATE articles SET cluster_id = ? WHERE id = ?", (best_cluster_id, article_id))
        conn.execute("""
            UPDATE clusters
            SET article_count = (SELECT COUNT(*) FROM articles WHERE cluster_id = ?),
                outlet_count  = (SELECT COUNT(DISTINCT media_outlet) FROM articles WHERE cluster_id = ?)
            WHERE id = ?
        """, (best_cluster_id, best_cluster_id, best_cluster_id))
        conn.commit()
        conn.close()
        return best_cluster_id
    else:
        # 새 클러스터 생성
        c = conn.cursor()
        c.execute(
            "INSERT INTO clusters (event_title, event_date) VALUES (?, ?)",
            (article['title'], pub_date)
        )
        cluster_id = c.lastrowid
        conn.execute("UPDATE articles SET cluster_id = ? WHERE id = ?", (cluster_id, article_id))
        conn.commit()
        conn.close()
        return cluster_id


def backfill_clusters():
    """기존 기사들에 클러스터를 일괄 배정 (초기 마이그레이션용)."""
    conn = get_db()
    rows = conn.execute(
        "SELECT id FROM articles WHERE cluster_id IS NULL ORDER BY published_at ASC"
    ).fetchall()
    conn.close()
    for row in rows:
        assign_cluster(row['id'])
    return len(rows)


# ── 클러스터 조회 ─────────────────────────────────────────

def get_cluster(cluster_id: int):
    """클러스터 + 소속 기사 목록 반환."""
    conn = get_db()
    cluster = conn.execute("SELECT * FROM clusters WHERE id = ?", (cluster_id,)).fetchone()
    if not cluster:
        conn.close()
        return None, []

    articles = conn.execute("""
        SELECT a.*,
               GROUP_CONCAT(DISTINCT t.tag)         AS tags,
               GROUP_CONCAT(DISTINCT tp.topic)      AS topics,
               GROUP_CONCAT(DISTINCT f.figure_name) AS figures
        FROM articles a
        LEFT JOIN article_tags    t  ON t.article_id  = a.id
        LEFT JOIN article_topics  tp ON tp.article_id = a.id
        LEFT JOIN article_figures f  ON f.article_id  = a.id
        WHERE a.cluster_id = ?
        GROUP BY a.id
        ORDER BY a.published_at ASC, a.media_outlet ASC
    """, (cluster_id,)).fetchall()

    conn.close()
    return cluster, articles


def get_timeline_clusters(days: int = 30):
    """메인 타임라인용 — 최근 N일간 클러스터를 날짜별로 그룹핑."""
    conn = get_db()
    rows = conn.execute("""
        SELECT
            c.id            AS cluster_id,
            c.event_title,
            c.event_date,
            c.article_count,
            c.outlet_count,
            rep.id          AS rep_article_id,
            rep.summary     AS rep_summary,
            rep.published_at,
            (
                SELECT GROUP_CONCAT(DISTINCT a2.media_outlet)
                FROM articles a2
                WHERE a2.cluster_id = c.id
            )               AS outlets,
            GROUP_CONCAT(DISTINCT tp.topic) AS topics
        FROM clusters c
        JOIN articles rep ON rep.id = (
            SELECT id FROM articles
            WHERE cluster_id = c.id
            ORDER BY published_at DESC LIMIT 1
        )
        LEFT JOIN article_topics tp ON tp.article_id = rep.id
        WHERE c.event_date >= date('now', ?)
        GROUP BY c.id
        ORDER BY c.event_date DESC, c.id DESC
    """, (f'-{days} days',)).fetchall()
    conn.close()

    # 날짜별로 그룹핑
    from collections import OrderedDict
    grouped = OrderedDict()
    for r in rows:
        date = (r['event_date'] or r['published_at'] or '')[:10]
        if date not in grouped:
            grouped[date] = []
        grouped[date].append(dict(r))
    return grouped


def get_monthly_keywords(months: int = 36):
    """월별 상위 태그 + 주제 집계 (메인 월별 타임라인용)."""
    from collections import OrderedDict
    conn = get_db()

    article_counts = {
        r['month']: r['cnt']
        for r in conn.execute("""
            SELECT substr(published_at, 1, 7) AS month, COUNT(*) AS cnt
            FROM articles
            WHERE published_at IS NOT NULL
            GROUP BY month
            ORDER BY month DESC
            LIMIT ?
        """, (months,)).fetchall()
    }

    tag_rows = conn.execute("""
        SELECT substr(a.published_at, 1, 7) AS month,
               t.tag, COUNT(*) AS cnt
        FROM articles a
        JOIN article_tags t ON t.article_id = a.id
        WHERE a.published_at IS NOT NULL
        GROUP BY month, t.tag
        ORDER BY month DESC, cnt DESC
    """).fetchall()

    topic_rows = conn.execute("""
        SELECT substr(a.published_at, 1, 7) AS month,
               tp.topic, COUNT(*) AS cnt
        FROM articles a
        JOIN article_topics tp ON tp.article_id = a.id
        WHERE a.published_at IS NOT NULL
        GROUP BY month, tp.topic
        ORDER BY month DESC, cnt DESC
    """).fetchall()

    conn.close()

    monthly = OrderedDict()
    for month in sorted(article_counts.keys(), reverse=True)[:months]:
        monthly[month] = {
            'month': month,
            'tags': [],
            'topics': [],
            'article_count': article_counts[month],
        }

    for r in tag_rows:
        m = r['month']
        if m in monthly and len(monthly[m]['tags']) < 8:
            monthly[m]['tags'].append({'tag': r['tag'], 'cnt': r['cnt']})

    for r in topic_rows:
        m = r['month']
        if m in monthly and len(monthly[m]['topics']) < 4:
            monthly[m]['topics'].append({'topic': r['topic'], 'cnt': r['cnt']})

    return list(monthly.values())


def get_recent_clusters(limit: int = 12):
    """메인 페이지용 — 최신 클러스터 목록 (대표 기사 + 전체 언론사 목록 포함)."""
    conn = get_db()
    rows = conn.execute("""
        SELECT
            c.id            AS cluster_id,
            c.event_title,
            c.event_date,
            c.article_count,
            c.outlet_count,
            rep.id          AS rep_article_id,
            rep.title       AS rep_title,
            rep.summary     AS rep_summary,
            rep.published_at,
            (
                SELECT GROUP_CONCAT(DISTINCT a2.media_outlet)
                FROM articles a2
                WHERE a2.cluster_id = c.id
            )               AS outlets,
            GROUP_CONCAT(DISTINCT tp.topic) AS topics,
            GROUP_CONCAT(DISTINCT t.tag)    AS tags
        FROM clusters c
        JOIN articles rep ON rep.id = (
            SELECT id FROM articles
            WHERE cluster_id = c.id
            ORDER BY published_at DESC LIMIT 1
        )
        LEFT JOIN article_topics tp ON tp.article_id = rep.id
        LEFT JOIN article_tags   t  ON t.article_id  = rep.id
        GROUP BY c.id
        ORDER BY c.event_date DESC, c.id DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def url_exists(url):
    conn = get_db()
    row = conn.execute("SELECT id FROM articles WHERE url = ?", (url,)).fetchone()
    conn.close()
    return row is not None


def insert_article(data):
    """Insert article, return new id. data는 dict."""
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO articles
            (url, title, author, media_outlet, language,
             published_at, content_text, fetch_method, summary)
        VALUES
            (:url, :title, :author, :media_outlet, :language,
             :published_at, :content_text, :fetch_method, :summary)
    """, data)
    article_id = c.lastrowid
    conn.commit()
    conn.close()
    return article_id


def set_article_tags(article_id, tags):
    conn = get_db()
    conn.execute("DELETE FROM article_tags WHERE article_id = ?", (article_id,))
    for tag in tags:
        tag = tag.strip()
        if tag:
            conn.execute(
                "INSERT OR IGNORE INTO article_tags (article_id, tag) VALUES (?, ?)",
                (article_id, tag)
            )
    conn.commit()
    conn.close()


def set_article_topics(article_id, topics):
    conn = get_db()
    conn.execute("DELETE FROM article_topics WHERE article_id = ?", (article_id,))
    for topic in topics:
        topic = topic.strip()
        if topic:
            conn.execute(
                "INSERT OR IGNORE INTO article_topics (article_id, topic) VALUES (?, ?)",
                (article_id, topic)
            )
    conn.commit()
    conn.close()


def set_article_figures(article_id, figures):
    conn = get_db()
    conn.execute("DELETE FROM article_figures WHERE article_id = ?", (article_id,))
    for fig in figures:
        fig = fig.strip()
        if fig:
            conn.execute(
                "INSERT OR IGNORE INTO article_figures (article_id, figure_name) VALUES (?, ?)",
                (article_id, fig)
            )
    conn.commit()
    conn.close()


def log_scrape(source, new_articles, errors, notes=''):
    conn = get_db()
    conn.execute(
        "INSERT INTO scrape_log (source, new_articles, errors, notes) VALUES (?, ?, ?, ?)",
        (source, new_articles, errors, notes)
    )
    conn.commit()
    conn.close()


# ── 통계 / 집계 ───────────────────────────────────────────

def get_stats():
    conn = get_db()
    stats = {
        'total':    conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0],
        'ko':       conn.execute("SELECT COUNT(*) FROM articles WHERE language='ko'").fetchone()[0],
        'en':       conn.execute("SELECT COUNT(*) FROM articles WHERE language='en'").fetchone()[0],
        'outlets':  conn.execute("SELECT COUNT(DISTINCT media_outlet) FROM articles").fetchone()[0],
        'last_run': conn.execute("SELECT run_at FROM scrape_log ORDER BY id DESC LIMIT 1").fetchone(),
    }
    conn.close()
    if stats['last_run']:
        stats['last_run'] = stats['last_run']['run_at']
    return stats


def get_timeline_data():
    """연도 × 주제 집계 (타임라인 히트맵용)."""
    conn = get_db()
    rows = conn.execute("""
        SELECT substr(a.published_at, 1, 4) AS year,
               tp.topic,
               COUNT(*) AS cnt
        FROM articles a
        JOIN article_topics tp ON tp.article_id = a.id
        WHERE year IS NOT NULL AND year != ''
        GROUP BY year, tp.topic
        ORDER BY year ASC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_top_tags(n=50):
    conn = get_db()
    rows = conn.execute("""
        SELECT tag, COUNT(*) AS cnt FROM article_tags
        GROUP BY tag ORDER BY cnt DESC LIMIT ?
    """, (n,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_figures_summary():
    conn = get_db()
    rows = conn.execute("""
        SELECT figure_name, COUNT(*) AS cnt FROM article_figures
        GROUP BY figure_name ORDER BY cnt DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_outlets():
    conn = get_db()
    rows = conn.execute("""
        SELECT media_outlet, language, COUNT(*) AS cnt FROM articles
        WHERE media_outlet IS NOT NULL
        GROUP BY media_outlet ORDER BY cnt DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


if __name__ == '__main__':
    init_db()

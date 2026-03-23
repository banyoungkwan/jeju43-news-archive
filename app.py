"""
제주4.3 뉴스 아카이브
Jeju April 3rd News Archive
"""

import os
import json
import math
import hmac
import hashlib
from functools import wraps

from flask import (
    Flask, render_template, request, redirect,
    url_for, jsonify, abort
)

import database as db

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-in-prod')

PER_PAGE = 20

SCRAPE_SECRET = os.environ.get('SCRAPE_SECRET', '')

TOPICS = [
    '진상규명', '추모/기념', '유해발굴', '법제화',
    '해외반응', '명예회복', '관련인물', '부정/논쟁',
    '문화/예술', '교육',
]

TOPIC_COLORS = {
    '진상규명':  '#e74c3c',
    '추모/기념': '#9b59b6',
    '유해발굴':  '#1abc9c',
    '법제화':    '#2980b9',
    '해외반응':  '#f39c12',
    '명예회복':  '#27ae60',
    '관련인물':  '#e67e22',
    '부정/논쟁': '#7f8c8d',
    '문화/예술': '#8e44ad',
    '교육':      '#16a085',
}


# ── 유틸 ─────────────────────────────────────────────────────

def _paginate(total, page, per_page):
    total_pages = max(1, math.ceil(total / per_page))
    return {
        'page':        page,
        'per_page':    per_page,
        'total':       total,
        'total_pages': total_pages,
        'has_prev':    page > 1,
        'has_next':    page < total_pages,
    }


def _verify_scrape_secret(key: str) -> bool:
    if not SCRAPE_SECRET:
        return False
    return hmac.compare_digest(key, SCRAPE_SECRET)


# ── 메인 페이지 ───────────────────────────────────────────────

@app.route('/')
def index():
    # 최신 뉴스 타임라인 (날짜별 클러스터)
    timeline_clusters = db.get_timeline_clusters(days=30)
    # 월별 키워드 타임라인
    monthly_keywords = db.get_monthly_keywords(months=24)

    # 타임라인 데이터
    timeline_raw = db.get_timeline_data()

    # 연도 목록 및 주제별 집계 구성
    year_topic_map = {}
    for row in timeline_raw:
        y, t, c = row['year'], row['topic'], row['cnt']
        if y not in year_topic_map:
            year_topic_map[y] = {}
        year_topic_map[y][t] = c

    years = sorted(year_topic_map.keys())

    stats = db.get_stats()
    top_tags = db.get_top_tags(30)

    return render_template(
        'index.html',
        timeline_clusters=timeline_clusters,
        monthly_keywords=monthly_keywords,
        years=years,
        year_topic_map=json.dumps(year_topic_map),
        topics=TOPICS,
        topic_colors=TOPIC_COLORS,
        stats=stats,
        top_tags=top_tags,
    )


# ── 클러스터 상세 ─────────────────────────────────────────────

@app.route('/cluster/<int:cluster_id>')
def cluster_detail(cluster_id):
    cluster, articles = db.get_cluster(cluster_id)
    if not cluster:
        abort(404)
    return render_template(
        'cluster.html',
        cluster=cluster,
        articles=articles,
        topic_colors=TOPIC_COLORS,
    )


# ── 기사 목록 ─────────────────────────────────────────────────

@app.route('/articles')
def articles():
    page         = max(1, request.args.get('page', 1, type=int))
    language     = request.args.get('lang', '')
    media_outlet = request.args.get('outlet', '')
    topic        = request.args.get('topic', '')
    tag          = request.args.get('tag', '')
    figure       = request.args.get('figure', '')
    year_from    = request.args.get('year_from', '', type=str)
    year_to      = request.args.get('year_to', '', type=str)
    sort         = request.args.get('sort', 'published_at')
    sort_dir     = request.args.get('dir', 'desc')

    rows, total = db.get_articles(
        page=page, per_page=PER_PAGE,
        language=language or None,
        media_outlet=media_outlet or None,
        topic=topic or None,
        tag=tag or None,
        figure=figure or None,
        year_from=year_from or None,
        year_to=year_to or None,
        sort=sort,
        sort_dir=sort_dir,
    )

    pagination = _paginate(total, page, PER_PAGE)
    outlets = db.get_outlets()

    return render_template(
        'articles.html',
        articles=rows,
        pagination=pagination,
        outlets=outlets,
        topics=TOPICS,
        topic_colors=TOPIC_COLORS,
        filters={
            'lang': language, 'outlet': media_outlet,
            'topic': topic, 'tag': tag, 'figure': figure,
            'year_from': year_from, 'year_to': year_to,
            'sort': sort, 'dir': sort_dir,
        },
    )


# ── 기사 상세 ─────────────────────────────────────────────────

@app.route('/articles/<int:article_id>')
def article_detail(article_id):
    article, tags, topics, figures = db.get_article(article_id)
    if not article:
        abort(404)

    # 관련 기사: 같은 태그 또는 주제
    related = []
    if tags:
        rel_rows, _ = db.get_articles(tag=tags[0], per_page=5)
        related = [r for r in rel_rows if r['id'] != article_id][:4]

    return render_template(
        'article.html',
        article=article,
        tags=tags,
        topics=topics,
        figures=figures,
        related=related,
        topic_colors=TOPIC_COLORS,
    )


# ── 검색 ─────────────────────────────────────────────────────

@app.route('/search')
def search():
    query = request.args.get('q', '').strip()
    page  = max(1, request.args.get('page', 1, type=int))

    rows, total = [], 0
    if query:
        rows, total = db.search_articles(query, page=page, per_page=PER_PAGE)

    pagination = _paginate(total, page, PER_PAGE)

    return render_template(
        'search.html',
        query=query,
        articles=rows,
        pagination=pagination,
        topic_colors=TOPIC_COLORS,
    )


# ── 타임라인 ──────────────────────────────────────────────────

@app.route('/timeline')
def timeline():
    timeline_raw = db.get_timeline_data()

    year_topic_map = {}
    for row in timeline_raw:
        y, t, c = row['year'], row['topic'], row['cnt']
        if y not in year_topic_map:
            year_topic_map[y] = {}
        year_topic_map[y][t] = c

    years = sorted(year_topic_map.keys())

    return render_template(
        'timeline.html',
        years=years,
        year_topic_map=json.dumps(year_topic_map),
        topics=TOPICS,
        topic_colors=TOPIC_COLORS,
    )


# ── 인물별 모아보기 ───────────────────────────────────────────

@app.route('/figures')
def figures():
    figures_summary = db.get_figures_summary()
    selected = request.args.get('name', '')
    page = max(1, request.args.get('page', 1, type=int))

    rows, total = [], 0
    if selected:
        rows, total = db.get_articles(figure=selected, page=page, per_page=PER_PAGE)

    pagination = _paginate(total, page, PER_PAGE)

    return render_template(
        'figures.html',
        figures_summary=figures_summary,
        selected=selected,
        articles=rows,
        pagination=pagination,
        topic_colors=TOPIC_COLORS,
    )


# ── 소개 ─────────────────────────────────────────────────────

@app.route('/about')
def about():
    outlets = db.get_outlets()
    stats   = db.get_stats()
    return render_template('about.html', outlets=outlets, stats=stats)


# ── API: 스크래핑 트리거 (GitHub Actions용) ───────────────────

@app.route('/api/scrape', methods=['POST'])
def api_scrape():
    key = request.args.get('key', '') or request.headers.get('X-Scrape-Key', '')
    if not _verify_scrape_secret(key):
        abort(403)

    try:
        import scraper
        import tagger

        new_count, err_count = scraper.run_all()
        tagged = tagger.tag_untagged(batch_size=100)

        return jsonify({
            'status': 'ok',
            'new_articles': new_count,
            'errors': err_count,
            'tagged': tagged,
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ── API: 네이버 백필 ─────────────────────────────────────────

@app.route('/api/backfill', methods=['POST'])
def api_backfill():
    key = request.args.get('key', '') or request.headers.get('X-Scrape-Key', '')
    if not _verify_scrape_secret(key):
        abort(403)

    try:
        import backfill_naver
        pages = int(request.args.get('pages', 10))
        query = request.args.get('query', None)
        queries = [query] if query else backfill_naver.QUERIES
        total = backfill_naver.backfill(queries, max_pages=pages)

        import tagger
        tagged = tagger.tag_untagged(batch_size=200)

        return jsonify({'status': 'ok', 'new_articles': total, 'tagged': tagged})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ── API: 통계 (헬스체크 겸) ───────────────────────────────────

@app.route('/api/stats')
def api_stats():
    return jsonify(db.get_stats())


# ── 에러 핸들러 ───────────────────────────────────────────────

@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404


@app.errorhandler(403)
def forbidden(e):
    return render_template('403.html'), 403


# ── 초기화 ───────────────────────────────────────────────────

with app.app_context():
    db.init_db()


if __name__ == '__main__':
    app.run(debug=True, port=5001)

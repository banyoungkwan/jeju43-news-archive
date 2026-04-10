"""OpenRouter Qwen API 연결 테스트
Fly.io에 설정된 OPENROUTER_API_KEY를 사용하여 실제 API 호출을 검증합니다.

사용법:
  fly ssh console -C "python test_openrouter.py"
"""
import os
import sys
import json
import re
import time

try:
    from openai import OpenAI
except ImportError:
    print("[FAIL] openai 라이브러리가 설치되지 않았습니다.")
    print("       pip install openai 를 실행하세요.")
    sys.exit(1)

# ── 설정 ─────────────────────────────────────────────────────
API_KEY = os.environ.get('OPENROUTER_API_KEY')
MODEL   = os.environ.get('OPENROUTER_MODEL', 'qwen/qwen3-max-thinking')

print("=" * 60)
print("OpenRouter Qwen API 테스트")
print("=" * 60)

# 1) API 키 확인
if not API_KEY:
    print("[FAIL] OPENROUTER_API_KEY 환경변수가 설정되지 않았습니다.")
    sys.exit(1)
print(f"[OK]   API 키 확인됨: {API_KEY[:12]}...")
print(f"[INFO] 모델: {MODEL}")
print()

# 2) 클라이언트 생성 및 API 호출
client = OpenAI(
    base_url='https://openrouter.ai/api/v1',
    api_key=API_KEY,
)

SAMPLE_ARTICLE = """제목: 제주4·3 76주년 추념식 대통령 참석
매체: 한겨레
날짜: 2024-04-03

본문:
제주4·3사건 76주년을 맞아 제주시 봉개동 제주4·3평화공원에서 추념식이 열렸다.
대통령이 직접 참석해 유족들에게 위로의 말을 전했으며,
추가 진상규명과 명예회복을 위한 노력을 계속하겠다고 밝혔다.
유족회 측은 아직도 밝혀지지 않은 희생자들의 진샱규명을 촉구했다."""

SYSTEM_PROMPT = """당싨은 제주4.3사건 관련 뉴스 기사를 분석하는 전문가입니다.
주어진 기사에서 다음을 추출하여 JSON으로 반환하세요.

반환 형식:
{
  "summary": "기사 요약 2~3문장",
  "tags": ["태그1", "태그2"],
  "topics": ["대주제1"],
  "figures": ["인물1"]
}

- JSON만 반환. 설명 텍스트 없이."""

print("[TEST] API 호출 중...")
start = time.time()

try:
    response = client.chat.completions.create(
        model=MODEL,
        max_tokens=512,
        messages=[
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': SAMPLE_ARTICLE},
        ],
        extra_headers={
            'HTTP-Referer': 'https://news.jeju43.info',
            'X-Title': 'Jeju43 News Archive',
        },
    )
    elapsed = time.time() - start

    raw = response.choices[0].message.content.strip()
    print(f"[OK]   API 응답 수신 ({elapsed:.1f}초)")
    print()

    # thinking 태그 제거
    cleaned = raw
    if '<think>' in cleaned:
        cleaned = re.sub(r'<think>.*?</think>', '', cleaned, flags=re.DOTALL).strip()

    # 코드 블록 제거
    if cleaned.startswith('```'):
        cleaned = cleaned.split('```')[1]
        if cleaned.startswith('json'):
            cleaned = cleaned[4:]
    cleaned = cleaned.strip()

    # JSON 파싱 테스트
    result = json.loads(cleaned)
    print("[OK]   JSON 파싱 성공")
    print()
    print("── 분석 결과 ──────────────────────────────────────")
    print(f"  요약:   {result.get('summary', '(없음)')}")
    print(f"  태그:   {result.get('tags', [])}")
    print(f"  주제:   {result.get('topics', [])}")
    print(f"  인물:   {result.get('figures', [])}")
    print()

    # 토큰 사용량
    if hasattr(response, 'usage') and response.usage:
        u = response.usage
        print(f"  토큰:   입력 {u.prompt_tokens} / 출력 {u.completion_tokens} / 합계 {u.total_tokens}")
        print()

    print("=" * 60)
    print("[PASS] 모든 테스트 통과! API 연동이 정상 작동합니다.")
    print("=" * 60)

except json.JSONDecodeError as e:
    print(f"[FAIL] JSON 파싱 실패: {e}")
    print(f"       원본 응답: {raw[:300]}")
    sys.exit(1)
except Exception as e:
    print(f"[FAIL] API 호출 실패: {e}")
    sys.exit(1)

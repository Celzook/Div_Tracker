# 📊 ETF 고배당 유니버스 v3

배당/고배당 ETF 분석 + 구성종목 배당 수익률 전략 시스템

## 🚀 Streamlit Cloud 배포

1. 이 레포를 GitHub에 push
2. [share.streamlit.io](https://share.streamlit.io) 접속 → GitHub 연결
3. **Main file: `dividend_app.py`** 선택
4. Deploy 클릭

> 기존 유니버스 탐색기를 사용하려면 Main file을 `app.py`로 변경

### 로컬 실행

```bash
pip install -r requirements.txt
streamlit run dividend_app.py
```

## 📁 파일 구조

| 파일 | 역할 |
|------|------|
| `dividend_app.py` | 🆕 배당 전략 Streamlit 앱 (Phase 0~6) |
| `config_dividend.py` | 🆕 설정 (API키, 분할이벤트, 연도범위) |
| `dividend_collector.py` | 🆕 KRX 배당 수집 + Gate 검증 |
| `trailing_yield.py` | 🆕 Trailing 수익률 시계열 |
| `portfolio_builder.py` | 🆕 100억 포트폴리오 3가지 |
| `buy_strategy.py` | 🆕 Z-Score 밴드 매수전략 |
| `main_pipeline.py` | 🆕 CLI 배치 실행 |
| `etf_universe_builder.py` | 기존: ETF 유니버스 엔진 |
| `app.py` | 기존: ETF 유니버스 탐색기 |

## 🎯 Phase 0~6

| Phase | 기능 | 데이터 소스 |
|-------|------|------------|
| 0 | 데이터 소스 검증 | KRX, 네이버 |
| 1 | 배당 ETF 필터 | 네이버 금융 API |
| 2 | 구성종목 → 종목코드 매핑 | KRX HTTP |
| 3 | 5년+ 배당금 이력 | KRX 연도별 전종목 |
| 4 | Trailing 배당수익률 시계열 | 네이버 수정주가 |
| 5 | 100억 포트폴리오 (3가지) | 계산 |
| 6 | Z-Score 밴드 + 백테스트 | 계산 |

## ⚙️ Open DART API (선택)

`config_dividend.py`에서 설정:
```python
DART_API_KEY = 'your_key'  # https://opendart.fss.or.kr 무료 발급
```

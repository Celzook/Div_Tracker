"""
==============================================================================
 ETF 고배당 유니버스 — Streamlit App v3
 Phase 0~6 전체 파이프라인을 인터랙티브 대시보드로 제공
==============================================================================
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import time, gc, traceback, json

st.set_page_config(
    page_title="ETF 고배당 유니버스",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

from etf_universe_builder import (
    Config, step1_get_tickers_and_names, step2_type_filter_and_classify,
    step3_market_cap_filter, find_latest_business_date,
    krx_get_etf_holdings, naver_get_price_history,
    _load_cache, _save_cache, _http_post,
)
from config_dividend import (
    SPLIT_EVENTS, DIVIDEND_START_YEAR, DIVIDEND_END_YEAR, get_trdDd_for_biz_year,
)
from dividend_collector import (
    DividendCollector, krx_get_dividend_data, adjust_dividend,
)
from trailing_yield import TrailingYieldCalculator, calc_etf_trailing_yield
from portfolio_builder import PortfolioBuilder
from buy_strategy import BuyStrategy


# ============================================================================
# 세션 상태 초기화
# ============================================================================
DEFAULTS = {
    'phase': 0,
    'base_date': None,
    'gate0_ok': False,
    'collector': None,
    # Phase 1
    'df_div_etfs': None,
    'df_covered_call': None,
    # Phase 2
    'holdings_dict': None,
    'unique_stocks': None,
    # Phase 3
    'df_dividend': None,
    # Phase 4
    'df_yield': None,
    'df_etf_yield': None,
    # Phase 5
    'port_results': None,
    'port_summary': None,
    # Phase 6
    'diagnosis': None,
    'backtest': None,
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ============================================================================
# 캐시 함수들
# ============================================================================
@st.cache_data(ttl=3600 * 6, show_spinner=False)
def cached_krx_dividend(trdDd, mktId):
    return krx_get_dividend_data(trdDd, mktId)


@st.cache_data(ttl=3600 * 6, show_spinner=False)
def cached_step1(base_date):
    return step1_get_tickers_and_names(base_date)


@st.cache_data(ttl=3600 * 6, show_spinner=False)
def cached_price_history(code, start, end):
    return naver_get_price_history(code, start, end)


# ============================================================================
# 사이드바
# ============================================================================
def render_sidebar():
    st.sidebar.title("💰 ETF 고배당 유니버스")
    st.sidebar.caption("Phase 0~6 배당 투자 전략")
    st.sidebar.markdown("---")

    min_cap = st.sidebar.number_input("최소 시총 (억원)", value=100, step=50, min_value=50)
    Config.MIN_MARKET_CAP_BILLIONS = min_cap

    years = st.sidebar.slider(
        "배당 수집 연도", 2018, 2024,
        (DIVIDEND_START_YEAR, DIVIDEND_END_YEAR),
    )

    total_억 = st.sidebar.number_input("포트폴리오 (억원)", value=100, step=10)

    st.sidebar.markdown("---")

    # 실행 상태 표시
    phase = st.session_state.phase
    phases = [
        ("Phase 0", "데이터 소스 검증"),
        ("Phase 1", "배당 ETF 필터"),
        ("Phase 2", "구성종목 추출"),
        ("Phase 3", "배당금 이력"),
        ("Phase 4", "Trailing 수익률"),
        ("Phase 5", "포트폴리오"),
        ("Phase 6", "매수 전략"),
    ]
    for i, (name, desc) in enumerate(phases):
        if i < phase:
            st.sidebar.success(f"✅ {name}: {desc}")
        elif i == phase:
            st.sidebar.info(f"▶️ {name}: {desc}")
        else:
            st.sidebar.caption(f"⬜ {name}: {desc}")

    return min_cap, years, int(total_억 * 1e8)


# ============================================================================
# Phase 0: 데이터 소스 검증
# ============================================================================
def render_phase0():
    st.header("Phase 0: 데이터 소스 검증")
    st.caption("KRX 배당 데이터 + 네이버 주가가 정상 작동하는지 확인합니다.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📡 KRX 배당 데이터")
        if st.button("KRX 테스트 실행", key="test_krx", type="primary"):
            with st.spinner("KRX 조회 중..."):
                trdDd = get_trdDd_for_biz_year(DIVIDEND_END_YEAR)
                df = cached_krx_dividend(trdDd, 'STK')

                if df.empty:
                    # 재시도
                    for m in ['0801', '0901', '1001', '1101']:
                        alt = f'{DIVIDEND_END_YEAR + 1}{m}'
                        df = cached_krx_dividend(alt, 'STK')
                        if not df.empty:
                            trdDd = alt
                            break
                    if df.empty:
                        for y in range(DIVIDEND_END_YEAR, DIVIDEND_START_YEAR - 1, -1):
                            alt = f'{y}1001'
                            df = cached_krx_dividend(alt, 'STK')
                            if not df.empty:
                                trdDd = alt
                                break

                if not df.empty:
                    sec = df[df['종목코드'] == '005930']
                    st.success(f"✅ KRX 전종목 {len(df)}개 조회 (trdDd={trdDd})")
                    if not sec.empty:
                        st.metric("삼성전자 주당배당금",
                                  f"{sec.iloc[0]['주당배당금']:,}원",
                                  f"배당수익률 {sec.iloc[0]['배당수익률']}%")
                    st.dataframe(df.head(10), use_container_width=True)

                    # name_to_code 매핑 구축 (KRX)
                    collector = DividendCollector()
                    n2c = {}
                    for _, row in df.iterrows():
                        n2c[row['종목명']] = row['종목코드']
                    df_ksq = cached_krx_dividend(trdDd, 'KSQ')
                    if not df_ksq.empty:
                        for _, row in df_ksq.iterrows():
                            n2c[row['종목명']] = row['종목코드']
                    collector.name_to_code = n2c
                    st.session_state.collector = collector
                    st.session_state.base_date = trdDd
                    st.session_state.gate0_ok = True
                else:
                    st.warning("⚠️ KRX 응답 없음 — FinanceDataReader fallback 시도 중...")
                    from dividend_collector import _fdr_build_name_code_map
                    n2c = _fdr_build_name_code_map()
                    if n2c:
                        collector = DividendCollector()
                        collector.name_to_code = n2c
                        from etf_universe_builder import find_latest_business_date
                        base = find_latest_business_date()
                        st.session_state.collector = collector
                        st.session_state.base_date = base
                        st.session_state.gate0_ok = True
                        st.success(f"✅ FDR fallback 성공: {len(n2c)}개 종목 매핑 완료")
                    else:
                        st.error("❌ KRX·FDR 모두 실패. 네트워크 환경을 확인하세요.")

    with col2:
        st.subheader("📈 네이버 주가 데이터")
        if st.button("네이버 테스트 실행", key="test_naver"):
            with st.spinner("주가 조회 중..."):
                end = datetime.now().strftime("%Y%m%d")
                start = (datetime.now() - timedelta(days=365 * 5)).strftime("%Y%m%d")
                prices = cached_price_history('005930', start, end)
                if len(prices) > 500:
                    st.success(f"✅ 삼성전자 {len(prices)}일 데이터")
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=prices.index, y=prices.values,
                                            name='삼성전자', line=dict(width=1)))
                    fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0),
                                      yaxis_title="종가 (원)")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning(f"⚠️ 데이터 부족: {len(prices)}일")

    # Gate 0 통과 여부
    if st.session_state.gate0_ok:
        st.success("🎉 Gate 0 통과! Phase 1로 진행하세요.")
        if st.button("▶️ Phase 1 시작", type="primary"):
            st.session_state.phase = 1
            st.rerun()


# ============================================================================
# Phase 1: 배당 ETF 필터링
# ============================================================================
def render_phase1():
    st.header("Phase 1: 배당/고배당 ETF 필터링")

    if st.button("🔍 배당 ETF 조회", type="primary"):
        with st.spinner("네이버 금융에서 ETF 리스트 조회 중..."):
            base_date = st.session_state.base_date or find_latest_business_date()
            Config.BASE_DATE = base_date

            df = cached_step1(base_date)
            if df.empty:
                st.error("ETF 전종목 조회 실패")
                return

            df = step2_type_filter_and_classify(df)

            # 배당/인컴 필터
            df_div = df[df['대카테고리'] == '배당/인컴'].copy()
            if len(df_div) > 0:
                df_div = step3_market_cap_filter(df_div, base_date,
                                                  Config.MIN_MARKET_CAP_BILLIONS)

            # 커버드콜 참고
            cc_kw = ['커버드콜', 'COVERED']
            df_cc = df[df['ETF명'].str.upper().apply(
                lambda x: any(k in x for k in cc_kw))]

            st.session_state.df_div_etfs = df_div
            st.session_state.df_covered_call = df_cc

    df_div = st.session_state.df_div_etfs
    if df_div is not None and not df_div.empty:
        st.success(f"✅ 배당/인컴 ETF: **{len(df_div)}개**")

        # 카테고리 분포
        col1, col2 = st.columns([2, 1])
        with col1:
            show_cols = [c for c in ['ETF명', '시가총액(억원)', '종가', '중카테고리'] if c in df_div.columns]
            st.dataframe(df_div[show_cols], use_container_width=True, height=400)
        with col2:
            if '중카테고리' in df_div.columns:
                cat_cnt = df_div['중카테고리'].value_counts()
                fig = px.pie(values=cat_cnt.values, names=cat_cnt.index,
                             title="중카테고리 분포")
                fig.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig, use_container_width=True)

        # 커버드콜 참고
        df_cc = st.session_state.df_covered_call
        if df_cc is not None and not df_cc.empty:
            with st.expander(f"📋 커버드콜 ETF 참고 리스트 ({len(df_cc)}개)"):
                st.dataframe(df_cc[['ETF명']].head(20), use_container_width=True)

        # Gate 1
        if len(df_div) >= 10:
            st.success("Gate 1 통과!")
            if st.button("▶️ Phase 2 시작", type="primary"):
                st.session_state.phase = 2
                st.rerun()
        else:
            st.warning(f"Gate 1 미통과: {len(df_div)}개 (10개 이상 필요)")


# ============================================================================
# Phase 2: 구성종목 추출
# ============================================================================
def render_phase2():
    st.header("Phase 2: PDF 구성종목 → 종목코드 매핑")

    df_etfs = st.session_state.df_div_etfs
    collector = st.session_state.collector

    if df_etfs is None or df_etfs.empty:
        st.warning("Phase 1을 먼저 실행하세요.")
        return

    if collector is None or not collector.name_to_code:
        st.warning("⚠️ 종목명↔코드 매핑 테이블이 없습니다. Phase 0의 'KRX 테스트 실행'을 먼저 완료하세요.")
        return

    if st.button("📄 구성종목 추출", type="primary"):
        base_date = st.session_state.base_date
        Config.TOP_N_HOLDINGS = 30
        n2c = collector.name_to_code
        NON_STOCK = ['현금', '원화예금', '달러', 'CASH', '예수금', 'RP', '선물', 'USD']

        holdings_dict = {}
        all_stocks = {}
        ok, fail = 0, 0
        tickers = df_etfs.index.tolist()

        progress = st.progress(0, text="구성종목 수집 중...")
        for i, ticker in enumerate(tickers):
            items = krx_get_etf_holdings(ticker, base_date)
            if items:
                mapped = []
                for sn, w in items:
                    if any(k in sn for k in NON_STOCK):
                        continue
                    code = n2c.get(sn)
                    if not code:
                        for fn, c in n2c.items():
                            if sn in fn or fn in sn:
                                code = c
                                break
                    if code:
                        mapped.append((sn, code, w))
                        ok += 1
                        if code not in all_stocks:
                            all_stocks[code] = {'종목명': sn, '편입ETF수': 0, '비중합': 0}
                        all_stocks[code]['편입ETF수'] += 1
                        all_stocks[code]['비중합'] += w
                    else:
                        fail += 1
                if mapped:
                    holdings_dict[ticker] = mapped
            progress.progress((i + 1) / len(tickers), text=f"{i + 1}/{len(tickers)} ETF...")
            time.sleep(Config.API_DELAY)

        progress.empty()

        us = pd.DataFrame.from_dict(all_stocks, orient='index')
        us.index.name = '종목코드'
        if not us.empty:
            us['평균비중'] = us['비중합'] / us['편입ETF수']
            us = us.reset_index().sort_values('편입ETF수', ascending=False)
        else:
            us = pd.DataFrame(columns=['종목코드', '종목명', '편입ETF수', '비중합', '평균비중'])
            st.warning("⚠️ 구성종목 매핑 결과 없음. Phase 0에서 KRX 데이터 소스를 먼저 검증하세요.")

        st.session_state.holdings_dict = holdings_dict
        st.session_state.unique_stocks = us

    us = st.session_state.unique_stocks
    if us is not None and not us.empty:
        st.success(f"✅ 고유 종목: **{len(us)}개** (ETF {len(st.session_state.holdings_dict or {})}개)")
        st.dataframe(us.head(30), use_container_width=True, height=400)

        if len(us) >= 10:
            if st.button("▶️ Phase 3 시작", type="primary"):
                st.session_state.phase = 3
                st.rerun()


# ============================================================================
# Phase 3: 배당금 이력 수집
# ============================================================================
def render_phase3(start_year, end_year):
    st.header("Phase 3: 종목별 배당금 이력 (5년+)")

    us = st.session_state.unique_stocks
    if us is None or us.empty:
        st.warning("Phase 2를 먼저 실행하세요.")
        return

    if st.button("💰 배당금 수집 시작", type="primary"):
        all_frames = []
        progress = st.progress(0, text="연도별 KRX 배당 데이터 수집 중...")
        years = list(range(start_year, end_year + 1))

        for i, year in enumerate(years):
            trdDd = get_trdDd_for_biz_year(year)
            frames = []
            for mktId in ['STK', 'KSQ']:
                df = cached_krx_dividend(trdDd, mktId)
                if not df.empty:
                    df['사업연도'] = year
                    df['소스'] = 'KRX'
                    frames.append(df)
                else:
                    # fallback 월
                    for m in ['0801', '0901', '1001']:
                        alt = f'{year + 1}{m}'
                        df = cached_krx_dividend(alt, mktId)
                        if not df.empty:
                            df['사업연도'] = year
                            df['소스'] = 'KRX'
                            frames.append(df)
                            break
            if frames:
                all_frames.append(pd.concat(frames, ignore_index=True))
            progress.progress((i + 1) / len(years), text=f"사업연도 {year}...")

        progress.empty()

        if not all_frames:
            st.error("배당 데이터 수집 실패")
            return

        df_all = pd.concat(all_frames, ignore_index=True)
        df_all['주당배당금_원본'] = df_all['주당배당금']
        df_all['주당배당금_수정'] = df_all.apply(
            lambda r: adjust_dividend(r['종목코드'], r['사업연도'], r['주당배당금']), axis=1)

        # 대상 종목 필터
        target_codes = us['종목코드'].tolist()
        df_target = df_all[df_all['종목코드'].isin(target_codes)].copy()

        # 무배당 제거
        nz = df_target.groupby('종목코드')['주당배당금_수정'].sum()
        df_target = df_target[df_target['종목코드'].isin(nz[nz > 0].index)]

        st.session_state.df_dividend = df_target

    df_div = st.session_state.df_dividend
    if df_div is not None and not df_div.empty:
        n_stocks = df_div['종목코드'].nunique()
        n_years = df_div['사업연도'].nunique()
        st.success(f"✅ **{n_stocks}개** 종목 × **{n_years}년** 배당 데이터")

        # 피벗 테이블
        pivot = df_div.pivot_table(index=['종목코드', '종목명'], columns='사업연도',
                                    values='주당배당금_수정', aggfunc='first')
        st.dataframe(pivot, use_container_width=True, height=400)

        # 삼성전자 검증
        sec = df_div[df_div['종목코드'] == '005930'].sort_values('사업연도')
        if not sec.empty:
            with st.expander("🔍 삼성전자 수정배당금 검증"):
                st.dataframe(sec[['사업연도', '주당배당금_원본', '주당배당금_수정']])

        if n_stocks >= 10:
            if st.button("▶️ Phase 4 시작", type="primary"):
                st.session_state.phase = 4
                st.rerun()


# ============================================================================
# Phase 4: Trailing 수익률
# ============================================================================
def render_phase4():
    st.header("Phase 4: Trailing 배당수익률 시계열")

    df_div = st.session_state.df_dividend
    if df_div is None or df_div.empty:
        st.warning("Phase 3을 먼저 실행하세요.")
        return

    if st.button("📊 수익률 계산 시작", type="primary"):
        calc = TrailingYieldCalculator(df_div, price_days=2500)
        codes = df_div['종목코드'].unique().tolist()

        # 주가 수집
        base = st.session_state.base_date or datetime.now().strftime("%Y%m%d")
        with st.spinner(f"주가 수집 중... ({len(codes)}개 종목)"):
            calc.fetch_prices(codes, base)

        # 수익률 계산
        with st.spinner("Trailing 수익률 계산 중..."):
            df_yield = calc.calc_all(codes)

        # ETF 레벨 Trailing 수익률 (구성종목 비중 가중평균)
        holdings_dict = st.session_state.holdings_dict
        df_etf_yield = pd.DataFrame()
        if holdings_dict and not df_yield.empty:
            with st.spinner("ETF Trailing 수익률 집계 중..."):
                df_etf_yield = calc_etf_trailing_yield(holdings_dict, df_yield)

        st.session_state.df_yield = df_yield
        st.session_state.df_etf_yield = df_etf_yield

    df_yield = st.session_state.df_yield
    df_etf_yield = st.session_state.df_etf_yield

    if df_yield is not None and not df_yield.empty:
        n_stocks = df_yield['종목코드'].nunique()
        st.success(f"✅ **{n_stocks}개** 종목 Trailing 수익률 시계열")

        tab_stock, tab_etf = st.tabs(["📈 종목별 수익률", "🏦 ETF별 Trailing 수익률"])

        with tab_stock:
            # 종목 선택 차트
            stock_options = df_yield.groupby('종목코드').first().reset_index()
            if '종목명' in stock_options.columns:
                options = stock_options[['종목코드', '종목명']].values.tolist()
                labels = [f"{c} {n}" for c, n in options]
            else:
                labels = stock_options['종목코드'].tolist()

            selected = st.multiselect("차트에 표시할 종목", labels, default=labels[:5])
            if selected:
                fig = go.Figure()
                for label in selected:
                    code = label.split()[0]
                    sub = df_yield[df_yield['종목코드'] == code]
                    fig.add_trace(go.Scatter(x=sub['기준월'], y=sub['Trailing수익률'],
                                            name=label, mode='lines'))
                fig.update_layout(height=400, yaxis_title="Trailing 수익률 (%)",
                                  legend=dict(orientation="h", yanchor="bottom", y=1.02),
                                  margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig, use_container_width=True)

            latest = df_yield.loc[df_yield.groupby('종목코드')['기준월'].idxmax()]
            latest_sorted = latest.sort_values('Trailing수익률', ascending=False)
            show_cols = [c for c in ['종목코드', '종목명', '수정종가', 'T12M배당', 'Trailing수익률']
                         if c in latest_sorted.columns]
            st.dataframe(latest_sorted[show_cols].head(30), use_container_width=True)

        with tab_etf:
            if df_etf_yield is not None and not df_etf_yield.empty:
                st.caption("구성종목 수익률을 ETF 포트폴리오 비중으로 가중평균한 Trailing 배당수익률입니다.")

                # ETF 시계열 차트
                etf_tickers = sorted(df_etf_yield['ETF티커'].unique())
                selected_etfs = st.multiselect("ETF 선택", etf_tickers, default=etf_tickers[:5])
                if selected_etfs:
                    fig2 = go.Figure()
                    for ticker in selected_etfs:
                        sub = df_etf_yield[df_etf_yield['ETF티커'] == ticker]
                        fig2.add_trace(go.Scatter(x=sub['기준월'], y=sub['ETF_Trailing수익률'],
                                                  name=ticker, mode='lines'))
                    fig2.update_layout(height=400, yaxis_title="ETF Trailing 수익률 (%)",
                                       legend=dict(orientation="h", yanchor="bottom", y=1.02),
                                       margin=dict(l=0, r=0, t=40, b=0))
                    st.plotly_chart(fig2, use_container_width=True)

                # 최신 ETF 수익률 테이블
                etf_latest = df_etf_yield.loc[
                    df_etf_yield.groupby('ETF티커')['기준월'].idxmax()
                ].sort_values('ETF_Trailing수익률', ascending=False).reset_index(drop=True)
                st.dataframe(etf_latest, use_container_width=True)
            else:
                st.info("ETF 구성종목 매핑 결과가 없습니다. Phase 2를 먼저 실행하세요.")

        if st.button("▶️ Phase 5 시작", type="primary"):
            st.session_state.phase = 5
            st.rerun()


# ============================================================================
# Phase 5: 포트폴리오
# ============================================================================
def render_phase5(total):
    st.header("Phase 5: 100억 모의 포트폴리오")

    df_yield = st.session_state.df_yield
    df_div = st.session_state.df_dividend
    if df_yield is None or df_yield.empty:
        st.warning("Phase 4를 먼저 실행하세요.")
        return

    if st.button("🏦 포트폴리오 구성", type="primary"):
        builder = PortfolioBuilder(df_yield, df_div, total=total)
        builder.filter_candidates(min_cap_억=10000, min_years=3)

        if builder.candidates is None or len(builder.candidates) < 5:
            builder.filter_candidates(min_cap_억=5000, min_years=2)

        if builder.candidates is None or builder.candidates.empty:
            st.error("편입 후보 종목 없음")
            return

        results = {}
        summaries = []
        for method in ['yield_weighted', 'equal', 'cap_weighted']:
            df_p, summary = builder.build(method)
            results[method] = (df_p, summary)
            summary['방법론'] = method
            summaries.append(summary)

        st.session_state.port_results = results
        st.session_state.port_summary = pd.DataFrame(summaries)

    if st.session_state.port_summary is not None:
        df_sum = st.session_state.port_summary
        results = st.session_state.port_results

        # 비교 요약
        st.subheader("📊 3가지 방법론 비교")
        col1, col2, col3 = st.columns(3)
        for i, (method, label) in enumerate([
            ('yield_weighted', '배당수익률 가중'),
            ('equal', '동일비중'),
            ('cap_weighted', '시가총액 가중'),
        ]):
            row = df_sum[df_sum['방법론'] == method].iloc[0]
            with [col1, col2, col3][i]:
                st.metric(label, f"{row['포트폴리오수익률']:.2f}%",
                          f"{int(row['편입종목수'])}종목 | 잔여 {row['잔여현금']/1e8:.1f}억")

        # 탭으로 상세
        tab1, tab2, tab3 = st.tabs(["배당수익률 가중", "동일비중", "시가총액 가중"])
        for tab, method in zip([tab1, tab2, tab3],
                                ['yield_weighted', 'equal', 'cap_weighted']):
            with tab:
                df_p, _ = results[method]
                show_cols = [c for c in ['종목코드', '종목명', '수정종가', 'Trailing수익률',
                                         '목표비중', '매수주수', '실투자금액', '실제비중']
                             if c in df_p.columns]
                st.dataframe(df_p[show_cols], use_container_width=True, height=400)

                # 비중 차트
                if '종목명' in df_p.columns and '실제비중' in df_p.columns:
                    top15 = df_p.nlargest(15, '실제비중')
                    fig = px.bar(top15, x='종목명', y='실제비중',
                                 title=f"종목별 비중 (Top 15)",
                                 labels={'실제비중': '비중', '종목명': ''})
                    fig.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0))
                    st.plotly_chart(fig, use_container_width=True)

        if st.button("▶️ Phase 6 시작", type="primary"):
            st.session_state.phase = 6
            st.rerun()


# ============================================================================
# Phase 6: 매수 전략
# ============================================================================
def render_phase6():
    st.header("Phase 6: 배당수익률 밴드 매수 전략")

    df_yield = st.session_state.df_yield
    if df_yield is None or df_yield.empty:
        st.warning("Phase 4를 먼저 실행하세요.")
        return

    if st.button("🎯 매수 전략 분석", type="primary"):
        strat = BuyStrategy(df_yield)
        diagnosis = strat.current_diagnosis()
        backtest = strat.backtest()
        st.session_state.diagnosis = diagnosis
        st.session_state.backtest = backtest

    diagnosis = st.session_state.diagnosis
    backtest = st.session_state.backtest

    if diagnosis is not None and not diagnosis.empty:
        # 신호 분포
        st.subheader("📡 현재 시점 신호 분포")
        sig_cnt = diagnosis['신호'].value_counts()
        col1, col2 = st.columns([1, 2])
        with col1:
            for sig, cnt in sig_cnt.items():
                emoji = {"강력매수": "🔴", "매수": "🟠", "중립": "⚪",
                         "매도/회피": "🔵", "밴드불가(σ작음)": "⬜"}.get(sig, "⬜")
                st.metric(f"{emoji} {sig}", f"{cnt}개")
        with col2:
            fig = px.pie(values=sig_cnt.values, names=sig_cnt.index,
                         color=sig_cnt.index,
                         color_discrete_map={
                             "강력매수": "#d62728", "매수": "#ff7f0e",
                             "중립": "#7f7f7f", "매도/회피": "#1f77b4",
                             "밴드불가(σ작음)": "#d3d3d3"},
                         title="신호 분포")
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)

        # 매수 신호 종목
        buy = diagnosis[diagnosis['신호'].isin(['매수', '강력매수'])].sort_values(
            'Z-Score', ascending=False)
        if not buy.empty:
            st.subheader(f"🎯 매수 신호 종목 ({len(buy)}개)")
            show_cols = [c for c in ['종목코드', '종목명', '현재가', '현재수익률',
                                     '5Y평균', 'σ', 'Z-Score', '신호', '배당성장률%']
                         if c in buy.columns]
            st.dataframe(buy[show_cols], use_container_width=True, height=300)

        # 전체 진단표
        with st.expander("📋 전체 종목 진단표"):
            st.dataframe(diagnosis, use_container_width=True, height=500)

        # Z-Score 분포 차트
        valid = diagnosis[diagnosis['Z-Score'].notna()]
        if not valid.empty:
            fig = px.histogram(valid, x='Z-Score', nbins=20,
                               color='신호',
                               color_discrete_map={
                                   "강력매수": "#d62728", "매수": "#ff7f0e",
                                   "중립": "#7f7f7f", "매도/회피": "#1f77b4"},
                               title="Z-Score 분포")
            fig.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)

    # 백테스트 결과
    if backtest is not None and not backtest.empty:
        st.subheader("📈 백테스트 결과")
        for h in [3, 6, 12]:
            col = f'{h}M수익률'
            if col in backtest.columns:
                valid = backtest[col].dropna()
                if len(valid) > 0:
                    avg = valid.mean()
                    wr = (valid > 0).mean() * 100
                    st.caption(f"**{h}개월**: 평균 {avg:+.1f}%, 승률 {wr:.0f}% ({len(valid)}건)")

        with st.expander("📋 백테스트 상세"):
            st.dataframe(backtest, use_container_width=True, height=400)

    if diagnosis is not None and not diagnosis.empty:
        st.success("🎉 Phase 6 완료! 전체 파이프라인 종료.")


# ============================================================================
# 메인
# ============================================================================
def main():
    min_cap, years, total = render_sidebar()

    # 탭 구성
    tabs = st.tabs([
        "Phase 0: 검증",
        "Phase 1: ETF 필터",
        "Phase 2: 구성종목",
        "Phase 3: 배당 이력",
        "Phase 4: 수익률",
        "Phase 5: 포트폴리오",
        "Phase 6: 매수전략",
    ])

    with tabs[0]:
        render_phase0()
    with tabs[1]:
        render_phase1()
    with tabs[2]:
        render_phase2()
    with tabs[3]:
        render_phase3(years[0], years[1])
    with tabs[4]:
        render_phase4()
    with tabs[5]:
        render_phase5(total)
    with tabs[6]:
        render_phase6()


if __name__ == "__main__":
    main()

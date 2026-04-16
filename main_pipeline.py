"""
ETF 고배당 유니버스 — 전체 파이프라인 (Phase 0~6)
===================================================================
실행: python main_pipeline.py
===================================================================
"""

import pandas as pd
import numpy as np
import time
import os
import sys
from datetime import datetime

from etf_universe_builder import (
    Config, step1_get_tickers_and_names,
    step2_type_filter_and_classify, step3_market_cap_filter,
    find_latest_business_date, krx_get_etf_holdings,
    _load_cache, _save_cache,
)
from config_dividend import (
    DIVIDEND_START_YEAR, DIVIDEND_END_YEAR,
    PORTFOLIO_TOTAL, MAX_SINGLE_WEIGHT,
)
from dividend_collector import (
    DividendCollector, validate_gate, validate_phase3,
)
from trailing_yield import TrailingYieldCalculator, validate_phase4, calc_etf_trailing_yield
from portfolio_builder import PortfolioBuilder, validate_phase5
from buy_strategy import BuyStrategy, validate_phase6


def run_phase1(base_date):
    print("\n" + "=" * 60)
    print(" Phase 1: 배당/고배당 ETF 필터링")
    print("=" * 60)
    df_all = step1_get_tickers_and_names(base_date)
    if df_all.empty:
        return pd.DataFrame(), pd.DataFrame()
    df_all = step2_type_filter_and_classify(df_all)
    df_div = df_all[df_all['대카테고리'] == '배당/인컴'].copy()
    print(f"\n  → 배당/인컴 ETF: {len(df_div)}개")

    cc_kw = ['커버드콜', 'COVERED CALL', 'COVERED']
    cc = df_all[df_all['ETF명'].str.upper().apply(
        lambda x: any(k.upper() in x for k in cc_kw))]
    print(f"  → 커버드콜 참고: {len(cc)}개")

    if len(df_div) > 0:
        df_div = step3_market_cap_filter(df_div, base_date, min_cap=100)
    return df_div, cc


def run_phase2(df_etfs, name_to_code, base_date):
    print("\n" + "=" * 60)
    print(" Phase 2: PDF 구성종목 → 종목코드 매핑")
    print("=" * 60)
    Config.TOP_N_HOLDINGS = 30
    NON_STOCK = ['현금','원화예금','달러예금','CASH','예수금','RP','선물','원화','USD','예치금']
    holdings_dict = {}
    all_stocks = {}
    ok, fail = 0, 0
    tickers = df_etfs.index.tolist()
    for ticker in tickers:
        items = krx_get_etf_holdings(ticker, base_date)
        if not items: continue
        mapped = []
        for sn, w in items:
            if any(k in sn for k in NON_STOCK): continue
            code = name_to_code.get(sn)
            if not code:
                for fn, c in name_to_code.items():
                    if sn in fn or fn in sn: code = c; break
            if code:
                mapped.append((sn, code, w)); ok += 1
                if code not in all_stocks:
                    all_stocks[code] = {'종목명': sn, '편입ETF수': 0, '비중합': 0}
                all_stocks[code]['편입ETF수'] += 1
                all_stocks[code]['비중합'] += w
            else:
                fail += 1
        if mapped: holdings_dict[ticker] = mapped
        time.sleep(Config.API_DELAY)
    us = pd.DataFrame.from_dict(all_stocks, orient='index')
    us.index.name = '종목코드'
    if not us.empty:
        us['평균비중'] = us['비중합'] / us['편입ETF수']
    us = us.reset_index().sort_values('편입ETF수', ascending=False) if not us.empty else pd.DataFrame()
    total = ok + fail
    print(f"  → 매핑: {ok}/{total} ({ok/total*100:.0f}%)" if total else "  → 매핑 대상 없음")
    print(f"  → 고유 종목: {len(us)}개")
    return holdings_dict, us


def save_outputs(df_etfs, df_cc, holdings, us, df_div, df_yield, df_etf_yield,
                 port_res, port_sum, diag, bt, gate_log):
    d = Config.OUTPUT_DIR; os.makedirs(d, exist_ok=True)
    print(f"\n  산출물 → {d}")
    if not df_etfs.empty:
        df_etfs.to_csv(f"{d}/01_배당ETF_리스트.csv", encoding='utf-8-sig')
    if us is not None and not us.empty:
        us.to_csv(f"{d}/02_구성종목.csv", index=False, encoding='utf-8-sig')
    if df_div is not None and not df_div.empty:
        pv = df_div.pivot_table(index=['종목코드','종목명'], columns='사업연도',
                                values='주당배당금_수정', aggfunc='first')
        pv.to_excel(f"{d}/03_배당금_이력.xlsx")
    if df_yield is not None and not df_yield.empty:
        with pd.ExcelWriter(f"{d}/04_trailing_수익률.xlsx") as w:
            df_yield.to_excel(w, sheet_name='종목별', index=False)
            if df_etf_yield is not None and not df_etf_yield.empty:
                df_etf_yield.to_excel(w, sheet_name='ETF별', index=False)
                # ETF별 최신 수익률 요약
                latest = df_etf_yield.loc[
                    df_etf_yield.groupby('ETF티커')['기준월'].idxmax()
                ].sort_values('ETF_Trailing수익률', ascending=False)
                latest.to_excel(w, sheet_name='ETF_최신', index=False)
    if port_res:
        with pd.ExcelWriter(f"{d}/05_포트폴리오_비교.xlsx") as w:
            for m, (dp, _) in port_res.items(): dp.to_excel(w, sheet_name=m, index=False)
            if port_sum is not None: port_sum.to_excel(w, sheet_name='비교요약', index=False)
    if diag is not None and not diag.empty:
        with pd.ExcelWriter(f"{d}/06_매수전략_리포트.xlsx") as w:
            diag.to_excel(w, sheet_name='밴드진단', index=False)
            if bt is not None and not bt.empty: bt.to_excel(w, sheet_name='백테스트', index=False)
    with open(f"{d}/검증_리포트.txt", 'w', encoding='utf-8') as f:
        f.write(f"생성일: {datetime.now()}\n\n")
        for l in gate_log: f.write(l + "\n")
    print("  ✅ 저장 완료")


def run_pipeline():
    t0 = time.time()
    gate_log = []
    print("╔" + "═" * 58 + "╗")
    print("║  ETF 고배당 유니버스 — 전체 파이프라인 v3                 ║")
    print("╚" + "═" * 58 + "╝")

    # Phase 0
    collector = DividendCollector()
    g0 = collector.verify_source()
    gate_log.append(f"Phase 0: KRX={g0['krx']}, DART={g0['dart']}")
    if not g0['krx']:
        print("\n⚠️ KRX 접속 불가. 로컬 환경에서 재실행하세요.")
        return

    base_date = Config.BASE_DATE or find_latest_business_date()
    Config.BASE_DATE = base_date
    collector.build_name_code_map(base_date)

    # Phase 1
    df_etfs, df_cc = run_phase1(base_date)
    g1 = len(df_etfs) >= 10
    gate_log.append(f"Phase 1: {len(df_etfs)}개 — {'PASS' if g1 else 'FAIL'}")
    if not g1:
        Config.MIN_MARKET_CAP_BILLIONS = 50
        df_etfs, df_cc = run_phase1(base_date)

    # Phase 2
    holdings, us = run_phase2(df_etfs, collector.name_to_code, base_date)
    gate_log.append(f"Phase 2: {len(us)}개 종목")

    # Phase 3
    df_div = collector.collect_all_years()
    df_div = collector.adjust_for_splits(df_div)
    codes = us['종목코드'].tolist() if not us.empty else []
    df_div_t = df_div[df_div['종목코드'].isin(codes)].copy()
    nz = df_div_t.groupby('종목코드')['주당배당금_수정'].sum()
    df_div_t = df_div_t[df_div_t['종목코드'].isin(nz[nz > 0].index)]
    g3 = validate_phase3(df_div_t)
    gate_log.append(f"Phase 3: {df_div_t['종목코드'].nunique()}개 — {'PASS' if g3 else 'FAIL'}")

    # Phase 4: 종목별 Trailing 수익률
    calc = TrailingYieldCalculator(df_div_t)
    calc.fetch_prices(df_div_t['종목코드'].unique().tolist(), base_date)
    df_yield = calc.calc_all(df_div_t['종목코드'].unique().tolist())
    g4 = validate_phase4(df_yield)
    gate_log.append(f"Phase 4: {'PASS' if g4 else 'FAIL'}")

    # Phase 4-B: ETF 레벨 Trailing 수익률 (구성종목 비중 가중평균)
    print("\n" + "=" * 60)
    print(" Phase 4-B: ETF Trailing 배당수익률 집계")
    print("=" * 60)
    df_etf_yield = calc_etf_trailing_yield(holdings, df_yield)
    if not df_etf_yield.empty:
        gate_log.append(f"Phase 4-B: ETF {df_etf_yield['ETF티커'].nunique()}개 PASS")
    else:
        gate_log.append("Phase 4-B: ETF 수익률 집계 결과 없음")

    # Phase 5
    builder = PortfolioBuilder(df_yield, df_div_t)
    builder.filter_candidates(min_cap_억=10000, min_years=3)
    if builder.candidates is not None and len(builder.candidates) < 5:
        builder.filter_candidates(min_cap_억=5000, min_years=2)
    port_res, port_sum = builder.compare_all()
    g5 = validate_phase5(port_res, port_sum)
    gate_log.append(f"Phase 5: {'PASS' if g5 else 'FAIL'}")

    # Phase 6
    strat = BuyStrategy(df_yield)
    diag = strat.current_diagnosis()
    bt = strat.backtest()
    g6 = validate_phase6(diag if diag is not None else pd.DataFrame(),
                         bt if bt is not None else pd.DataFrame())
    gate_log.append(f"Phase 6: {'PASS' if g6 else 'FAIL'}")

    # Save
    save_outputs(df_etfs, df_cc, holdings, us, df_div_t, df_yield, df_etf_yield,
                 port_res, port_sum, diag, bt, gate_log)

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f" ✅ 완료! {elapsed:.0f}초 ({elapsed/60:.1f}분)")
    for l in gate_log: print(f"    {l}")


if __name__ == "__main__":
    run_pipeline()

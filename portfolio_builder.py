"""
ETF 고배당 유니버스 — 100억 모의 포트폴리오 (Phase 5)
"""

import pandas as pd
import numpy as np

from config_dividend import PORTFOLIO_TOTAL, MAX_SINGLE_WEIGHT
from dividend_collector import validate_gate


class PortfolioBuilder:
    def __init__(self, yield_data, div_history, total=None, max_weight=None):
        """
        yield_data: Phase 4 결과 (df_trailing_yield)
        div_history: Phase 3 결과 (df_dividend_history)
        """
        self.yield_data = yield_data
        self.div_history = div_history
        self.total = total or PORTFOLIO_TOTAL
        self.max_weight = max_weight or MAX_SINGLE_WEIGHT
        self.candidates = None

    def filter_candidates(self, min_cap_억=10000, min_years=3):
        """편입 후보 필터링

        Args:
            min_cap_억: 시가총액 하한 (억원), 기본 1조=10000억
            min_years: 최근 N년 연속 배당 요구
        """
        df = self.yield_data.copy()
        if df.empty:
            print("  ⚠️ 수익률 데이터 비어있음")
            self.candidates = pd.DataFrame()
            return self.candidates

        # 최신 월 데이터 추출
        latest_month = df['기준월'].max()
        df_latest = df[df['기준월'] == latest_month].copy()

        # Trailing수익률 > 0
        df_latest = df_latest[df_latest['Trailing수익률'] > 0]

        # 연속배당 체크
        div = self.div_history.copy()
        recent_years = sorted(div['사업연도'].unique())[-min_years:]
        continuous = []
        for code in df_latest['종목코드'].unique():
            code_div = div[(div['종목코드'] == code) & (div['사업연도'].isin(recent_years))]
            if len(code_div) >= min_years and (code_div['주당배당금_수정'] > 0).all():
                continuous.append(code)

        df_latest = df_latest[df_latest['종목코드'].isin(continuous)]

        # 최신 배당금 추가
        latest_div = {}
        for code in df_latest['종목코드']:
            cd = div[div['종목코드'] == code].sort_values('사업연도')
            if not cd.empty:
                latest_div[code] = cd.iloc[-1]['주당배당금_수정']
        df_latest['최신배당금'] = df_latest['종목코드'].map(latest_div)

        df_latest = df_latest.sort_values('Trailing수익률', ascending=False).reset_index(drop=True)
        self.candidates = df_latest
        print(f"  → 편입 후보: {len(df_latest)}개 종목 (수익률>0, {min_years}년 연속배당)")
        return df_latest

    def build(self, method='yield_weighted'):
        """포트폴리오 구성

        Args:
            method: 'yield_weighted' | 'equal' | 'cap_weighted'
        """
        if self.candidates is None or self.candidates.empty:
            print("  ⚠️ filter_candidates()를 먼저 실행하세요")
            return pd.DataFrame()

        df = self.candidates.copy()
        total = self.total
        max_w = self.max_weight

        # 비중 계산
        if method == 'yield_weighted':
            raw_w = df['Trailing수익률'] / df['Trailing수익률'].sum()
        elif method == 'equal':
            raw_w = pd.Series(1 / len(df), index=df.index)
        elif method == 'cap_weighted':
            if '시가총액' in df.columns and df['시가총액'].sum() > 0:
                raw_w = df['시가총액'] / df['시가총액'].sum()
            else:
                # 시가총액 없으면 동일비중 fallback
                raw_w = pd.Series(1 / len(df), index=df.index)
        else:
            raise ValueError(f"Unknown method: {method}")

        # max_weight cap + 재정규화
        w = raw_w.clip(upper=max_w)
        w = w / w.sum()

        df['방법론'] = method
        df['목표비중'] = w.values
        df['투자금액'] = (total * w).astype(int).values
        df['매수주수'] = (df['투자금액'] / df['수정종가']).astype(int)
        df['실투자금액'] = df['매수주수'] * df['수정종가']
        total_invested = df['실투자금액'].sum()
        df['실제비중'] = df['실투자금액'] / total_invested if total_invested > 0 else 0

        # 포트폴리오 배당수익률
        df['예상배당금총액'] = df['최신배당금'] * df['매수주수']
        port_yield = df['예상배당금총액'].sum() / total_invested * 100 if total_invested > 0 else 0

        잔여현금 = total - total_invested

        print(f"\n  [{method}] 편입: {len(df)}종목, "
              f"투자: {total_invested / 1e8:,.0f}억, "
              f"잔여: {잔여현금 / 1e8:,.1f}억, "
              f"수익률: {port_yield:.2f}%")

        return df, {'총투자금': total_invested, '잔여현금': 잔여현금,
                    '포트폴리오수익률': port_yield, '편입종목수': len(df)}

    def compare_all(self):
        """3가지 방법론 비교"""
        print("\n" + "=" * 60)
        print(" Phase 5: 포트폴리오 구성 — 3가지 방법론 비교")
        print("=" * 60)

        if self.candidates is None:
            self.filter_candidates()

        results = {}
        summaries = []
        for method in ['yield_weighted', 'equal', 'cap_weighted']:
            df, summary = self.build(method)
            results[method] = df
            summary['방법론'] = method
            summaries.append(summary)

        df_summary = pd.DataFrame(summaries)
        print(f"\n  비교표:")
        print(df_summary[['방법론', '편입종목수', '포트폴리오수익률', '잔여현금']].to_string(index=False))

        return results, df_summary


def validate_phase5(results, df_summary):
    """Phase 5 Gate 검증"""
    if not results or df_summary.empty:
        return validate_gate("Phase 5", [("결과 존재", False, "비어있음")])

    # yield_weighted 기준 검증
    yw = results.get('yield_weighted')
    if yw is None or (isinstance(yw, tuple) and yw[0].empty):
        return validate_gate("Phase 5", [("yield_weighted 존재", False, "결과 없음")])

    df_yw = yw[0] if isinstance(yw, tuple) else yw
    n_stocks = len(df_yw)
    max_weight = df_yw['실제비중'].max() if not df_yw.empty else 0
    port_yield = df_summary[df_summary['방법론'] == 'yield_weighted']['포트폴리오수익률'].values[0]
    cash = df_summary[df_summary['방법론'] == 'yield_weighted']['잔여현금'].values[0]

    checks = [
        ("편입 종목 10~30개", 10 <= n_stocks <= 30,
         f"현재 {n_stocks}개"),
        ("단일종목 비중 <= 10%", max_weight <= 0.105,  # 반올림 여유
         f"최대 {max_weight:.1%}"),
        ("포트폴리오 수익률 > 2%", port_yield > 2.0,
         f"현재 {port_yield:.2f}%"),
        ("잔여현금 < 5억", cash < 5e8,
         f"현재 {cash / 1e8:.1f}억"),
    ]
    return validate_gate("Phase 5", checks)


if __name__ == "__main__":
    print("portfolio_builder.py — Phase 5에서 사용. main_pipeline.py에서 실행하세요.")

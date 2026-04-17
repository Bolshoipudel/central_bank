"""
Исполняемые тесты для test_cases.md.

Проверяют 3 сценария:
  1. Equity меняет знак в окне ROE 12m → ROE = NaN
  2. |LLP| > Loans_Total_Net → flag_llp_exceeds_net, LDR считается
  3. Пропуск квартальной точки NII → NaN только для пропущенного квартала

Запуск: python test_ratios.py
"""

import sys

import numpy as np
import pandas as pd

from calculate_ratios import (
    add_error_flags,
    add_outlier_flags,
    compute_ldr,
    compute_loan_yield_3m,
    compute_roe_12m,
    parse_value,
    restore_monthly_profit_from_ytd,
)


def _build_df(rows):
    """Собирает минимальный DataFrame в схеме bank_data с заполненными NaN-колонками."""
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    for c in ['Assets', 'Loans_Total_Net', 'Loans_LLP', 'Equity',
              'Net_Income_Current', 'Client_Deposits', 'Net_Interest_Income']:
        if c not in df.columns:
            df[c] = np.nan
    df = df.sort_values(['regn', 'date']).reset_index(drop=True)
    df['month'] = df['date'].dt.month
    return df


def _run_pipeline(df):
    df = add_error_flags(df)
    df = restore_monthly_profit_from_ytd(df)
    df = compute_roe_12m(df)
    df = compute_loan_yield_3m(df)
    df = compute_ldr(df)
    df = add_outlier_flags(df)
    return df


# ---------- parse_value (регрессия) ----------

def test_parse_value():
    cases = [
        ('729 873,3 млн руб.',         729873.3),
        ('272 267 300,0 тыс. руб.',    272267.3),
        ('-321 279 200,0 тыс. руб.',   -321279.2),
        ('11 414 945 300,0 тыс. руб.', 11414945.3),
        ('667,0 млн руб.',             667.0),
        ('-244,9 млн руб.',            -244.9),
        ('1,5 млрд руб.',              1500.0),
        (602749.6,                     602749.6),
    ]
    for raw, expected in cases:
        got = parse_value(raw)
        assert abs(got - expected) < 1e-6, f'parse_value({raw!r}) = {got}, ожидалось {expected}'
    assert pd.isna(parse_value(None))
    assert pd.isna(parse_value(''))
    assert pd.isna(parse_value('nan'))
    print('  OK  parse_value: 11 кейсов')


# ---------- кейс 1: Equity меняет знак в окне ROE 12m ----------

def test_case_1_equity_sign_change():
    """
    13 точек: Equity сначала отрицательный, потом положительный.
    Среднее Equity ≈ 142 > 0, но в окне есть отрицательные → ROE должен быть NaN
    """
    equity = [-500, -300, -100, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500]
    nic    = [1000, 100, 200, 350, 500, 700, 900, 1100, 1350, 1600, 1900, 2200, 2500]
    dates  = pd.date_range('2024-01-01', periods=13, freq='MS')

    rows = [{'regn': 'X', 'date': d, 'Equity': e, 'Net_Income_Current': n,
             'Loans_Total_Net': 1000, 'Loans_LLP': -100, 'Client_Deposits': 2000}
            for d, e, n in zip(dates, equity, nic)]
    df = _build_df(rows)
    df = _run_pipeline(df)

    last = df.iloc[-1]
    assert pd.isna(last['ROE_12m']), f'ROE_12m должен быть NaN, получили {last["ROE_12m"]}'
    assert bool(last['equity_any_neg_in_window']) is True
    print('  OK  test_case_1: ROE=NaN при смене знака Equity в окне')


# ---------- кейс 2: |LLP| > Loans_Total_Net ----------

def test_case_2_llp_exceeds_net():
    """
    Loans_Total_Net = 5000, LLP = −8000.
    Gross = 5000 − (−8000) = 13000 → LDR считается, но флаг выставлен
    """
    rows = [{
        'regn': 'Y', 'date': '2024-01-01',
        'Loans_Total_Net': 5000, 'Loans_LLP': -8000,
        'Client_Deposits': 10000, 'Equity': 1000, 'Assets': 20000,
    }]
    df = _build_df(rows)
    df = _run_pipeline(df)

    row = df.iloc[0]
    assert bool(row['flag_llp_exceeds_net']) is True, 'flag_llp_exceeds_net должен быть True'
    assert not pd.isna(row['LDR']), 'LDR должен считаться (Gross > 0)'
    assert abs(row['LDR'] - 1.3) < 1e-9, f'LDR должен быть 1.3, получили {row["LDR"]}'
    print('  OK  test_case_2: flag_llp_exceeds_net=True, LDR=1.3')


# ---------- кейс 3: пропуск квартальной точки NII ----------

def test_case_3_missing_quarter():
    """
    Пропуск NII на 1 Apr 2024 не должен ломать расчёт на 1 Jul / 1 Oct
    (NII — квартальный flow, каждое значение самодостаточно)
    """
    # Нужны 4 точки Loans до каждого квартала, используем длинный ряд
    dates = pd.date_range('2023-10-01', '2024-10-01', freq='MS')
    rows = []
    nii_map = {
        pd.Timestamp('2024-01-01'): 500,   # Q4 2023
        pd.Timestamp('2024-04-01'): np.nan,  # пропуск
        pd.Timestamp('2024-07-01'): 1200,
        pd.Timestamp('2024-10-01'): 1100,
    }
    for d in dates:
        rows.append({
            'regn': 'Z', 'date': d,
            'Loans_Total_Net': 10000,
            'Loans_LLP': -500,
            'Client_Deposits': 12000,
            'Equity': 2000,
            'Net_Income_Current': 100,
            'Net_Interest_Income': nii_map.get(d, np.nan),
        })
    df = _build_df(rows)
    df = _run_pipeline(df)

    by_date = df.set_index('date')
    assert pd.isna(by_date.loc['2024-04-01', 'LoanYield_3m']), 'Apr: NaN (нет NII)'
    # Jul: 4 × 1200 / 10000 = 0.48
    assert abs(by_date.loc['2024-07-01', 'LoanYield_3m'] - 0.48) < 1e-9, \
        f'Jul LoanYield = {by_date.loc["2024-07-01", "LoanYield_3m"]}, ожидалось 0.48'
    # Oct: 4 × 1100 / 10000 = 0.44
    assert abs(by_date.loc['2024-10-01', 'LoanYield_3m'] - 0.44) < 1e-9, \
        f'Oct LoanYield = {by_date.loc["2024-10-01", "LoanYield_3m"]}, ожидалось 0.44'
    print('  OK  test_case_3: NaN для Apr, корректно для Jul/Oct')


# ---------- runner ----------

def main():
    tests = [
        test_parse_value,
        test_case_1_equity_sign_change,
        test_case_2_llp_exceeds_net,
        test_case_3_missing_quarter,
    ]
    failed = 0
    for t in tests:
        print(f'{t.__name__}:')
        try:
            t()
        except AssertionError as e:
            print(f'  FAIL  {e}')
            failed += 1
        except Exception as e:
            print(f'  ERROR  {type(e).__name__}: {e}')
            failed += 1

    print()
    if failed:
        print(f'{failed} тестов упало')
        sys.exit(1)
    print('Все тесты прошли')


if __name__ == '__main__':
    main()

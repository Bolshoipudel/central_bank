"""
Расчёт финансовых коэффициентов (ROE 12m, LoanYield 3m, LDR) по bank_data.xlsx.

Методология и обоснования — second_task/report.md. Ключевые решения:
  - NIC трактуется как YTD (ratio-тест: sum_year / FY ≈ 6.5 ⇒ YTD-гипотеза)
  - NII трактуется как квартальный flow (монотонность опровергает YTD-гипотезу)
  - Equity < 0 в любой точке 13-месячного окна → ROE не считается
  - Loans_Gross = Loans_Total_Net − Loans_LLP (LLP обычно отрицательный)

Использование:
  python calculate_ratios.py -i /path/to/bank_data.xlsx
  python calculate_ratios.py -i /path/to/bank_data.xlsx -o results.csv
"""

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd


NUM_COLS = [
    'Assets', 'Loans_Total_Net', 'Loans_LLP', 'Equity',
    'Net_Income_Current', 'Client_Deposits', 'Net_Interest_Income',
]

NBSP = '\u00a0'
NARROW_NBSP = '\u202f'

# Диапазоны для outlier-флагов (не блокируют расчёт — только помечают)
ROE_OUTLIER_ABS = 1.0         # |ROE| > 100%
YIELD_OUTLIER_RANGE = (0.0, 0.5)
LDR_OUTLIER_RANGE = (0.1, 3.0)


# ---------- парсинг ----------

def parse_value(x):
    """
    Приводит значение к float в единицах «млн руб.».
    Поддерживает: числа, 'NNN,N', 'N NNN,N млн руб.', 'N NNN NNN,N тыс. руб.',
                  'N,N млрд руб.', NBSP/narrow-NBSP как разделители тысяч.
    Пустые/нераспознанные значения → np.nan.
    """
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip().replace(NBSP, ' ').replace(NARROW_NBSP, ' ')
    unit_mult = 1.0
    low = s.lower()
    if 'тыс' in low:
        unit_mult = 1e-3
        s = re.sub(r'тыс\.?\s*руб\.?', '', s, flags=re.IGNORECASE)
    elif 'млрд' in low:
        unit_mult = 1e3
        s = re.sub(r'млрд\.?\s*руб\.?', '', s, flags=re.IGNORECASE)
    elif 'млн' in low:
        unit_mult = 1.0
        s = re.sub(r'млн\.?\s*руб\.?', '', s, flags=re.IGNORECASE)
    s = s.replace(' ', '').replace(',', '.').strip()
    if s in ('', '-', 'nan', 'None'):
        return np.nan
    return float(s) * unit_mult


def load_and_normalize(path):
    """Читает xlsx, нормализует единицы, сортирует по (regn, date)."""
    df = pd.read_excel(path)
    for c in NUM_COLS:
        df[c] = df[c].apply(parse_value)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['regn', 'date']).reset_index(drop=True)
    df['month'] = df['date'].dt.month
    return df


# ---------- флаги ошибок ----------

def add_error_flags(df):
    """Флаги ошибок данных и предупреждений (не блокируют расчёт сами по себе)."""
    df['flag_equity_negative']   = df['Equity'] < 0
    df['flag_loans_negative']    = df['Loans_Total_Net'] < 0
    df['flag_deposits_negative'] = df['Client_Deposits'] < 0
    df['flag_assets_negative']   = df['Assets'] < 0
    df['flag_llp_exceeds_net']   = df['Loans_LLP'].abs() > df['Loans_Total_Net'].abs()
    df['flag_llp_positive_sign'] = df['Loans_LLP'] > 0
    return df


# ---------- восстановление потоков прибыли ----------

def restore_monthly_profit_from_ytd(df):
    """
    Восстанавливает месячный поток прибыли из YTD NIC.
      profit_month(t) = NIC(t)                если month(t) == 2   (январский flow)
                      = NIC(t) − NIC(t−1мес)  иначе                (включая t=1 янв)
    Предполагает, что NIC(1 янв Y) = FY(Y−1) и YTD для Y стартует с 0 в феврале.
    Если в ряду пропуск (предыдущая точка не на t−1мес) → flow=NaN.
    """
    df = df.sort_values(['regn', 'date']).reset_index(drop=True)
    df['profit_month'] = np.nan
    for _, idx in df.groupby('regn').indices.items():
        g = df.loc[idx]
        nic = g['Net_Income_Current'].values
        dates = pd.to_datetime(g['date']).values
        flows = np.full(len(g), np.nan)
        for i in range(1, len(g)):
            d, prev = pd.Timestamp(dates[i]), pd.Timestamp(dates[i - 1])
            if prev != d - pd.DateOffset(months=1):
                continue
            flows[i] = nic[i] if d.month == 2 else nic[i] - nic[i - 1]
        df.loc[idx, 'profit_month'] = flows
    return df


# ---------- коэффициенты ----------

def compute_roe_12m(df):
    """
    Основная (YTD-корректная) версия:
        ROE_12m(t) = Σ profit_month(t−11..t) / mean(Equity(t−12..t))
    Условия: 13 заполненных точек в окне, Equity(s) ≥ 0 для всех s в окне.

    Control-версия (для сверки с «наивным» расчётом):
        ROE_12m_flow_check(t) = rolling_sum(NIC, 12) / equity_avg_13
    Ожидается, что она завышена в ~6.5× при истинной YTD-семантике NIC —
    использовать только как диагностический индикатор.
    """
    g = df.groupby('regn', group_keys=False)
    df['profit_12m']    = g['profit_month'].apply(lambda s: s.rolling(12, min_periods=12).sum())
    df['equity_avg_13'] = g['Equity'].apply(lambda s: s.rolling(13, min_periods=13).mean())
    df['equity_any_neg_in_window'] = g['Equity'].apply(
        lambda s: (s < 0).rolling(13, min_periods=13).sum() > 0
    )

    mask = (df['profit_12m'].notna()
            & df['equity_avg_13'].notna()
            & ~df['equity_any_neg_in_window'].fillna(True)
            & (df['Equity'] >= 0))
    df['ROE_12m'] = np.where(mask, df['profit_12m'] / df['equity_avg_13'], np.nan)

    # control: наивная flow-интерпретация NIC (для диагностики)
    nic_sum_12 = g['Net_Income_Current'].apply(lambda s: s.rolling(12, min_periods=12).sum())
    df['ROE_12m_flow_check'] = np.where(mask, nic_sum_12 / df['equity_avg_13'], np.nan)
    return df


def compute_loan_yield_3m(df):
    """
    LoanYield_3m(t) = 4 × NII(t) / loans_avg_Q(t),  t ∈ {Jan, Apr, Jul, Oct}
    NII используется как квартальный flow — обосновано в report.md
    (ТЗ говорит YTD, но данные монотонно не растут внутри года).
    """
    g = df.groupby('regn', group_keys=False)
    df['loans_avg_Q'] = g['Loans_Total_Net'].apply(lambda s: s.rolling(4, min_periods=4).mean())
    df['loans_any_neg_in_Q'] = g['Loans_Total_Net'].apply(
        lambda s: (s < 0).rolling(4, min_periods=4).sum() > 0
    )
    is_qdate = df['month'].isin([1, 4, 7, 10])
    mask = (is_qdate
            & df['Net_Interest_Income'].notna()
            & df['loans_avg_Q'].notna()
            & ~df['loans_any_neg_in_Q'].fillna(True)
            & (df['loans_avg_Q'] > 0))
    df['LoanYield_3m'] = np.where(
        mask, 4 * df['Net_Interest_Income'] / df['loans_avg_Q'], np.nan
    )
    return df


def compute_ldr(df):
    """
    LDR(t) = Loans_Gross(t) / Client_Deposits(t),  Loans_Gross = Net − LLP.
    Строки с flag_llp_exceeds_net не блокируются, но помечаются — см. флаг.
    """
    df['Loans_Gross'] = df['Loans_Total_Net'] - df['Loans_LLP']
    mask = (df['Loans_Gross'] >= 0) & (df['Client_Deposits'] > 0)
    df['LDR'] = np.where(mask, df['Loans_Gross'] / df['Client_Deposits'], np.nan)
    return df


def add_outlier_flags(df):
    """Диапазонные флаги для ручной проверки. Значения не удаляются."""
    df['flag_roe_outlier']   = df['ROE_12m'].abs() > ROE_OUTLIER_ABS
    df['flag_yield_outlier'] = ((df['LoanYield_3m'] < YIELD_OUTLIER_RANGE[0])
                                | (df['LoanYield_3m'] > YIELD_OUTLIER_RANGE[1]))
    df['flag_ldr_outlier']   = ((df['LDR'] < LDR_OUTLIER_RANGE[0])
                                | (df['LDR'] > LDR_OUTLIER_RANGE[1]))
    return df


# ---------- пайплайн ----------

def compute_all(path):
    df = load_and_normalize(path)
    df = add_error_flags(df)
    df = restore_monthly_profit_from_ytd(df)
    df = compute_roe_12m(df)
    df = compute_loan_yield_3m(df)
    df = compute_ldr(df)
    df = add_outlier_flags(df)
    return df


# ---------- вывод ----------

def print_summary(df):
    total = len(df)
    print(f'Строк: {total}, банков: {df["regn"].nunique()}, '
          f'период: {df["date"].min().date()} → {df["date"].max().date()}\n')

    print('Коэффициенты:')
    for col, scale, suffix in [('ROE_12m', 100, '%'),
                               ('LoanYield_3m', 100, '%'),
                               ('LDR', 1, '')]:
        s = df[col].dropna()
        if len(s) == 0:
            print(f'  {col:20s}  нет значений')
            continue
        print(f'  {col:20s}  посчитано {len(s):4d}/{total}  '
              f'median={s.median()*scale:7.2f}{suffix}  '
              f'mean={s.mean()*scale:7.2f}{suffix}  '
              f'max={s.max()*scale:8.2f}{suffix}')

    # диагностический control — покажет, если YTD/flow гипотеза расходятся
    roe_ytd = df['ROE_12m'].dropna()
    roe_flow = df['ROE_12m_flow_check'].dropna()
    if len(roe_ytd) and len(roe_flow):
        ratio = roe_flow.median() / roe_ytd.median() if roe_ytd.median() else np.nan
        print(f'\n  diag: ROE_12m_flow_check/ROE_12m (median ratio) = {ratio:.2f} '
              f'(ожидается ≈ 6–7 при YTD-семантике NIC)')

    print('\nФлаги (число строк):')
    for c in [c for c in df.columns if c.startswith('flag_')]:
        n = int(df[c].sum())
        if n:
            print(f'  {c:28s}  {n:4d}')


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('-i', '--input', required=True, type=Path, help='Путь к bank_data.xlsx')
    ap.add_argument('-o', '--output', type=Path, default=None,
                    help='Путь для CSV с полными результатами (опционально)')
    args = ap.parse_args()

    df = compute_all(args.input)
    print_summary(df)

    if args.output:
        df.to_csv(args.output, index=False)
        print(f'\nСохранено: {args.output}')


if __name__ == '__main__':
    main()

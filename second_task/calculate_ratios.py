"""
Расчёт финансовых коэффициентов (ROE 12m, LoanYield 3m, LDR) по bank_data.xlsx.

Методология и обоснования — second_task/README.md. Ключевые решения:
  - NIC трактуется как YTD — по ТЗ «прибыль за последние 12 мес ... отражается
    накопленным итогом»; годовой скоуп коэффициента ⇒ накопление за год
  - NII трактуется как QTD (квартальный накопленный итог) — по ТЗ «ЧПД за квартал
    ... отражается накопленным итогом»; квартальный скоуп ⇒ накопление за квартал
  - Equity < 0 в любой точке 13-месячного окна → ROE не считается
  - Loans_Gross = Loans_Total_Net − Loans_LLP (LLP обычно отрицательный)
  - profit_month(t) — поток прибыли за месяц, заканчивающийся датой t
    (Σ profit_month(1 фев Y … 1 янв Y+1) = FY(Y), см. restore_monthly_profit_from_ytd)

Запуск:
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
    # LLP в норме отрицателен, Net ≥ 0. Флаг сработает только на валидных
    # по знаку рядах: резерв по модулю больше портфеля. Строки с Net < 0
    # ловятся отдельным flag_loans_negative — не смешиваем две разные аномалии.
    df['flag_llp_exceeds_net']   = (df['Loans_Total_Net'] >= 0) & ((-df['Loans_LLP']) > df['Loans_Total_Net'])
    df['flag_llp_positive_sign'] = df['Loans_LLP'] > 0
    return df


# ---------- восстановление потоков прибыли ----------

def restore_monthly_profit_from_ytd(df):
    """
    Восстанавливает месячный поток прибыли из YTD NIC.

    Конвенция: profit_month(t) — это прибыль за календарный месяц,
    заканчивающийся датой t (даты в данных — 1-е число месяца):
      • t = 1 фев Y   → flow января Y = NIC(1 фев Y)
      • t = 1 мар Y   → flow февраля Y = NIC(1 мар Y) − NIC(1 фев Y)
      • …
      • t = 1 янв Y+1 → flow декабря Y = NIC(1 янв Y+1) − NIC(1 дек Y)
                         (NIC(1 янв Y+1) = FY(Y), NIC(1 дек Y) = YTD на 1 дек)

    Следствие конвенции: Σ profit_month(1 фев Y … 1 янв Y+1) = FY(Y).
    Если в ряду пропуск (предыдущая точка не на t−1мес) → flow=NaN.
    """
    df = df.sort_values(['regn', 'date']).reset_index(drop=True)
    g = df.groupby('regn', group_keys=False, sort=False)
    prev_date = g['date'].shift(1)
    prev_nic  = g['Net_Income_Current'].shift(1)
    expected_prev = df['date'] - pd.DateOffset(months=1)
    gap_ok = prev_date == expected_prev
    is_feb = df['date'].dt.month == 2
    flow = np.where(is_feb,
                    df['Net_Income_Current'],
                    df['Net_Income_Current'] - prev_nic)
    df['profit_month'] = np.where(gap_ok, flow, np.nan)
    return df


# ---------- коэффициенты ----------

def compute_roe_12m(df):
    """
    Основная (YTD-корректная) версия:
        ROE_12m(t) = Σ profit_month(t−11..t) / mean(Equity(t−12..t))
    Условия: 13 заполненных точек в окне, Equity(s) ≥ 0 для всех s в окне.

    Диагностическая колонка ROE_12m_naive_sum:
        ROE_12m_naive_sum(t) = rolling_sum(NIC, 12) / equity_avg_13
    Это не «flow-гипотеза ROE», а именно наивная сумма 12 YTD-значений,
    делённая на средний капитал. Нужна как индикатор YTD/flow-семантики
    NIC: под истинной YTD (наш случай) отношение к ROE_12m систематически
    ≈ 6.5× при ~равных потоках и 7–10× при росте; если NIC окажется
    flow — колонки совпадут. См. diag-строку в print_summary.
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

    # диагностика: наивная сумма 12 YTD-значений NIC (см. докстринг)
    nic_sum_12 = g['Net_Income_Current'].apply(lambda s: s.rolling(12, min_periods=12).sum())
    df['ROE_12m_naive_sum'] = np.where(mask, nic_sum_12 / df['equity_avg_13'], np.nan)
    return df


def compute_loan_yield_3m(df):
    """
    LoanYield_3m(t) = 4 × NII(t) / loans_avg_Q(t),  t ∈ {Jan, Apr, Jul, Oct}
    NII — QTD (квартальный накопленный итог): каждое значение = накопление ЧПД
    внутри своего квартала. Соответствует ТЗ («ЧПД за квартал ... отражается
    накопленным итогом») — скоуп «накопленного итога» у квартального
    коэффициента квартальный, а не годовой.
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
    # LDR посчитан, но |LLP| > Net — значение формально валидно (Gross > 0),
    # но завышено, см. §3.1 отчёта. Помечаем, не удаляем — решение за аналитиком.
    df['flag_ldr_suspect']   = df['LDR'].notna() & df['flag_llp_exceeds_net']
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

    # диагностический индикатор YTD/flow-семантики NIC
    roe_ytd = df['ROE_12m'].dropna()
    roe_naive = df['ROE_12m_naive_sum'].dropna()
    if len(roe_ytd) and len(roe_naive):
        ratio = roe_naive.median() / roe_ytd.median() if roe_ytd.median() else np.nan
        print(f'\n  diag: ROE_12m_naive_sum/ROE_12m (median ratio) = {ratio:.2f} '
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
    ap.add_argument('-o', '--output', type=Path, default=Path('ratios.csv'),
                    help='Путь для CSV с полными результатами (по умолчанию ./ratios.csv)')
    args = ap.parse_args()

    df = compute_all(args.input)
    print_summary(df)

    df.to_csv(args.output, index=False)
    print(f'\nСохранено: {args.output}')


if __name__ == '__main__':
    main()

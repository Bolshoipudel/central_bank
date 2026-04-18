"""
Сравнение трёх методов усреднения Equity для ROE 12m

Воспроизводит таблицу в README.md §3.2 пункт 4:
  - 13 точек (принято в calculate_ratios.py): mean(Equity(t−12..t))
  - 12 точек:                                 mean(Equity(t−11..t))
  - 2 точки (begin+end)/2:                    (Equity(t−12) + Equity(t)) / 2

Запуск:
  python compare_averaging.py -i /path/to/bank_data.xlsx
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from calculate_ratios import load_and_normalize, restore_monthly_profit_from_ytd


def compute_comparison(df):
    """Возвращает DataFrame с валидными строками и тремя версиями ROE"""
    df = restore_monthly_profit_from_ytd(df)
    g = df.groupby('regn', group_keys=False)

    df['profit_12m'] = g['profit_month'].apply(lambda s: s.rolling(12, min_periods=12).sum())
    df['eq_2pt']  = g['Equity'].apply(lambda s: (s + s.shift(12)) / 2)
    df['eq_12pt'] = g['Equity'].apply(lambda s: s.rolling(12, min_periods=12).mean())
    df['eq_13pt'] = g['Equity'].apply(lambda s: s.rolling(13, min_periods=13).mean())
    df['eq_any_neg'] = g['Equity'].apply(
        lambda s: (s < 0).rolling(13, min_periods=13).sum() > 0
    )

    mask = (df['profit_12m'].notna()
            & df['eq_2pt'].notna()
            & df['eq_12pt'].notna()
            & df['eq_13pt'].notna()
            & ~df['eq_any_neg'].fillna(True)
            & (df['Equity'] >= 0))
    d = df[mask].copy()
    d['ROE_2pt']  = d['profit_12m'] / d['eq_2pt']
    d['ROE_12pt'] = d['profit_12m'] / d['eq_12pt']
    d['ROE_13pt'] = d['profit_12m'] / d['eq_13pt']
    return d


def print_report(d):
    print(f'Валидных строк: {len(d)}, банков: {d["regn"].nunique()}\n')

    print('Распределение ROE (%):')
    print(f'  {"метод":15s} {"median":>8s} {"mean":>8s} {"p25":>8s} {"p75":>8s}')
    for label, col in [('13 точек', 'ROE_13pt'),
                       ('12 точек', 'ROE_12pt'),
                       ('(begin+end)/2', 'ROE_2pt')]:
        s = d[col] * 100
        print(f'  {label:15s} {s.median():8.2f} {s.mean():8.2f} '
              f'{s.quantile(.25):8.2f} {s.quantile(.75):8.2f}')

    print('\nРазницы vs 13pt (п.п.):')
    print(f'  {"метод":15s} {"median Δ":>10s} {"|median Δ|":>12s} {"p95 |Δ|":>10s} {"max |Δ|":>10s}')
    for label, col in [('12 точек', 'ROE_12pt'),
                       ('(begin+end)/2', 'ROE_2pt')]:
        dpp = (d[col] - d['ROE_13pt']) * 100
        a = dpp.abs()
        print(f'  {label:15s} {dpp.median():+10.3f} {a.median():12.3f} '
              f'{a.quantile(.95):10.3f} {a.max():10.3f}')

    print('\nДоля строк с малой разницей (2pt vs 13pt):')
    dpp = ((d['ROE_2pt'] - d['ROE_13pt']) * 100).abs()
    for thr in (0.5, 1.0, 2.0, 5.0):
        print(f'  |Δ| < {thr:3.1f} пп: {(dpp < thr).mean() * 100:5.1f}%')

    print('\nТоп-5 строк с макс. |ROE_2pt − ROE_13pt|:')
    top = d.reindex(((d['ROE_2pt'] - d['ROE_13pt']).abs()).sort_values(ascending=False).index).head(5)
    for _, r in top.iterrows():
        dpp = (r['ROE_2pt'] - r['ROE_13pt']) * 100
        print(f'  regn={r["regn"]:8s} date={r["date"].date()} '
              f'Δ={dpp:+8.2f} пп  ROE_2pt={r["ROE_2pt"]*100:7.2f}%  '
              f'ROE_13pt={r["ROE_13pt"]*100:7.2f}%')


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('-i', '--input', required=True, type=Path, help='Путь к bank_data.xlsx')
    ap.add_argument('-o', '--output', type=Path, default=Path('averaging_comparison.csv'),
                    help='CSV с построчным сравнением (по умолчанию ./averaging_comparison.csv)')
    args = ap.parse_args()

    df = load_and_normalize(args.input)
    d = compute_comparison(df)
    print_report(d)

    cols = ['regn', 'date', 'Equity', 'profit_12m',
            'eq_2pt', 'eq_12pt', 'eq_13pt',
            'ROE_2pt', 'ROE_12pt', 'ROE_13pt']
    d[cols].to_csv(args.output, index=False)
    print(f'\nСохранено: {args.output}')


if __name__ == '__main__':
    main()

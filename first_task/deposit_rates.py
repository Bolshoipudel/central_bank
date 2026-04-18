"""
Сбор и анализ ставок по вкладам ФЛ для топ-10 банков России.

Скрипт загружает данные из трёх источников (sravni.ru, banki.ru, офиц. сайты),
нормализует, сравнивает по каждому банку и выдаёт:
  - сводку в консоль,
  - файл data/results.json для ручного переноса цифр в report.md.

Сам report.md ведётся вручную — скрипт его не перезаписывает.
"""

import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

# Фикс кодировки для Windows-консоли
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

DATA_DIR = Path(__file__).parent / "data"
RESULTS_PATH = DATA_DIR / "results.json"

TOP10_BANKS = [
    "СберБанк",
    "ВТБ",
    "Газпромбанк",
    "Альфа-Банк",
    "Т-Банк",
    "Россельхозбанк",
    "МКБ",
    "ДОМ.РФ",
    "Совкомбанк",
    "Райффайзенбанк",
]

# Порог для «сомнительной» фиксированной ставки на 3 года
SUSPICIOUS_FIXED_RATE_THRESHOLD = 15.0
# Порог для незначительного расхождения (п.п.)
MINOR_DISCREPANCY_THRESHOLD = 0.5


@dataclass
class DepositRecord:
    bank: str
    product: str | None
    rate_3y: float | None
    source: str
    rate_type: str | None = None       # "fixed" / "floating"
    base_rate: float | None = None
    conditions: str = ""
    note: str = ""
    max_term_months: int | None = None
    max_term_rate: float | None = None


def load_source_data(filepath: Path) -> list[DepositRecord]:
    """Загрузка JSON-файла источника и конвертация в список DepositRecord"""
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    source_name = data["source"]
    records = []
    for d in data["deposits"]:
        records.append(DepositRecord(
            bank=d["bank"],
            product=d.get("product"),
            rate_3y=d.get("rate_3y"),
            source=source_name,
            rate_type=d.get("rate_type"),
            base_rate=d.get("base_rate"),
            conditions=d.get("conditions", ""),
            note=d.get("note", ""),
            max_term_months=d.get("max_term_months"),
            max_term_rate=d.get("max_term_rate"),
        ))
    return records


def filter_deposits_3y(records: list[DepositRecord]) -> list[DepositRecord]:
    """Оставляет только записи-вклады со ставкой на 3 года (rate_3y != None)"""
    return [r for r in records if r.rate_3y is not None and r.product is not None]


def check_suspicious(rate: float | None, rate_type: str | None) -> str | None:
    if rate is None:
        return None
    if rate_type == "fixed" and rate > SUSPICIOUS_FIXED_RATE_THRESHOLD:
        return f"Подозрительное значение: фикс. ставка {rate}% > {SUSPICIOUS_FIXED_RATE_THRESHOLD}% на 3 года — возможно, плавающая"
    if rate > 25:
        return f"Подозрительное значение: ставка {rate}% выглядит нереалистично высокой"
    if rate < 1:
        return f"Подозрительное значение: ставка {rate}% подозрительно низкая"
    return None


@dataclass
class FinalRate:
    bank: str
    product: str | None
    rate: float | None
    rate_type: str | None
    status: str
    status_detail: str
    sources: dict[str, float | None] = field(default_factory=dict)
    floating_products: list[dict] = field(default_factory=list)
    max_term_info: list[dict] = field(default_factory=list)


def select_final_rate(
    bank: str,
    rates: dict[str, DepositRecord | None],
    floating: dict[str, DepositRecord | None],
) -> FinalRate:
    """
    Выбор итоговой ставки по банку из нескольких источников

    Правила:
    1. Сравниваем только фиксированные ставки
    2. Совпадение → итоговая
    3. ≤ 0.5 п.п. → среднее, «незначительное расхождение»
    4. > 0.5 п.п. → «противоречие»
    5. Один источник → как есть, «один источник»
    6. Нет данных → «нет данных»
    """
    available = {s: r for s, r in rates.items() if r is not None and r.rate_3y is not None}

    # Плавающие продукты для справки
    float_list = []
    for s, r in floating.items():
        if r is not None and r.rate_3y is not None:
            float_list.append({
                "source": s,
                "product": r.product,
                "rate": r.rate_3y,
                "note": r.note or r.conditions,
            })

    if not available:
        return FinalRate(
            bank=bank, product=None, rate=None, rate_type=None,
            status="no_data", status_detail="Нет данных о вкладах на 3 года",
            floating_products=float_list,
        )

    if len(available) == 1:
        src, rec = next(iter(available.items()))
        susp = check_suspicious(rec.rate_3y, rec.rate_type)
        status = "suspicious" if susp else "single"
        detail = susp or f"Данные только из источника: {src}"
        return FinalRate(
            bank=bank, product=rec.product, rate=rec.rate_3y,
            rate_type=rec.rate_type,
            status=status, status_detail=detail,
            sources={s: (r.rate_3y if r else None) for s, r in rates.items()},
            floating_products=float_list,
        )

    #Сравниваем несколько источников
    vals = list(available.values())
    rate_values = [v.rate_3y for v in vals]
    max_diff = max(rate_values) - min(rate_values)

    if max_diff == 0:
        rec = vals[0]
        return FinalRate(
            bank=bank, product=rec.product, rate=rec.rate_3y,
            rate_type=rec.rate_type,
            status="ok", status_detail="Все источники совпадают",
            sources={s: (r.rate_3y if r else None) for s, r in rates.items()},
            floating_products=float_list,
        )

    if max_diff <= MINOR_DISCREPANCY_THRESHOLD:
        src_str = ", ".join(f"{s}={r.rate_3y}%" for s, r in available.items())
        if "official" in available:
            rec = available["official"]
            detail = f"Незначительное расхождение ({max_diff:.2f} п.п.): {src_str}. Итог по офиц. сайту."
            final_rate = rec.rate_3y
        else:
            avg = round(sum(rate_values) / len(rate_values), 2)
            rec = max(vals, key=lambda v: v.rate_3y or 0)
            detail = f"Незначительное расхождение ({max_diff:.2f} п.п.): {src_str}. Итог = среднее агрегаторов."
            final_rate = avg
        return FinalRate(
            bank=bank, product=rec.product, rate=final_rate,
            rate_type="fixed",
            status="minor",
            status_detail=detail,
            sources={s: (r.rate_3y if r else None) for s, r in rates.items()},
            floating_products=float_list,
        )

    # Противоречие > 0.5 п.п.
    src_str = ", ".join(f"{s}={r.rate_3y}%" for s, r in available.items())
    # Приоритет: официальный сайт > banki.ru > sravni.ru
    priority = ["official", "banki.ru", "sravni.ru"]
    chosen_src = None
    for p in priority:
        if p in available:
            chosen_src = p
            break
    if chosen_src is None:
        chosen_src = next(iter(available))

    rec = available[chosen_src]
    return FinalRate(
        bank=bank, product=rec.product, rate=rec.rate_3y,
        rate_type=rec.rate_type,
        status="conflict",
        status_detail=f"Противоречие ({max_diff:.2f} п.п.): {src_str}. Выбран {chosen_src} (приоритет).",
        sources={s: (r.rate_3y if r else None) for s, r in rates.items()},
        floating_products=float_list,
    )


def cross_validate_all(
    sources: dict[str, list[DepositRecord]],
) -> list[FinalRate]:
    """Перекрёстная проверка всех источников по каждому банку."""
    results = []

    for bank in TOP10_BANKS:
        fixed_per_src: dict[str, DepositRecord | None] = {}
        float_per_src: dict[str, DepositRecord | None] = {}

        for src_name, records in sources.items():
            filtered = filter_deposits_3y(records)
            bank_records = [r for r in filtered if r.bank == bank]

            fixed = [r for r in bank_records if r.rate_type == "fixed"]
            floats = [r for r in bank_records if r.rate_type == "floating"]

            if fixed:
                best = max(fixed, key=lambda r: r.rate_3y or 0)
                fixed_per_src[src_name] = best
            else:
                fixed_per_src[src_name] = None

            if floats:
                best_f = max(floats, key=lambda r: r.rate_3y or 0)
                float_per_src[src_name] = best_f
            else:
                float_per_src[src_name] = None

        final = select_final_rate(bank, fixed_per_src, float_per_src)

        # Если 3-летних вкладов нет — собираем инфу про максимальный доступный срок
        if final.status == "no_data":
            for src_name, records in sources.items():
                for r in records:
                    if r.bank == bank and r.max_term_months and r.max_term_rate:
                        final.max_term_info.append({
                            "source": src_name,
                            "product": r.product,
                            "max_term_months": r.max_term_months,
                            "rate": r.max_term_rate,
                            "note": r.note,
                        })

        results.append(final)

    return results


def main():
    # Загрузка данных из всех источников
    sources: dict[str, list[DepositRecord]] = {}

    sravni_path = DATA_DIR / "source_sravni.json"
    banki_path = DATA_DIR / "source_banki.json"
    official_path = DATA_DIR / "source_official.json"

    if sravni_path.exists():
        sources["sravni.ru"] = load_source_data(sravni_path)
        print(f"Загружено {len(sources['sravni.ru'])} записей из sravni.ru")

    if banki_path.exists():
        sources["banki.ru"] = load_source_data(banki_path)
        print(f"Загружено {len(sources['banki.ru'])} записей из banki.ru")

    if official_path.exists():
        sources["official"] = load_source_data(official_path)
        print(f"Загружено {len(sources['official'])} записей из official")

    if not sources:
        print("Ошибка: не найден ни один файл данных в data/")
        return

    # Перекрёстная проверка
    results = cross_validate_all(sources)

    # Вывод сводки в консоль
    print("\n=== ИТОГОВЫЕ СТАВКИ (фиксированные, 3 года, 1 млн ₽) ===\n")
    for i, r in enumerate(results, 1):
        rate_str = f"{r.rate}%" if r.rate else "—"
        print(f"{i:2d}. {r.bank:<20s} | {rate_str:>8s} | {r.status:<10s} | {r.product or '—'}")

    # Запись структурированного результата для ручного переноса в report.md
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in results], f, ensure_ascii=False, indent=2)

    print(f"\nРезультаты сохранены: {RESULTS_PATH}")


if __name__ == "__main__":
    main()

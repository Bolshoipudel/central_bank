"""
Сбор и анализ ставок по вкладам ФЛ для топ-10 банков России.

Скрипт загружает данные из трёх источников (sravni.ru, banki.ru, офиц. сайты),
нормализует, сравнивает и генерирует итоговый отчёт report.md.
"""

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Фикс кодировки для Windows-консоли
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

DATA_DIR = Path(__file__).parent / "data"
REPORT_PATH = Path(__file__).parent / "report.md"

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

# Ключевая ставка ЦБ РФ на 14.04.2026
CB_KEY_RATE = 15.0
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


def best_fixed_rate_per_bank(records: list[DepositRecord]) -> dict[str, DepositRecord]:
    """Для каждого банка выбирает вклад с лучшей фиксированной ставкой на 3 года"""
    best: dict[str, DepositRecord] = {}
    for r in records:
        if r.rate_type != "fixed":
            continue
        if r.bank not in best or (r.rate_3y or 0) > (best[r.bank].rate_3y or 0):
            best[r.bank] = r
    return best


def best_any_rate_per_bank(records: list[DepositRecord]) -> dict[str, DepositRecord]:
    """Для каждого банка выбираем вклад с самой высокой ставкой на 3 года"""
    best: dict[str, DepositRecord] = {}
    for r in records:
        if r.bank not in best or (r.rate_3y or 0) > (best[r.bank].rate_3y or 0):
            best[r.bank] = r
    return best


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


def generate_report(results: list[FinalRate], sources_meta: dict) -> str:
    """Генерация Markdown-отчёта."""
    lines = []
    lines.append("# Отчёт: Ставки по вкладам ФЛ — топ-10 банков России")
    lines.append("")
    lines.append(f"**Дата сбора данных:** 14.04.2026")
    lines.append(f"**Параметры:** сумма 1 000 000 ₽, срок 3 года, рубли")
    lines.append("")

    # --- 1. Описание подхода ---
    lines.append("## 1. Описание подхода")
    lines.append("")
    lines.append("### Выбор банков")
    lines.append("")
    lines.append("Топ-10 банков выбраны по объёму активов на основании рейтинга mainfin.ru/ЦБ РФ (апрель 2026):")
    lines.append("")
    banks_info = [
        ("СберБанк", "65.8 трлн ₽"),
        ("ВТБ", "35.3 трлн ₽"),
        ("Газпромбанк", "17.8 трлн ₽"),
        ("Альфа-Банк", "13.2 трлн ₽"),
        ("Т-Банк", "5.5 трлн ₽"),
        ("Россельхозбанк", "5.4 трлн ₽"),
        ("МКБ", "5.1 трлн ₽"),
        ("Банк ДОМ.РФ", "4.7 трлн ₽"),
        ("Совкомбанк", "4.1 трлн ₽"),
        ("Райффайзенбанк", "2.0 трлн ₽"),
    ]
    for i, (name, assets) in enumerate(banks_info, 1):
        lines.append(f"{i}. **{name}** — {assets}")
    lines.append("")
    lines.append("### Источники данных")
    lines.append("")
    lines.append("Использованы **3 источника** для перекрёстной проверки:")
    lines.append("")
    lines.append("| # | Источник | Тип | Описание |")
    lines.append("|---|---------|-----|----------|")
    lines.append("| 1 | sravni.ru | Агрегатор | Фильтр: 1 млн ₽, срок вклада 3 года |")
    lines.append("| 2 | banki.ru | Агрегатор | Карточки вкладов с детальными параметрами |")
    lines.append("| 3 | Офиц. сайты банков | Первоисточник | Калькуляторы и тарифные страницы |")
    lines.append("")

    # --- 2. Итоговая таблица ---
    lines.append("## 2. Итоговая таблица ставок")
    lines.append("")
    lines.append("### Фиксированные ставки (основная таблица)")
    lines.append("")
    lines.append("| № | Банк | Продукт | Ставка (3 года) | sravni.ru | banki.ru | Офиц. сайт | Статус |")
    lines.append("|---|------|---------|----------------|-----------|----------|------------|--------|")

    status_icons = {
        "ok": "Совпадение",
        "minor": "Незнач. расхождение",
        "conflict": "Противоречие",
        "single": "Один источник",
        "no_data": "Нет данных",
        "suspicious": "Сомнительное",
    }

    for i, r in enumerate(results, 1):
        product = r.product or "—"
        rate = f"**{r.rate}%**" if r.rate else "—"
        s1 = f"{r.sources.get('sravni.ru', '—')}%" if r.sources.get('sravni.ru') else "—"
        s2 = f"{r.sources.get('banki.ru', '—')}%" if r.sources.get('banki.ru') else "—"
        s3 = f"{r.sources.get('official', '—')}%" if r.sources.get('official') else "—"
        status = status_icons.get(r.status, r.status)
        lines.append(f"| {i} | {r.bank} | {product} | {rate} | {s1} | {s2} | {s3} | {status} |")

    lines.append("")

    # Плавающие ставки — отдельная секция
    has_floating = any(r.floating_products for r in results)
    if has_floating:
        lines.append("### Плавающие ставки (справочно)")
        lines.append("")
        lines.append("Эти продукты имеют ставку, привязанную к ключевой ставке ЦБ РФ, и **не участвуют** в основном сравнении:")
        lines.append("")
        lines.append("| Банк | Продукт | Текущая ставка | Источник | Примечание |")
        lines.append("|------|---------|---------------|----------|------------|")
        for r in results:
            for fp in r.floating_products:
                lines.append(f"| {r.bank} | {fp['product']} | {fp['rate']}% | {fp['source']} | {fp['note'][:80]} |")
        lines.append("")
        lines.append(f"> **Примечание:** Ключевая ставка ЦБ РФ на 14.04.2026 = {CB_KEY_RATE}%. "
                     f"Плавающие ставки будут снижаться при снижении ключевой ставки.")
        lines.append("")

    # --- 3. Расхождения ---
    lines.append("## 3. Расхождения между источниками и их обработка")
    lines.append("")
    lines.append("### Правило выбора итоговой ставки")
    lines.append("")
    lines.append("1. Сравниваются только **фиксированные** ставки (плавающие выделены отдельно)")
    lines.append("2. Все источники совпадают → ставка итоговая")
    lines.append(f"3. Расхождение ≤ {MINOR_DISCREPANCY_THRESHOLD} п.п. → если есть офиц. сайт, берём его; иначе среднее арифметическое по агрегаторам")
    lines.append(f"4. Расхождение > {MINOR_DISCREPANCY_THRESHOLD} п.п. → «противоречие», приоритет: офиц. сайт > banki.ru > sravni.ru")
    lines.append("5. Данные из одного источника → принимаются как есть")
    lines.append(f"6. Фикс. ставка > {SUSPICIOUS_FIXED_RATE_THRESHOLD}% на 3 года → проверка (возможно плавающая)")
    lines.append("")

    lines.append("### Выявленные расхождения")
    lines.append("")

    discrepancies = [r for r in results if r.status in ("minor", "conflict", "no_data", "suspicious")]
    if discrepancies:
        lines.append("| Банк | Тип | Описание |")
        lines.append("|------|-----|----------|")
        for r in discrepancies:
            dtype = status_icons.get(r.status, r.status)
            lines.append(f"| {r.bank} | {dtype} | {r.status_detail} |")
        lines.append("")
    else:
        lines.append("Расхождений не выявлено.")
        lines.append("")

    # Подробный разбор расхождений
    conflict_cases = [r for r in results if r.status in ("conflict", "no_data")]
    if conflict_cases:
        lines.append("### Подробный разбор")
        lines.append("")
        for r in conflict_cases:
            lines.append(f"**{r.bank}** — {status_icons.get(r.status)}")
            lines.append("")
            lines.append(f"- {r.status_detail}")
            if r.floating_products:
                for fp in r.floating_products:
                    lines.append(f"- Плавающий продукт: {fp['product']} ({fp['rate']}%) — {fp['note'][:100]}")
            if r.max_term_info:
                lines.append("- Ставка по **максимально доступному сроку** по источникам:")
                for mt in r.max_term_info:
                    lines.append(
                        f"    - {mt['source']}: {mt['product']} — {mt['max_term_months']} мес, "
                        f"ставка {mt['rate']}%"
                    )
                rates_mt = [mt["rate"] for mt in r.max_term_info]
                if len(rates_mt) > 1:
                    spread = max(rates_mt) - min(rates_mt)
                    lines.append(
                        f"- Расхождение между источниками по макс. сроку: {spread:.2f} п.п. "
                        f"(от {min(rates_mt)}% до {max(rates_mt)}%)."
                    )
            lines.append("")

    # --- 4. Использование LLM ---
    lines.append("## 4. Использование LLM в решении")
    lines.append("")
    lines.append("### Роль LLM на каждом этапе")
    lines.append("")
    lines.append("#### 4.1. Парсинг / извлечение данных")
    lines.append("")
    lines.append("- **Проблема:** Агрегаторы (sravni.ru, banki.ru) рендерят данные через JavaScript — ")
    lines.append("  классический HTTP-парсинг (requests + BeautifulSoup) не работает.")
    lines.append("- **Решение:** LLM (Claude) анализировал **скриншоты** веб-страниц и извлекал ")
    lines.append("  структурированные данные: название продукта, ставку, срок, тип продукта.")
    lines.append("- **Пример:** Из скриншота страницы sravni.ru для Сбербанка LLM извлёк 7 продуктов ")
    lines.append("  с названиями, ставками на 6 мес / 1 год / 3 года и метками (накопительный счёт vs вклад).")
    lines.append("")
    lines.append("#### 4.2. Нормализация")
    lines.append("")
    lines.append("- **Классификация продуктов:** LLM различал «вклад» от «накопительного счёта» по ")
    lines.append("  меткам на скриншотах и характеристикам (пополнение/снятие, срок).")
    lines.append("- **Тип ставки:** LLM определял фиксированную vs плавающую ставку по описанию условий ")
    lines.append("  (например, «ставка = ключевая ставка ЦБ − 2 п.п.» → плавающая).")
    lines.append("- **Унификация названий:** «МКБ», «Московский Кредитный Банк», «Московский Кре...» → единое «МКБ».")
    lines.append("")
    lines.append("#### 4.3. Валидация и проверка")
    lines.append("")
    lines.append("- **Обнаружение аномалий:** LLM выявил, что Сбербанк «Ключевой» (15.8%) и ")
    lines.append("  Альфа-Банк «Ключевой» (16.53%) — плавающие ставки, хотя визуально ")
    lines.append("  отображались как обычные вклады.")
    lines.append("- **Причины расхождений:** LLM объяснил разницу в ставках Газпромбанка ")
    lines.append("  (5.3% vs 8.3%) наличием бонуса за опцию «Накопления» на banki.ru.")
    lines.append(f"- **Проверка разумности:** Сравнение ставок с ключевой ставкой ЦБ ({CB_KEY_RATE}%) — ")
    lines.append("  фиксированные ставки 6-12% на 3 года адекватны при ожидании снижения КС.")
    lines.append("")

    # --- 5. Кейсы банков ---
    lines.append("## 5. Кейсы: обоснование выбора ставки для 3 банков")
    lines.append("")

    # Кейс 1: Сбербанк
    lines.append("### Кейс 1: СберБанк")
    lines.append("")
    sber = next((r for r in results if r.bank == "СберБанк"), None)
    if sber:
        lines.append(f"**Итоговая ставка:** {sber.rate}% ({sber.product})")
        lines.append("")
        lines.append("**Как система пришла к значению:**")
        lines.append("")
        lines.append("1. **sravni.ru** показал 5 вкладов. Лучшая фикс. ставка на 3 года — 6.5% ")
        lines.append("   (продукты «Лучший %» и «СберВклад»).")
        lines.append("2. **banki.ru** показал вклад «Ключевой» со ставкой 15.8% на 3 года. ")
        lines.append("   Однако анализ условий выявил: ставка **плавающая** (= КС ЦБ − 2 п.п.), ")
        lines.append("   т.е. при снижении ключевой ставки доходность упадёт.")
        lines.append("3. **Решение:** Плавающий «Ключевой» вынесен в отдельную таблицу. ")
        lines.append("   Итоговая фикс. ставка = 6.5%. Это корректно: Сбербанк традиционно ")
        lines.append("   предлагает низкие долгосрочные ставки, компенсируя надёжностью.")
        lines.append("")

    # Кейс 2: Россельхозбанк
    lines.append("### Кейс 2: Россельхозбанк")
    lines.append("")
    rshb = next((r for r in results if r.bank == "Россельхозбанк"), None)
    if rshb:
        lines.append(f"**Итоговая ставка:** {rshb.rate}% ({rshb.product})")
        lines.append("")
        lines.append("**Как система пришла к значению:**")
        lines.append("")
        lines.append("1. **sravni.ru** показал «Свой вклад» со ставкой 8.2% на 3 года.")
        lines.append("2. **banki.ru** показал тот же продукт «Свой вклад» со ставкой 7.8%.")
        lines.append("3. **Офиц. сайт (rshb.ru)** показал «Свой вклад» со ставкой 8.2% ")
        lines.append("   (калькулятор: 1 млн ₽, 36 мес, без пополнения/снятия, «Новый сберегатель»).")
        lines.append("4. Расхождение = 0.4 п.п. (≤ 0.5 п.п.) → «незначительное расхождение».")
        lines.append("5. **Причина расхождения:** banki.ru, вероятно, отображает устаревшую ставку ")
        lines.append("   или ставку без опции «Новый сберегатель».")
        lines.append("6. **Решение:** Итоговая ставка = ставка с офиц. сайта = 8.2% ")
        lines.append("   (офиц. сайт приоритетнее агрегаторов как первоисточник).")
        lines.append("")

    # Кейс 3: Газпромбанк
    lines.append("### Кейс 3: Газпромбанк")
    lines.append("")
    gpb = next((r for r in results if r.bank == "Газпромбанк"), None)
    if gpb:
        lines.append(f"**Итоговая ставка:** {gpb.rate}% ({gpb.product})")
        lines.append("")
        lines.append("**Как система пришла к значению:**")
        lines.append("")
        lines.append("1. **sravni.ru** показал несколько вкладов. Лучшие — «Новые деньги» и «Копить» ")
        lines.append("   по 8.3% на 3 года. «В Плюсе» — 5.3%.")
        lines.append("2. **banki.ru** показал «В плюсе» со ставкой 8.3%, но с примечанием: ")
        lines.append("   базовая ставка 5.3% + бонус 3% за опцию «Накопления» (требует мин. остаток ")
        lines.append("   500 000 ₽/мес на накопительных счетах или подписку Газпром Бонус).")
        lines.append("3. **Расхождение** sravni/banki по «В Плюсе»: 5.3% vs 8.3% (3 п.п.) — ")
        lines.append("   **противоречие**, но вызвано разным учётом бонусов, а не ошибкой данных.")
        lines.append("4. **Решение:** Итоговая ставка = 8.3% (по продуктам «Новые деньги» / «Копить» ")
        lines.append("   на sravni.ru, которые дают ту же ставку без дополнительных условий).")
        lines.append("")

    # --- 6. Сравнение с другими LLM ---
    lines.append("## 6. Сравнение с другими LLM: типичные ошибки ИИ при сборе ставок")
    lines.append("")
    lines.append("Для оценки качества подхода было проведено сравнение с результатами двух других LLM-агентов")
    lines.append("(GigaChat от Сбера и Алиса от Яндекса), которым был дан аналогичный промпт.")
    lines.append("")
    lines.append("### 6.1. GigaChat (Сбер)")
    lines.append("")
    lines.append("**Основные ошибки:**")
    lines.append("")
    lines.append("| Банк | GigaChat | Реальность | Проблема |")
    lines.append("|------|----------|------------|----------|")
    lines.append("| Альфа-Банк | «Максимальный доход» 13,5% | Альфа-Вклад **7,30%** | Выдуманное название, ставка завышена в 1.8 раза |")
    lines.append("| Т-Банк | «Надежный» 12,5% на 3 года | **Нет вкладов на 3 года** | Выдуманный продукт, несуществующий срок |")
    lines.append("| Газпромбанк | «Ключевой момент» 17% фикс. | **Такого продукта нет** | Полная галлюцинация |")
    lines.append("| СберБанк | «СберВклад» 12,06% | СберВклад **6,5%** | Ставка завышена в 1.85 раза |")
    lines.append("| Россельхозбанк | «Премиум» 13% | Свой вклад **8,0%** | Выдуманное название, ставка завышена |")
    lines.append("| МКБ | «Оптимальный МКБ» 13,5% | МКБ. Гранд **12,1%** | Выдуманное название |")
    lines.append("| Совкомбанк | «Золотой стандарт» 13,5% | Весенний доход **12,0%** | Выдуманное название |")
    lines.append("")
    lines.append("**Диагноз:** GigaChat выдумывает названия продуктов и генерирует правдоподобные, но фальшивые")
    lines.append("ставки. Ни один из показанных продуктов (кроме Райффайзенбанка) не соответствует действительности.")
    lines.append("")
    lines.append("### 6.2. Алиса (Яндекс)")
    lines.append("")
    lines.append("Алиса показала более качественные результаты: названия продуктов в основном реальные,")
    lines.append("Т-Банк и Райффайзенбанк определены корректно. Однако **ключевая ошибка** — завышенные ставки:")
    lines.append("")
    lines.append("| Банк | Алиса | Реальность | Причина расхождения |")
    lines.append("|------|-------|------------|---------------------|")
    lines.append("| Альфа-Банк | «до 14%» | **7,30%** (офиц. сайт) | Ставка 14% — для срока 3-4 мес, не 3 года |")
    lines.append("| СберБанк | «до 14%» | **6,5%** | 14% — для 6 мес на «новые деньги» |")
    lines.append("| ВТБ | «до 14,3%» | **11,5%** | 14,3% — для 61 дня |")
    lines.append("| Газпромбанк | «до 14%» | **8,3%** | 14% — для 120 дней |")
    lines.append("| Россельхозбанк | «до 13,4%» | **8,0%** | «Ультра Доходный» требует от 1,5 млн руб. |")
    lines.append("| МКБ | «13,7%» | **12,05%** | «Простая выгода» нет на 3 года |")
    lines.append("| Совкомбанк | «до 14,5%» | **12,0%** | С «Халвой», вероятно короткий срок |")
    lines.append("| Т-Банк | макс. 24 мес | макс. **24 мес** | Срок указан корректно (единственный случай) |")
    lines.append("")
    lines.append("**Диагноз:** Алиса не фильтрует ставки по конкретному сроку. Формулировка «до X%» берётся")
    lines.append("из рекламных заголовков банков, где всегда указан максимум по короткому сроку.")
    lines.append("")
    lines.append("### 6.3. Выводы по сравнению LLM")
    lines.append("")
    lines.append("1. **Главная ошибка обоих агентов** — использование ставок «до X%» без привязки к сроку 3 года.")
    lines.append("   Банки рекламируют максимальные ставки (для сроков 3-6 мес), а LLM копируют их.")
    lines.append("2. **GigaChat** галлюцинирует сильнее: выдумывает названия продуктов и конкретные цифры.")
    lines.append("3. **Алиса** более аккуратна с названиями, но так же не умеет фильтровать по сроку.")
    lines.append("4. **Единственный надёжный путь** — визуальная проверка через калькулятор на сайте банка")
    lines.append("   или агрегатор с фильтром по сроку + анализ скриншотов с помощью LLM.")
    lines.append("5. **LLM полезен не для генерации данных, а для их обработки**: извлечение из скриншотов,")
    lines.append("   классификация (вклад vs счёт, фикс. vs плавающая), нормализация и перекрёстная проверка.")
    lines.append("")

    # --- Заключение ---
    lines.append("## 7. Заключение")
    lines.append("")
    lines.append("### Ключевые наблюдения")
    lines.append("")
    lines.append("1. **Долгосрочные фикс. ставки значительно ниже краткосрочных**: банки закладывают ")
    lines.append("   ожидание снижения ключевой ставки ЦБ, поэтому 3-летние ставки (6-12%) ")
    lines.append("   существенно ниже 6-месячных (12-15%).")
    lines.append("2. **Плавающие вклады** (Сбербанк «Ключевой», Альфа «Ключевой») дают высокие ")
    lines.append("   текущие ставки (15-16%), но несут риск снижения при смягчении ДКП.")
    lines.append("3. **Два банка не предлагают 3-летних вкладов**: Т-Банк (макс. 24 мес) и ")
    lines.append("   Райффайзенбанк (вклады закрыты с 2024 г.).")
    lines.append("4. **Расхождения между агрегаторами** вызваны: разным учётом бонусных опций, ")
    lines.append("   различием продуктовых каталогов, условиями «для новых клиентов».")
    lines.append("")

    return "\n".join(lines)


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

    # Генерация отчёта
    sources_meta = {name: len(recs) for name, recs in sources.items()}
    report = generate_report(results, sources_meta)

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"\nОтчёт сохранён: {REPORT_PATH}")


if __name__ == "__main__":
    main()

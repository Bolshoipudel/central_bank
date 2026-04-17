"""
download_data.py — одноразовая выгрузка архивов формы 101 и формы 102 с сайта ЦБ РФ

Скачивает .rar-архивы из раздела «Отчётность кредитных организаций» и распаковывает
из них нужные DBF-файлы в data/raw/

URL-шаблон ЦБ: https://www.cbr.ru/vfs/credit/forms/<FORM>-<YYYYMMDD>.rar
  — FORM   : 101 (оборотная ведомость) или 102 (отчёт о фин. результатах)
  — YYYYMMDD: первый день месяца, следующий за отчётным (01.06.2021 = данные за май 2021)

Для распаковки .rar используется bsdtar 

Запускать один раз — результаты работы process_form_101.py уже не требуют интернета
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import requests

# Корневая директория задачи
BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"

URL_TEMPLATE = "https://www.cbr.ru/vfs/credit/forms/{form}-{yyyymmdd}.rar"

# Список архивов для загрузки.
# Каждая запись: (номер формы, дата в URL = первый день СЛЕДУЮЩЕГО за отчётным месяца).
# В именах DBF у ЦБ используется номер самого отчётного месяца/квартала — см. main().
#
# Форма 101 (MMYYYYB1.DBF, MM — отчётный месяц):
#   052021B1.DBF — YTD на 01.06.2021 (май 2021)
#   042021B1.DBF — YTD на 01.05.2021 (для очистки от накопления мая и апреля)
#   032021B1.DBF — YTD на 01.04.2021 (для вычисления прибыли за апрель)
#   052020B1.DBF — YTD на 01.06.2020 (май 2020, для YoY)
#   042020B1.DBF — YTD на 01.05.2020 (для вычисления прибыли за май 2020)
# Форма 102 (<квартал><год>_P1.DBF):
#   12021_P1.DBF — Q1 2021 YTD на 01.04.2021 (для валидации расчёта)
# Дополнительно извлекаем N1.DBF (справочник названий банков) хотя бы из одного архива.
DOWNLOADS = [
    ("101", "20210601"),  # май 2021
    ("101", "20210501"),  # апрель 2021
    ("101", "20210401"),  # март 2021
    ("101", "20200601"),  # май 2020
    ("101", "20200501"),  # апрель 2020
    ("102", "20210401"),  # первый квартал 2021 — для сверки с формой 101
]


def fetch(url: str) -> bytes:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }
    print(f"  GET {url}")
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.content


def extract_rar(rar_path: Path, dest_dir: Path) -> list[str]:
    """
    Распаковвываем .rar-архив через bsdtar (libarchive) в dest_dir
    Возвращает список имён извлечённых файлов
    """
    result = subprocess.run(
        ["bsdtar", "-xf", str(rar_path), "-C", str(dest_dir)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"bsdtar не смог распаковать {rar_path.name}:\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    return [p.name for p in dest_dir.iterdir()]


def main() -> int:
    # Проверяем, что bsdtar доступен
    if shutil.which("bsdtar") is None:
        print(
            "ОШИБКА: утилита bsdtar не найдена. Установите libarchive:\n"
            "  macOS:  brew install libarchive\n"
            "  Ubuntu: sudo apt install libarchive-tools",
            file=sys.stderr,
        )
        return 1

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    failed: list[str] = []
    for form, yyyymmdd in DOWNLOADS:
        # В URL ЦБ стоит первый день месяца, СЛЕДУЮЩЕГО за отчётным
        # (20210601 = данные «после закрытия мая 2021»).
        # А в имени DBF внутри архива — номер самого отчётного месяца (май = 05).
        # Поэтому сдвигаем (yyyy, mm) на один месяц назад с переходом через январь → декабрь.
        url_yyyy, url_mm = int(yyyymmdd[:4]), int(yyyymmdd[4:6])
        if url_mm == 1:
            rep_yyyy, rep_mm = url_yyyy - 1, 12
        else:
            rep_yyyy, rep_mm = url_yyyy, url_mm - 1
        yyyy, mm = f"{rep_yyyy:04d}", f"{rep_mm:02d}"

        if form == "101":
            # Форма 101 — помесячная: MMYYYYB1.DBF (например, 052021B1.DBF = май 2021).
            main_dbf = f"{mm}{yyyy}B1.DBF"
        else:
            # Форма 102 — поквартальная: <N><YYYY>_P1.DBF, где N — номер квартала (1..4).
            # rep_mm — последний месяц квартала: 3→Q1, 6→Q2, 9→Q3, 12→Q4.
            quarter = (rep_mm - 1) // 3 + 1
            main_dbf = f"{quarter}{yyyy}_P1.DBF"
        target = RAW_DIR / main_dbf

        if target.exists():
            print(f"[skip] уже есть: {target.name}")
            continue

        url = URL_TEMPLATE.format(form=form, yyyymmdd=yyyymmdd)
        print(f"[{form} / {yyyymmdd}] {main_dbf}")
        try:
            data = fetch(url)
        except requests.HTTPError as e:
            print(f"  ОШИБКА загрузки: {e}")
            failed.append(url)
            continue

        # Сохраняем .rar во временную директорию, распаковываем, перекладываем нужные DBF
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            rar_path = tmp_dir / f"{form}-{yyyymmdd}.rar"
            rar_path.write_bytes(data)

            try:
                extracted = extract_rar(rar_path, tmp_dir)
            except RuntimeError as e:
                print(f"  {e}")
                failed.append(url)
                continue

            # Основной DBF с данными
            src = tmp_dir / main_dbf
            if not src.exists():
                # На всякий случай ищем без учёта регистра
                candidates = [p for p in tmp_dir.iterdir() if p.name.upper() == main_dbf]
                if not candidates:
                    print(f"  ОШИБКА: в архиве нет {main_dbf}. Содержимое: {extracted}")
                    failed.append(url)
                    continue
                src = candidates[0]
            shutil.move(str(src), str(target))
            print(f"  -> {target}")

            # Справочник банков (N1.DBF) кладём только если его ещё нет
            names_target = RAW_DIR / f"{mm}{yyyy}N1.DBF"
            names_src = tmp_dir / f"{mm}{yyyy}N1.DBF"
            if form == "101" and names_src.exists() and not names_target.exists():
                shutil.move(str(names_src), str(names_target))
                print(f"  -> {names_target}")

    print()
    if failed:
        print(f"Завершено с ошибками. Не удалось скачать: {len(failed)} шт.")
        for u in failed:
            print(f"  {u}")
        print(
            "\nСкачайте вручную со страницы "
            "https://www.cbr.ru/banking_sector/otchetnost-kreditnykh-organizaciy/"
            f" и положите DBF-файлы в {RAW_DIR}"
        )
        return 2

    print("Все архивы скачаны и распакованы.")
    print(f"Файлы в: {RAW_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

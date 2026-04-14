from __future__ import annotations

import os
import sys
import time
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import pandas as pd
from clickhouse_driver import Client
from dotenv import load_dotenv


def load_config() -> dict[str, Any]:
    load_dotenv()

    ch_host = os.getenv("CH_HOST", "").strip().strip('"').strip("'")
    ch_port_raw = os.getenv("CH_PORT", "").strip().strip('"').strip("'")
    ch_user = os.getenv("CH_USER", "").strip().strip('"').strip("'")
    ch_password = os.getenv("CH_PASSWORD", "").strip().strip('"').strip("'")
    ch_database = os.getenv("CH_DATABASE", "").strip().strip('"').strip("'")
    table_name = os.getenv("TABLE_NAME_ADFOX", "").strip().strip('"').strip("'")

    output_dir_raw = os.getenv("OUTPUT_DIR", "output").strip().strip('"').strip("'")
    report_date = os.getenv("REPORT_DATE", "").strip()

    csv_path_env = os.getenv("ADFOX_PREPARED_CSV_PATH", "").strip().strip('"').strip("'")

    if not ch_host:
        raise ValueError("В .env не заполнен CH_HOST")
    if not ch_port_raw:
        raise ValueError("В .env не заполнен CH_PORT")
    if not ch_user:
        raise ValueError("В .env не заполнен CH_USER")
    if not ch_password:
        raise ValueError("В .env не заполнен CH_PASSWORD")
    if not ch_database:
        raise ValueError("В .env не заполнен CH_DATABASE")
    if not table_name:
        raise ValueError("В .env не заполнен TABLE_NAME_ADFOX")
    if not report_date:
        raise ValueError("В .env не заполнен REPORT_DATE")

    try:
        ch_port = int(ch_port_raw)
    except ValueError as exc:
        raise ValueError("CH_PORT должен быть числом") from exc

    if csv_path_env:
        csv_path = Path(csv_path_env)
    else:
        csv_path = Path(output_dir_raw) / report_date / "prepared_for_db" / f"adfox_prepared_for_db_{report_date}.csv"

    return {
        "ch_host": ch_host,
        "ch_port": ch_port,
        "ch_user": ch_user,
        "ch_password": ch_password,
        "ch_database": ch_database,
        "table_name": table_name,
        "csv_path": csv_path,
        "report_date": report_date,
    }


def quote_identifier(value: str) -> str:
    escaped = value.replace("`", "``")
    return f"`{escaped}`"


def build_full_table_name(database: str, table_name: str) -> str:
    return f"{quote_identifier(database)}.{quote_identifier(table_name)}"


def create_client(config: dict[str, Any]) -> Client:
    return Client(
        host=config["ch_host"],
        port=config["ch_port"],
        user=config["ch_user"],
        password=config["ch_password"],
        database=config["ch_database"],
        settings={"use_numpy": False},
    )


def table_exists(client: Client, database: str, table_name: str) -> bool:
    query = """
        SELECT count()
        FROM system.tables
        WHERE database = %(database)s
          AND name = %(table_name)s
    """
    result = client.execute(query, {"database": database, "table_name": table_name})
    return bool(result and result[0][0] > 0)


def read_prepared_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV не найден: {csv_path}")

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        raise RuntimeError(f"CSV пустой: {csv_path}")

    required_columns = [
        "День",
        "Час",
        "CPM",
        "Название кампании",
        "Название сайта",
        "CPC",
        "Название раздела",
        "Название площадки",
        "Запросы кода",
        "Загрузки баннеров",
        "Показы",
        "Доход",
        "Событие 4",
        "Событие 5",
        "Событие 6",
        "Событие 7",
    ]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise RuntimeError(f"В CSV не хватает колонок: {missing}")

    return df


def decimal_to_str_6(value: Any) -> str:
    dec = Decimal(str(value)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    return format(dec, "f")


def normalize_prepared_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    text_columns = [
        "День",
        "Название кампании",
        "Название сайта",
        "CPC",
        "Название раздела",
        "Название площадки",
    ]
    for col in text_columns:
        result[col] = result[col].fillna("").astype(str).str.strip()

    int_columns = [
        "Час",
        "CPM",
        "Запросы кода",
        "Загрузки баннеров",
        "Показы",
        "Событие 4",
        "Событие 5",
        "Событие 6",
        "Событие 7",
    ]
    for col in int_columns:
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0).astype("int64")

    result["Доход"] = pd.to_numeric(result["Доход"], errors="coerce").fillna(0.0)

    if (result["Запросы кода"] < 0).any():
        raise RuntimeError("Обнаружены отрицательные значения в 'Запросы кода'")
    if (result["Загрузки баннеров"] < 0).any():
        raise RuntimeError("Обнаружены отрицательные значения в 'Загрузки баннеров'")
    if (result["Показы"] < 0).any():
        raise RuntimeError("Обнаружены отрицательные значения в 'Показы'")

    return result


def map_to_clickhouse_schema(df: pd.DataFrame) -> pd.DataFrame:
    mapped = pd.DataFrame()

    mapped["platform"] = df["Название сайта"].astype(str)
    mapped["fill_rate"] = (
        (df["Загрузки баннеров"] / df["Запросы кода"].replace({0: pd.NA})) * 100
    ).fillna(0.0).astype(float)
    mapped["show_rate"] = (
        (df["Показы"] / df["Загрузки баннеров"].replace({0: pd.NA})) * 100
    ).fillna(0.0).astype(float)
    mapped["cpm"] = pd.to_numeric(df["CPM"], errors="coerce").fillna(0.0).astype(float)

    # Для текущего сценария фиксируем источник данных
    mapped["inventory_type"] = "adfox"

    mapped["event_date"] = pd.to_datetime(df["День"], format="%Y-%m-%d", errors="raise").dt.date
    mapped["event_hour"] = pd.to_numeric(df["Час"], errors="coerce").fillna(0).astype("uint8")

    # Идентификаторы/атрибуты строки в рамках текущей таблицы
    mapped["source_id"] = df["Название площадки"].astype(str)
    mapped["dsp"] = df["Название кампании"].astype(str)
    mapped["section_name"] = df["Название раздела"].astype(str)
    mapped["section_id"] = df["Название площадки"].astype(str)

    mapped["requested"] = pd.to_numeric(df["Запросы кода"], errors="coerce").fillna(0).astype("uint64")
    mapped["received"] = pd.to_numeric(df["Загрузки баннеров"], errors="coerce").fillna(0).astype("uint64")
    mapped["shown"] = pd.to_numeric(df["Показы"], errors="coerce").fillna(0).astype("uint64")

    mapped["revenue"] = df["Доход"].apply(decimal_to_str_6)

    mapped["v25"] = pd.to_numeric(df["Событие 4"], errors="coerce").fillna(0).astype("uint64")
    mapped["v50"] = pd.to_numeric(df["Событие 5"], errors="coerce").fillna(0).astype("uint64")
    mapped["v75"] = pd.to_numeric(df["Событие 6"], errors="coerce").fillna(0).astype("uint64")
    mapped["v100"] = pd.to_numeric(df["Событие 7"], errors="coerce").fillna(0).astype("uint64")

    mapped["revenue_share_percent"] = 0.0

    expected_columns = [
        "platform",
        "fill_rate",
        "show_rate",
        "cpm",
        "inventory_type",
        "event_date",
        "event_hour",
        "source_id",
        "dsp",
        "section_name",
        "section_id",
        "requested",
        "received",
        "shown",
        "revenue",
        "v25",
        "v50",
        "v75",
        "v100",
        "revenue_share_percent",
    ]
    mapped = mapped[expected_columns]

    return mapped


def build_insert_rows(df: pd.DataFrame) -> list[tuple]:
    rows: list[tuple] = []
    for _, row in df.iterrows():
        rows.append(
            (
                str(row["platform"]),
                float(row["fill_rate"]),
                float(row["show_rate"]),
                float(row["cpm"]),
                str(row["inventory_type"]),
                row["event_date"],
                int(row["event_hour"]),
                str(row["source_id"]),
                str(row["dsp"]),
                str(row["section_name"]),
                str(row["section_id"]),
                int(row["requested"]),
                int(row["received"]),
                int(row["shown"]),
                str(row["revenue"]),
                int(row["v25"]),
                int(row["v50"]),
                int(row["v75"]),
                int(row["v100"]),
                float(row["revenue_share_percent"]),
            )
        )
    return rows


def fetch_existing_row_count_for_dates(client: Client, full_table_name: str, dates: list[str]) -> int:
    if not dates:
        return 0

    query = f"""
        SELECT count()
        FROM {full_table_name}
        WHERE event_date IN %(dates)s
    """
    result = client.execute(query, {"dates": dates})
    return int(result[0][0]) if result else 0


def delete_rows_for_dates(client: Client, full_table_name: str, dates: list[str]) -> None:
    if not dates:
        return

    date_list_sql = ", ".join([f"toDate('{date_value}')" for date_value in dates])
    query = f"""
        ALTER TABLE {full_table_name}
        DELETE WHERE event_date IN ({date_list_sql})
    """
    client.execute(query)


def wait_for_mutations(client: Client, database: str, table_name: str, timeout_sec: int = 180, poll_sec: int = 2) -> None:
    deadline = time.time() + timeout_sec

    while True:
        query = """
            SELECT count()
            FROM system.mutations
            WHERE database = %(database)s
              AND table = %(table)s
              AND is_done = 0
        """
        result = client.execute(query, {"database": database, "table": table_name})
        pending = int(result[0][0]) if result else 0

        if pending == 0:
            return

        if time.time() >= deadline:
            raise TimeoutError(f"Не дождались завершения мутаций для {database}.{table_name}")

        time.sleep(poll_sec)


def insert_rows(client: Client, full_table_name: str, rows: list[tuple]) -> None:
    if not rows:
        return

    query = f"""
        INSERT INTO {full_table_name} (
            platform,
            fill_rate,
            show_rate,
            cpm,
            inventory_type,
            event_date,
            event_hour,
            source_id,
            dsp,
            section_name,
            section_id,
            requested,
            received,
            shown,
            revenue,
            v25,
            v50,
            v75,
            v100,
            revenue_share_percent
        ) VALUES
    """
    client.execute(query, rows)


def main() -> None:
    config = load_config()

    print("Загрузка в ClickHouse:")
    print(f"host={config['ch_host']}")
    print(f"port={config['ch_port']}")
    print(f"database={config['ch_database']}")
    print(f"table={config['table_name']}")
    print(f"csv_path={config['csv_path']}")

    raw_df = read_prepared_csv(config["csv_path"])
    prepared_df = normalize_prepared_dataframe(raw_df)
    db_df = map_to_clickhouse_schema(prepared_df)

    full_table_name = build_full_table_name(config["ch_database"], config["table_name"])
    dates_to_replace = sorted({str(value) for value in db_df["event_date"].astype(str).tolist()})

    print("-" * 120)
    print(f"Строк в подготовленном CSV: {len(raw_df)}")
    print(f"Строк после mapping в БД: {len(db_df)}")
    print(f"Даты для замены: {dates_to_replace}")

    rows_to_insert = build_insert_rows(db_df)

    client = create_client(config)

    try:
        exists = table_exists(client, config["ch_database"], config["table_name"])
        if not exists:
            raise RuntimeError(
                f"Таблица {config['ch_database']}.{config['table_name']} не найдена в ClickHouse"
            )

        existing_before = fetch_existing_row_count_for_dates(client, full_table_name, dates_to_replace)
        print(f"Строк в БД за эти даты до удаления: {existing_before}")

        if existing_before > 0:
            print("Удаляю старые строки за эти даты...")
            delete_rows_for_dates(client, full_table_name, dates_to_replace)
            wait_for_mutations(client, config["ch_database"], config["table_name"])
            print("Удаление завершено.")
        else:
            print("Старых строк за эти даты нет, удаление не требуется.")

        print("Вставляю новые строки...")
        insert_rows(client, full_table_name, rows_to_insert)
        print(f"Вставлено строк: {len(rows_to_insert)}")

        existing_after = fetch_existing_row_count_for_dates(client, full_table_name, dates_to_replace)
        print(f"Строк в БД за эти даты после вставки: {existing_after}")

        if existing_after != len(rows_to_insert):
            print(
                "WARNING: количество строк после вставки не совпало с числом вставленных строк. "
                "Проверь таблицу и движок."
            )

    finally:
        client.disconnect()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)
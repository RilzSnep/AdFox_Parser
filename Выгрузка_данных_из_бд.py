from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from clickhouse_driver import Client
from dotenv import load_dotenv


def load_config() -> dict[str, Any]:
    load_dotenv()

    host = os.getenv("CH_HOST", "").strip().strip('"').strip("'")
    port_raw = os.getenv("CH_PORT", "").strip().strip('"').strip("'")
    user = os.getenv("CH_USER", "").strip().strip('"').strip("'")
    password = os.getenv("CH_PASSWORD", "").strip().strip('"').strip("'")
    database = os.getenv("CH_DATABASE", "").strip().strip('"').strip("'")
    table_name = os.getenv("TABLE_NAME_ADFOX", "").strip().strip('"').strip("'")
    output_dir_raw = os.getenv("OUTPUT_DIR", "output").strip().strip('"').strip("'")

    if not host:
        raise ValueError("В .env не заполнен CH_HOST")
    if not port_raw:
        raise ValueError("В .env не заполнен CH_PORT")
    if not user:
        raise ValueError("В .env не заполнен CH_USER")
    if not password:
        raise ValueError("В .env не заполнен CH_PASSWORD")
    if not database:
        raise ValueError("В .env не заполнен CH_DATABASE")
    if not table_name:
        raise ValueError("В .env не заполнен TABLE_NAME_ADFOX")

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ValueError("CH_PORT должен быть числом") from exc

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "table_name": table_name,
        "output_dir": Path(output_dir_raw),
    }


def quote_identifier(value: str) -> str:
    escaped = value.replace("`", "``")
    return f"`{escaped}`"


def build_full_table_name(database: str, table_name: str) -> str:
    return f"{quote_identifier(database)}.{quote_identifier(table_name)}"


def create_client(config: dict[str, Any]) -> Client:
    return Client(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        database=config["database"],
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


def fetch_all_rows_sorted(client: Client, full_table_name: str) -> tuple[list[str], list[tuple]]:
    query = f"""
        SELECT *
        FROM {full_table_name}
        ORDER BY
            event_date,
            event_hour,
            platform,
            dsp,
            source_id,
            section_id
    """
    rows, columns_with_types = client.execute(query, with_column_types=True)
    column_names = [column_name for column_name, _column_type in columns_with_types]
    return column_names, rows


def autosize_worksheet(worksheet) -> None:
    for column_cells in worksheet.columns:
        max_length = 0
        column_letter = column_cells[0].column_letter
        for cell in column_cells:
            cell_value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(cell_value))
        worksheet.column_dimensions[column_letter].width = min(max_length + 2, 50)


def save_outputs(output_dir: Path, table_name: str, df: pd.DataFrame) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_path = output_dir / f"{table_name}_check_export_{timestamp}.csv"
    xlsx_path = output_dir / f"{table_name}_check_export_{timestamp}.xlsx"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="data")
        worksheet = writer.sheets["data"]
        autosize_worksheet(worksheet)

    return csv_path, xlsx_path


def print_summary(df: pd.DataFrame) -> None:
    print("-" * 120)
    print(f"Строк выгружено: {len(df)}")

    if df.empty:
        print("Таблица пустая.")
        return

    if "event_date" in df.columns:
        unique_dates = sorted(df["event_date"].astype(str).unique().tolist())
        print(f"Даты: {unique_dates}")

    numeric_columns = ["requested", "received", "shown", "v25", "v50", "v75", "v100"]
    for col in numeric_columns:
        if col in df.columns:
            print(f"{col}: {int(pd.to_numeric(df[col], errors='coerce').fillna(0).sum())}")

    if "revenue" in df.columns:
        revenue_sum = pd.to_numeric(df["revenue"], errors="coerce").fillna(0).sum()
        print(f"revenue: {float(revenue_sum):.6f}")


def main() -> None:
    config = load_config()

    print("Подключение к ClickHouse:")
    print(f"host={config['host']}")
    print(f"port={config['port']}")
    print(f"database={config['database']}")
    print(f"table={config['table_name']}")

    client = create_client(config)

    try:
        exists = table_exists(client, config["database"], config["table_name"])
        if not exists:
            raise RuntimeError(
                f"Таблица {config['database']}.{config['table_name']} не найдена в ClickHouse"
            )

        full_table_name = build_full_table_name(config["database"], config["table_name"])
        columns, rows = fetch_all_rows_sorted(client, full_table_name)
        df = pd.DataFrame(rows, columns=columns)

        print_summary(df)

        export_dir = config["output_dir"] / "clickhouse_check_exports"
        csv_path, xlsx_path = save_outputs(export_dir, config["table_name"], df)

        print("-" * 120)
        print(f"CSV: {csv_path}")
        print(f"XLSX: {xlsx_path}")

    finally:
        client.disconnect()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)
from __future__ import annotations

import os
import sys
import time
from typing import Any

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
    }


def quote_identifier(value: str) -> str:
    escaped = value.replace("`", "``")
    return f"`{escaped}`"


def full_table_name(database: str, table_name: str) -> str:
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


def column_exists(client: Client, database: str, table_name: str, column_name: str) -> bool:
    query = """
        SELECT count()
        FROM system.columns
        WHERE database = %(database)s
          AND table = %(table_name)s
          AND name = %(column_name)s
    """
    result = client.execute(
        query,
        {
            "database": database,
            "table_name": table_name,
            "column_name": column_name,
        },
    )
    return bool(result and result[0][0] > 0)


def wait_for_mutations(
    client: Client,
    database: str,
    table_name: str,
    timeout_sec: int = 180,
    poll_sec: int = 2,
    fallback_sleep_sec: int = 20,
) -> None:
    deadline = time.time() + timeout_sec

    while True:
        query = """
            SELECT count()
            FROM system.mutations
            WHERE database = %(database)s
              AND table = %(table)s
              AND is_done = 0
        """

        try:
            result = client.execute(query, {"database": database, "table": table_name})
            pending = int(result[0][0]) if result else 0

            if pending == 0:
                return

            if time.time() >= deadline:
                raise TimeoutError(f"Не дождались завершения мутаций для {database}.{table_name}")

            time.sleep(poll_sec)

        except Exception as exc:
            error_text = str(exc)
            if "system.mutations" in error_text or "Not enough privileges" in error_text:
                print(
                    "WARNING: нет прав на чтение system.mutations. "
                    f"Жду {fallback_sleep_sec} сек. вслепую."
                )
                time.sleep(fallback_sleep_sec)
                return
            raise


def main() -> None:
    config = load_config()
    client = create_client(config)

    db = config["database"]
    table = config["table_name"]
    full_name = full_table_name(db, table)

    try:
        if not table_exists(client, db, table):
            raise RuntimeError(f"Таблица {db}.{table} не найдена")

        print(f"Работаю с таблицей: {db}.{table}")

        if not column_exists(client, db, table, "DSP_NAME"):
            add_query = f"""
                ALTER TABLE {full_name}
                ADD COLUMN `DSP_NAME` LowCardinality(String)
                AFTER `dsp`
            """
            client.execute(add_query)
            print("Колонка DSP_NAME добавлена.")
        else:
            print("Колонка DSP_NAME уже существует.")

        update_dsp_name_query = f"""
            ALTER TABLE {full_name}
            UPDATE
                `DSP_NAME` = trim(BOTH ' ' FROM splitByChar('|', dsp)[1])
            WHERE 1
        """
        client.execute(update_dsp_name_query)
        print("Запущено обновление DSP_NAME из dsp.")

        wait_for_mutations(client, db, table)

        update_revenue_share_query = f"""
            ALTER TABLE {full_name}
            UPDATE
                `revenue_share_percent` = toFloat64(revenue)
            WHERE 1
        """
        client.execute(update_revenue_share_query)
        print("Запущено обновление revenue_share_percent = revenue.")

        wait_for_mutations(client, db, table)

        print("Готово. Таблица обновлена.")

    finally:
        client.disconnect()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)
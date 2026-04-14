import os
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv('CH_HOST')
PORT = int(os.getenv('CH_PORT', 8123))
USER = os.getenv('CH_USER', 'default')
PASSWORD = os.getenv('CH_PASSWORD', '')
DATABASE = os.getenv('CH_DATABASE', 'default')


def get_client():
    if not HOST:
        raise ValueError("CH_HOST не задан в .env файле.")
    return clickhouse_connect.get_client(
        host=HOST, port=PORT, username=USER,
        password=PASSWORD, database=DATABASE
    )


def inspect_table(table_name):
    try:
        client = get_client()
        print(f"\n--- Изучаем таблицу: {table_name} ---")

        # 1. Получаем структуру таблицы
        print("\n[ Структура (колонки и типы) ]")
        schema_query = f"DESCRIBE TABLE {table_name}"
        schema_result = client.query(schema_query)

        # В ClickHouse DESCRIBE возвращает: name, type, default_kind, default_expression, comment, ...
        for row in schema_result.result_rows:
            col_name, col_type = row[0], row[1]
            comment = row[4] if len(row) > 4 and row[4] else ""
            comment_str = f"  # {comment}" if comment else ""
            print(f"  - {col_name:<30} | {col_type:<20}{comment_str}")

        # 2. Получаем первые 5 строк данных
        print(f"\n[ Первые 5 строк из {table_name} ]")
        data_query = f"SELECT * FROM {table_name} LIMIT 5"
        data_result = client.query(data_query)

        col_names = data_result.column_names
        rows = data_result.result_rows

        if not rows:
            print("  (Таблица пуста)")
            return

        # Вычисляем ширину каждого столбца для красивого вывода
        col_widths = [len(name) for name in col_names]
        for row in rows:
            for i, val in enumerate(row):
                # Ограничиваем длину выводимой строки, чтобы не ломать консоль
                str_val = str(val) if val is not None else "NULL"
                col_widths[i] = max(col_widths[i], min(len(str_val), 50))

        # Рисуем заголовок
        header = " | ".join(name.ljust(col_widths[i]) for i, name in enumerate(col_names))
        print(header)
        print("-" * len(header))

        # Рисуем строки
        for row in rows:
            formatted_row = []
            for i, val in enumerate(row):
                str_val = str(val) if val is not None else "NULL"
                # Обрезаем длинные значения и добавляем "...", если они слишком длинные
                if len(str_val) > 50:
                    str_val = str_val[:47] + "..."
                formatted_row.append(str_val.ljust(col_widths[i]))
            print(" | ".join(formatted_row))

        # Выводим общее кол-во строк в таблице на footer
        count_result = client.query(f"SELECT count() FROM {table_name}")
        total_rows = count_result.result_rows[0][0]
        print("-" * len(header))
        print(f"Всего строк в таблице: {total_rows:,}".replace(",", " "))

    except Exception as e:
        print(f"\n[ОШИБКА] {e}")
    finally:
        if 'client' in locals():
            client.close()


if __name__ == "__main__":
    inspect_table("vtochku_dsp")
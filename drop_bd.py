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


def truncate_table(table_name):
    """Полностью очищает таблицу (быстро и без нагрузки)"""
    query = f"TRUNCATE TABLE {table_name}"
    client = get_client()

    confirm = input(f"⚠️  ВНИМАНИЕ: Ты собираешься ПОЛНОСТЬЮ ОЧИСТИТЬ таблицу `{table_name}`. Ты уверен? (y/n): ")
    if confirm.lower() != 'y':
        print("Отменено.")
        return

    try:
        print(f"Выполняю: {query}")
        client.command(query)  # Используем .command() для DDL операций
        print("✅ Таблица успешно очищена!")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        client.close()


def delete_by_condition(table_name, condition):
    """Удаляет строки по условию (тяжелая фоновая мутация)"""
    query = f"ALTER TABLE {table_name} DELETE WHERE {condition}"
    client = get_client()

    print(f"\n⚠️  ВНИМАНИЕ: Будет запущена мутация.")
    print(f"Запрос: {query}")
    print("В ClickHouse это работает АСИНХРОННО и может занять время!")

    confirm = input("Продолжить? (y/n): ")
    if confirm.lower() != 'y':
        print("Отменено.")
        return

    try:
        # Команда вернет сразу, не дожидаясь фактического удаления
        client.command(query)
        print("✅ Запрос на удаление отправлен в фоновую очередь!")
        print("👉 Проверить статус мутации можно запросом: SELECT * FROM system.mutations WHERE table = 'vtochku_dsp';")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    TABLE = "vtochku_dsp"

    print(f"Что делаем с таблицей {TABLE}?")
    print("1. Удалить вообще ВСЕ данные (TRUNCATE)")
    print("2. Удалить по условию (ALTER TABLE DELETE)")
    print("0. Ничего, я передумал")

    choice = input("\nВыбери вариант (1/2/0): ")

    if choice == '1':
        truncate_table(TABLE)
    elif choice == '2':
        print("\nПример условия: created_at < '2023-01-01' OR status = 'cancelled'")
        condition = input("Введи условие для DELETE WHERE (без слова WHERE): ")
        if condition.strip():
            delete_by_condition(TABLE, condition)
        else:
            print("Пустое условие, отмена.")
    else:
        print("Пока!")
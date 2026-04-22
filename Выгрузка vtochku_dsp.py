import os

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

HOST = "172.19.95.127"
PORT = 8123
USERNAME = "sandbox_analytic"
PASSWORD = os.getenv("CH_PASSWORD")
DATABASE = "sandbox"
TABLE = "vtochku_between"

OUTPUT_XLSX = "vtochku_between_export.xlsx"
OUTPUT_CSV = "vtochku_between_export.csv"


def export_between_with_metrics(to_excel: bool = True, to_csv: bool = False):
    client = clickhouse_connect.get_client(
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        database=DATABASE
    )

    query = f"""
    SELECT
        event_date,
        event_hour,
        publisher_id,
        section_name,
        section_id,
        cp_bidder_name,
        bid_responses,
        responses,
        impressions,
        net_payable,
        actual_pub,
        v_firstq,
        v_midpoint,
        v_thirdq,
        v_complete,
        inserted_at,
        DSP_NAME,
        PLATFORM,

        if(
            bid_responses = 0,
            0.0,
            round(toFloat64(responses) / toFloat64(bid_responses) * 100, 2)
        ) AS FILL_RATE,

        if(
            responses = 0,
            0.0,
            round(toFloat64(impressions) / toFloat64(responses) * 100, 2)
        ) AS SHOW_RATE,

        if(
            impressions = 0,
            0.0,
            round(toFloat64(net_payable) / toFloat64(impressions) * 1000, 2)
        ) AS CPM,

        INVENTORY_TYPE
    FROM {DATABASE}.{TABLE}
    ORDER BY event_date, event_hour
    """

    print("Выполняю выгрузку...")
    df = client.query_df(query)

    print(f"Получено строк: {len(df)}")
    print(f"Получено столбцов: {len(df.columns)}")

    if to_excel:
        df.to_excel(OUTPUT_XLSX, index=False)
        print(f"Excel сохранен: {OUTPUT_XLSX}")

    if to_csv:
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        print(f"CSV сохранен: {OUTPUT_CSV}")


if __name__ == "__main__":
    export_between_with_metrics(to_excel=True, to_csv=False)
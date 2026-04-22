from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from clickhouse_driver import Client
from dotenv import load_dotenv
from zoneinfo import ZoneInfo


COMMON_REPORT_NAME = "custom_14072"

TARGET_SUPERCAMPAIGNS: list[dict[str, Any]] = [
    {"name": "SELL", "supercampaign_id": 296740},
    {"name": "SSP", "supercampaign_id": 296675},
    {"name": "BK", "supercampaign_id": 317352},
]

VALID_LEVELS = {"owner", "supercampaign", "campaign", "banner", "site", "section", "place"}


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def load_config() -> dict[str, Any]:
    load_dotenv()

    adfox_token = os.getenv("ADFOX_TOKEN", "").strip()
    adfox_base_url = os.getenv("ADFOX_BASE_URL", "https://adfox.yandex.ru").strip()
    adfox_precision = os.getenv("ADFOX_PRECISION", "normal").strip()
    report_timezone = os.getenv("REPORT_TIMEZONE", "Europe/Moscow").strip()
    output_dir_raw = os.getenv("OUTPUT_DIR", "output").strip()
    save_raw_json_raw = os.getenv("SAVE_RAW_JSON", "false").strip().lower()

    ch_host = os.getenv("CH_HOST", "").strip().strip('"').strip("'")
    ch_port_raw = os.getenv("CH_PORT", "").strip().strip('"').strip("'")
    ch_user = os.getenv("CH_USER", "").strip().strip('"').strip("'")
    ch_password = os.getenv("CH_PASSWORD", "").strip().strip('"').strip("'")
    ch_database = os.getenv("CH_DATABASE", "").strip().strip('"').strip("'")
    ch_table_name = os.getenv("TABLE_NAME_ADFOX", "").strip().strip('"').strip("'")

    if not adfox_token:
        raise ValueError("В .env не заполнен ADFOX_TOKEN")
    if not adfox_base_url:
        raise ValueError("В .env не заполнен ADFOX_BASE_URL")
    if not adfox_precision:
        raise ValueError("В .env не заполнен ADFOX_PRECISION")

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
    if not ch_table_name:
        raise ValueError("В .env не заполнен TABLE_NAME_ADFOX")

    try:
        ch_port = int(ch_port_raw)
    except ValueError as exc:
        raise ValueError("CH_PORT должен быть числом") from exc

    save_raw_json = save_raw_json_raw in {"1", "true", "yes", "y"}

    return {
        "adfox_token": adfox_token,
        "adfox_base_url": adfox_base_url,
        "adfox_precision": adfox_precision,
        "report_timezone": report_timezone,
        "output_dir": Path(output_dir_raw),
        "save_raw_json": save_raw_json,
        "ch_host": ch_host,
        "ch_port": ch_port,
        "ch_user": ch_user,
        "ch_password": ch_password,
        "ch_database": ch_database,
        "ch_table_name": ch_table_name,
    }


def build_adfox_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"OAuth {token}",
        "Accept": "application/json",
        "User-Agent": "adfox-common-custom-report-loader/2.0",
    }


def safe_get_json(response: requests.Response) -> dict[str, Any]:
    try:
        return response.json()
    except Exception as exc:
        raise RuntimeError(
            f"Сервер вернул не JSON. HTTP {response.status_code}. Тело ответа: {response.text[:1000]}"
        ) from exc


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
    timeout: int = 60,
    retries: int = 3,
    sleep_sec: int = 3,
) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            response = session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=timeout,
            )

            if response.status_code == 429:
                if attempt == retries:
                    raise RuntimeError("Превышен лимит запросов AdFox (429)")
                time.sleep(sleep_sec * attempt)
                continue

            response.raise_for_status()
            return safe_get_json(response)

        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(sleep_sec * attempt)

    raise RuntimeError(f"Ошибка запроса {url}: {last_error}")


def create_report_task(
    session: requests.Session,
    token: str,
    base_url: str,
    level: str,
    params: dict[str, Any],
) -> str:
    if level not in VALID_LEVELS:
        raise ValueError(f"Недопустимый уровень отчёта: {level}")

    url = f"{base_url}/api/report/{level}"
    payload = request_with_retry(
        session=session,
        method="GET",
        url=url,
        headers=build_adfox_headers(token),
        params=params,
    )

    if payload.get("error"):
        raise RuntimeError(f"Ошибка создания отчёта: {json.dumps(payload, ensure_ascii=False)}")

    task_id = payload.get("result", {}).get("taskId")
    if not task_id:
        raise RuntimeError(f"В ответе нет taskId: {json.dumps(payload, ensure_ascii=False)}")

    return task_id


def wait_for_report_result(
    session: requests.Session,
    token: str,
    base_url: str,
    task_id: str,
    timeout_sec: int = 180,
    poll_sec: int = 2,
) -> dict[str, Any]:
    url = f"{base_url}/api/report/result"
    deadline = time.time() + timeout_sec

    while True:
        payload = request_with_retry(
            session=session,
            method="GET",
            url=url,
            headers=build_adfox_headers(token),
            params={"taskId": task_id},
        )

        if payload.get("error"):
            raise RuntimeError(
                f"Ошибка получения результата taskId={task_id}: "
                f"{json.dumps(payload, ensure_ascii=False)}"
            )

        state = payload.get("result", {}).get("state")

        if state == "SUCCESS":
            return payload

        if state in {"PENDING", "STARTED"}:
            if time.time() >= deadline:
                raise TimeoutError(f"Таймаут ожидания taskId={task_id}, state={state}")
            time.sleep(poll_sec)
            continue

        raise RuntimeError(
            f"Неожиданное состояние taskId={task_id}: {json.dumps(payload, ensure_ascii=False)}"
        )


def make_safe_filename(value: str) -> str:
    invalid_chars = '<>:"/\\|?*'
    safe = value
    for ch in invalid_chars:
        safe = safe.replace(ch, "_")
    safe = " ".join(safe.split())
    safe = safe.strip(" .")
    return safe or "report"


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def build_result_hash(payload: dict[str, Any]) -> str:
    result = payload.get("result", {})
    canonical = {
        "fields": result.get("fields", []),
        "table": result.get("table", []),
    }
    raw = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_yesterday_window(timezone_name: str) -> tuple[str, str]:
    tz = ZoneInfo(timezone_name)
    now_local = datetime.now(tz)
    yesterday = now_local.date() - timedelta(days=1)
    date_str = yesterday.isoformat()
    return date_str, date_str


def payload_to_rows(
    payload: dict[str, Any],
    supercampaign_id: int,
    source_name: str,
    report_name: str,
    task_id: str,
) -> list[dict[str, Any]]:
    result = payload.get("result", {})
    fields = result.get("fields", [])
    table = result.get("table", [])

    if not fields:
        raise RuntimeError(
            f"Пустой список fields в отчёте {report_name} для supercampaignId={supercampaign_id}"
        )

    rows: list[dict[str, Any]] = []
    fetched_at = datetime.now().isoformat(timespec="seconds")

    for raw_row in table:
        row_dict = {field_name: raw_row[idx] for idx, field_name in enumerate(fields)}
        row_dict["sourceSupercampaignId"] = supercampaign_id
        row_dict["sourceSupercampaignName"] = source_name
        row_dict["sourceReportName"] = report_name
        row_dict["sourceTaskId"] = task_id
        row_dict["sourceFetchedAt"] = fetched_at
        rows.append(row_dict)

    return rows


def ensure_required_fields_in_payload(payload: dict[str, Any]) -> None:
    fields = payload.get("result", {}).get("fields", [])
    required_fields = [
        "date",
        "hour",
        "cpmInstantDirectDict1000",
        "campaignName",
        "siteName",
        "campaignCpcDict",
        "sectionName",
        "placeName",
        "loadsTotal",
        "loadsCommercial",
        "impressionsCommercial",
        "calculatedRevenueByEvent",
        "event4Count",
        "event5Count",
        "event6Count",
        "event7Count",
        "event8Count",
        "event9Count",
    ]
    missing = [field for field in required_fields if field not in fields]
    if missing:
        raise RuntimeError(f"В custom report не хватает полей: {missing}. fields={fields}")


def normalize_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    text_columns = [
        "date",
        "campaignName",
        "siteName",
        "campaignCpcDict",
        "sectionName",
        "placeName",
        "sourceSupercampaignName",
        "sourceReportName",
        "sourceTaskId",
        "sourceFetchedAt",
    ]

    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    numeric_columns = [
        "sourceSupercampaignId",
        "hour",
        "cpmInstantDirectDict1000",
        "loadsTotal",
        "loadsCommercial",
        "impressionsCommercial",
        "calculatedRevenueByEvent",
        "event4Count",
        "event5Count",
        "event6Count",
        "event7Count",
        "event8Count",
        "event9Count",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def apply_event_mapping(df: pd.DataFrame) -> pd.DataFrame:
    burger_king_supercampaign_id = 317352

    df["mappedEvent4"] = 0
    df["mappedEvent5"] = 0
    df["mappedEvent6"] = 0
    df["mappedEvent7"] = 0

    regular_mask = df["sourceSupercampaignId"] != burger_king_supercampaign_id
    burger_mask = df["sourceSupercampaignId"] == burger_king_supercampaign_id

    df.loc[regular_mask, "mappedEvent4"] = df.loc[regular_mask, "event4Count"]
    df.loc[regular_mask, "mappedEvent5"] = df.loc[regular_mask, "event5Count"]
    df.loc[regular_mask, "mappedEvent6"] = df.loc[regular_mask, "event6Count"]
    df.loc[regular_mask, "mappedEvent7"] = df.loc[regular_mask, "event7Count"]

    df.loc[burger_mask, "mappedEvent4"] = df.loc[burger_mask, "event6Count"]
    df.loc[burger_mask, "mappedEvent5"] = df.loc[burger_mask, "event7Count"]
    df.loc[burger_mask, "mappedEvent6"] = df.loc[burger_mask, "event8Count"]
    df.loc[burger_mask, "mappedEvent7"] = df.loc[burger_mask, "event9Count"]

    return df


def build_final_dataframe(raw_rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not raw_rows:
        raise RuntimeError("Нет сырых строк для построения итогового отчёта")

    df = pd.DataFrame(raw_rows)
    df = normalize_raw_dataframe(df)
    df = apply_event_mapping(df)

    dedup_columns = [
        "sourceSupercampaignId",
        "date",
        "hour",
        "cpmInstantDirectDict1000",
        "campaignName",
        "siteName",
        "campaignCpcDict",
        "sectionName",
        "placeName",
        "loadsTotal",
        "loadsCommercial",
        "impressionsCommercial",
        "calculatedRevenueByEvent",
        "mappedEvent4",
        "mappedEvent5",
        "mappedEvent6",
        "mappedEvent7",
    ]

    df = df.drop_duplicates(subset=dedup_columns).reset_index(drop=True)

    group_columns = [
        "sourceSupercampaignId",
        "date",
        "hour",
        "cpmInstantDirectDict1000",
        "campaignName",
        "siteName",
        "campaignCpcDict",
        "sectionName",
        "placeName",
    ]

    value_columns = [
        "loadsTotal",
        "loadsCommercial",
        "impressionsCommercial",
        "calculatedRevenueByEvent",
        "mappedEvent4",
        "mappedEvent5",
        "mappedEvent6",
        "mappedEvent7",
    ]

    final_df = (
        df.groupby(group_columns, dropna=False, as_index=False)[value_columns]
        .sum()
        .sort_values(
            by=[
                "sourceSupercampaignId",
                "date",
                "hour",
                "campaignName",
                "siteName",
                "sectionName",
                "placeName",
                "cpmInstantDirectDict1000",
            ]
        )
        .reset_index(drop=True)
    )

    final_df = final_df.rename(
        columns={
            "sourceSupercampaignId": "ID суперкампании",
            "date": "День",
            "hour": "Час",
            "cpmInstantDirectDict1000": "CPM",
            "campaignName": "Название кампании",
            "siteName": "Название сайта",
            "campaignCpcDict": "CPC",
            "sectionName": "Название раздела",
            "placeName": "Название площадки",
            "loadsTotal": "Запросы кода",
            "loadsCommercial": "Загрузки баннеров",
            "impressionsCommercial": "Показы",
            "calculatedRevenueByEvent": "Доход",
            "mappedEvent4": "Событие 4",
            "mappedEvent5": "Событие 5",
            "mappedEvent6": "Событие 6",
            "mappedEvent7": "Событие 7",
        }
    )

    int_columns = [
        "ID суперкампании",
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
        final_df[col] = pd.to_numeric(final_df[col], errors="coerce").fillna(0).round().astype("int64")

    final_df["Доход"] = pd.to_numeric(final_df["Доход"], errors="coerce").fillna(0.0).astype(float)

    ordered_columns = [
        "ID суперкампании",
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
    return final_df[ordered_columns]


def decimal_to_str_6(value: Any) -> str:
    dec = Decimal(str(value)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)
    return format(dec, "f")


def detect_platform_from_source_id(source_id: str) -> str:
    source_upper = (source_id or "").upper()
    if "CTV" in source_upper:
        return "CTV"
    if "MOBILE" in source_upper:
        return "Mobile"
    return ""


def detect_inventory_type_from_section_name(section_name: str) -> str:
    section_upper = (section_name or "").upper()
    if "SSP" in section_upper:
        return "SSP"
    if "SELL" in section_upper:
        return "SELL"
    return ""


def extract_dsp_name(dsp_value: str) -> str:
    value = (dsp_value or "").strip()
    if not value:
        return ""
    if "|" in value:
        return value.split("|", 1)[0].strip()
    return value


def map_to_clickhouse_schema(df: pd.DataFrame) -> pd.DataFrame:
    mapped = pd.DataFrame()

    source_id_series = df["Название площадки"].astype(str)
    section_name_series = df["Название раздела"].astype(str)
    dsp_series = df["Название кампании"].astype(str)

    mapped["DSP_NAME"] = dsp_series.apply(extract_dsp_name)
    mapped["platform"] = source_id_series.apply(detect_platform_from_source_id)

    mapped["fill_rate"] = (
        (df["Загрузки баннеров"] / df["Запросы кода"].replace({0: pd.NA})) * 100
    ).fillna(0.0).round(2).astype(float)

    mapped["show_rate"] = (
        (df["Показы"] / df["Загрузки баннеров"].replace({0: pd.NA})) * 100
    ).fillna(0.0).round(2).astype(float)

    mapped["cpm"] = pd.to_numeric(df["CPM"], errors="coerce").fillna(0.0).astype(float)
    mapped["inventory_type"] = section_name_series.apply(detect_inventory_type_from_section_name)

    mapped["event_date"] = pd.to_datetime(df["День"], format="%Y-%m-%d", errors="raise").dt.date
    mapped["event_hour"] = pd.to_numeric(df["Час"], errors="coerce").fillna(0).astype("uint8")

    mapped["source_id"] = source_id_series
    mapped["dsp"] = dsp_series
    mapped["section_name"] = section_name_series
    mapped["section_id"] = section_name_series

    mapped["requested"] = pd.to_numeric(df["Запросы кода"], errors="coerce").fillna(0).astype("uint64")
    mapped["received"] = pd.to_numeric(df["Загрузки баннеров"], errors="coerce").fillna(0).astype("uint64")
    mapped["shown"] = pd.to_numeric(df["Показы"], errors="coerce").fillna(0).astype("uint64")

    mapped["revenue"] = df["Доход"].apply(decimal_to_str_6)

    mapped["v25"] = pd.to_numeric(df["Событие 4"], errors="coerce").fillna(0).astype("uint64")
    mapped["v50"] = pd.to_numeric(df["Событие 5"], errors="coerce").fillna(0).astype("uint64")
    mapped["v75"] = pd.to_numeric(df["Событие 6"], errors="coerce").fillna(0).astype("uint64")
    mapped["v100"] = pd.to_numeric(df["Событие 7"], errors="coerce").fillna(0).astype("uint64")

    mapped["revenue_share_percent"] = 0.0

    ordered_columns = [
        "DSP_NAME",
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
    return mapped[ordered_columns]


def build_insert_rows(df: pd.DataFrame) -> list[tuple]:
    rows: list[tuple] = []
    for _, row in df.iterrows():
        rows.append(
            (
                str(row["DSP_NAME"]),
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


def quote_identifier(value: str) -> str:
    escaped = value.replace("`", "``")
    return f"`{escaped}`"


def build_full_table_name(database: str, table_name: str) -> str:
    return f"{quote_identifier(database)}.{quote_identifier(table_name)}"


def create_clickhouse_client(config: dict[str, Any]) -> Client:
    return Client(
        host=config["ch_host"],
        port=config["ch_port"],
        user=config["ch_user"],
        password=config["ch_password"],
        database=config["ch_database"],
        settings={"use_numpy": False},
    )


def clickhouse_table_exists(client: Client, database: str, table_name: str) -> bool:
    query = """
        SELECT count()
        FROM system.tables
        WHERE database = %(database)s
          AND name = %(table_name)s
    """
    result = client.execute(query, {"database": database, "table_name": table_name})
    return bool(result and result[0][0] > 0)


def delete_rows_for_date(
    client: Client,
    full_table_name: str,
    target_date: str,
) -> None:
    query = f"""
        ALTER TABLE {full_table_name}
        DELETE WHERE event_date = toDate('{target_date}')
    """
    client.execute(query)


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
                logging.warning(
                    "Нет прав на чтение system.mutations. Жду %s сек. вслепую.",
                    fallback_sleep_sec,
                )
                time.sleep(fallback_sleep_sec)
                return
            raise


def insert_rows(client: Client, full_table_name: str, rows: list[tuple]) -> None:
    if not rows:
        return

    query = f"""
        INSERT INTO {full_table_name} (
            DSP_NAME,
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


def save_debug_outputs(
    base_output_dir: Path,
    report_date: str,
    final_df: pd.DataFrame,
    raw_rows_df: pd.DataFrame,
    audit_df: pd.DataFrame,
) -> tuple[Path, Path]:
    base_output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = base_output_dir / f"adfox_common_report_{report_date}.csv"
    xlsx_path = base_output_dir / f"adfox_common_report_{report_date}.xlsx"

    final_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="report")
        raw_rows_df.to_excel(writer, index=False, sheet_name="raw_rows")
        audit_df.to_excel(writer, index=False, sheet_name="audit")

        for sheet_name in ["report", "raw_rows", "audit"]:
            worksheet = writer.sheets[sheet_name]
            for column_cells in worksheet.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter
                for cell in column_cells:
                    cell_value = "" if cell.value is None else str(cell.value)
                    max_length = max(max_length, len(cell_value))
                worksheet.column_dimensions[column_letter].width = min(max_length + 2, 60)

    return csv_path, xlsx_path


def collect_supercampaign_report(
    session: requests.Session,
    config: dict[str, Any],
    date_from: str,
    date_to: str,
    supercampaign_id: int,
    supercampaign_name: str,
    raw_json_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    logging.info(
        "Собираю отчёт | supercampaign=%s (%s) | dateFrom=%s | dateTo=%s | report=%s",
        supercampaign_name,
        supercampaign_id,
        date_from,
        date_to,
        COMMON_REPORT_NAME,
    )

    params = {
        "name": COMMON_REPORT_NAME,
        "supercampaignId": supercampaign_id,
        "dateFrom": date_from,
        "dateTo": date_to,
        "precision": config["adfox_precision"],
    }

    task_id = create_report_task(
        session=session,
        token=config["adfox_token"],
        base_url=config["adfox_base_url"],
        level="supercampaign",
        params=params,
    )

    logging.info("Получен taskId=%s для supercampaign=%s", task_id, supercampaign_id)

    payload = wait_for_report_result(
        session=session,
        token=config["adfox_token"],
        base_url=config["adfox_base_url"],
        task_id=task_id,
    )

    ensure_required_fields_in_payload(payload)

    if config["save_raw_json"]:
        save_json(
            raw_json_dir / f"{supercampaign_id}_{COMMON_REPORT_NAME}_{date_from}_{date_to}.json",
            payload,
        )

    rows = payload_to_rows(
        payload=payload,
        supercampaign_id=supercampaign_id,
        source_name=supercampaign_name,
        report_name=COMMON_REPORT_NAME,
        task_id=task_id,
    )

    result_hash = build_result_hash(payload)

    impressions_sum = 0
    revenue_sum = 0.0

    for row in rows:
        impressions_sum += int(pd.to_numeric(row.get("impressionsCommercial", 0), errors="coerce") or 0)
        revenue_sum += float(pd.to_numeric(row.get("calculatedRevenueByEvent", 0), errors="coerce") or 0)

    audit_row = {
        "sourceSupercampaignId": supercampaign_id,
        "sourceSupercampaignName": supercampaign_name,
        "sourceReportName": COMMON_REPORT_NAME,
        "taskId": task_id,
        "resultHash": result_hash,
        "rowCount": len(rows),
        "impressionsSum": impressions_sum,
        "revenueSum": round(revenue_sum, 6),
        "dateFrom": date_from,
        "dateTo": date_to,
        "fetchedAt": datetime.now().isoformat(timespec="seconds"),
    }

    logging.info(
        "Готов отчёт | supercampaign=%s | rows=%s | impressions=%s | revenue=%.6f | hash=%s",
        supercampaign_id,
        len(rows),
        impressions_sum,
        revenue_sum,
        result_hash,
    )

    return rows, audit_row


def run_once() -> None:
    setup_logging()
    config = load_config()

    date_from, date_to = get_yesterday_window(config["report_timezone"])
    report_output_dir = config["output_dir"] / date_to / "adfox_common_report"
    raw_json_dir = report_output_dir / "raw_json"

    logging.info("Старт выгрузки за вчера: %s", date_from)

    all_raw_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    with requests.Session() as session:
        for item in TARGET_SUPERCAMPAIGNS:
            rows, audit_row = collect_supercampaign_report(
                session=session,
                config=config,
                date_from=date_from,
                date_to=date_to,
                supercampaign_id=int(item["supercampaign_id"]),
                supercampaign_name=str(item["name"]),
                raw_json_dir=raw_json_dir,
            )
            all_raw_rows.extend(rows)
            audit_rows.append(audit_row)

    if not all_raw_rows:
        raise RuntimeError("Не удалось собрать ни одной строки из AdFox")

    raw_rows_df = pd.DataFrame(all_raw_rows)
    audit_df = pd.DataFrame(audit_rows)
    final_df = build_final_dataframe(all_raw_rows)

    debug_csv_path, debug_xlsx_path = save_debug_outputs(
        base_output_dir=report_output_dir,
        report_date=date_to,
        final_df=final_df,
        raw_rows_df=raw_rows_df,
        audit_df=audit_df,
    )

    logging.info("DEBUG CSV: %s", debug_csv_path)
    logging.info("DEBUG XLSX: %s", debug_xlsx_path)
    logging.info("Финальных строк: %s", len(final_df))

    db_df = map_to_clickhouse_schema(final_df)
    rows_to_insert = build_insert_rows(db_df)

    full_table_name = build_full_table_name(config["ch_database"], config["ch_table_name"])

    client = create_clickhouse_client(config)
    try:
        exists = clickhouse_table_exists(client, config["ch_database"], config["ch_table_name"])
        if not exists:
            raise RuntimeError(
                f"Таблица {config['ch_database']}.{config['ch_table_name']} не найдена в ClickHouse"
            )

        logging.info("Удаляю старые строки за дату %s", date_to)
        delete_rows_for_date(client, full_table_name, date_to)

        wait_for_mutations(
            client,
            config["ch_database"],
            config["ch_table_name"],
            timeout_sec=180,
            poll_sec=2,
            fallback_sleep_sec=20,
        )

        logging.info("Вставляю новые строки: %s", len(rows_to_insert))
        insert_rows(client, full_table_name, rows_to_insert)

    finally:
        client.disconnect()

    logging.info("Итоговые суммы:")
    logging.info("Запросы кода: %s", int(final_df["Запросы кода"].sum()))
    logging.info("Загрузки баннеров: %s", int(final_df["Загрузки баннеров"].sum()))
    logging.info("Показы: %s", int(final_df["Показы"].sum()))
    logging.info("Доход: %.6f", float(final_df["Доход"].sum()))
    logging.info("Событие 4: %s", int(final_df["Событие 4"].sum()))
    logging.info("Событие 5: %s", int(final_df["Событие 5"].sum()))
    logging.info("Событие 6: %s", int(final_df["Событие 6"].sum()))
    logging.info("Событие 7: %s", int(final_df["Событие 7"].sum()))


def main() -> None:
    try:
        run_once()
    except Exception as exc:
        logging.exception("Ошибка выполнения: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
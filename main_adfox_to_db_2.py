from __future__ import annotations

import hashlib
import json
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


VALID_LEVELS = {"owner", "supercampaign", "campaign", "banner", "site", "section", "place"}

TARGET_REPORTS: list[dict[str, Any]] = [
    {
        "supercampaign_id": 296740,
        "report_name": "custom_13699",
        "supercampaign_name": "SELL",
    },
    {
        "supercampaign_id": 296675,
        "report_name": "custom_13697",
        "supercampaign_name": "SSP",
    },
    {
        "supercampaign_id": 317352,
        "report_name": "custom_13698",
        "supercampaign_name": "BK",
    },
]


def load_config() -> dict[str, Any]:
    load_dotenv()

    adfox_token = os.getenv("ADFOX_TOKEN", "").strip()
    adfox_base_url = os.getenv("ADFOX_BASE_URL", "https://adfox.yandex.ru").strip()
    adfox_campaigns_report_name = os.getenv("ADFOX_CAMPAIGNS_REPORT_NAME", "campaigns").strip()
    adfox_precision = os.getenv("ADFOX_PRECISION", "normal").strip()
    report_timezone = os.getenv("REPORT_TIMEZONE", "Europe/Moscow").strip()
    output_dir_raw = os.getenv("OUTPUT_DIR", "output").strip()
    save_raw_json_raw = os.getenv("SAVE_RAW_JSON", "false").strip().lower()
    state_dir_raw = os.getenv("STATE_DIR", "state").strip()

    run_continuous_raw = os.getenv("RUN_CONTINUOUS", "true").strip().lower()
    base_interval_minutes_raw = os.getenv("BASE_INTERVAL_MINUTES", "90").strip()
    retry_interval_minutes_raw = os.getenv("RETRY_INTERVAL_MINUTES", "10").strip()

    mutation_fallback_sleep_sec_raw = os.getenv("MUTATION_FALLBACK_SLEEP_SEC", "40").strip()
    post_delete_sleep_sec_raw = os.getenv("POST_DELETE_SLEEP_SEC", "5").strip()
    post_insert_sleep_sec_raw = os.getenv("POST_INSERT_SLEEP_SEC", "3").strip()

    request_timeout_sec_raw = os.getenv("REQUEST_TIMEOUT_SEC", "60").strip()
    adfox_result_timeout_sec_raw = os.getenv("ADFOX_RESULT_TIMEOUT_SEC", "180").strip()
    clickhouse_connect_timeout_sec_raw = os.getenv("CLICKHOUSE_CONNECT_TIMEOUT_SEC", "10").strip()
    clickhouse_send_receive_timeout_sec_raw = os.getenv("CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SEC", "120").strip()

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
    if not adfox_campaigns_report_name:
        raise ValueError("В .env не заполнен ADFOX_CAMPAIGNS_REPORT_NAME")
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
        base_interval_minutes = int(base_interval_minutes_raw)
        retry_interval_minutes = int(retry_interval_minutes_raw)
        mutation_fallback_sleep_sec = int(mutation_fallback_sleep_sec_raw)
        post_delete_sleep_sec = int(post_delete_sleep_sec_raw)
        post_insert_sleep_sec = int(post_insert_sleep_sec_raw)
        request_timeout_sec = int(request_timeout_sec_raw)
        adfox_result_timeout_sec = int(adfox_result_timeout_sec_raw)
        clickhouse_connect_timeout_sec = int(clickhouse_connect_timeout_sec_raw)
        clickhouse_send_receive_timeout_sec = int(clickhouse_send_receive_timeout_sec_raw)
    except ValueError as exc:
        raise ValueError("Один из числовых параметров в .env заполнен некорректно") from exc

    save_raw_json = save_raw_json_raw in {"1", "true", "yes", "y"}
    run_continuous = run_continuous_raw in {"1", "true", "yes", "y"}

    return {
        "adfox_token": adfox_token,
        "adfox_base_url": adfox_base_url,
        "adfox_campaigns_report_name": adfox_campaigns_report_name,
        "adfox_precision": adfox_precision,
        "report_timezone": report_timezone,
        "output_dir": Path(output_dir_raw),
        "state_dir": Path(state_dir_raw),
        "save_raw_json": save_raw_json,
        "run_continuous": run_continuous,
        "base_interval_minutes": base_interval_minutes,
        "retry_interval_minutes": retry_interval_minutes,
        "mutation_fallback_sleep_sec": mutation_fallback_sleep_sec,
        "post_delete_sleep_sec": post_delete_sleep_sec,
        "post_insert_sleep_sec": post_insert_sleep_sec,
        "request_timeout_sec": request_timeout_sec,
        "adfox_result_timeout_sec": adfox_result_timeout_sec,
        "clickhouse_connect_timeout_sec": clickhouse_connect_timeout_sec,
        "clickhouse_send_receive_timeout_sec": clickhouse_send_receive_timeout_sec,
        "ch_host": ch_host,
        "ch_port": ch_port,
        "ch_user": ch_user,
        "ch_password": ch_password,
        "ch_database": ch_database,
        "ch_table_name": ch_table_name,
    }


def build_state_file_path(config: dict[str, Any]) -> Path:
    return config["state_dir"] / "adfox_task_state.json"


def read_state_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def write_state_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")

    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    tmp_path.replace(path)


def build_adfox_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"OAuth {token}",
        "Accept": "application/json",
        "User-Agent": "adfox-clickhouse-direct-loader/2.0",
    }


def extract_date_hour_keys(df: pd.DataFrame) -> list[tuple[str, int]]:
    keys_df = df[["event_date", "event_hour"]].copy()
    keys_df["event_date"] = keys_df["event_date"].astype(str)
    keys_df["event_hour"] = pd.to_numeric(keys_df["event_hour"], errors="coerce").fillna(0).astype(int)
    keys_df = keys_df.drop_duplicates().sort_values(["event_date", "event_hour"]).reset_index(drop=True)

    return [(row["event_date"], int(row["event_hour"])) for _, row in keys_df.iterrows()]


def fetch_existing_row_count_for_date_hours(
    client: Client,
    full_table_name: str,
    date_hour_keys: list[tuple[str, int]],
) -> int:
    if not date_hour_keys:
        return 0

    conditions = []
    for event_date, event_hour in date_hour_keys:
        conditions.append(f"(event_date = toDate('{event_date}') AND event_hour = {event_hour})")

    where_clause = " OR ".join(conditions)
    query = f"""
        SELECT count()
        FROM {full_table_name}
        WHERE {where_clause}
    """
    result = client.execute(query)
    return int(result[0][0]) if result else 0


def delete_rows_for_date_hours(
    client: Client,
    full_table_name: str,
    date_hour_keys: list[tuple[str, int]],
) -> None:
    if not date_hour_keys:
        return

    conditions = []
    for event_date, event_hour in date_hour_keys:
        conditions.append(f"(event_date = toDate('{event_date}') AND event_hour = {event_hour})")

    where_clause = " OR ".join(conditions)
    query = f"""
        ALTER TABLE {full_table_name}
        DELETE WHERE {where_clause}
    """
    client.execute(query)


def safe_get_json(response: requests.Response) -> dict:
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
) -> dict:
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
    timeout_sec: int,
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
        timeout=timeout_sec,
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
    request_timeout_sec: int,
    timeout_sec: int = 180,
    poll_sec: int = 2,
) -> dict:
    url = f"{base_url}/api/report/result"
    deadline = time.time() + timeout_sec

    while True:
        payload = request_with_retry(
            session=session,
            method="GET",
            url=url,
            headers=build_adfox_headers(token),
            params={"taskId": task_id},
            timeout=request_timeout_sec,
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


def save_json(path: Path, payload: dict) -> None:
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


def extract_active_campaigns(payload: dict, source_supercampaign_id: int) -> list[dict[str, Any]]:
    result = payload.get("result", {})
    fields = result.get("fields", [])
    table = result.get("table", [])

    if not fields:
        raise RuntimeError("В отчёте campaigns нет fields")
    if not table:
        return []

    required_fields = [
        "supercampaignId",
        "campaignId",
        "campaignName",
        "loadsCommercial",
        "impressionsCommercial",
        "clicksCommercial",
    ]
    missing = [field for field in required_fields if field not in fields]
    if missing:
        raise RuntimeError(f"В отчёте campaigns не хватает полей: {missing}. fields={fields}")

    supercampaign_idx = fields.index("supercampaignId")
    campaign_id_idx = fields.index("campaignId")
    campaign_name_idx = fields.index("campaignName")
    loads_idx = fields.index("loadsCommercial")
    impressions_idx = fields.index("impressionsCommercial")
    clicks_idx = fields.index("clicksCommercial")

    campaigns: list[dict[str, Any]] = []
    for row in table:
        campaigns.append(
            {
                "supercampaignId": int(row[supercampaign_idx]),
                "campaignId": int(row[campaign_id_idx]),
                "campaignName": str(row[campaign_name_idx]).strip(),
                "loadsCommercial": int(row[loads_idx] or 0),
                "impressionsCommercial": int(row[impressions_idx] or 0),
                "clicksCommercial": int(row[clicks_idx] or 0),
                "sourceSupercampaignId": source_supercampaign_id,
            }
        )

    campaigns.sort(key=lambda x: int(x["campaignId"]))
    return campaigns


def payload_to_rows(
    payload: dict,
    source_supercampaign_id: int,
    source_campaign_id: int,
    source_campaign_name: str,
    custom_report_name: str,
    source_task_id: str,
) -> list[dict[str, Any]]:
    result = payload.get("result", {})
    fields = result.get("fields", [])
    table = result.get("table", [])

    if not fields:
        return []

    rows: list[dict[str, Any]] = []
    for raw_row in table:
        row_dict = {field_name: raw_row[idx] for idx, field_name in enumerate(fields)}
        row_dict["sourceSupercampaignId"] = source_supercampaign_id
        row_dict["sourceCampaignId"] = source_campaign_id
        row_dict["sourceCampaignName"] = source_campaign_name
        row_dict["sourceCustomReportName"] = custom_report_name
        row_dict["sourceTaskId"] = source_task_id
        rows.append(row_dict)

    return rows


def get_required_fields_for_supercampaign(supercampaign_id: int) -> list[str]:
    common_fields = [
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
    ]
    if supercampaign_id == 317352:
        return common_fields + ["event6Count", "event7Count", "event8Count", "event9Count"]
    return common_fields + ["event4Count", "event5Count", "event6Count", "event7Count"]


def ensure_required_columns(df: pd.DataFrame) -> None:
    required = [
        "sourceSupercampaignId",
        "sourceCampaignName",
        "date",
        "hour",
        "cpmInstantDirectDict1000",
        "siteName",
        "campaignCpcDict",
        "sectionName",
        "placeName",
        "loadsTotal",
        "loadsCommercial",
        "impressionsCommercial",
        "calculatedRevenueByEvent",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise RuntimeError(f"В сырых данных не хватает обязательных колонок: {missing}")


def normalize_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    text_columns = [
        "date",
        "campaignName",
        "siteName",
        "campaignCpcDict",
        "sectionName",
        "placeName",
        "sourceCampaignName",
        "sourceCustomReportName",
        "sourceTaskId",
    ]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    numeric_columns = [
        "sourceSupercampaignId",
        "sourceCampaignId",
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
    for col in ["event4Count", "event5Count", "event6Count", "event7Count", "event8Count", "event9Count"]:
        if col not in df.columns:
            df[col] = 0

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
    ensure_required_columns(df)
    df = normalize_raw_dataframe(df)
    df = apply_event_mapping(df)

    if "campaignName" not in df.columns:
        df["campaignName"] = ""
    if "sourceCampaignName" not in df.columns:
        df["sourceCampaignName"] = ""

    df["reportCampaignName"] = df["campaignName"]

    override_campaign_name_ids = {296740}
    override_mask = df["sourceSupercampaignId"].isin(override_campaign_name_ids)
    df.loc[override_mask, "reportCampaignName"] = df.loc[override_mask, "sourceCampaignName"]

    empty_name_mask = df["reportCampaignName"].fillna("").astype(str).str.strip() == ""
    df.loc[empty_name_mask, "reportCampaignName"] = df.loc[empty_name_mask, "sourceCampaignName"]

    dedup_columns = [
        "sourceSupercampaignId",
        "date",
        "hour",
        "cpmInstantDirectDict1000",
        "reportCampaignName",
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
        "date",
        "hour",
        "cpmInstantDirectDict1000",
        "reportCampaignName",
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
                "date",
                "hour",
                "reportCampaignName",
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
            "date": "День",
            "hour": "Час",
            "cpmInstantDirectDict1000": "CPM",
            "reportCampaignName": "Название кампании",
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

    final_df["Доход"] = (
        pd.to_numeric(final_df["Доход"], errors="coerce")
        .fillna(0)
        .apply(lambda x: float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)))
    )

    ordered_columns = [
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


def save_debug_outputs(
    base_output_dir: Path,
    report_date: str,
    final_df: pd.DataFrame,
    active_campaigns_df: pd.DataFrame,
    raw_rows_df: pd.DataFrame,
    audit_df: pd.DataFrame,
    db_df: pd.DataFrame,
) -> tuple[Path, Path]:
    base_output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = base_output_dir / f"adfox_combined_custom_reports_{report_date}.csv"
    xlsx_path = base_output_dir / f"adfox_combined_custom_reports_{report_date}.xlsx"
    audit_csv_path = base_output_dir / f"adfox_audit_{report_date}.csv"
    db_ready_csv_path = base_output_dir / f"adfox_db_ready_{report_date}.csv"

    final_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    audit_df.to_csv(audit_csv_path, index=False, encoding="utf-8-sig")
    db_df.to_csv(db_ready_csv_path, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="report")
        active_campaigns_df.to_excel(writer, index=False, sheet_name="active_campaigns")
        raw_rows_df.to_excel(writer, index=False, sheet_name="raw_rows")
        audit_df.to_excel(writer, index=False, sheet_name="audit")
        db_df.to_excel(writer, index=False, sheet_name="db_ready")

        for sheet_name in ["report", "active_campaigns", "raw_rows", "audit", "db_ready"]:
            worksheet = writer.sheets[sheet_name]
            for column_cells in worksheet.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter
                for cell in column_cells:
                    cell_value = "" if cell.value is None else str(cell.value)
                    max_length = max(max_length, len(cell_value))
                worksheet.column_dimensions[column_letter].width = min(max_length + 2, 60)

    return csv_path, xlsx_path


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
    mapped["section_id"] = source_id_series

    mapped["requested"] = pd.to_numeric(df["Запросы кода"], errors="coerce").fillna(0).astype("uint64")
    mapped["received"] = pd.to_numeric(df["Загрузки баннеров"], errors="coerce").fillna(0).astype("uint64")
    mapped["shown"] = pd.to_numeric(df["Показы"], errors="coerce").fillna(0).astype("uint64")

    mapped["revenue"] = df["Доход"].apply(decimal_to_str_6)

    mapped["v25"] = pd.to_numeric(df["Событие 4"], errors="coerce").fillna(0).astype("uint64")
    mapped["v50"] = pd.to_numeric(df["Событие 5"], errors="coerce").fillna(0).astype("uint64")
    mapped["v75"] = pd.to_numeric(df["Событие 6"], errors="coerce").fillna(0).astype("uint64")
    mapped["v100"] = pd.to_numeric(df["Событие 7"], errors="coerce").fillna(0).astype("uint64")

    mapped["revenue_share_percent"] = pd.to_numeric(df["Доход"], errors="coerce").fillna(0.0).round(6).astype(float)

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
        connect_timeout=config["clickhouse_connect_timeout_sec"],
        send_receive_timeout=config["clickhouse_send_receive_timeout_sec"],
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


def wait_for_mutations(
    client: Client,
    database: str,
    table_name: str,
    timeout_sec: int = 180,
    poll_sec: int = 2,
    fallback_sleep_sec: int = 15,
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
                    f"Жду {fallback_sleep_sec} сек. вслепую перед вставкой."
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


def check_clickhouse_available(config: dict[str, Any]) -> None:
    client = None
    try:
        client = create_clickhouse_client(config)
        client.execute("SELECT 1")
    finally:
        if client is not None:
            client.disconnect()


def collect_single_custom_report(
    session: requests.Session,
    token: str,
    base_url: str,
    campaigns_report_name: str,
    precision: str,
    date_from: str,
    date_to: str,
    save_raw_json: bool,
    raw_json_dir: Path,
    supercampaign_id: int,
    supercampaign_name: str,
    custom_report_name: str,
    request_timeout_sec: int,
    adfox_result_timeout_sec: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    print("-" * 120)
    print("Собираю данные:")
    print(f"supercampaignId={supercampaign_id}")
    print(f"supercampaignName={supercampaign_name}")
    print(f"custom report={custom_report_name}")
    print(f"dateFrom={date_from} | dateTo={date_to}")

    campaigns_task_id = create_report_task(
        session=session,
        token=token,
        base_url=base_url,
        level="supercampaign",
        params={
            "name": campaigns_report_name,
            "supercampaignId": supercampaign_id,
            "dateFrom": date_from,
            "dateTo": date_to,
            "precision": precision,
        },
        timeout_sec=request_timeout_sec,
    )

    print(f"  taskId списка кампаний: {campaigns_task_id}")

    campaigns_payload = wait_for_report_result(
        session=session,
        token=token,
        base_url=base_url,
        task_id=campaigns_task_id,
        request_timeout_sec=request_timeout_sec,
        timeout_sec=adfox_result_timeout_sec,
    )

    if save_raw_json:
        save_json(
            raw_json_dir / f"campaigns_report_supercampaign_{supercampaign_id}_{date_from}_{date_to}.json",
            campaigns_payload,
        )

    campaigns = extract_active_campaigns(campaigns_payload, source_supercampaign_id=supercampaign_id)

    print(f"  найдено активных кампаний: {len(campaigns)}")

    all_campaigns: list[dict[str, Any]] = campaigns.copy()
    all_raw_rows: list[dict[str, Any]] = []

    if not campaigns:
        audit_row = {
            "supercampaign_id": supercampaign_id,
            "supercampaign_name": supercampaign_name,
            "report_name": custom_report_name,
            "task_id": "",
            "result_hash": "",
            "row_count": 0,
            "impressions_sum": 0,
            "revenue_sum": 0.0,
            "date_from": date_from,
            "date_to": date_to,
            "note": "no_active_campaigns",
        }
        return all_campaigns, all_raw_rows, audit_row

    base_campaign = campaigns[0]
    base_campaign_id = int(base_campaign["campaignId"])
    base_campaign_name = str(base_campaign["campaignName"]).strip()

    print(
        f"  Использую один отчёт по первой активной кампании: "
        f"campaignId={base_campaign_id} | campaignName={base_campaign_name}"
    )

    base_task_id = create_report_task(
        session=session,
        token=token,
        base_url=base_url,
        level="campaign",
        params={
            "name": custom_report_name,
            "campaignId": base_campaign_id,
            "dateFrom": date_from,
            "dateTo": date_to,
            "precision": precision,
        },
        timeout_sec=request_timeout_sec,
    )

    print(f"  taskId custom report: {base_task_id}")

    base_payload = wait_for_report_result(
        session=session,
        token=token,
        base_url=base_url,
        task_id=base_task_id,
        request_timeout_sec=request_timeout_sec,
        timeout_sec=adfox_result_timeout_sec,
    )

    if save_raw_json:
        safe_name = make_safe_filename(base_campaign_name)
        save_json(
            raw_json_dir / f"{supercampaign_id}_{base_campaign_id}_{safe_name}_{date_from}_{date_to}.json",
            base_payload,
        )

    fields = base_payload.get("result", {}).get("fields", [])
    required_fields = get_required_fields_for_supercampaign(supercampaign_id)
    missing_fields = [field for field in required_fields if field not in fields]
    if missing_fields:
        raise RuntimeError(
            f"В отчёте {custom_report_name} для supercampaignId={supercampaign_id} "
            f"не хватает полей: {missing_fields}. fields={fields}"
        )

    all_raw_rows = payload_to_rows(
        payload=base_payload,
        source_supercampaign_id=supercampaign_id,
        source_campaign_id=base_campaign_id,
        source_campaign_name=base_campaign_name,
        custom_report_name=custom_report_name,
        source_task_id=base_task_id,
    )

    result_hash = build_result_hash(base_payload)

    impressions_sum = 0
    revenue_sum = 0.0
    for row in all_raw_rows:
        impressions_sum += int(pd.to_numeric(row.get("impressionsCommercial", 0), errors="coerce") or 0)
        revenue_sum += float(pd.to_numeric(row.get("calculatedRevenueByEvent", 0), errors="coerce") or 0)

    print(f"  строк в single-fetch отчёте: {len(all_raw_rows)}")
    print(f"  result_hash: {result_hash}")

    audit_row = {
        "supercampaign_id": supercampaign_id,
        "supercampaign_name": supercampaign_name,
        "report_name": custom_report_name,
        "task_id": base_task_id,
        "result_hash": result_hash,
        "row_count": len(all_raw_rows),
        "impressions_sum": impressions_sum,
        "revenue_sum": round(revenue_sum, 6),
        "date_from": date_from,
        "date_to": date_to,
        "campaign_id": base_campaign_id,
        "campaign_name": base_campaign_name,
    }

    return all_campaigns, all_raw_rows, audit_row


def get_report_window(timezone_name: str) -> tuple[str, str]:
    tz = ZoneInfo(timezone_name)
    now_local = datetime.now(tz)

    today = now_local.date()
    yesterday = today - timedelta(days=1)

    return yesterday.isoformat(), today.isoformat()


def detect_any_updates(
    previous_state: dict[str, Any],
    audit_rows: list[dict[str, Any]],
) -> bool:
    previous_reports = previous_state.get("reports", {})

    for row in audit_rows:
        supercampaign_id = str(int(row["supercampaign_id"]))
        current_task_id = str(row.get("task_id", ""))
        current_hash = str(row.get("result_hash", ""))

        previous_report = previous_reports.get(supercampaign_id)
        if previous_report is None:
            return True

        previous_task_id = str(previous_report.get("task_id", ""))
        previous_hash = str(previous_report.get("result_hash", ""))

        if current_task_id != previous_task_id or current_hash != previous_hash:
            return True

    return False


def save_new_state(
    state_file_path: Path,
    audit_rows: list[dict[str, Any]],
    date_from: str,
    date_to: str,
) -> None:
    payload = {
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "date_from": date_from,
        "date_to": date_to,
        "reports": {},
    }

    for row in audit_rows:
        supercampaign_id = str(int(row["supercampaign_id"]))
        payload["reports"][supercampaign_id] = {
            "supercampaign_name": str(row["supercampaign_name"]),
            "report_name": str(row["report_name"]),
            "task_id": str(row["task_id"]),
            "result_hash": str(row["result_hash"]),
            "row_count": int(row["row_count"]),
            "impressions_sum": int(row["impressions_sum"]),
            "revenue_sum": float(row["revenue_sum"]),
        }

    write_state_file(state_file_path, payload)


def insert_rows_with_retry(
    config: dict[str, Any],
    full_table_name: str,
    rows_to_insert: list[tuple],
    retries: int = 3,
    sleep_sec: int = 15,
) -> None:
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        client = None
        try:
            client = create_clickhouse_client(config)
            print(f"Вставляю новые строки... попытка {attempt}/{retries}")
            insert_rows(client, full_table_name, rows_to_insert)
            print(f"Вставлено строк: {len(rows_to_insert)}")
            return
        except Exception as exc:
            last_error = exc
            print(f"WARNING: ошибка вставки в ClickHouse на попытке {attempt}/{retries}: {exc}")
            if attempt < retries:
                time.sleep(sleep_sec)
        finally:
            if client is not None:
                try:
                    client.disconnect()
                except Exception:
                    pass

    raise RuntimeError(f"Не удалось вставить данные в ClickHouse: {last_error}")


def run_once(config: dict[str, Any]) -> bool:
    """
    Возвращает True, если была выполнена новая успешная загрузка в БД.
    Возвращает False, если новых taskId/hash нет и загрузка не нужна.
    Бросает исключение при сетевой/базовой ошибке.
    """
    date_from, date_to = get_report_window(config["report_timezone"])

    report_output_dir = config["output_dir"] / date_to / "adfox_to_db"
    raw_json_dir = report_output_dir / "raw_json"
    state_file_path = build_state_file_path(config)
    previous_state = read_state_file(state_file_path)

    print(f"Окно выгрузки: {date_from} -> {date_to}")
    print("Будут собраны отчёты:")
    for item in TARGET_REPORTS:
        print(
            f"  supercampaignId={item['supercampaign_id']} | "
            f"report={item['report_name']} | name={item['supercampaign_name']}"
        )
    print("-" * 120)

    all_campaigns: list[dict[str, Any]] = []
    all_raw_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    with requests.Session() as session:
        for item in TARGET_REPORTS:
            campaigns, raw_rows, audit_row = collect_single_custom_report(
                session=session,
                token=config["adfox_token"],
                base_url=config["adfox_base_url"],
                campaigns_report_name=config["adfox_campaigns_report_name"],
                precision=config["adfox_precision"],
                date_from=date_from,
                date_to=date_to,
                save_raw_json=config["save_raw_json"],
                raw_json_dir=raw_json_dir,
                supercampaign_id=int(item["supercampaign_id"]),
                supercampaign_name=str(item["supercampaign_name"]),
                custom_report_name=str(item["report_name"]),
                request_timeout_sec=config["request_timeout_sec"],
                adfox_result_timeout_sec=config["adfox_result_timeout_sec"],
            )
            all_campaigns.extend(campaigns)
            all_raw_rows.extend(raw_rows)
            audit_rows.append(audit_row)

    audit_df = pd.DataFrame(audit_rows)

    print("-" * 120)
    print("Результаты audit:")
    print(audit_df.to_string(index=False))

    has_updates = detect_any_updates(previous_state, audit_rows)

    if not has_updates:
        print("-" * 120)
        print("Новых taskId/result_hash не найдено. Загрузка в ClickHouse не требуется.")
        return False

    if not all_campaigns:
        print("Активные кампании не найдены.")
        return False

    if not all_raw_rows:
        print("Сырые строки не собраны.")
        return False

    active_campaigns_df = pd.DataFrame(all_campaigns)
    raw_rows_df = pd.DataFrame(all_raw_rows)
    final_df = build_final_dataframe(all_raw_rows)
    db_df = map_to_clickhouse_schema(final_df)

    debug_csv_path, debug_xlsx_path = save_debug_outputs(
        base_output_dir=report_output_dir,
        report_date=date_to,
        final_df=final_df,
        active_campaigns_df=active_campaigns_df,
        raw_rows_df=raw_rows_df,
        audit_df=audit_df,
        db_df=db_df,
    )

    print("-" * 120)
    print(f"Всего активных кампаний: {len(active_campaigns_df)}")
    print(f"Всего сырых строк: {len(raw_rows_df)}")
    print(f"Финальных строк: {len(final_df)}")
    print(f"DEBUG CSV: {debug_csv_path}")
    print(f"DEBUG XLSX: {debug_xlsx_path}")

    rows_to_insert = build_insert_rows(db_df)
    date_hour_keys = extract_date_hour_keys(db_df)

    print("-" * 120)
    print("Подготовка к загрузке в ClickHouse:")
    print(f"Строк к вставке: {len(rows_to_insert)}")
    print(f"Часов к замене: {len(date_hour_keys)}")

    check_clickhouse_available(config)

    full_table_name = build_full_table_name(config["ch_database"], config["ch_table_name"])

    client_check_before = create_clickhouse_client(config)
    try:
        exists = clickhouse_table_exists(client_check_before, config["ch_database"], config["ch_table_name"])
        if not exists:
            raise RuntimeError(
                f"Таблица {config['ch_database']}.{config['ch_table_name']} не найдена в ClickHouse"
            )

        existing_before = fetch_existing_row_count_for_date_hours(
            client_check_before,
            full_table_name,
            date_hour_keys,
        )
        print(f"Строк в БД по этим часам до удаления: {existing_before}")
    finally:
        client_check_before.disconnect()

    if existing_before > 0:
        client_delete = create_clickhouse_client(config)
        try:
            print("Удаляю старые строки по этим часам...")
            delete_rows_for_date_hours(client_delete, full_table_name, date_hour_keys)
            wait_for_mutations(
                client_delete,
                config["ch_database"],
                config["ch_table_name"],
                timeout_sec=180,
                poll_sec=2,
                fallback_sleep_sec=config["mutation_fallback_sleep_sec"],
            )
            print("Удаление завершено или выдержана пауза перед вставкой.")
        finally:
            client_delete.disconnect()
    else:
        print("Старых строк по этим часам нет, удаление не требуется.")

    print(f"Дополнительная пауза {config['post_delete_sleep_sec']} сек. перед вставкой...")
    time.sleep(config["post_delete_sleep_sec"])

    insert_rows_with_retry(
        config=config,
        full_table_name=full_table_name,
        rows_to_insert=rows_to_insert,
        retries=3,
        sleep_sec=15,
    )

    print(f"Жду {config['post_insert_sleep_sec']} сек. перед проверкой...")
    time.sleep(config["post_insert_sleep_sec"])

    client_check_after = create_clickhouse_client(config)
    try:
        existing_after = fetch_existing_row_count_for_date_hours(
            client_check_after,
            full_table_name,
            date_hour_keys,
        )
        print(f"Строк в БД по этим часам после вставки: {existing_after}")

        if existing_after != len(rows_to_insert):
            print(
                "WARNING: количество строк после вставки не совпало с числом вставленных строк. "
                "Проверь таблицу и движок."
            )
    finally:
        client_check_after.disconnect()

    save_new_state(
        state_file_path=state_file_path,
        audit_rows=audit_rows,
        date_from=date_from,
        date_to=date_to,
    )

    print("-" * 120)
    print("Итоговые суммы:")
    print(f"Запросы кода: {int(final_df['Запросы кода'].sum())}")
    print(f"Загрузки баннеров: {int(final_df['Загрузки баннеров'].sum())}")
    print(f"Показы: {int(final_df['Показы'].sum())}")
    print(f"Доход: {float(final_df['Доход'].sum()):.2f}")
    print(f"Событие 4: {int(final_df['Событие 4'].sum())}")
    print(f"Событие 5: {int(final_df['Событие 5'].sum())}")
    print(f"Событие 6: {int(final_df['Событие 6'].sum())}")
    print(f"Событие 7: {int(final_df['Событие 7'].sum())}")

    return True


def main() -> None:
    config = load_config()

    while True:
        cycle_started_at = datetime.now()
        print("\n" + "=" * 120)
        print(f"Старт цикла: {cycle_started_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 120)

        next_sleep_minutes = config["base_interval_minutes"]
        status_text = ""

        try:
            inserted = run_once(config)

            if inserted:
                next_sleep_minutes = config["base_interval_minutes"]
                status_text = (
                    f"Новая успешная выгрузка выполнена. "
                    f"Следующий запуск через {next_sleep_minutes} мин."
                )
            else:
                next_sleep_minutes = config["retry_interval_minutes"]
                status_text = (
                    f"Новых taskId/hash пока нет. "
                    f"Повторная попытка через {next_sleep_minutes} мин."
                )

        except Exception as exc:
            next_sleep_minutes = config["retry_interval_minutes"]
            status_text = (
                f"Интернет / AdFox / ClickHouse могут быть недоступны. "
                f"Повторная попытка через {next_sleep_minutes} мин."
            )
            print("-" * 120)
            print(status_text)
            print(f"Ошибка в цикле выгрузки: {exc}")
        else:
            print("-" * 120)
            print(status_text)

        if not config["run_continuous"]:
            break

        cycle_finished_at = datetime.now()
        cycle_duration_sec = int((cycle_finished_at - cycle_started_at).total_seconds())

        sleep_seconds = next_sleep_minutes * 60
        next_run_at = cycle_finished_at + timedelta(seconds=sleep_seconds)

        print("-" * 120)
        print(f"Цикл завершён: {cycle_finished_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(
            f"Длительность цикла: {cycle_duration_sec} сек. "
            f"({cycle_duration_sec / 60:.1f} мин.)"
        )
        print(
            f"Следующий запуск через {next_sleep_minutes} мин. "
            f"в {next_run_at.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        time.sleep(sleep_seconds)

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        sys.exit(1)
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv
from zoneinfo import ZoneInfo


VALID_LEVELS = {"owner", "supercampaign", "campaign", "banner", "site", "section", "place"}

TARGET_REPORTS: list[dict[str, Any]] = [
    {
        "supercampaign_id": 296740,
        "report_name": "custom_13699",
    },
    {
        "supercampaign_id": 296675,
        "report_name": "custom_13697",
    },
    {
        "supercampaign_id": 317352,
        "report_name": "custom_13698",
    },
]


def load_config() -> dict[str, Any]:
    load_dotenv()

    token = os.getenv("ADFOX_TOKEN", "").strip()
    base_url = os.getenv("ADFOX_BASE_URL", "https://adfox.yandex.ru").strip()
    campaigns_report_name = os.getenv("ADFOX_CAMPAIGNS_REPORT_NAME", "campaigns").strip()
    precision = os.getenv("ADFOX_PRECISION", "normal").strip()
    timezone_name = os.getenv("REPORT_TIMEZONE", "Europe/Moscow").strip()
    report_date_raw = os.getenv("REPORT_DATE", "").strip()
    output_dir_raw = os.getenv("OUTPUT_DIR", "output").strip()
    save_raw_json_raw = os.getenv("SAVE_RAW_JSON", "false").strip().lower()

    if not token:
        raise ValueError("В .env не заполнен ADFOX_TOKEN")
    if not base_url:
        raise ValueError("В .env не заполнен ADFOX_BASE_URL")
    if not campaigns_report_name:
        raise ValueError("В .env не заполнен ADFOX_CAMPAIGNS_REPORT_NAME")
    if not precision:
        raise ValueError("В .env не заполнен ADFOX_PRECISION")

    save_raw_json = save_raw_json_raw in {"1", "true", "yes", "y"}

    return {
        "token": token,
        "base_url": base_url,
        "campaigns_report_name": campaigns_report_name,
        "precision": precision,
        "timezone_name": timezone_name,
        "report_date_raw": report_date_raw,
        "output_dir": Path(output_dir_raw),
        "save_raw_json": save_raw_json,
    }


def get_report_date(report_date_raw: str, timezone_name: str) -> str:
    if report_date_raw:
        try:
            datetime.strptime(report_date_raw, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("REPORT_DATE должен быть в формате YYYY-MM-DD") from exc
        return report_date_raw

    tz = ZoneInfo(timezone_name)
    now_local = datetime.now(tz)
    yesterday = now_local.date() - timedelta(days=1)
    return yesterday.isoformat()


def build_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"OAuth {token}",
        "Accept": "application/json",
        "User-Agent": "adfox-combined-single-fetch-custom-reports/1.0",
    }


def safe_get_json(response: requests.Response) -> dict:
    try:
        return response.json()
    except Exception as exc:
        raise RuntimeError(
            f"Сервер вернул не JSON. HTTP {response.status_code}. Тело ответа: {response.text[:1000]}"
        ) from exc


def request_with_retry(
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
            response = requests.request(
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
            payload = safe_get_json(response)
            return payload

        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(sleep_sec * attempt)

    raise RuntimeError(f"Ошибка запроса {url}: {last_error}")


def create_report_task(
    token: str,
    base_url: str,
    level: str,
    params: dict[str, Any],
) -> str:
    if level not in VALID_LEVELS:
        raise ValueError(f"Недопустимый уровень отчёта: {level}")

    url = f"{base_url}/api/report/{level}"
    payload = request_with_retry(
        method="GET",
        url=url,
        headers=build_headers(token),
        params=params,
    )

    if payload.get("error"):
        raise RuntimeError(f"Ошибка создания отчёта: {json.dumps(payload, ensure_ascii=False)}")

    task_id = payload.get("result", {}).get("taskId")
    if not task_id:
        raise RuntimeError(f"В ответе нет taskId: {json.dumps(payload, ensure_ascii=False)}")

    return task_id


def wait_for_report_result(
    token: str,
    base_url: str,
    task_id: str,
    timeout_sec: int = 180,
    poll_sec: int = 2,
) -> dict:
    url = f"{base_url}/api/report/result"
    deadline = time.time() + timeout_sec

    while True:
        payload = request_with_retry(
            method="GET",
            url=url,
            headers=build_headers(token),
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

    if not safe:
        safe = "report"

    return safe


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


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
                "loadsCommercial": row[loads_idx],
                "impressionsCommercial": row[impressions_idx],
                "clicksCommercial": row[clicks_idx],
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
) -> list[dict[str, Any]]:
    result = payload.get("result", {})
    fields = result.get("fields", [])
    table = result.get("table", [])

    if not fields:
        return []

    rows: list[dict[str, Any]] = []
    for raw_row in table:
        row_dict = {}
        for idx, field_name in enumerate(fields):
            row_dict[field_name] = raw_row[idx]

        row_dict["sourceSupercampaignId"] = source_supercampaign_id
        row_dict["sourceCampaignId"] = source_campaign_id
        row_dict["sourceCampaignName"] = source_campaign_name
        row_dict["sourceCustomReportName"] = custom_report_name
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

    final_df["Доход"] = pd.to_numeric(final_df["Доход"], errors="coerce").fillna(0).round(2)

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


def save_final_outputs(
    base_output_dir: Path,
    report_date: str,
    final_df: pd.DataFrame,
    active_campaigns_df: pd.DataFrame,
    raw_rows_df: pd.DataFrame,
) -> tuple[Path, Path]:
    base_output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = base_output_dir / f"adfox_combined_custom_reports_{report_date}.csv"
    xlsx_path = base_output_dir / f"adfox_combined_custom_reports_{report_date}.xlsx"

    final_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="report")
        active_campaigns_df.to_excel(writer, index=False, sheet_name="active_campaigns")
        raw_rows_df.to_excel(writer, index=False, sheet_name="raw_rows")

        for sheet_name in ["report", "active_campaigns", "raw_rows"]:
            worksheet = writer.sheets[sheet_name]
            for column_cells in worksheet.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter
                for cell in column_cells:
                    cell_value = "" if cell.value is None else str(cell.value)
                    max_length = max(max_length, len(cell_value))
                worksheet.column_dimensions[column_letter].width = min(max_length + 2, 45)

    return csv_path, xlsx_path


def collect_single_custom_report(
    token: str,
    base_url: str,
    campaigns_report_name: str,
    precision: str,
    report_date: str,
    save_raw_json: bool,
    raw_json_dir: Path,
    supercampaign_id: int,
    custom_report_name: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    print("-" * 120)
    print(f"Собираю данные:")
    print(f"supercampaignId={supercampaign_id}")
    print(f"custom report={custom_report_name}")

    campaigns_task_id = create_report_task(
        token=token,
        base_url=base_url,
        level="supercampaign",
        params={
            "name": campaigns_report_name,
            "supercampaignId": supercampaign_id,
            "dateFrom": report_date,
            "dateTo": report_date,
            "precision": precision,
        },
    )

    print(f"  taskId списка кампаний: {campaigns_task_id}")

    campaigns_payload = wait_for_report_result(
        token=token,
        base_url=base_url,
        task_id=campaigns_task_id,
    )

    if save_raw_json:
        save_json(raw_json_dir / f"campaigns_report_supercampaign_{supercampaign_id}.json", campaigns_payload)

    campaigns = extract_active_campaigns(campaigns_payload, source_supercampaign_id=supercampaign_id)

    print(f"  найдено активных кампаний: {len(campaigns)}")

    all_campaigns: list[dict[str, Any]] = campaigns.copy()
    all_raw_rows: list[dict[str, Any]] = []

    if not campaigns:
        return all_campaigns, all_raw_rows

    base_campaign = campaigns[0]
    base_campaign_id = int(base_campaign["campaignId"])
    base_campaign_name = str(base_campaign["campaignName"]).strip()

    print(
        f"  Использую один отчёт по первой активной кампании: "
        f"campaignId={base_campaign_id} | campaignName={base_campaign_name}"
    )

    base_task_id = create_report_task(
        token=token,
        base_url=base_url,
        level="campaign",
        params={
            "name": custom_report_name,
            "campaignId": base_campaign_id,
            "dateFrom": report_date,
            "dateTo": report_date,
            "precision": precision,
        },
    )

    print(f"  taskId custom report: {base_task_id}")

    base_payload = wait_for_report_result(
        token=token,
        base_url=base_url,
        task_id=base_task_id,
    )

    if save_raw_json:
        safe_name = make_safe_filename(base_campaign_name)
        save_json(
            raw_json_dir / f"{supercampaign_id}_{base_campaign_id}_{safe_name}.json",
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
    )

    print(f"  строк в single-fetch отчёте: {len(all_raw_rows)}")

    return all_campaigns, all_raw_rows


def main() -> None:
    config = load_config()

    token = config["token"]
    base_url = config["base_url"]
    campaigns_report_name = config["campaigns_report_name"]
    precision = config["precision"]
    timezone_name = config["timezone_name"]
    report_date = get_report_date(config["report_date_raw"], timezone_name)
    output_dir: Path = config["output_dir"]
    save_raw_json = config["save_raw_json"]

    report_output_dir = output_dir / report_date
    raw_json_dir = report_output_dir / "raw_json"

    print(f"Дата отчёта: {report_date}")
    print("Будут собраны отчёты:")
    for item in TARGET_REPORTS:
        print(
            f"  supercampaignId={item['supercampaign_id']} | "
            f"report={item['report_name']}"
        )
    print("-" * 120)

    all_campaigns: list[dict[str, Any]] = []
    all_raw_rows: list[dict[str, Any]] = []

    for item in TARGET_REPORTS:
        campaigns, raw_rows = collect_single_custom_report(
            token=token,
            base_url=base_url,
            campaigns_report_name=campaigns_report_name,
            precision=precision,
            report_date=report_date,
            save_raw_json=save_raw_json,
            raw_json_dir=raw_json_dir,
            supercampaign_id=int(item["supercampaign_id"]),
            custom_report_name=str(item["report_name"]),
        )

        all_campaigns.extend(campaigns)
        all_raw_rows.extend(raw_rows)

    if not all_campaigns:
        print("Активные кампании не найдены.")
        return

    if not all_raw_rows:
        print("Сырые строки не собраны.")
        return

    active_campaigns_df = pd.DataFrame(all_campaigns)
    raw_rows_df = pd.DataFrame(all_raw_rows)
    final_df = build_final_dataframe(all_raw_rows)

    csv_path, xlsx_path = save_final_outputs(
        base_output_dir=report_output_dir,
        report_date=report_date,
        final_df=final_df,
        active_campaigns_df=active_campaigns_df,
        raw_rows_df=raw_rows_df,
    )

    print("-" * 120)
    print(f"Всего активных кампаний: {len(active_campaigns_df)}")
    print(f"Всего сырых строк: {len(raw_rows_df)}")
    print(f"Финальных строк: {len(final_df)}")
    print(f"CSV: {csv_path}")
    print(f"Excel: {xlsx_path}")
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


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        sys.exit(1)
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


def load_config() -> dict[str, Any]:
    load_dotenv()

    token = os.getenv("ADFOX_TOKEN", "").strip()
    base_url = os.getenv("ADFOX_BASE_URL", "https://adfox.yandex.ru").strip()
    supercampaign_id_raw = os.getenv("ADFOX_SUPERCAMPAIGN_ID", "").strip()
    campaigns_report_name = os.getenv("ADFOX_CAMPAIGNS_REPORT_NAME", "campaigns").strip()
    custom_report_name = os.getenv("ADFOX_CUSTOM_REPORT_NAME", "custom_13697").strip()
    precision = os.getenv("ADFOX_PRECISION", "normal").strip()
    timezone_name = os.getenv("REPORT_TIMEZONE", "Europe/Moscow").strip()
    report_date_raw = os.getenv("REPORT_DATE", "").strip()
    output_dir_raw = os.getenv("OUTPUT_DIR", "output").strip()
    save_raw_json_raw = os.getenv("SAVE_RAW_JSON", "false").strip().lower()

    if not token:
        raise ValueError("В .env не заполнен ADFOX_TOKEN")
    if not base_url:
        raise ValueError("В .env не заполнен ADFOX_BASE_URL")
    if not supercampaign_id_raw:
        raise ValueError("В .env не заполнен ADFOX_SUPERCAMPAIGN_ID")
    if not campaigns_report_name:
        raise ValueError("В .env не заполнен ADFOX_CAMPAIGNS_REPORT_NAME")
    if not custom_report_name:
        raise ValueError("В .env не заполнен ADFOX_CUSTOM_REPORT_NAME")
    if not precision:
        raise ValueError("В .env не заполнен ADFOX_PRECISION")

    try:
        supercampaign_id = int(supercampaign_id_raw)
    except ValueError as exc:
        raise ValueError("ADFOX_SUPERCAMPAIGN_ID должен быть числом") from exc

    save_raw_json = save_raw_json_raw in {"1", "true", "yes", "y"}

    return {
        "token": token,
        "base_url": base_url,
        "supercampaign_id": supercampaign_id,
        "campaigns_report_name": campaigns_report_name,
        "custom_report_name": custom_report_name,
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
        "User-Agent": "adfox-full-pipeline/1.0",
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
                    raise RuntimeError("Превышен лимит запросов Adfox (429)")
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


def extract_active_campaigns(payload: dict) -> list[dict[str, Any]]:
    result = payload.get("result", {})
    fields = result.get("fields", [])
    table = result.get("table", [])

    if not fields:
        raise RuntimeError("В отчёте campaigns нет fields")

    if not table:
        return []

    try:
        supercampaign_idx = fields.index("supercampaignId")
        campaign_id_idx = fields.index("campaignId")
        campaign_name_idx = fields.index("campaignName")
        loads_idx = fields.index("loadsCommercial")
        impressions_idx = fields.index("impressionsCommercial")
        clicks_idx = fields.index("clicksCommercial")
    except ValueError as exc:
        raise RuntimeError(f"В отчёте campaigns не найдены нужные поля. fields={fields}") from exc

    campaigns: list[dict[str, Any]] = []
    for row in table:
        campaigns.append(
            {
                "supercampaignId": row[supercampaign_idx],
                "campaignId": int(row[campaign_id_idx]),
                "campaignName": str(row[campaign_name_idx]).strip(),
                "loadsCommercial": row[loads_idx],
                "impressionsCommercial": row[impressions_idx],
                "clicksCommercial": row[clicks_idx],
            }
        )

    campaigns.sort(key=lambda x: int(x["campaignId"]))
    return campaigns


def payload_to_rows(payload: dict, source_campaign_id: int, source_campaign_name: str) -> list[dict[str, Any]]:
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

        row_dict["sourceCampaignId"] = source_campaign_id
        row_dict["sourceCampaignName"] = source_campaign_name
        rows.append(row_dict)

    return rows


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


def ensure_required_columns(df: pd.DataFrame) -> None:
    required = [
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
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise RuntimeError(f"В сырых данных не хватает колонок: {missing}")


def normalize_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    text_columns = [
        "date",
        "campaignName",
        "siteName",
        "campaignCpcDict",
        "sectionName",
        "placeName",
    ]

    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    numeric_columns = [
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
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def build_final_dataframe(raw_rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not raw_rows:
        raise RuntimeError("Нет сырых строк для построения итогового отчёта")

    df = pd.DataFrame(raw_rows)
    ensure_required_columns(df)
    df = normalize_raw_dataframe(df)

    group_columns = [
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
        "event4Count",
        "event5Count",
        "event6Count",
        "event7Count",
    ]

    final_df = (
        df.groupby(group_columns, dropna=False, as_index=False)[value_columns]
        .sum()
        .sort_values(
            by=[
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
            "date": "День",
            "hour": "Час",
            "cpmInstantDirectDict1000": "CPM",
            "campaignName": "Название кампании",
            "siteName": "Название сайта",
            "campaignCpcDict": "CPC",
            "sectionName": "Название раздела",
            "placeName": "Название площадки",
            "loadsTotal": "Запросыкода",
            "loadsCommercial": "Загрузкибаннеров",
            "impressionsCommercial": "Показы",
            "calculatedRevenueByEvent": "Доход",
            "event4Count": "Событие4",
            "event5Count": "Событие5",
            "event6Count": "Событие6",
            "event7Count": "Событие7",
        }
    )

    int_columns = [
        "Час",
        "CPM",
        "Запросыкода",
        "Загрузкибаннеров",
        "Показы",
        "Событие4",
        "Событие5",
        "Событие6",
        "Событие7",
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
        "Запросыкода",
        "Загрузкибаннеров",
        "Показы",
        "Доход",
        "Событие4",
        "Событие5",
        "Событие6",
        "Событие7",
    ]

    final_df = final_df[ordered_columns]
    return final_df


def save_final_outputs(base_output_dir: Path, report_date: str, final_df: pd.DataFrame) -> tuple[Path, Path]:
    base_output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = base_output_dir / f"adfox_final_report_{report_date}.csv"
    xlsx_path = base_output_dir / f"adfox_final_report_{report_date}.xlsx"

    final_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="report")
        worksheet = writer.sheets["report"]

        for column_cells in worksheet.columns:
            max_length = 0
            column_letter = column_cells[0].column_letter
            for cell in column_cells:
                cell_value = "" if cell.value is None else str(cell.value)
                max_length = max(max_length, len(cell_value))
            worksheet.column_dimensions[column_letter].width = min(max_length + 2, 45)

    return csv_path, xlsx_path


def main() -> None:
    config = load_config()

    token = config["token"]
    base_url = config["base_url"]
    supercampaign_id = config["supercampaign_id"]
    campaigns_report_name = config["campaigns_report_name"]
    custom_report_name = config["custom_report_name"]
    precision = config["precision"]
    timezone_name = config["timezone_name"]
    report_date = get_report_date(config["report_date_raw"], timezone_name)
    output_dir: Path = config["output_dir"]
    save_raw_json = config["save_raw_json"]

    report_output_dir = output_dir / report_date
    raw_json_dir = report_output_dir / "raw_json"

    print(f"Дата отчёта: {report_date}")
    print(f"supercampaignId: {supercampaign_id}")
    print("-" * 100)

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

    print(f"taskId списка кампаний: {campaigns_task_id}")

    campaigns_payload = wait_for_report_result(
        token=token,
        base_url=base_url,
        task_id=campaigns_task_id,
    )

    if save_raw_json:
        save_json(raw_json_dir / "campaigns_report.json", campaigns_payload)

    campaigns = extract_active_campaigns(campaigns_payload)

    if not campaigns:
        print("Активные кампании за выбранную дату не найдены.")
        return

    print(f"Найдено активных кампаний: {len(campaigns)}")
    print("-" * 100)

    all_raw_rows: list[dict[str, Any]] = []

    for idx, campaign in enumerate(campaigns, start=1):
        campaign_id = int(campaign["campaignId"])
        campaign_name = str(campaign["campaignName"]).strip()

        print(f"[{idx}/{len(campaigns)}] campaignId={campaign_id} | campaignName={campaign_name}")

        campaign_task_id = create_report_task(
            token=token,
            base_url=base_url,
            level="campaign",
            params={
                "name": custom_report_name,
                "campaignId": campaign_id,
                "dateFrom": report_date,
                "dateTo": report_date,
                "precision": precision,
            },
        )

        print(f"  taskId={campaign_task_id}")

        campaign_payload = wait_for_report_result(
            token=token,
            base_url=base_url,
            task_id=campaign_task_id,
        )

        if save_raw_json:
            safe_name = make_safe_filename(campaign_name)
            save_json(raw_json_dir / f"{campaign_id}_{safe_name}.json", campaign_payload)

        campaign_rows = payload_to_rows(
            payload=campaign_payload,
            source_campaign_id=campaign_id,
            source_campaign_name=campaign_name,
        )

        print(f"  строк: {len(campaign_rows)}")
        all_raw_rows.extend(campaign_rows)

        time.sleep(0.5)

    print("-" * 100)
    print(f"Всего сырых строк: {len(all_raw_rows)}")

    final_df = build_final_dataframe(all_raw_rows)

    csv_path, xlsx_path = save_final_outputs(
        base_output_dir=report_output_dir,
        report_date=report_date,
        final_df=final_df,
    )

    print(f"Финальных строк: {len(final_df)}")
    print("-" * 100)
    print(f"CSV: {csv_path}")
    print(f"Excel: {xlsx_path}")

    total_requests = int(final_df["Запросыкода"].sum())
    total_loads = int(final_df["Загрузкибаннеров"].sum())
    total_impressions = int(final_df["Показы"].sum())
    total_revenue = float(final_df["Доход"].sum())

    print("-" * 100)
    print("Итоговые суммы:")
    print(f"Запросыкода: {total_requests}")
    print(f"Загрузкибаннеров: {total_loads}")
    print(f"Показы: {total_impressions}")
    print(f"Доход: {total_revenue:.2f}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        sys.exit(1)
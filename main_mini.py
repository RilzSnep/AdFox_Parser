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


def load_config() -> dict[str, Any]:
    load_dotenv()

    token = os.getenv("ADFOX_TOKEN", "").strip()
    base_url = os.getenv("ADFOX_BASE_URL", "https://adfox.yandex.ru").strip()
    supercampaign_id_raw = os.getenv("ADFOX_SUPERCAMPAIGN_ID", "").strip()
    campaign_name = os.getenv("TV_STREAM_CAMPAIGN_NAME", "TV-STREAM").strip()
    report_date_raw = os.getenv("REPORT_DATE", "").strip()
    output_dir_raw = os.getenv("OUTPUT_DIR", "output").strip()
    timezone_name = os.getenv("REPORT_TIMEZONE", "Europe/Moscow").strip()
    precision = os.getenv("ADFOX_PRECISION", "normal").strip()

    if not token:
        raise ValueError("В .env не заполнен ADFOX_TOKEN")
    if not base_url:
        raise ValueError("В .env не заполнен ADFOX_BASE_URL")
    if not supercampaign_id_raw:
        raise ValueError("В .env не заполнен ADFOX_SUPERCAMPAIGN_ID")
    if not campaign_name:
        raise ValueError("В .env не заполнен TV_STREAM_CAMPAIGN_NAME")

    try:
        supercampaign_id = int(supercampaign_id_raw)
    except ValueError as exc:
        raise ValueError("ADFOX_SUPERCAMPAIGN_ID должен быть числом") from exc

    return {
        "token": token,
        "base_url": base_url,
        "supercampaign_id": supercampaign_id,
        "campaign_name": campaign_name,
        "report_date_raw": report_date_raw,
        "output_dir": Path(output_dir_raw),
        "timezone_name": timezone_name,
        "precision": precision,
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
        "User-Agent": "tv-stream-impressions-by-hour/1.1",
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
            return safe_get_json(response)

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


def extract_campaigns(payload: dict) -> list[dict[str, Any]]:
    result = payload.get("result", {})
    fields = result.get("fields", [])
    table = result.get("table", [])

    if not fields:
        raise RuntimeError("В отчёте campaigns нет fields")

    if not table:
        return []

    campaign_id_idx = fields.index("campaignId")
    campaign_name_idx = fields.index("campaignName")
    impressions_idx = fields.index("impressionsCommercial")

    campaigns: list[dict[str, Any]] = []
    for row in table:
        campaigns.append(
            {
                "campaignId": int(row[campaign_id_idx]),
                "campaignName": str(row[campaign_name_idx]).strip(),
                "impressionsCommercial": row[impressions_idx],
            }
        )

    return campaigns


def find_tv_stream_campaign_id(campaigns: list[dict[str, Any]], expected_name: str) -> int:
    normalized_expected = expected_name.strip().lower()

    exact_matches = [
        campaign for campaign in campaigns
        if campaign["campaignName"].strip().lower() == normalized_expected
    ]

    if exact_matches:
        return int(exact_matches[0]["campaignId"])

    contains_matches = [
        campaign for campaign in campaigns
        if normalized_expected in campaign["campaignName"].strip().lower()
    ]

    if contains_matches:
        return int(contains_matches[0]["campaignId"])

    available = sorted({campaign["campaignName"] for campaign in campaigns})
    raise RuntimeError(
        "Кампания TV-STREAM не найдена среди активных кампаний. "
        f"Доступные кампании: {available}"
    )


def build_hourly_dataframe(payload: dict, fallback_campaign_name: str, report_date: str) -> pd.DataFrame:
    result = payload.get("result", {})
    fields = result.get("fields", [])
    table = result.get("table", [])

    if not fields:
        raise RuntimeError("В почасовом отчёте нет fields")

    rows: list[dict[str, Any]] = []
    for raw_row in table:
        row_dict = {fields[idx]: raw_row[idx] for idx in range(len(fields))}
        rows.append(row_dict)

    df = pd.DataFrame(rows)

    if "hour" not in df.columns:
        df["hour"] = 0
    if "campaignName" not in df.columns:
        df["campaignName"] = fallback_campaign_name
    if "impressionsCommercial" not in df.columns:
        df["impressionsCommercial"] = 0

    df["date"] = report_date
    df["hour"] = pd.to_numeric(df["hour"], errors="coerce").fillna(0).astype(int)
    df["campaignName"] = fallback_campaign_name
    df["impressionsCommercial"] = pd.to_numeric(df["impressionsCommercial"], errors="coerce").fillna(0).astype(int)

    final_df = df[["date", "hour", "campaignName", "impressionsCommercial"]].copy()
    final_df = final_df.rename(
        columns={
            "date": "День",
            "hour": "Час",
            "campaignName": "Название кампании",
            "impressionsCommercial": "Показы",
        }
    )

    final_df = (
        final_df.groupby(["День", "Час", "Название кампании"], as_index=False)["Показы"]
        .sum()
        .sort_values(by=["День", "Час", "Название кампании"])
        .reset_index(drop=True)
    )

    return final_df


def save_outputs(output_dir: Path, report_date: str, df: pd.DataFrame) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / f"tv_stream_impressions_by_hour_{report_date}.csv"
    xlsx_path = output_dir / f"tv_stream_impressions_by_hour_{report_date}.xlsx"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="tv_stream")

        worksheet = writer.sheets["tv_stream"]
        for column_cells in worksheet.columns:
            max_length = 0
            column_letter = column_cells[0].column_letter
            for cell in column_cells:
                cell_value = "" if cell.value is None else str(cell.value)
                max_length = max(max_length, len(cell_value))
            worksheet.column_dimensions[column_letter].width = min(max_length + 2, 30)

    return csv_path, xlsx_path


def main() -> None:
    config = load_config()

    token = config["token"]
    base_url = config["base_url"]
    supercampaign_id = config["supercampaign_id"]
    campaign_name = config["campaign_name"]
    report_date = get_report_date(config["report_date_raw"], config["timezone_name"])
    output_dir = config["output_dir"] / "tv_stream_only" / report_date
    precision = config["precision"]

    print(f"Дата отчёта: {report_date}")
    print(f"supercampaignId: {supercampaign_id}")
    print(f"Ищу кампанию: {campaign_name}")
    print("-" * 100)

    campaigns_task_id = create_report_task(
        token=token,
        base_url=base_url,
        level="supercampaign",
        params={
            "name": "campaigns",
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

    campaigns = extract_campaigns(campaigns_payload)

    print("Активные кампании:")
    for campaign in campaigns:
        print(
            f"campaignId={campaign['campaignId']} | "
            f"campaignName={campaign['campaignName']} | "
            f"impressions={campaign['impressionsCommercial']}"
        )

    tv_stream_campaign_id = find_tv_stream_campaign_id(campaigns, campaign_name)
    print("-" * 100)
    print(f"Найден campaignId для TV-STREAM: {tv_stream_campaign_id}")

    hourly_task_id = create_report_task(
        token=token,
        base_url=base_url,
        level="campaign",
        params={
            "name": "daysHours",
            "campaignId": tv_stream_campaign_id,
            "dateFrom": report_date,
            "dateTo": report_date,
            "precision": precision,
        },
    )

    print(f"taskId почасового отчёта: {hourly_task_id}")

    hourly_payload = wait_for_report_result(
        token=token,
        base_url=base_url,
        task_id=hourly_task_id,
    )

    final_df = build_hourly_dataframe(
        hourly_payload,
        fallback_campaign_name=campaign_name,
        report_date=report_date,
    )

    csv_path, xlsx_path = save_outputs(output_dir, report_date, final_df)

    print("-" * 100)
    print(f"Строк в итоговой таблице: {len(final_df)}")
    print(f"Сумма показов: {int(final_df['Показы'].sum()) if not final_df.empty else 0}")
    print(f"CSV: {csv_path}")
    print(f"XLSX: {xlsx_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        sys.exit(1)
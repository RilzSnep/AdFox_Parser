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
    supercampaign_ids_raw = os.getenv("ADFOX_SUPERCAMPAIGN_IDS", "").strip()
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
    if not supercampaign_ids_raw:
        raise ValueError("В .env не заполнен ADFOX_SUPERCAMPAIGN_IDS")
    if not campaigns_report_name:
        raise ValueError("В .env не заполнен ADFOX_CAMPAIGNS_REPORT_NAME")
    if not precision:
        raise ValueError("В .env не заполнен ADFOX_PRECISION")

    supercampaign_ids: list[int] = []
    for part in supercampaign_ids_raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            supercampaign_ids.append(int(part))
        except ValueError as exc:
            raise ValueError(f"Некорректный ID в ADFOX_SUPERCAMPAIGN_IDS: {part}") from exc

    if not supercampaign_ids:
        raise ValueError("В ADFOX_SUPERCAMPAIGN_IDS не найдено ни одного ID")

    report_map: dict[int, str] = {}
    for sc_id in supercampaign_ids:
        env_key = f"ADFOX_REPORT_FOR_{sc_id}"
        report_name = os.getenv(env_key, "").strip()
        if not report_name:
            raise ValueError(f"В .env не заполнен {env_key}")
        report_map[sc_id] = report_name

    save_raw_json = save_raw_json_raw in {"1", "true", "yes", "y"}

    return {
        "token": token,
        "base_url": base_url,
        "supercampaign_ids": supercampaign_ids,
        "campaigns_report_name": campaigns_report_name,
        "report_map": report_map,
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
        "User-Agent": "adfox-multi-supercampaign-pipeline/1.1",
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


def extract_active_campaigns(payload: dict, source_supercampaign_id: int) -> list[dict[str, Any]]:
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

    special_supercampaign_id = 317352

    df["mappedEvent4"] = 0
    df["mappedEvent5"] = 0
    df["mappedEvent6"] = 0
    df["mappedEvent7"] = 0

    regular_mask = df["sourceSupercampaignId"] != special_supercampaign_id
    special_mask = df["sourceSupercampaignId"] == special_supercampaign_id

    df.loc[regular_mask, "mappedEvent4"] = df.loc[regular_mask, "event4Count"]
    df.loc[regular_mask, "mappedEvent5"] = df.loc[regular_mask, "event5Count"]
    df.loc[regular_mask, "mappedEvent6"] = df.loc[regular_mask, "event6Count"]
    df.loc[regular_mask, "mappedEvent7"] = df.loc[regular_mask, "event7Count"]

    df.loc[special_mask, "mappedEvent4"] = df.loc[special_mask, "event6Count"]
    df.loc[special_mask, "mappedEvent5"] = df.loc[special_mask, "event7Count"]
    df.loc[special_mask, "mappedEvent6"] = df.loc[special_mask, "event8Count"]
    df.loc[special_mask, "mappedEvent7"] = df.loc[special_mask, "event9Count"]

    return df


def build_final_dataframe(raw_rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not raw_rows:
        raise RuntimeError("Нет сырых строк для построения итогового отчёта")

    df = pd.DataFrame(raw_rows)
    ensure_required_columns(df)
    df = normalize_raw_dataframe(df)
    df = apply_event_mapping(df)

    # Для суперкампании 296740 saved report возвращает чужие campaignName,
    # поэтому берём исходное имя кампании, по которой реально строили отчёт.
    if "sourceSupercampaignId" not in df.columns:
        df["sourceSupercampaignId"] = 0
    if "sourceCampaignName" not in df.columns:
        df["sourceCampaignName"] = ""

    df["reportCampaignName"] = df["campaignName"]

    special_campaign_name_override_ids = {296740}

    override_mask = df["sourceSupercampaignId"].isin(special_campaign_name_override_ids)
    df.loc[override_mask, "reportCampaignName"] = df.loc[override_mask, "sourceCampaignName"]

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
            "loadsTotal": "Запросыкода",
            "loadsCommercial": "Загрузкибаннеров",
            "impressionsCommercial": "Показы",
            "calculatedRevenueByEvent": "Доход",
            "mappedEvent4": "Событие4",
            "mappedEvent5": "Событие5",
            "mappedEvent6": "Событие6",
            "mappedEvent7": "Событие7",
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

def save_final_outputs(
    base_output_dir: Path,
    report_date: str,
    final_df: pd.DataFrame,
    active_campaigns_df: pd.DataFrame,
    raw_rows_df: pd.DataFrame,
) -> tuple[Path, Path]:
    base_output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = base_output_dir / f"adfox_final_report_{report_date}.csv"
    xlsx_path = base_output_dir / f"adfox_final_report_{report_date}.xlsx"

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


def main() -> None:
    config = load_config()

    token = config["token"]
    base_url = config["base_url"]
    supercampaign_ids = config["supercampaign_ids"]
    campaigns_report_name = config["campaigns_report_name"]
    report_map = config["report_map"]
    precision = config["precision"]
    timezone_name = config["timezone_name"]
    report_date = get_report_date(config["report_date_raw"], timezone_name)
    output_dir: Path = config["output_dir"]
    save_raw_json = config["save_raw_json"]

    report_output_dir = output_dir / report_date
    raw_json_dir = report_output_dir / "raw_json"

    print(f"Дата отчёта: {report_date}")
    print(f"Суперкампании: {supercampaign_ids}")
    print(f"Маппинг отчётов: {report_map}")
    print("-" * 120)

    all_campaigns: list[dict[str, Any]] = []
    all_raw_rows: list[dict[str, Any]] = []

    for supercampaign_id in supercampaign_ids:
        custom_report_name = report_map[supercampaign_id]

        print(f"Собираю список кампаний для supercampaignId={supercampaign_id}")

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
        print(f"  custom report: {custom_report_name}")

        all_campaigns.extend(campaigns)

        for idx, campaign in enumerate(campaigns, start=1):
            campaign_id = int(campaign["campaignId"])
            campaign_name = str(campaign["campaignName"]).strip()

            print(
                f"    [{idx}/{len(campaigns)}] "
                f"supercampaignId={supercampaign_id} | campaignId={campaign_id} | campaignName={campaign_name}"
            )

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

            print(f"      taskId={campaign_task_id}")

            campaign_payload = wait_for_report_result(
                token=token,
                base_url=base_url,
                task_id=campaign_task_id,
            )

            if save_raw_json:
                safe_name = make_safe_filename(campaign_name)
                save_json(
                    raw_json_dir / f"{supercampaign_id}_{campaign_id}_{safe_name}.json",
                    campaign_payload,
                )

            campaign_rows = payload_to_rows(
                payload=campaign_payload,
                source_supercampaign_id=supercampaign_id,
                source_campaign_id=campaign_id,
                source_campaign_name=campaign_name,
            )

            print(f"      строк: {len(campaign_rows)}")
            all_raw_rows.extend(campaign_rows)

            time.sleep(0.5)

        print("-" * 120)

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

    print(f"Всего активных кампаний: {len(active_campaigns_df)}")
    print(f"Всего сырых строк: {len(raw_rows_df)}")
    print(f"Финальных строк: {len(final_df)}")
    print("-" * 120)
    print(f"CSV: {csv_path}")
    print(f"Excel: {xlsx_path}")

    total_requests = int(final_df["Запросыкода"].sum())
    total_loads = int(final_df["Загрузкибаннеров"].sum())
    total_impressions = int(final_df["Показы"].sum())
    total_revenue = float(final_df["Доход"].sum())
    total_event4 = int(final_df["Событие4"].sum())
    total_event5 = int(final_df["Событие5"].sum())
    total_event6 = int(final_df["Событие6"].sum())
    total_event7 = int(final_df["Событие7"].sum())

    print("-" * 120)
    print("Итоговые суммы:")
    print(f"Запросыкода: {total_requests}")
    print(f"Загрузкибаннеров: {total_loads}")
    print(f"Показы: {total_impressions}")
    print(f"Доход: {total_revenue:.2f}")
    print(f"Событие4: {total_event4}")
    print(f"Событие5: {total_event5}")
    print(f"Событие6: {total_event6}")
    print(f"Событие7: {total_event7}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        sys.exit(1)
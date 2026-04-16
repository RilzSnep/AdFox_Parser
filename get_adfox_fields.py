from __future__ import annotations

import json
import os
import time
from typing import Any

import requests
from dotenv import load_dotenv


# =========================
# CONFIG
# =========================
CAMPAIGN_ID = 3525428  # ВСТАВЬ ЛЮБОЙ ИЗ ЛОГОВ
CUSTOM_REPORT_NAME = "custom_13699"

DATE_FROM = "2026-04-15"
DATE_TO = "2026-04-16"


# =========================
# UTILS
# =========================
def load_config() -> dict[str, str]:
    load_dotenv()

    token = os.getenv("ADFOX_TOKEN", "").strip()
    base_url = os.getenv("ADFOX_BASE_URL", "https://adfox.yandex.ru").strip()

    if not token:
        raise ValueError("Не указан ADFOX_TOKEN")

    return {
        "token": token,
        "base_url": base_url,
    }


def build_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"OAuth {token}",
        "Accept": "application/json",
        "User-Agent": "adfox-fields-debugger/1.0",
    }


def request_with_retry(
    method: str,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any],
    retries: int = 3,
) -> dict:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=60,
            )

            if resp.status_code == 429:
                time.sleep(2 * attempt)
                continue

            resp.raise_for_status()
            return resp.json()

        except Exception:
            if attempt == retries:
                raise
            time.sleep(2 * attempt)

    raise RuntimeError("Ошибка запроса")


# =========================
# API
# =========================
def create_report_task(token: str, base_url: str) -> str:
    url = f"{base_url}/api/report/campaign"

    params = {
        "name": CUSTOM_REPORT_NAME,
        "campaignId": CAMPAIGN_ID,
        "dateFrom": DATE_FROM,
        "dateTo": DATE_TO,
        "precision": "normal",
    }

    payload = request_with_retry(
        "GET",
        url,
        build_headers(token),
        params,
    )

    if payload.get("error"):
        raise RuntimeError(json.dumps(payload, ensure_ascii=False))

    return payload["result"]["taskId"]


def wait_report(token: str, base_url: str, task_id: str) -> dict:
    url = f"{base_url}/api/report/result"

    while True:
        payload = request_with_retry(
            "GET",
            url,
            build_headers(token),
            {"taskId": task_id},
        )

        state = payload.get("result", {}).get("state")

        if state == "SUCCESS":
            return payload

        if state in ("PENDING", "STARTED"):
            time.sleep(2)
            continue

        raise RuntimeError(f"Ошибка статуса: {payload}")


# =========================
# MAIN
# =========================
def main():
    cfg = load_config()

    print(f"Используем campaignId: {CAMPAIGN_ID}")

    print("Создаю отчёт...")
    task_id = create_report_task(cfg["token"], cfg["base_url"])
    print(f"taskId: {task_id}")

    print("Жду результат...")
    result = wait_report(cfg["token"], cfg["base_url"], task_id)

    fields = result.get("result", {}).get("fields", [])

    print("\n" + "=" * 80)
    print("СПИСОК ВСЕХ ПОЛЕЙ / МЕТРИК:")
    print("=" * 80)

    for f in fields:
        print(f)

    print("\nВсего полей:", len(fields))


if __name__ == "__main__":
    main()
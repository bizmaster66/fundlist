# -*- coding: utf-8 -*-

import time
from typing import Any, Dict, List

import requests

from .schema import VCS_COLUMNS
from .utils import clean_spaces, normalize_date

BASE_URL = "https://dipa.kban.or.kr/pblntf/pblntfList"

MAX_RETRIES = 3
SLEEP_BETWEEN_RETRIES = 1.2


def _request_json_with_retry(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_BETWEEN_RETRIES)
    raise last  # type: ignore[misc]


def _base_row() -> Dict[str, Any]:
    return {c: "" for c in VCS_COLUMNS}


def _map_dipa_to_vcs(item: Dict[str, Any]) -> Dict[str, Any]:
    row = _base_row()
    row["operInstId"] = item.get("operInstId", "")
    row["operInstNm"] = clean_spaces(item.get("operInstNm", ""))
    # dataCreatDdtm is YYYYMM
    reg = normalize_date(str(item.get("dataCreatDdtm", "")))
    row["regDd"] = reg
    # asctTotAmt is amount with commas
    row["formTotamt"] = item.get("asctTotAmt", "")
    row["comIndNm"] = item.get("operInstTpNm", "")
    # 펀드 관련 항목은 DIPA에 없으므로 비움
    return row


def fetch_dipa_year(year: int) -> List[Dict[str, Any]]:
    # DIPA uses dataCreatYM=YYYY-12
    ym = f"{year}-12"
    page_no = 1
    all_items: List[Dict[str, Any]] = []

    while True:
        params = {
            "pageNo": page_no,
            "operInstNm": "",
            "dataCreatYM": ym,
            "pblntfType": "fdrm",
        }
        data = _request_json_with_retry(params)
        if not data:
            break
        for item in data:
            all_items.append(_map_dipa_to_vcs(item))
        page_no += 1

    return all_items


def fetch_dipa_range(year_from: int, year_to: int) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    for y in range(year_from, year_to + 1):
        all_rows.extend(fetch_dipa_year(y))
    return all_rows

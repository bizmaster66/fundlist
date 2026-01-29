# -*- coding: utf-8 -*-

import math
import time
from typing import Any, Dict, List, Tuple

import requests

from .schema import VCS_COLUMNS
from .utils import clean_spaces, normalize_date, year_from_reg_dd

BASE_URL = "https://www.vcs.go.kr/web/portal/rsh/search"

MAX_RETRIES = 3
SLEEP_BETWEEN_RETRIES = 1.2


def _request_json_with_retry(params: Dict[str, Any]) -> Dict[str, Any]:
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


def _pick_invest_and_name(name_cell: str) -> Tuple[str, str, str]:
    if not name_cell:
        return "", "", ""
    parts = [p.strip() for p in str(name_cell).split("\n") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1], " / ".join(parts)
    if len(parts) == 1:
        return "", parts[0], parts[0]
    return "", "", ""


def _fill_missing_columns(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {c: row.get(c, "") for c in VCS_COLUMNS}
    return out


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    if "fundNm" in row:
        inv, fn, raw = _pick_invest_and_name(row.get("fundNm", ""))
        row["투자분야"] = inv
        row["펀드명_분리"] = fn
        row["펀드명_원문"] = raw
    row["regDd"] = normalize_date(row.get("regDd", ""))
    row["operInstNm"] = clean_spaces(row.get("operInstNm", ""))
    return row


def fetch_vcs_all(year_from: int, year_to: int) -> List[Dict[str, Any]]:
    # first page to get total and page size
    first = _request_json_with_retry({"cp": 1})
    total = first.get("total", 0)
    page_size = first.get("rshSearch", {}).get("pageSize", 10) or 10
    total_pages = max(1, math.ceil(total / page_size))

    all_items: List[Dict[str, Any]] = []

    def add_items(items: List[Dict[str, Any]]):
        for item in items:
            item = _normalize_row(item)
            y = year_from_reg_dd(item)
            if year_from <= y <= year_to:
                all_items.append(_fill_missing_columns(item))

    add_items(first.get("list", []))

    for cp in range(2, total_pages + 1):
        data = _request_json_with_retry({"cp": cp})
        add_items(data.get("list", []))

    return all_items

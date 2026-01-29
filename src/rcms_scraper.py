# -*- coding: utf-8 -*-

import re
import time
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

from .schema import VCS_COLUMNS
from .utils import clean_spaces, normalize_date

BASE_URL = "https://techfin.rcms.go.kr/ivsm/fndinfo/tchnFndInfo.do"

MAX_RETRIES = 3
SLEEP_BETWEEN_RETRIES = 1.2


def _request_html_with_retry(params: Dict[str, Any]) -> str:
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_BETWEEN_RETRIES)
    raise last  # type: ignore[misc]


def _base_row() -> Dict[str, Any]:
    return {c: "" for c in VCS_COLUMNS}


def _parse_item(item) -> Dict[str, Any]:
    row = _base_row()

    year_label = item.select_one(".label__green")
    field_label = item.select_one(".label__inner .label:not(.label__green):not(.label__orange)")
    fund_type_label = item.select_one(".label__orange")
    title_el = item.select_one(".top .title")

    year_text = year_label.get_text(strip=True) if year_label else ""
    year = re.sub(r"[^0-9]", "", year_text)

    row["fundNm"] = clean_spaces(title_el.get_text(strip=True) if title_el else "")
    row["comIndNm"] = clean_spaces(field_label.get_text(strip=True) if field_label else "")
    row["투자분야"] = row["comIndNm"]
    row["펀드명_분리"] = row["fundNm"]
    row["펀드명_원문"] = row["fundNm"]

    # left column
    left_cols = item.select(".left .col")
    for col in left_cols:
        t = col.select_one(".title")
        c = col.select_one(".content")
        key = t.get_text(strip=True) if t else ""
        val = c.get_text(" ", strip=True) if c else ""
        if key == "위탁운용사":
            row["operInstNm"] = clean_spaces(val)

    # right column
    right_cols = item.select(".right .col")
    for col in right_cols:
        t = col.select_one(".title")
        c = col.select_one(".content")
        key = t.get_text(strip=True) if t else ""
        val = c.get_text(" ", strip=True) if c else ""
        if key == "펀드규모":
            row["formTotamt"] = clean_spaces(val)
        elif key == "결성일자":
            row["regDd"] = normalize_date(val)
        elif key == "만기일자":
            row["continPd"] = clean_spaces(val)
        elif key == "투자 집행률":
            row["prsntInvstAmt"] = clean_spaces(val)
        elif key == "투자기업":
            row["invstPd"] = clean_spaces(val)

    # store year if regDd missing
    if not row.get("regDd") and year:
        row["regDd"] = f"{year}-01-01"

    # store fund type in comIndCd if present
    if fund_type_label:
        row["comIndCd"] = clean_spaces(fund_type_label.get_text(strip=True))

    return row


def _extract_items_from_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select(".fund__item")
    return [_parse_item(it) for it in items]


def _max_page_from_html(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    pager = soup.select_one(".paging") or soup.select_one(".pagination")
    if not pager:
        return 1
    nums = []
    for a in pager.find_all("a"):
        text = a.get_text(strip=True)
        if text.isdigit():
            nums.append(int(text))
    return max(nums) if nums else 1


def fetch_rcms_all(year_from: int, year_to: int) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []

    html = _request_html_with_retry({"pageIndex": 1})
    max_page = _max_page_from_html(html)
    items = _extract_items_from_html(html)
    all_rows.extend(items)

    for page in range(2, max_page + 1):
        html = _request_html_with_retry({"pageIndex": page})
        items = _extract_items_from_html(html)
        all_rows.extend(items)

    # filter by year label or regDd
    filtered = []
    for row in all_rows:
        year = 0
        if row.get("regDd"):
            try:
                year = int(str(row["regDd"])[:4])
            except Exception:
                year = 0
        if year_from <= year <= year_to:
            filtered.append(row)

    return filtered

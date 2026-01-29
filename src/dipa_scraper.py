# -*- coding: utf-8 -*-

import time
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from .schema import VCS_COLUMNS
from .utils import clean_spaces, normalize_date

LIST_URL = "https://dipa.kban.or.kr/pblntf/pblntfList"
DETAIL_URL = "https://dipa.kban.or.kr/pblntf/detail"

MAX_RETRIES = 3
SLEEP_BETWEEN_RETRIES = 1.2


def _request_json_with_retry(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(LIST_URL, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_BETWEEN_RETRIES)
    raise last  # type: ignore[misc]


def _base_row() -> Dict[str, Any]:
    return {c: "" for c in VCS_COLUMNS}


def _request_detail_html(oper_inst_id: str, data_creat_ym: str) -> str:
    params = {"operInstId": oper_inst_id, "dataCreatYM": data_creat_ym}
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(DETAIL_URL, params=params, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_BETWEEN_RETRIES)
    raise last  # type: ignore[misc]


def _find_fund_table(soup: BeautifulSoup):
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if "조합명" in headers and ("등록일" in headers or "만기예정일" in headers):
            return table
    return None


def _parse_detail_rows(html: str, oper_inst_id: str, oper_inst_nm: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    table = _find_fund_table(soup)
    if not table:
        return []

    rows = []
    for tr in table.find_all("tr"):
        tds = [clean_spaces(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
        if len(tds) < 5:
            continue

        row = _base_row()
        row["operInstId"] = oper_inst_id
        row["operInstNm"] = clean_spaces(oper_inst_nm)
        row["fundNm"] = tds[0]
        row["regDd"] = normalize_date(tds[1])
        row["continPd"] = normalize_date(tds[2])  # 만기예정일
        row["formTotamt"] = tds[3]
        row["prsntInvstAmt"] = tds[4]  # 투자금액

        row["펀드명_분리"] = row["fundNm"]
        row["펀드명_원문"] = row["fundNm"]
        rows.append(row)

    return rows


def fetch_dipa_year(year: int) -> List[Dict[str, Any]]:
    ym = f"{year}-12"
    page_no = 1
    all_rows: List[Dict[str, Any]] = []

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
            oper_inst_id = item.get("operInstId", "")
            oper_inst_nm = clean_spaces(item.get("operInstNm", ""))
            data_creat_ym = item.get("dataCreatDdtm", "")
            if not oper_inst_id or not data_creat_ym:
                continue
            html = _request_detail_html(oper_inst_id, data_creat_ym)
            all_rows.extend(_parse_detail_rows(html, oper_inst_id, oper_inst_nm))

        page_no += 1

    # sort by GP name
    all_rows.sort(key=lambda r: r.get("operInstNm", ""))
    return all_rows


def fetch_dipa_range(year_from: int, year_to: int) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    for y in range(year_from, year_to + 1):
        all_rows.extend(fetch_dipa_year(y))
    all_rows.sort(key=lambda r: r.get("operInstNm", ""))
    return all_rows

# -*- coding: utf-8 -*-

import time
from typing import Any, Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

from .schema import VCS_COLUMNS
from .utils import clean_spaces, normalize_date

BASE_URL = "http://diva.kvca.or.kr/div/dii/DivItmAssoInq"

MAX_RETRIES = 3
SLEEP_BETWEEN_RETRIES = 1.2
MAX_PAGES = 500


def _request_with_retry(session: requests.Session, method: str, params: Dict[str, Any]) -> str:
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if method == "GET":
                r = session.get(BASE_URL, params=params, timeout=30)
            else:
                r = session.post(BASE_URL, data=params, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            if attempt < MAX_RETRIES:
                time.sleep(SLEEP_BETWEEN_RETRIES)
    raise last  # type: ignore[misc]


def _get_form_defaults(session: requests.Session) -> Dict[str, Any]:
    html = _request_with_retry(session, "GET", {})
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", {"id": "asctInfo"})
    if not form:
        return {}

    payload: Dict[str, Any] = {}
    for inp in form.find_all("input"):
        name = inp.get("name")
        if not name:
            continue
        value = inp.get("value") or ""
        payload[name] = value

    return payload


def _base_row() -> Dict[str, Any]:
    return {c: "" for c in VCS_COLUMNS}


def _parse_rows(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tbody tr")
    out: List[Dict[str, Any]] = []

    for tr in rows:
        tds = [clean_spaces(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
        if len(tds) < 9:
            continue

        row = _base_row()
        # tds: 번호, 회사명, 조합명, 등록일, 결성총액(원), 만기일, 투자분야/구분, 목적구분, 지원구분
        row["operInstNm"] = tds[1]
        row["fundNm"] = tds[2]
        row["regDd"] = normalize_date(tds[3])
        row["formTotamt"] = tds[4]
        row["continPd"] = tds[5]
        row["comIndNm"] = tds[6]
        row["comIndCd"] = tds[7]
        row["invstPd"] = tds[8]

        row["투자분야"] = row["comIndNm"]
        row["펀드명_분리"] = row["fundNm"]
        row["펀드명_원문"] = row["fundNm"]

        out.append(row)

    return out


def fetch_diva_all(year_from: int, year_to: int) -> List[Dict[str, Any]]:
    session = requests.Session()
    payload = _get_form_defaults(session)

    all_rows: List[Dict[str, Any]] = []

    for page in range(1, MAX_PAGES + 1):
        payload["PAGE_INDEX"] = str(page)
        html = _request_with_retry(session, "POST", payload)
        rows = _parse_rows(html)
        if not rows:
            break

        for row in rows:
            year = 0
            if row.get("regDd"):
                try:
                    year = int(str(row["regDd"])[:4])
                except Exception:
                    year = 0
            if year_from <= year <= year_to:
                all_rows.append(row)

    return all_rows

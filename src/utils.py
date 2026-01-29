# -*- coding: utf-8 -*-

import re
from datetime import datetime
from typing import Any, Dict


def normalize_date(date_str: str) -> str:
    """
    Return YYYY-MM-DD if possible, else empty string.
    """
    if not date_str:
        return ""
    s = str(date_str).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    if re.match(r"^\d{8}$", s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    if re.match(r"^\d{6}$", s):
        return f"{s[0:4]}-{s[4:6]}-01"
    return ""


def year_from_reg_dd(row: Dict[str, Any]) -> int:
    reg = normalize_date(row.get("regDd", ""))
    if not reg:
        return 0
    try:
        return datetime.strptime(reg, "%Y-%m-%d").year
    except Exception:
        return 0


def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

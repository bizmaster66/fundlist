# -*- coding: utf-8 -*-

import io
import pandas as pd
import streamlit as st

from src.dipa_scraper import fetch_dipa_range
from src.vcs_scraper import fetch_vcs_all


st.set_page_config(page_title="펀드/개인투자조합 수집", layout="wide")

st.title("펀드/개인투자조합 CSV 수집")

with st.sidebar:
    st.header("수집 옵션")
    year_from = st.selectbox("시작 연도", [2023, 2024, 2025], index=0)
    year_to = st.selectbox("종료 연도", [2023, 2024, 2025], index=2)
    if year_from > year_to:
        st.error("시작 연도가 종료 연도보다 클 수 없습니다.")

    sources = st.multiselect(
        "소스 선택",
        ["VCS(펀드)", "DIPA(개인투자조합 공시)"] ,
        default=["VCS(펀드)", "DIPA(개인투자조합 공시)"],
    )

run = st.button("스크래핑 실행")

if run:
    if year_from > year_to:
        st.stop()

    if "VCS(펀드)" in sources:
        with st.spinner("VCS 수집 중..."):
            vcs_rows = fetch_vcs_all(year_from, year_to)
        st.success(f"VCS 수집 완료: {len(vcs_rows)} rows")
        df_vcs = pd.DataFrame(vcs_rows)
        st.subheader("VCS 미리보기")
        st.dataframe(df_vcs.head(50), use_container_width=True)

        buf = io.BytesIO()
        df_vcs.to_csv(buf, index=False, encoding="utf-8-sig")
        st.download_button(
            "VCS CSV 다운로드",
            data=buf.getvalue(),
            file_name="vcs_rsh_funds_all.csv",
            mime="text/csv",
        )

    if "DIPA(개인투자조합 공시)" in sources:
        with st.spinner("DIPA 수집 중..."):
            dipa_rows = fetch_dipa_range(year_from, year_to)
        st.success(f"DIPA 수집 완료: {len(dipa_rows)} rows")
        df_dipa = pd.DataFrame(dipa_rows)
        st.subheader("DIPA 미리보기")
        st.dataframe(df_dipa.head(50), use_container_width=True)

        buf = io.BytesIO()
        df_dipa.to_csv(buf, index=False, encoding="utf-8-sig")
        st.download_button(
            "DIPA CSV 다운로드",
            data=buf.getvalue(),
            file_name="dipa_funds_all.csv",
            mime="text/csv",
        )

else:
    st.info("왼쪽에서 옵션을 선택하고 '스크래핑 실행'을 눌러주세요.")

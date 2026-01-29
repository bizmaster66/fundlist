# -*- coding: utf-8 -*-

import io
import pandas as pd
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.dipa_scraper import fetch_dipa_range
from src.vcs_scraper import fetch_vcs_all
from src.rcms_scraper import fetch_rcms_all
from src.diva_scraper import fetch_diva_all


st.set_page_config(page_title="펀드/개인투자조합 수집", layout="wide")

st.title("펀드/개인투자조합 CSV 수집")

with st.sidebar:
    st.header("수집 옵션")
    years = list(range(2023, 2031))
    year_from = st.selectbox("시작 연도", years, index=0)
    year_to = st.selectbox("종료 연도", years, index=len(years) - 1)
    if year_from > year_to:
        st.error("시작 연도가 종료 연도보다 클 수 없습니다.")

    sources = st.multiselect(
        "소스 선택",
        ["VCS(펀드)", "DIPA(개인투자조합 공시)", "R&D기술금융플랫폼", "DIVA(전자공시)"],
        default=["VCS(펀드)", "DIPA(개인투자조합 공시)", "R&D기술금융플랫폼", "DIVA(전자공시)"],
    )

run = st.button("스크래핑 실행")

if run:
    if year_from > year_to:
        st.stop()

    tasks = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        if "VCS(펀드)" in sources:
            tasks["VCS(펀드)"] = ex.submit(fetch_vcs_all, year_from, year_to)
        if "DIPA(개인투자조합 공시)" in sources:
            tasks["DIPA(개인투자조합 공시)"] = ex.submit(fetch_dipa_range, year_from, year_to)
        if "R&D기술금융플랫폼" in sources:
            tasks["R&D기술금융플랫폼"] = ex.submit(fetch_rcms_all, year_from, year_to)
        if "DIVA(전자공시)" in sources:
            tasks["DIVA(전자공시)"] = ex.submit(fetch_diva_all, year_from, year_to)

        results = {}
        for name, fut in tasks.items():
            with st.spinner(f"{name} 수집 중..."):
                results[name] = fut.result()

    if "VCS(펀드)" in results:
        vcs_rows = results["VCS(펀드)"]
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

    if "DIPA(개인투자조합 공시)" in results:
        dipa_rows = results["DIPA(개인투자조합 공시)"]
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

    if "R&D기술금융플랫폼" in results:
        rcms_rows = results["R&D기술금융플랫폼"]
        st.success(f"R&D기술금융플랫폼 수집 완료: {len(rcms_rows)} rows")
        df_rcms = pd.DataFrame(rcms_rows)
        st.subheader("R&D기술금융플랫폼 미리보기")
        st.dataframe(df_rcms.head(50), use_container_width=True)

        buf = io.BytesIO()
        df_rcms.to_csv(buf, index=False, encoding="utf-8-sig")
        st.download_button(
            "R&D기술금융플랫폼 CSV 다운로드",
            data=buf.getvalue(),
            file_name="rcms_funds_all.csv",
            mime="text/csv",
        )

    if "DIVA(전자공시)" in results:
        diva_rows = results["DIVA(전자공시)"]
        st.success(f"DIVA 수집 완료: {len(diva_rows)} rows")
        df_diva = pd.DataFrame(diva_rows)
        st.subheader("DIVA 미리보기")
        st.dataframe(df_diva.head(50), use_container_width=True)

        buf = io.BytesIO()
        df_diva.to_csv(buf, index=False, encoding="utf-8-sig")
        st.download_button(
            "DIVA CSV 다운로드",
            data=buf.getvalue(),
            file_name="diva_rsh_funds_all.csv",
            mime="text/csv",
        )

else:
    st.info("왼쪽에서 옵션을 선택하고 '스크래핑 실행'을 눌러주세요.")

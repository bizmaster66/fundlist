# fundlist

VCS(벤처투자종합포털)와 DIPA(개인투자조합 공시)에서 2023–2025 데이터를 스크래핑하여 CSV로 내려받는 Streamlit 앱입니다.

## 실행

```bash
pip install -r requirements.txt
python -m playwright install --with-deps chromium
streamlit run app.py
```

## CSV 스키마

`backup_20260129_135741/vcs_rsh_funds_all.csv` 컬럼을 기준으로 동일한 컬럼을 출력합니다.

## 주의

- 스크래핑 결과 CSV는 GitHub에 커밋하지 않습니다.
- Streamlit Cloud에서 실행 시 네트워크/브라우저 제약이 있을 수 있습니다.

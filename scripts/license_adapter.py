"""NZ（Pharmac 藥品給付清單）下載與狀態對應，給 license_refresh_20261029.py 用（2026-10-30）。

注意：NZ 站的「本地許可資料」原本就取自 Pharmac 的 CPSReporting（給付清單），不是 Medsafe 的許可證資料庫；
狀態欄是「Fully subsidised」（Yes／空白），不是許可狀態。這裡沿用同一來源，讓頁面與來源保持一致；
改接 Medsafe 屬於資料來源變更，30 站推完後與其他國家級資料一起評估。
來源：https://schedule.pharmac.govt.nz/latest/CPSReporting.xlsx（會轉址到當期檔，免金鑰），工作表「Community Medicines」（站上原本用的就是社區用藥表，status 為 Yes／空白）。
證據包紀錄 license_id＝Pharmacode、status＝Fully subsidised 原文。新檔沒有的 Pharmacode 保留原狀態。
"""
import io
import time

import openpyxl
import requests

RAW_FILE = "raw/pharmac_fully_subsidised.json"
URL = "https://schedule.pharmac.govt.nz/latest/CPSReporting.xlsx"
SHEET = "Community Medicines"


def fetch():
    for attempt in range(4):
        try:
            r = requests.get(URL, timeout=180, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0 (compatible; TxGNN/1.0; research)"})
            r.raise_for_status()
            if r.content[:2] != b"PK":
                raise RuntimeError("回傳的不是 xlsx")
            break
        except Exception:
            if attempt == 3:
                raise
            time.sleep(15 * (attempt + 1))
    wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True)
    if SHEET not in wb.sheetnames:
        raise RuntimeError(f"找不到工作表 {SHEET}：{wb.sheetnames}")
    head, out = None, []
    for row in wb[SHEET].iter_rows(values_only=True):
        if head is None:
            if row and "Pharmacode" in row:
                head = [" ".join(str(x or "").split()) for x in row]
            continue
        rec = dict(zip(head, row))
        k = str(rec.get("Pharmacode") or "").strip()
        if k:
            out.append({"pharmacode": k, "fully_subsidised": str(rec.get("Fully subsidised") or "").strip()})
    if head is None or "Fully subsidised" not in head:
        raise RuntimeError(f"表頭改變：{head}")
    return out


def validate(new, old):
    p = []
    if len(new) < 1500:  # 2026-11 社區用藥表約 2,500 筆
        p.append(f"只解析出 {len(new)} 筆，疑似格式改變")
    if old and len(new) < 0.9 * len(old):
        p.append(f"筆數 {len(new)} 比上次 {len(old)} 少超過 10%")
    return p


def status_index(new):
    idx = {}
    for r in new:
        # 同一 Pharmacode 可能在社區／醫院各一列：任一列 Yes 即 Yes
        idx[r["pharmacode"]] = "Yes" if r["fully_subsidised"] == "Yes" or idx.get(r["pharmacode"]) == "Yes" else r["fully_subsidised"]
    return idx


def record_id(rec):
    return str(rec.get("license_id") or "").strip()


def update(rec, new):
    if (rec.get("status") or "") == new:
        return False
    rec["status"] = new
    return True

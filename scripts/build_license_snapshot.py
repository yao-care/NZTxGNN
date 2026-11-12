#!/usr/bin/env python3
"""產生許可證快照 snapshots/nz_licenses.json.gz（2026-11-12 建，本站查核機制；比照 TwTxGNN snapshots/tfda_licenses.json.gz）。

為什麼要有這支：
  藥物頁概覽表的狀態列與張數列、許可證表是程式依證據包（data/bundles，放在 Release）與事實層
  （.claude/cache/license_facts，dump_license_facts 依證據包算）產生或同步的。gate（scripts/check_seo_docs.py）要在沒有
  data/ 的環境也能驗證頁面上的號碼屬於本藥、狀態列與張數列跟事實一致，所以把驗證要用的部分存成快照放進 git。

內容（鍵＝藥物頁檔名去 .md）：
  facts：事實層那一筆（market_status、total、valid、cancelled、partial、licenses{號碼: 狀態} …）
  own：本藥證據包 tfda.records 的號碼（正規化：check_drug_page 的 mv2.norm_id）
  rows／show：有程式許可證表的站（scripts/sync_license_tables.py）才有：程式算出的表格列（最多 20 列）與表下的程式文字
  meta：產生日、Release tag、規則所在檔案

什麼時候跑：證據包或事實層變了之後（每週許可證排程與依成分重建都會在改頁後自動跑，並把 snapshots/ 跟 docs/ 一起 commit）。
用法（repo 根目錄，先還原 data/ 並在工作目錄跑 dump_license_facts）：
  python3 scripts/build_license_snapshot.py --ws <工作目錄> [--release-tag data-YYYYMMDD-HHMMSS]
"""
import datetime as dt
import gzip
import importlib.util
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SITE = "NZTxGNN"
OUT = ROOT / "snapshots" / "nz_licenses.json.gz"


def _load(name, path):
    s = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m


def bundle_for(page):
    for key in (page.stem, page.stem.replace("-", "_"), page.stem.replace("_", "-")):
        f = ROOT / "data/bundles" / key / "drug_bundle.json"
        if f.exists():
            return key, json.loads(f.read_text(encoding="utf-8"))
    return None, None


def build(ws, tag=""):
    p = Path(ws) / ".claude/cache/license_facts" / f"{SITE}.json"
    if not p.exists():
        sys.exit(f"找不到事實層 {p}（先跑 dump_license_facts）")
    fs = json.loads(p.read_text(encoding="utf-8"))
    ck = _load("ck", ROOT / "tools/txgnn/.claude/check_drug_page.py")
    slt = ROOT / "scripts" / "sync_license_tables.py"
    S = _load("slt", slt) if slt.exists() else None
    drugs = {}
    for page in sorted((ROOT / "docs/_drugs").glob("*.md")):
        head = page.read_text(encoding="utf-8", errors="replace")[:3000]
        if "layout: id_redirect" in head or 'http-equiv="refresh"' in head:
            continue
        key, b = bundle_for(page)
        if b is None:
            continue
        tf = b.get("tfda") or {}
        fc = fs.get(key) or fs.get(page.stem) or fs.get(page.stem.replace("-", "_"))
        own = sorted({ck.mv2.norm_id(str(r.get("license_id") or "")) for r in tf.get("records") or []
                      if str(r.get("license_id") or "").strip()} |
                     {ck.mv2.norm_id(k) for k in ((fc or {}).get("licenses") or {})})
        e = {"bundle": key, "facts": fc, "own": own}
        if S is not None and hasattr(S, "program_rows"):
            e["rows"], e["show"] = S.program_rows(tf, fc)
        drugs[page.stem] = e
    meta = {"built": dt.date.today().isoformat(), "release_tag": tag, "pages": len(drugs),
            "rule": "事實層：tools/txgnn/.claude/dump_license_facts_20261021.py；號碼正規化：check_drug_page mv2.norm_id"}
    OUT.parent.mkdir(exist_ok=True)
    raw = json.dumps({"meta": meta, "drugs": drugs}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with gzip.GzipFile(OUT, "wb", mtime=0) as fh:
        fh.write(raw)
    print(f"許可證快照：{len(drugs)} 頁 → {OUT.relative_to(ROOT)}（{OUT.stat().st_size // 1024} KB）")


if __name__ == "__main__":
    a = sys.argv[1:]
    ws = a[a.index("--ws") + 1]
    tag = a[a.index("--release-tag") + 1] if "--release-tag" in a else ""
    if not re.fullmatch(r"(data-[\w-]+)?", tag):
        sys.exit("--release-tag 要是 data-… 的 tag")
    build(ws, tag)

"""藥物頁許可證資訊的驗證（2026-11-12 建，本站查核機制；比照 TwTxGNN gate 第 6 項、UsTxGNN src/ustxgnn/regulatory/licenses.py）。

依 snapshots/nz_licenses.json.gz（scripts/build_license_snapshot.py 產生）檢查，不需要 data/：
  1. 「## New Zealand Market Information」段落裡許可證表第一欄的號碼（含數字的格）都屬於本藥（快照 own；正規化後相同，或格子以該號碼結尾，
     例「ARTG 10294T」對「10294T」）。
  2. 程式產生的許可證表（本站 有程式產生的許可證表（scripts/sync_license_tables.py））：<!-- licenses:begin --> … <!-- licenses:end --> 內＝依快照重算的結果
     （狀態欄以外逐列相同；狀態欄由每週排程依事實層改寫，由第 4 項驗）。
  3. 概覽表的狀態列與張數列＝事實層的寫法：用本站 tools/txgnn/.claude/check_drug_page.py 的 marketed_text／not_marketed_text／
     unknown_status_text／not_reg_text／count_text（checker M1／M4 的同一套函式）。
  4. 表格列寫的狀態不得跟事實相反（checker row_status_conflicts）。
修法：資料變了→跑每週排程（或 scripts/build_license_snapshot.py 重建快照）；正文的號碼、狀態不對→寫查核紀錄
（docs/_data/drug_reviews.json）再跑 scripts/apply_drug_reviews.py。
"""
from __future__ import annotations

import gzip
import importlib.util
import json
import re
import sys
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SNAP = ROOT / "snapshots" / "nz_licenses.json.gz"
SITE = "NZTxGNN"
SEC = "## New Zealand Market Information"
BEGIN = "<!-- licenses:begin -->"
END = "<!-- licenses:end -->"


def _load(name, path):
    s = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m


@lru_cache(maxsize=None)
def S():
    """本站的程式許可證表模組（scripts/sync_license_tables.py；沒有程式表的站回 None）"""
    p = ROOT / "scripts" / "sync_license_tables.py"
    return _load(f"{SITE}_sync_license_tables", p) if p.exists() else None


@lru_cache(maxsize=None)
def CK():
    return _load(f"{SITE}_check_drug_page", ROOT / "tools" / "txgnn" / ".claude" / "check_drug_page.py")


@lru_cache(maxsize=None)
def snapshot() -> dict:
    if not SNAP.exists():
        return {}
    return json.loads(gzip.open(SNAP).read().decode("utf-8"))


def norm(x):
    return CK().mv2.norm_id(x)


def _section(lines):
    if SEC not in lines:
        return None, None
    a = lines.index(SEC) + 1
    b = next((k for k in range(a, len(lines)) if lines[k].startswith("## ")), len(lines))
    return a, b


def _tables(lines):
    out, cur = [], []
    for ln in lines + [""]:
        if ln.startswith("|"):
            cur.append(ln)
        elif cur:
            out.append(cur)
            cur = []
    return out


def _rows(tbl):
    return [[c.strip() for c in ln.strip().strip("|").split("|")] for ln in tbl[2:]]


def _own(cell, own):
    c = norm(re.sub(r"[（(][^）)]*[）)]", "", cell))  # 號碼後面的說明括號（「14524K (as listed in the data)」）不算號碼
    if not re.search(r"\d", c):
        return True  # 不是號碼的格（「Not recorded」等）不驗
    # 格子以本藥號碼結尾、號碼前一字不是數字（「Registration No. 459」對「459」、「ARTG 10294T」對「10294T」；5 不會配到 15）
    return c in own or any(o and c.endswith(o) and not c[-len(o) - 1:-len(o)].isdigit() for o in own)


def _head(s):
    return s.HEAD[0] if isinstance(s.HEAD, (list, tuple)) else s.HEAD


def _nostatus(row):
    """程式表一列去掉最後一欄（狀態）：狀態由每週排程依事實層改寫，另由 row_status_conflicts 驗；其他欄（號碼、名稱）要跟快照一樣"""
    return row.strip().strip("|").rsplit("|", 1)[0].strip()


def expected_status(fc):
    ck, lang = CK(), CK().LANG.get(SITE, "en")
    ms, v, n, partial = fc.get("market_status"), fc.get("valid"), fc.get("total"), fc.get("partial")
    if ms == "Marketed":
        return ck.marketed_text(SITE, lang, v, n, partial, fc)
    if ms == "Approved, not marketed":
        return ck.not_marketed_text(SITE, lang, fc)
    if ms == "Status unknown" and SITE in ck.UNKNOWN_ST:
        return ck.unknown_status_text(SITE, lang, fc)
    if ms == "Not marketed" and SITE in ck.VET_ONLY:
        return ck.not_reg_text(SITE, lang, fc)
    return None


def page_problems(stem: str, text: str) -> list[str]:
    e = (snapshot().get("drugs") or {}).get(stem)
    if e is None:
        return []  # 快照沒有的頁（沒有證據包）：不驗
    ck = CK()
    body = re.sub(r"<!-- review:begin (\S+) -->.*?<!-- review:end \1 -->", "", text, flags=re.S)
    lines = body.replace("\r\n", "\n").split("\n")
    errs = []
    own = set(e["own"])
    a, b = _section(lines)
    if a is not None:
        sec = lines[a:b]
        for t in _tables(sec):
            bad = sorted({r[0] for r in _rows(t) if r and r[0] and not _own(r[0], own)})
            if bad:
                errs.append(f"市場段落許可證表的號碼不屬於本藥：{', '.join(bad[:5])}")
        s = S()
        has_b, has_e = BEGIN in sec, END in sec
        if has_b != has_e:
            errs.append("程式區塊標記不成對")
        if s is not None and has_b and has_e:
            i, j = sec.index(BEGIN), sec.index(END)
            blk = [x for x in sec[i + 1:j] if x.strip()]
            tb = [x for x in blk if x.startswith("|")]
            rest = [x.strip() for x in blk if not x.startswith("|")]
            n = len(tb) - 2
            if not e.get("rows"):
                if tb:
                    errs.append("程式區塊有表格，但快照沒有任何可列的許可證")
            elif not tb or tb[0].strip() != _head(s):
                errs.append("程式區塊的表頭不是程式表頭")
            elif [_nostatus(x) for x in tb[2:]] != [_nostatus(x) for x in e["rows"][:n]]:
                k = next((k for k, (x, y) in enumerate(zip(tb[2:], e["rows"])) if _nostatus(x) != _nostatus(y)), min(n, len(e["rows"])))
                errs.append(f"程式表第 {k + 1} 列跟快照重算的結果不同（程式表被手改或資料已更新：跑 scripts/sync_license_tables.py）")
            if e.get("show") is not None and tb and rest != e["show"]:
                errs.append(f"程式區塊表格下方的程式文字應為「{' '.join(e['show']) or '（無）'}」")
        elif s is not None and getattr(s, "HEAD", None) and any(x.strip() == _head(s) for x in sec):
            errs.append("程式表沒有程式區塊標記（跑 python3 scripts/sync_license_tables.py --mark-existing）")
    fc = e.get("facts")
    if fc:
        all_rows = [r for t in _tables(lines) for r in _rows(t)]
        st = ck.status_row(SITE, all_rows)
        want_s = expected_status(fc)
        if st is not None and want_s and st[1] != want_s:
            errs.append(f"狀態列「{st[1][:50]}」≠ 事實層寫法「{want_s}」")
        cr = ck.count_row(SITE, all_rows)
        if cr is not None and (fc.get("total") is not None or fc.get("partial")):
            want_c = ck.count_text(SITE, ck.LANG.get(SITE, "en"), fc)
            if cr[1] != want_c:
                errs.append(f"張數列「{cr[1][:50]}」≠ 事實層寫法「{want_c}」")
        lic = {norm(k): v for k, v in (fc.get("licenses") or {}).items()}
        if a is not None:
            for t in _tables(lines[a:b]):
                for code, d in ck.row_status_conflicts(SITE, _rows(t), lic):
                    errs.append(f"許可證表列的狀態跟事實相反：{d}")
    return errs


def site_problems(drugs_dir: Path | None = None) -> list[str]:
    drugs_dir = drugs_dir or ROOT / "docs" / "_drugs"
    if not snapshot():
        return ["找不到 snapshots/nz_licenses.json.gz（跑 scripts/build_license_snapshot.py）"]
    out = []
    for f in sorted(drugs_dir.glob("*.md")):
        t = f.read_text(encoding="utf-8")
        if 'http-equiv="refresh"' in t[:3000] or re.search(r"^layout:\s*id_redirect", t[:3000], re.M):
            continue
        out += [f"{f.name}：{x}" for x in page_problems(f.stem, t)]
    return out


if __name__ == "__main__":
    errs = site_problems()
    for x in errs[:50]:
        print("  ✗", x)
    print(f"許可證檢查：{'✗' if errs else '✓'} {len(errs)} 項")
    sys.exit(1 if errs else 0)

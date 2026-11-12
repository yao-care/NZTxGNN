"""依成分重建後，把藥物頁「## New Zealand Market Information」段落的表換成程式產生的 Pharmac 給付清單表（2026-11-06，用戶核准；全程不用 AI）。

前置：scripts/rebuild_licenses_by_ingredient.py 已重建 data/bundles（Pharmac Pharmaceutical Schedule 社區用藥表，依成分，含複方）。
NZ 站的本地資料是 Pharmac 的給付清單，不是 Medsafe 許可證：
- 段落裡的第一張表（原表頭多寫成「Authorization Number …」）換成「| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |」，
  Fully subsidised 照清單原文（Yes 寫 Yes，空白寫 No）；最多列 20 筆，表後註明來源與「Showing 20 of N Pharmac Schedule items.」。
- 表後 known_use_pages 寫的「Only part of the licence list is held in the data; the list may be incomplete.」一併拿掉（重建後是完整清單）。
- 概覽表有「New Zealand Market Status」列、卻沒有「Number of Pharmac Schedule Items」列的頁，在狀態列下面補一列（值＝清單筆數）。
- 新資料沒有任何品項時拿掉表，改一句「Not listed on the Pharmac Pharmaceutical Schedule (Community Medicines) used for this report.」
- 段落裡沒有表的頁（舊頁只有文字、或矛盾句刪完只剩標題）：在標題下補程式產生的表與來源句（沒有紀錄時補「Not listed …」句，已有就不重複；2026-11-06 第三輪）；
  表以外的文字不動（跟新事實矛盾的句子由 tools/txgnn/.claude/delete_license_sentences.py 處理）。
scripts/license_adapter.py 的 sync_tables 在每週更新後用同一套程式重排有變動的藥的表。
程式區塊標記（2026-11-12 起，本站查核機制，比照 UsTxGNN）：程式產生的表格（連同表下的 SHOW／SOURCE 句，或 NONE_LINE）前後各一行
「<!-- licenses:begin -->」「<!-- licenses:end -->」（前後空行），讓查核紀錄、刪句工具與 gate 分得出哪些字是程式寫的；
區塊內容由 block_for 決定（重排、補標記、快照共用）；--mark-existing 只替表頭＝HEAD 的現有程式表補標記、不改內容；
carry_block 給 sync_notes_to_docs.py 重寫整頁時移植舊頁的程式區塊；program_rows 給 scripts/build_license_snapshot.py。
gate（scripts/check_seo_docs.py）依 snapshots/nz_licenses.json.gz 檢查程式表（src/nztxgnn/regulatory/licenses.py）。
用法（repo 根目錄）：python3 scripts/sync_license_tables.py [--dry-run] [--mark-existing] [頁 ...]
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEC = "## New Zealand Market Information"
HEAD = ["| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |", "|------|------|------|------|------|"]
NONE_LINE = "Not listed on the Pharmac Pharmaceutical Schedule (Community Medicines) used for this report."
SOURCE = "Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data."
SHOW = "Showing {n} of {m} Pharmac Schedule items."
SHOW_RX = re.compile(r"^Showing \d+ of \d+ Pharmac Schedule items\.( .*)?$")
PART_NOTE = "Only part of the licence list is held in the data; the list may be incomplete."
MAX_ROWS = 20
BEGIN = "<!-- licenses:begin -->"
END = "<!-- licenses:end -->"


def cell(x, n=80):
    t = re.sub(r"\s+", " ", str(x or "").replace("|", "/")).strip()
    return t if len(t) <= n else t[:n - 1].rstrip() + "…"


def table_rows(recs):
    seen, rows = set(), []
    for r in recs:
        k = r.get("license_id")
        if not k or k in seen:
            continue
        seen.add(k)
        rows.append(f"| {k} | {cell(r.get('brand_name_en'), 50)} | {cell(r.get('ingredients'), 60)} | {cell(r.get('dosage_form'), 50)} | "
                    f"{'Yes' if str(r.get('status') or '').strip() == 'Yes' else 'No'} |")
    return rows


STATUS_LABEL, COUNT_LABEL = "| New Zealand Market Status |", "| Number of Pharmac Schedule Items |"


def ensure_count_row(lines, recs, partial):
    if any(l.startswith(COUNT_LABEL) for l in lines):
        return lines
    for i, l in enumerate(lines):
        if l.startswith(STATUS_LABEL):
            n = len({r.get("license_id") for r in recs if r.get("license_id")})
            return lines[:i + 1] + [f"{COUNT_LABEL} {('at least ' if partial else '')}{n} |"] + lines[i + 1:]
    return lines


def block_for(recs):
    """程式區塊的內容（不含標記）：表＋表下的 SHOW／SOURCE 句；沒有任何品項時是 NONE_LINE 一句"""
    rows = table_rows(recs)
    if not rows:
        return [NONE_LINE]
    return HEAD + rows[:MAX_ROWS] + [""] + ([SHOW.format(n=MAX_ROWS, m=len(rows)) + " " + SOURCE] if len(rows) > MAX_ROWS else [SOURCE])


def strip_marks(lines):
    return [l for l in lines if l.strip() not in (BEGIN, END)]


def wrap(block):
    return [BEGIN, ""] + block + ["", END]


def program_rows(tf, fc=None):
    """快照用：(程式表的列, 表下的程式文字)；沒有任何品項時 ([], None)"""
    blk = block_for(tf.get("records") or [])
    if blk == [NONE_LINE]:
        return [], None
    return [l for l in blk[2:] if l.startswith("|")], [l for l in blk if l.strip() and not l.startswith("|")]


def _bounds(lines):
    a = lines.index(SEC) + 1
    return a, next((k for k in range(a, len(lines)) if lines[k].startswith("## ")), len(lines))


def _is_prog_text(l):
    s = l.strip()
    return bool(SHOW_RX.match(s)) or s == SOURCE


def mark_existing(lines):
    """表頭＝HEAD 的現有程式表（連同緊接的 SHOW／SOURCE 句）或 NONE_LINE 補上標記，內容一字不改"""
    if BEGIN in lines or SEC not in lines:
        return lines
    a, b = _bounds(lines)
    tb = [k for k in range(a, b) if lines[k].startswith("|")]
    if not tb:
        if NONE_LINE in lines[a:b]:
            k = lines.index(NONE_LINE, a)
            return lines[:k] + wrap([NONE_LINE]) + lines[k + 1:]
        return lines
    x = y = tb[0]
    if lines[x].strip() != HEAD[0]:
        return lines
    while y + 1 < b and lines[y + 1].startswith("|"):
        y += 1
    blk, k = lines[x:y + 1], y + 1
    while k < b and not lines[k].strip():
        k += 1
    if k < b and _is_prog_text(lines[k]):
        blk, y = blk + ["", lines[k]], k
    return lines[:x] + wrap(blk) + lines[y + 1:]


def carry_block(old_text, new_text):
    """sync_notes_to_docs.py 重寫整頁時用：舊頁的程式區塊移植到新頁的市場段落，取代新頁模型寫的第一張表（連同表下程式文字）；
    新頁段落沒有表時接在標題下。舊頁沒有程式區塊就原樣回傳新頁。"""
    old = old_text.replace("\r\n", "\n").split("\n")
    if BEGIN not in old or END not in old:
        return new_text
    blk = old[old.index(BEGIN):old.index(END) + 1]
    lines = strip_marks(new_text.split("\n"))
    if SEC not in lines:
        return new_text
    a, b = _bounds(lines)
    tb = [k for k in range(a, b) if lines[k].startswith("|")]
    if not tb:
        out = lines[:a] + [""] + blk + [""] + lines[a:]
    else:
        x = y = tb[0]
        while y + 1 < b and lines[y + 1].startswith("|"):
            y += 1
        rest = [l for l in lines[y + 1:b] if not _is_prog_text(l)]
        out = lines[:x] + blk + rest + lines[b:]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(out))


def process(text, recs, partial=False):
    lines = ensure_count_row(strip_marks(text.split("\n")), recs, partial)
    if SEC not in lines:
        return None
    a = lines.index(SEC) + 1
    b = next((k for k in range(a, len(lines)) if lines[k].startswith("## ")), len(lines))
    tb = [k for k in range(a, b) if lines[k].startswith("|")]
    if not tb:
        rows = table_rows(recs)
        if rows or not any(l.strip() == NONE_LINE for l in lines[a:b]):
            lines = lines[:a] + [""] + wrap(block_for(recs)) + [""] + lines[a:]
        elif NONE_LINE in lines[a:b]:  # 已有 NONE_LINE：補標記（不重複加句）
            k = lines.index(NONE_LINE, a)
            lines = lines[:k] + wrap([NONE_LINE]) + lines[k + 1:]
        t2 = re.sub(r"\n{3,}", "\n\n", "\n".join(lines))
        return t2 if t2 != text else None
    x, y = tb[0], tb[0]
    while y + 1 < b and lines[y + 1].startswith("|"):
        y += 1
    rest = [l for l in lines[y + 1:b] if not SHOW_RX.match(l.strip()) and l.strip() != SOURCE and (partial or l.strip() != PART_NOTE)]
    out = lines[:x] + wrap(block_for(recs)) + [""] + rest + lines[b:]
    if out[x - 1].strip():
        out.insert(x, "")
    t2 = re.sub(r"\n{3,}", "\n\n", "\n".join(out))
    return t2 if t2 != text else None


def run(only=None, dry=False):
    """only：要處理的頁名集合（None＝全部）；回傳改到的頁名"""
    changed = []
    for page in sorted((ROOT / "docs/_drugs").glob("*.md")):
        if only and page.stem not in only:
            continue
        bf = ROOT / "data/bundles" / page.stem / "drug_bundle.json"
        if not bf.exists():
            bf = ROOT / "data/bundles" / page.stem.replace("-", "_") / "drug_bundle.json"
        if not bf.exists():
            continue
        raw = page.read_bytes().decode("utf-8")
        if "layout: id_redirect" in raw[:3000] or 'http-equiv="refresh"' in raw[:3000]:
            continue
        crlf = "\r\n" in raw[:500]
        reg = json.loads(bf.read_text(encoding="utf-8")).get("tfda") or {}
        recs = reg.get("records") or []
        new = process(raw.replace("\r\n", "\n"), recs, int(reg.get("total_matches") or 0) > len(recs))
        if new is None:
            continue
        changed.append(page.stem)
        if not dry:
            page.write_bytes((new.replace("\n", "\r\n") if crlf else new).encode("utf-8"))
    return changed


def main(argv):
    if "--mark-existing" in argv:
        n = 0
        for page in sorted((ROOT / "docs/_drugs").glob("*.md")):
            raw = page.read_bytes().decode("utf-8")
            if "layout: id_redirect" in raw[:3000] or 'http-equiv="refresh"' in raw[:3000]:
                continue
            crlf = "\r\n" in raw[:500]
            t = raw.replace("\r\n", "\n")
            new = re.sub(r"\n{3,}", "\n\n", "\n".join(mark_existing(t.split("\n"))))
            if BEGIN in new and BEGIN not in t:  # 只寫入真的補了標記的頁
                n += 1
                if "--dry-run" not in argv:
                    page.write_bytes((new.replace("\n", "\r\n") if crlf else new).encode("utf-8"))
        print(f"程式區塊標記：補上 {n} 頁")
        return
    only = {a for a in argv if not a.startswith("--")}
    print(f"許可證表改 {len(run(only or None, '--dry-run' in argv))} 頁")


if __name__ == "__main__":
    main(sys.argv[1:])

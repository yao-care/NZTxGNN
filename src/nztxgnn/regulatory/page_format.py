"""藥物頁的確定性格式規則：頁首證據等級、description 的等級、表格前後空行（2026-11-12 建）。

比照 TwTxGNN src/twtxgnn/regulatory/page_format.py 的核心流程（範本：UsTxGNN），用本站頁面的寫法。
scripts/apply_drug_reviews.py 與 gate（scripts/check_seo_docs.py）都呼叫這裡，規則只有一份。

頁首等級規則（本站的在地規則，2026-11-12）：
  頁首（front matter evidence_level、parent、「Evidence Level: **Lx**」列）＝「Quick Overview」表
  「Evidence Level」儲存格裡**第一個**等級＝主預測（rank 1）的等級。2026-11-12 實測本站 451 頁有總覽等級，頁首全部等於
  第一個等級（其中 15 頁的格裡另列次要預測等級，頁首仍是第一個），這條規則就是本站產線原本的行為；
  Tw 是整格取最高，本站不同（整格取最高會把列了次要預測、否定敘述或已核准用途等級的頁錯誤升級，見 UsTxGNN）。
  儲存格沒有任何 Lx（「N/A」「Not applicable」等）或沒有這一列：頁首不動。
  主預測重審撤回：查核紀錄的 table_updates 把總覽「Evidence Level」格換成「Withdrawn: …」（格內不再有 Lx）時，
  頁首改取本頁其他預測表（表頭有「Evidence Level」欄的表）保留的最高等級；沒有就是 N/A，parent 歸
  docs/known-uses.md 的 title。只看表格裡的等級欄，不解析敘述文字裡提到的等級。

description 的等級：front matter description 是產線依報告寫的（「(evidence level Lx)」），頁首重算後會過期。
  drug_reviews.json 的 action: description_level 紀錄（記 original_description）由 apply_drug_reviews.py
  呼叫 sync_description_level，只把 description 裡「evidence level Lx」換成目前頁首等級
  （N/A 頁換成「known uses only」，與 docs/known-uses.md 的標題一致），其他文字一字不動。
  母目錄的 .claude/sync_description_levels.py 走同一條規則（只改等級字樣、原文留在 docs/_data/description_level_log.json）。

表格前後空行：規則與修法在本站 scripts/fix_table_spacing.py（gate 第 6 項），這裡直接用它，不另寫一套。
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DOCS = ROOT / "docs"
EVIDENCE_PAGES = ("evidence-high.md", "evidence-medium.md", "evidence-low.md")
KNOWN_USES_PAGE = "known-uses.md"  # evidence_level N/A 的列表頁
NA = "N/A"
LEVEL_COL = "Evidence Level"
OVERVIEW_ROW_RE = re.compile(r"^\|\s*Evidence Level\s*\|([^|\n]*)\|", re.M)
WITHDRAWN_CELL_RE = re.compile(r"^\s*(\*\*)?Withdrawn\b", re.I)
REVIEW_BLOCK_RE = re.compile(r"<!-- review:begin (\S+) -->.*?<!-- review:end \1 -->", re.S)
LEVEL_RE = r"(?:L[1-5]|N/A)"
HEADER_RE = re.compile(rf"^(Evidence Level: \*\*)({LEVEL_RE})(\*\*)", re.M)
_PARENTS: dict = {}


def _fts():
    s = importlib.util.spec_from_file_location("us_fix_table_spacing", ROOT / "scripts" / "fix_table_spacing.py")
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m


_FTS = _fts()


def best_level(cell: str) -> str | None:
    lv = re.findall(r"L([1-5])", cell or "")
    return f"L{min(lv)}" if lv else None


def first_level(cell: str) -> str | None:
    m = re.search(r"L([1-5])", cell or "")
    return f"L{m.group(1)}" if m else None


def overview_level(text: str) -> str | None:
    """Quick Overview「Evidence Level」列的頁首等級（第一個等級＝主預測（rank 1）的等級）；沒有這列或列內沒有 Lx 回 None。"""
    m = OVERVIEW_ROW_RE.search(REVIEW_BLOCK_RE.sub("", text))
    return first_level(m.group(1)) if m else None


def overview_withdrawn(text: str) -> bool:
    """Quick Overview「Evidence Level」格是重審撤回後的「Withdrawn: …」（格內沒有 Lx）。"""
    m = OVERVIEW_ROW_RE.search(REVIEW_BLOCK_RE.sub("", text))
    return bool(m and WITHDRAWN_CELL_RE.match(m.group(1)) and not best_level(m.group(1)))


def other_prediction_levels(text: str) -> list[str]:
    """Quick Overview 以外、表頭有「Evidence Level」欄的表格裡，該欄的等級（查核框與程式碼區塊內不算）。"""
    text = REVIEW_BLOCK_RE.sub("", text)
    out, col, fence = [], None, False
    lines = text.split("\n")
    for i, ln in enumerate(lines):
        if ln.startswith("```"):
            fence = not fence
        if fence or not ln.startswith("|"):
            col = None
            continue
        cells = [c.strip() for c in ln.strip().strip("|").split("|")]
        prev = lines[i - 1] if i else ""
        if not prev.startswith("|"):  # 表頭列
            col = cells.index(LEVEL_COL) if LEVEL_COL in cells else None
            continue
        if col is None or set(ln.replace("|", "").strip()) <= set("-: "):
            continue
        if col < len(cells):
            lv = best_level(cells[col])
            if lv:
                out.append(lv)
    return out


def target_level(text: str) -> str | None:
    """頁首應有的等級；None＝頁首不動（總覽列沒有 Lx 也不是撤回、或沒有總覽列）。"""
    best = overview_level(text)
    if best:
        return best
    if overview_withdrawn(text):
        return best_level(" ".join(other_prediction_levels(text))) or NA
    return None


def parent_titles() -> dict:
    """證據等級 → 列表頁標題（docs/evidence-*.md 的 title 裡的 L 範圍決定歸屬；N/A → known-uses.md）。"""
    if not _PARENTS:
        p = DOCS / KNOWN_USES_PAGE
        if p.exists():
            m = re.search(r"^title:\s*(.+?)\s*$", p.read_text(encoding="utf-8"), re.M)
            if m:
                _PARENTS[NA] = m.group(1).strip().strip('"').strip("'")
        for fname in EVIDENCE_PAGES:
            p = DOCS / fname
            if not p.exists():
                continue
            m = re.search(r"^title:\s*(.+?)\s*$", p.read_text(encoding="utf-8"), re.M)
            if not m:
                continue
            title = m.group(1).strip().strip('"').strip("'")
            lv = [int(x) for x in re.findall(r"L([1-5])", title)]
            for n in range(min(lv), max(lv) + 1) if lv else []:
                _PARENTS.setdefault(f"L{n}", title)
    return _PARENTS


def _split_fm(text: str):
    if not text.startswith("---"):
        return None, text
    end = text.find("\n---", 3)
    return (text[:end], text[end:]) if end >= 0 else (None, text)


def header_state(text: str) -> dict:
    fm, rest = _split_fm(text)
    fm = fm or ""
    lv = re.search(rf"^evidence_level:[ \t]*({LEVEL_RE})[ \t]*$", fm, re.M)
    par = re.search(r"^parent:[ \t]*(.*?)[ \t]*$", fm, re.M)
    head = HEADER_RE.search(rest)
    return {"evidence_level": lv.group(1) if lv else None,
            "parent": par.group(1).strip().strip('"').strip("'") if par else None,
            "header": head.group(2) if head else None}


def sync_header_level(text: str) -> str:
    best = target_level(text)
    fm, rest = _split_fm(text)
    if not best or fm is None:
        return text
    fm = re.sub(rf"^(evidence_level:[ \t]*){LEVEL_RE}[ \t]*$", lambda x: x.group(1) + best, fm, count=1, flags=re.M)
    parent = parent_titles().get(best)
    if parent:
        fm = re.sub(r"^parent:.*$", lambda x: f"parent: {parent}", fm, count=1, flags=re.M)
    rest = HEADER_RE.sub(lambda x: x.group(1) + best + x.group(3), rest, count=1)
    return fm + rest


DESC_LEVEL_RE = re.compile(r"evidence level (L[1-5]|N/A)", re.I)
NA_DESC = "known uses only"


def _desc_line(fm: str):
    return re.search(r"^description:.*$", fm, re.M)


def description_level_text(level: str) -> str:
    return NA_DESC if level == NA else f"evidence level {level}"


def sync_description_level(text: str) -> str:
    """description 裡的「evidence level Lx」換成目前頁首等級（N/A→「known uses only」）；其他文字不動。"""
    fm, rest = _split_fm(text)
    lv = header_state(text)["evidence_level"]
    m = _desc_line(fm or "")
    if not m or not lv:
        return text
    line = DESC_LEVEL_RE.sub(lambda _: description_level_text(lv), m.group(0))
    return fm[:m.start()] + line + fm[m.end():] + rest


def description_level_problems(text: str) -> list[str]:
    fm, _ = _split_fm(text)
    lv = header_state(text)["evidence_level"]
    m = _desc_line(fm or "")
    if not m or not lv:
        return []
    bad = sorted({x.upper() for x in DESC_LEVEL_RE.findall(m.group(0)) if x.upper() != lv})
    return [f"description 寫「evidence level {x}」，頁首等級={lv}（加 description_level 查核紀錄後跑 scripts/apply_drug_reviews.py）"
            for x in bad]


def header_problems(text: str) -> list[str]:
    best = target_level(text)
    st = header_state(text)
    if not best:
        want = parent_titles().get(NA)
        if st["evidence_level"] == NA and want and st["parent"] != want:
            return [f"parent={st['parent']}，N/A 頁應為 {want}（docs/known-uses.md 的 title）"]
        return []
    errs = []
    if st["evidence_level"] != best:
        errs.append(f"front matter evidence_level={st['evidence_level']}，Quick Overview 主預測等級（主預測撤回時為其他預測表最高等級或 N/A）={best}")
    if st["header"] not in (None, best):
        errs.append(f"頁首「Evidence Level」={st['header']}，Quick Overview 主預測等級={best}")
    want = parent_titles().get(best)
    if want and st["parent"] != want:
        errs.append(f"parent={st['parent']}，應為 {want}")
    return errs


def fix_table_spacing(text: str) -> str:
    return _FTS.fix(text)


def table_spacing_problems(text: str) -> list[str]:
    return [f"第 {i + 1} 行的表格{'前' if k == 'before' else '後'}面缺空行" for k, i in _FTS.problems(text)]

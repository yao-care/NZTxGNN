#!/usr/bin/env python3
"""「Original Indication」列的程式化交叉驗證（只列待查，不改頁面）（2026-11-12 建，比照 TwTxGNN 同名腳本；範本 UsTxGNN）。

做法：把每頁 Quick Overview「Original Indication」的文字切成詞組，逐一比對
  (a) 本藥證據包（data/bundles/<藥>/drug_bundle.json）的許可證／仿單適應症文字；
  (b) 其他藥的許可證／仿單適應症文字。
某詞組在 (a) 找不到、卻在 (b) 找得到，就是「只出現在不屬於本藥的許可證」的詞組。一頁有這種詞組且占該列詞組一半以上，
就寫進 docs/_data/drug_reviews_uncertain.json（source＝本程式；每次執行整批替換這個 source 的項目），交人工判讀。
「Not recorded in the local data」這類「資料沒有」的寫法不比對。

2026-11-12 現況：證據包沒有適應症文字，可比對 0 頁（沒有原適應症 768 頁、證據包沒有適應症文字 71 頁）。

用法：python3 scripts/check_original_indications.py   （需要 data/bundles）
"""
import json
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UNCERTAIN = ROOT / "docs" / "_data" / "drug_reviews_uncertain.json"
SOURCE = "Original Indication 交叉驗證（scripts/check_original_indications.py）"
ROW_RE = re.compile(r"^\|\s*Original Indication\s*\|([^|\n]*)\|", re.M)
NONE_RE = re.compile(r"^\s*(not (recorded|available|listed)|none|n/a|no (approved )?indication)", re.I)
SPLIT_RE = re.compile(r"[;,/()]+|\band\b|\bor\b", re.I)
REVIEW_RE = re.compile(r"<!-- review:begin (\S+) -->.*?<!-- review:end \1 -->", re.S)


def phrases(cell):
    cell = re.sub(r"\*\*|\[[^\]]*\]\([^)]*\)", "", cell)
    return [p.strip().lower() for p in SPLIT_RE.split(cell) if len(p.strip()) >= 4]


STOP = {"with", "and", "the", "for", "from", "that", "this", "other", "patients", "adults", "adult", "years", "aged", "older",
        "treatment", "therapy", "combination", "used", "indicated", "disease", "medicinal", "products", "product", "who", "are",
        "first", "line", "second", "third", "prior", "after", "before", "previously", "monotherapy", "alone"}


def covered(phrase, text):
    """英文措辭常跟仿單不同（「HIV-1 infection in adults」對「HIV-1 infected adults」）：詞組的實詞（4 字母以上、不在 STOP）
    有七成以上出現在 text 就算有；整個詞組原樣出現也算（2026-11-12，Eu 實測整句比對 4 頁全是誤判）"""
    if phrase in text:
        return True
    tok = set(re.sub(r"[^a-z0-9]+", " ", text).split())  # 仿單常把連字號存成「?」（「ALK)?positive」）、「HIV-1」寫成「HIV 1」
    tok |= set(re.sub(r"[^a-z0-9 ]+", " ", re.sub(r"(?<=[a-z])[-?\u00a0](?=[a-z])", "", text)).split())  # 「anti-retroviral」＝antiretroviral
    words = [w for w in re.sub(r"[^a-z0-9]+", " ", phrase).split() if len(w) >= 4 and w not in STOP]
    if not words:
        return True
    stems = {x[:6] for x in tok if len(x) >= 6}  # 詞形不同（infection／infected）：前 6 個字母相同算同一詞
    return sum(1 for w in words if w in tok or (len(w) >= 6 and w[:6] in stems)) / len(words) >= 0.7


def label_text(bundle):
    tf = bundle.get("tfda") or {}
    parts = [r.get("indication") or "" for r in tf.get("records") or []]
    pi = bundle.get("package_insert") or {}
    parts += [pi.get("indications") or pi.get("indications_and_usage") or ""] if isinstance(pi, dict) else []
    return " ".join(p for p in parts if p).lower()


def main():
    bundles = {}
    for f in sorted((ROOT / "data/bundles").glob("*/drug_bundle.json")):
        txt = label_text(json.loads(f.read_text(encoding="utf-8")))
        if txt.strip():
            bundles[f.parent.name] = txt
    pages, items, stat = 0, [], Counter()
    for page in sorted((ROOT / "docs/_drugs").glob("*.md")):
        t = REVIEW_RE.sub("", page.read_text(encoding="utf-8"))
        m = ROW_RE.search(t)
        if not m or NONE_RE.match(m.group(1)):
            stat["no_original"] += 1
            continue
        own = bundles.get(page.stem) or bundles.get(page.stem.replace("-", "_"))
        if not own:
            stat["no_label_text"] += 1
            continue
        pages += 1
        ph = phrases(m.group(1))
        foreign = [p for p in ph if not covered(p, own) and any(covered(p, v) for k, v in bundles.items() if k != page.stem)]
        if ph and len(foreign) * 2 >= len(ph):
            items.append({"source": SOURCE, "drug": page.stem, "file": page.name, "status": "open",
                          "claim": m.group(1).strip(), "foreign_phrases": foreign,
                          "note": "原適應症列的詞組多半只出現在其他藥的許可證／仿單適應症，待人工對照仿單"})
    doc = json.loads(UNCERTAIN.read_text(encoding="utf-8")) if UNCERTAIN.exists() else {"_doc": DOC, "items": []}
    doc["items"] = [x for x in doc["items"] if x.get("source") != SOURCE] + items
    UNCERTAIN.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"原適應症交叉驗證：可比對 {pages} 頁、待查 {len(items)} 頁（沒有原適應症 {stat['no_original']} 頁、"
          f"證據包沒有適應症文字 {stat['no_label_text']} 頁）")


DOC = ("藥物頁待查清單（2026-11-12 建，比照 TwTxGNN docs/_data/drug_reviews_uncertain.json）。項目不刪：查清標 status: resolved"
       "（附 record_ids，指向 docs/_data/drug_reviews.json 的紀錄），查不出定論標 status: unresolvable（附 searched、unresolvable_reason）。")

if __name__ == "__main__":
    sys.exit(main())

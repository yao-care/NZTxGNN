#!/usr/bin/env python3
"""把藥物頁正文的刪改轉成查核紀錄（docs/_data/drug_reviews.json 的 action: correct）（2026-11-12 建，本站查核機制；範本 UsTxGNN）。

為什麼要有這支：
  2026-10-31～11-06 許可證修正時，正文裡跟新事實矛盾的句子是直接刪掉（只刪矛盾子句）的，
  頁面上看不出改過什麼，下次 scripts/sync_notes_to_docs.py 依 notes 重寫整頁時也會被蓋回去。
  用戶 2026-11-12 決定：這些刪改轉成查核紀錄（頁面上顯示 Review correction 框與 Review Log），以後的刪句也一樣。
  兩種用法共用同一套抽取規則：
    --git：依 git 提交前後的頁面轉（歷史的刪改，COMMITS 列出的提交）
    --dirs：依改寫前後兩個目錄轉（scripts/license_rebuild_run.py 刪句後呼叫）

抽取規則（只看正文）：
  - 去掉 front matter、查核框、程式區塊標記；表格、SHOW 句、NONE_LINE（程式產生的許可證文字）不算正文。
  - 正文切成段落、段落再切成行與句子，依序比對；有變的連續句子＝一筆（claim＝原句、replacement＝新句）。
  - 整句被刪（replacement 會是空的）：claim 往前（沒有前句就往後）多帶一句，replacement＝那一句，讓錨點在頁面上找得到；
    整段被刪：claim＝那一段、replacement 空、anchor＝前一段（頁面上要唯一）。
  - 同一處前後改過多次（例 10-31 刪了一半、11-05 再刪），依時間合併成一筆：claim＝最早的原句、replacement＝最後的版本。
理由（note 是頁面上顯示的英文、finding 是內部紀錄）：
  - scripts/license_sentence_fixes.json 的人工清單有這句（sentence／cut 落在 claim 裡）→ finding＝該筆 reason。
  - 其他依被刪文字分類（台灣模板殘留、單方／複方、全部註銷／停產、張數、產品描述），note 附快照裡目前的申請數與有效數。
狀態：
  - 頁面上找得到 replacement（或整段刪除時的 anchor）→ 照常顯示框。
  - 找不到、而且頁面在之後被 AI 重產過報告（REGEN_COMMITS）→ status: resolved_by_regeneration（紀錄保留、不顯示框）。
  - 其他找不到的 → 不寫入，列在輸出，交人工判斷。

用法（repo 根目錄）：
  python3 scripts/reviews_from_edits.py --git [--write]            # 歷史的刪改（COMMITS）
  python3 scripts/reviews_from_edits.py --git --also-dirs 前目錄 YYYY-MM-DD 說明 [--write]   # 歷史＋還沒 commit 的刪改一起合併
  python3 scripts/reviews_from_edits.py --dirs 前目錄 後目錄 --date YYYY-MM-DD --commit 說明 [--write]
沒有 --write 只列出結果，不寫檔。寫入後跑 python3 scripts/apply_drug_reviews.py。
"""
from __future__ import annotations

import difflib
import gzip
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "docs" / "_data" / "drug_reviews.json"
FIXES = ROOT / "scripts" / "license_sentence_fixes.json"
SNAP = ROOT / "snapshots" / "nz_licenses.json.gz"
sys.path.insert(0, str(ROOT / "scripts"))
try:
    import sync_license_tables as S  # noqa: E402  有程式許可證表的站才有
except ImportError:
    S = None
BEGIN, END = "<!-- licenses:begin -->", "<!-- licenses:end -->"
PROGRAM_TEXT = [re.compile(x) for x in ["^Showing \\d+ of \\d+ Pharmac Schedule items\\.( .*)?$", "^Source: Pharmac Pharmaceutical Schedule, Community Medicines .*$", "^Not listed on the Pharmac Pharmaceutical Schedule \\(Community Medicines\\) used for this report\\.$", "^Only part of the licence list is held in the data; the list may be incomplete\\.$"]]  # 程式產生、不算正文的整行文字（表下說明句等）

# 歷史的許可證刪改（git log docs/_drugs，2026-10-31～11-06；排程狀態格、排除表、AI 重產、description、noindex、表格空行不算）
# 本站逐筆確認：10-31 58ced9c7、11-06 80993f23／56ff65d7／827800db／e6c733d5／8445a9f3
COMMITS = ["58ced9c7", "80993f23", "56ff65d7", "827800db", "e6c733d5", "8445a9f3"]
# 期間內以 AI 整份重產報告的提交：之前的刪改在這些頁上可能已因重產而消失
REGEN_COMMITS = ["f0ae3471", "359bad11", "6dc082f7", "c2dcb0b7", "473ae2b9", "b63a7de7"]

SOURCES = [
    {"title": "Pharmac Pharmaceutical Schedule", "url": "https://schedule.pharmac.govt.nz/"},
]
SENT = re.compile(r'(?<=[.!?])\s+(?=[A-Z0-9(*"\[])|\n')
BLOCK_RE = re.compile(r"<!-- review:begin (\S+) -->.*?<!-- review:end \1 -->", re.S)


# ---------- 抽取 ----------
def body(t):
    t = t.replace("\r\n", "\n")
    if t.startswith("---"):
        e = t.find("\n---", 3)
        t = t[e + 4:] if e >= 0 else t
    t = BLOCK_RE.sub("", t)
    return t.replace(BEGIN, "").replace(END, "")


def is_prose(p):
    s = p.strip()
    return bool(s) and not s.startswith("|") and not s.startswith("{:") and not any(r.match(s) for r in PROGRAM_TEXT) \
        and not s.startswith("<!--") and not s.startswith("#")


def paras(t):
    """正文段落：空行分段；段落裡的標題行（# 開頭，例：清單後直接接「## Disclaimer」）也斷開，標題本身不算正文"""
    out = []
    for blk in re.split(r"\n[ \t]*\n", t):
        cur = []
        for ln in blk.split("\n"):
            if ln.startswith("#"):
                if cur:
                    out.append("\n".join(cur))
                cur = []
                continue
            cur.append(ln)
        if cur:
            out.append("\n".join(cur))
    return [p.strip("\n") for p in out if is_prose(p)]


def spans(p):
    out, i = [], 0
    for m in SENT.finditer(p):
        if m.start() > i:
            out.append((i, m.start()))
        i = m.end()
    if i < len(p):
        out.append((i, len(p)))
    return out


def in_para(a, b):
    """段落 a→b 有變的句子群 → [[(claim, replacement), …]]：每群由小到大的候選（往前後多帶相同的句子），
    claim 是 a 的子字串、replacement 是 b 的子字串；定稿時取第一個在頁面上唯一的候選"""
    sa, sb = spans(a), spans(b)
    ta, tb = [a[x:y] for x, y in sa], [b[x:y] for x, y in sb]
    out = []
    for op, i1, i2, j1, j2 in difflib.SequenceMatcher(None, ta, tb, autojunk=False).get_opcodes():
        if op == "equal":
            continue
        cands = []
        for k1 in range(0, 4):
            for k2 in range(0, 4):
                x1, x2, y1, y2 = i1 - k1, i2 + k2, j1 - k1, j2 + k2
                if x1 < 0 or y1 < 0 or x2 > len(ta) or y2 > len(tb) or y2 == y1:
                    continue
                ca0, ca1 = (sa[x1][0], sa[x2 - 1][1]) if x2 > x1 else (sa[x1][0], sa[x1][0]) if x1 < len(sa) else (len(a), len(a))
                cb0, cb1 = sb[y1][0], sb[y2 - 1][1]
                # 替換要剛好把 a 變成 b：claim 與 replacement 兩端緊鄰的字元要一樣（往後帶一句時，原句前的空格會留下、
                # 句後的換行被吃掉，套用後清單少一個換行；2026-11-12 Eu brigatinib）
                exact = a[ca0 - 1:ca0] == b[cb0 - 1:cb0] and a[ca1:ca1 + 1] == b[cb1:cb1 + 1]
                cands.append(((0 if exact else 1, k1 + k2, k2), (a[ca0:ca1], b[cb0:cb1])))
        if not cands:
            continue  # 段落只剩這句又整句刪：由段落層級處理
        cands = [c for _, c in sorted(cands, key=lambda x: x[0])]
        whole = [(f"\n{c}\n", f"\n{r}\n") for c, r in cands if c == a and r == b]  # 整段＝這幾句：前後帶換行，跟段落中間的同一句區分
        out.append(cands + whole)
    return out


def changes(before, after):
    """兩版頁面 → [(claim, replacement, anchor)]"""
    pa, pb = paras(body(before)), paras(body(after))
    out = []
    rb = body(before)

    def heading_of(p):
        """原段落所在章節的標題行（「## …」）；整段刪完、段落裡只剩程式表時當錨點（框放在標題下；2026-11-12 NZ cyclizine_lactate）"""
        k = rb.find(p)
        if k < 0:
            return ""
        hs = [m.group(0) for m in re.finditer(r"^#{2,3} .+$", rb[:k], re.M)]
        return hs[-1] if hs else ""

    def deleted(i, j1):
        """整段刪掉：錨點候選＝新版前後各三段，最後是所屬章節的標題（定稿時取第一個在頁面上唯一的）"""
        near = [pb[j] for j in (j1 - 1, j1, j1 - 2, j1 + 1, j1 - 3, j1 + 2) if 0 <= j < len(pb)]
        h = heading_of(pa[i])
        out.append([(pa[i], "", x) for x in near + ([h] if h else [])] or [(pa[i], "", "")])

    for op, i1, i2, j1, j2 in difflib.SequenceMatcher(None, pa, pb, autojunk=False).get_opcodes():
        if op == "equal" or op == "insert":
            continue  # 只有新增（沒有刪改）不是更正
        # 逐段配對：原段落依序配新段落裡最相似的一段（相似度 ≥0.5），配上的做段內比對，配不上的是整段刪除。
        # 不把多段接成一筆：段落之間常隔著表格，接起來的字串在頁面上不相連，套用時找不到（2026-11-12 atomoxetine 等 4 頁）
        j = j1
        for i in range(i1, i2):
            best, bj = 0.0, None
            for k in range(j, j2):
                r = difflib.SequenceMatcher(None, pa[i], pb[k], autojunk=False).ratio()
                wb, wa = re.findall(r"\w+", pb[k].lower()), set(re.findall(r"\w+", pa[i].lower()))
                if pb[k].strip() and (pb[k].strip() in pa[i] or (len(wb) >= 2 and set(wb) <= wa)):
                    r = max(r, 0.99)  # 新段落的字都在原段落裡（刪掉大半只剩一句，例 atropine「Five examples:」、
                    #                   ticagrelor「The drug is marketed, with 13 valid licenses, …」→「The drug is marketed.」）
                if r > best:
                    best, bj = r, k
            if bj is not None and best >= 0.5:
                for cands in in_para(pa[i], pb[bj]):
                    out.append([(c, r, "") for c, r in cands])
                j = bj + 1
            else:
                deleted(i, j)
    return out


# ---------- 理由 ----------
def load_fixes():
    if not FIXES.exists():
        return {}
    return json.loads(FIXES.read_text(encoding="utf-8"))


def snap_facts():
    if not SNAP.exists():
        return {}
    return {k: v.get("facts") or {} for k, v in json.loads(gzip.open(SNAP).read().decode("utf-8"))["drugs"].items()}


def removed_text(claim, repl):
    sm = difflib.SequenceMatcher(None, claim, repl, autojunk=False)
    return " ".join(claim[i1:i2] for op, i1, i2, j1, j2 in sm.get_opcodes() if op in ("delete", "replace"))


NUMW = r"(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|several|multiple|both|all)"
LIC = r"(?:licen[cs]es?|authori[sz]ations?|applications?|registrations?|ANDAs?|NDAs?|BLAs?|products?|approvals?|items?|listings?|DINs?|entries|records?)"


def classify(rm):
    if re.search(r"Taiwan|TFDA", rm, re.I):
        return "tw"
    # 有數字＋許可字（「with 20 valid single-ingredient licences」）先判張數：錯的是張數，單方字樣只是附帶（2026-11-12 Ca）
    if re.search(rf"\b{NUMW}\b[^.;]{{0,40}}\b{LIC}|\b{LIC}\b[^.;]{{0,30}}\b{NUMW}\b", rm, re.I):
        return "count"
    if re.search(r"single[- ]ingredient|combination|combo|multi-ingredient", rm, re.I):
        return "combo"
    if re.search(r"cancel|discontinu|withdrawn|revoked|no (?:valid|active|current)|not (?:currently )?(?:marketed|approved|valid|active)|only cancel", rm, re.I):
        return "cancel"
    if re.search(r"\bvalid\b", rm, re.I):
        return "count"
    if re.search(r"\b(?:as an?|available as|dosage form|topical|oral|inject|tablet|capsule|marketed)\b", rm, re.I):
        return "product"
    return "generic"


def figures(fc):
    if not fc or fc.get("total") is None:
        return ""
    t, v = fc["total"], fc.get("valid") or 0
    return (f" The Pharmaceutical Schedule lists {t} listing{'' if t == 1 else 's'} for this drug, {v} current; "
            "the Quick Overview and the table under New Zealand Market Information show the current figures.")


def note_for(kind, date, fc):
    d = f"the Pharmaceutical Schedule as of {date}"
    if kind == "tw":
        return "Removed: leftover template text about Taiwan's TFDA, which does not apply to this New Zealand page."
    if kind == "combo":
        return f"Removed: it described the products as single-ingredient or combination products in a way that did not match the Pharmac Schedule data ({d})."
    if kind == "cancel":
        return f"Removed: it said the Schedule listings were cancelled, discontinued, or not current, which did not match the Pharmac Schedule data ({d})." + figures(fc)
    if kind == "count":
        return f"Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data ({d})." + figures(fc)
    if kind == "product":
        return f"Removed: its description of the New Zealand products did not match the Pharmac Schedule data ({d})." + figures(fc)
    return f"Removed: it conflicted with the Pharmac Schedule data ({d})." + figures(fc)

def manual_reason(fixes, stem, claim):
    for it in fixes.get(stem) or []:
        s = (it.get("sentence") or "").strip()
        cuts = it.get("cut") or []
        cuts = [x.strip(" ,;") for x in (cuts if isinstance(cuts, list) else [cuts])]
        if (s and s in claim) or any(len(c) > 6 and c in claim for c in cuts):
            return it.get("reason") or ""
    return ""


# ---------- 合併與狀態 ----------
def compose(recs, cands, meta):
    """同一處前後改過多次 → 合併成一筆（claim＝最早原句、replacement＝最新版）。回傳是否已併入既有紀錄。
    合併用最小的候選比對；合併後候選只剩合併結果（再往外帶句子的候選要依新頁面重算，這裡不做）"""
    # 最小候選的 claim 是空的＝純新增（例 Ca 343992d0e 補回前一次連帶刪掉的簡介）：拿第一個有 claim 的候選來比
    claim, repl, anchor = next((c for c in cands if c[0]), cands[0])
    for r in recs:
        old = r["cands"][0][1]
        if old and claim and claim in old:
            new = [(c0, old.replace(claim, repl, 1), a0 or anchor) for c0, r0, a0 in r["cands"] if r0 == old] or \
                  [(r["cands"][0][0], old.replace(claim, repl, 1), anchor)]
            if not new[0][1]:  # 合成後整段刪光：錨點用這次刪除的全部錨點候選（含章節標題；2026-11-12 NZ cyclizine_lactate）
                anchors = list(dict.fromkeys([a for _, _, a in new if a] + [a for _, _, a in cands if a]))
                anchors.sort(key=lambda a: not a.startswith("#"))  # 章節標題優先：段落刪光時框放回原章節
                new = [(new[0][0], "", a) for a in anchors] or new
        elif old and claim and old in claim:
            new = [(c.replace(old, r["cands"][0][0], 1), rp, a) for c, rp, a in cands if old in c]
        else:
            continue
        r["cands"] = new
        r["_meta"].append(meta)
        return True
    return False


def git(*a):
    return subprocess.run(["git", *a], cwd=ROOT, capture_output=True, text=True).stdout


def show(rev, path):
    r = subprocess.run(["git", "show", f"{rev}:{path}"], cwd=ROOT, capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def commit_date(h):
    return git("log", "-1", "--format=%cs", h).strip()


def regen_pages():
    out = set()
    for h in REGEN_COMMITS:
        out |= {Path(f).name for f in git("diff", "--name-only", f"{h}^", h, "--", "docs/_drugs").split()}
    return out


def build(pairs):
    """pairs：[(檔名, 前文, 後文, 日期, 提交)]，依時間排序 → {檔名: [紀錄草稿]}"""
    by = {}
    for fname, before, after, date, commit in pairs:
        recs = by.setdefault(fname, [])
        for cands in changes(before, after):
            meta = {"date": date, "commit": commit}
            if not compose(recs, cands, meta):
                if not cands[0][0]:
                    continue  # 純新增、又不是補在既有紀錄上：不是更正，不轉成紀錄
                recs.append({"cands": cands, "_meta": [meta]})
    return by


def pick(cands, cur_body):
    """候選裡第一個能在目前頁面上唯一定位的：replacement（整段刪除時是 anchor）在頁面上剛好出現一次；
    或頁面上又是原句（claim 剛好出現一次，套用時會再更正）。都不行回 None"""
    for c, r, a in cands:
        loc = r or a
        if c == r or not c.strip():
            continue
        if loc and cur_body.count(loc) == 1:
            return c, r, a, "located"
    for c, r, a in cands:
        if c.strip() and c != r and cur_body.count(c) == 1 and (r or a):
            return c, r, a, "claim_on_page"
    return None


def finalize(by, current, regen, fixes, facts, existing_ids):
    """草稿 → 正式紀錄；回傳 (紀錄, 無法定位的草稿)"""
    out, lost = [], []
    for fname, recs in sorted(by.items()):
        stem = fname[:-3]
        cur = current(fname)
        if cur is None or 'http-equiv="refresh"' in cur[:3000]:
            continue
        cur_body = body(cur)
        n = 0
        for r in recs:
            got = pick(r["cands"], cur_body)
            if got is None:
                c0, r0, a0 = r["cands"][0]
                if c0 == r0 or not c0.strip():
                    continue
                if fname in regen:
                    got = (c0, r0, a0, "regen")
                else:
                    lost.append((fname, {"claim": c0, "replacement": r0}))
                    continue
            r = dict(r, claim=got[0], replacement=got[1], anchor=got[2], how=got[3])
            n += 1
            first, last = r["_meta"][0], r["_meta"][-1]
            rm = removed_text(r["claim"], r["replacement"])
            reason = manual_reason(fixes, stem, r["claim"])
            kind = classify(rm)
            rec = {
                "id": f"nz-lic-{first['date'].replace('-', '')}-{stem}-{n}",
                "drug": stem, "file": fname, "action": "correct",
                "claim": r["claim"], "replacement": r["replacement"],
                "note": note_for(kind, last["date"], facts.get(stem)),
                "finding": reason or f"依成分重建許可證後，正文跟新事實矛盾的子句由刪句工具刪除（分類：{kind}）",
                "sources": [dict(s, accessed=last["date"]) for s in SOURCES],
                "checked": last["date"],
                "summary": "Removed: \"" + " ".join((rm.strip(" ,;.\n-") or r["claim"]).split())[:140] + "\"",
                "method": ("2026-11-12 由 git 提交前後的頁面轉入查核紀錄（scripts/reviews_from_edits.py）；原本的刪改：" +
                           "、".join(f"{m['commit']}（{m['date']}）" for m in r["_meta"])),
            }
            if not r["replacement"]:
                rec["anchor"] = r["anchor"]
            if r["how"] == "regen":
                rec["status"] = "resolved_by_regeneration"
                rec["resolved_on"] = "2026-11-12"
                rec["resolved_note"] = "the report was regenerated in 2026-11 and no longer contains this statement"
            if rec["id"] in existing_ids:
                continue
            out.append(rec)
    return out, lost


def write(new):
    doc = json.loads(DATA.read_text(encoding="utf-8")) if DATA.exists() else {"_doc": DOC, "schema_version": 1, "records": []}
    have = {r["id"] for r in doc["records"]}
    doc["records"] += [r for r in new if r["id"] not in have]
    DATA.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


DOC = ("藥物頁人工查核紀錄（單一真實來源；2026-11-12 建，核心流程比照 TwTxGNN docs/_data/drug_reviews.json，欄位相同）。"
       "每筆＝一次查核：drug／file／claim（頁面原句）／replacement（更正後）／note（頁面上顯示的英文說明）／finding（內部查證結論）／"
       "sources（網址＋查閱日）／checked（查核日）／action（correct 更正｜annotate 加註｜rereview 待重審｜section 附來源段落｜"
       "rereview_result 重審結果｜header_level｜description_level）。由 scripts/apply_drug_reviews.py 套到 docs/_drugs/*.md；"
       "改寫頁面的程式結尾都會重套，scripts/check_seo_docs.py 第 17 項擋未套用的紀錄。原則：基本事實照主管機關資料更正；"
       "模型預測原文不刪不改寫，只加註；證據等級與決策只在重審結果為降級、升級或撤回時更新。"
       "2026-10-31～11-06 許可證修正時直接刪掉的矛盾子句（10-31 58ced9c7、11-06 80993f23／56ff65d7／827800db／e6c733d5／8445a9f3），2026-11-12 由 scripts/reviews_from_edits.py 依 git 前後版本轉入"
       "（id 以 nz-lic- 開頭）；報告在 11 月經 AI 重產、原句已不在頁面的標 status=resolved_by_regeneration。"
       "同 id 重發時前版存進 history（retired、reason），不覆蓋丟棄。")


def main(argv):
    fixes, facts = load_fixes(), snap_facts()
    existing = {r["id"] for r in json.loads(DATA.read_text(encoding="utf-8"))["records"]} if DATA.exists() else set()
    if "--git" in argv:
        pairs = []
        for h in COMMITS:
            d = commit_date(h)
            for f in git("diff", "--name-only", f"{h}^", h, "--", "docs/_drugs").split():
                b, a = show(f"{h}^", f), show(h, f)
                if b and a:
                    pairs.append((Path(f).name, b, a, d, h))
        if "--also-dirs" in argv:  # 還沒 commit 的刪改（改寫前目錄 → 目前頁面），接在歷史提交後面一起合併（同一處前後改過時合成一筆）
            i = argv.index("--also-dirs")
            bd, d, lab = Path(argv[i + 1]), argv[i + 2], argv[i + 3]
            pairs += [(f.name, f.read_text(encoding="utf-8"), (ROOT / "docs/_drugs" / f.name).read_text(encoding="utf-8"), d, lab)
                      for f in sorted(bd.glob("*.md")) if (ROOT / "docs/_drugs" / f.name).exists()]
        by = build(pairs)
        current = lambda fn: (ROOT / "docs/_drugs" / fn).read_text(encoding="utf-8") if (ROOT / "docs/_drugs" / fn).exists() else None
        recs, lost = finalize(by, current, regen_pages(), fixes, facts, existing)
    elif "--dirs" in argv:
        i = argv.index("--dirs")
        bd, ad = Path(argv[i + 1]), Path(argv[i + 2])
        d = argv[argv.index("--date") + 1]
        c = argv[argv.index("--commit") + 1] if "--commit" in argv else "license-rebuild"
        pairs = [(f.name, f.read_text(encoding="utf-8"), (ad / f.name).read_text(encoding="utf-8"), d, c)
                 for f in sorted(bd.glob("*.md")) if (ad / f.name).exists()]
        by = build(pairs)
        current = lambda fn: (ad / fn).read_text(encoding="utf-8") if (ad / fn).exists() else None
        recs, lost = finalize(by, current, set(), fixes, facts, existing)
    else:
        sys.exit(__doc__)
    act = sum(1 for r in recs if not r.get("status"))
    print(f"紀錄 {len(recs)} 筆（顯示框 {act}、報告重產後已無此錯誤 {len(recs) - act}），{len({r['file'] for r in recs})} 頁；"
          f"無法定位 {len(lost)} 筆")
    for fn, r in lost[:30]:
        print(f"  ? {fn}：{r['claim'][:90]!r} → {r['replacement'][:60]!r}")
    if "--write" in argv:
        write(recs)
        print(f"已寫入 {DATA.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

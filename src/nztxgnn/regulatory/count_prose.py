"""正文 Pharmac Schedule 品項張數的程式改寫（2026-11-24 建，用戶決定「正文張數自動改寫」；做法移植 AuTxGNN／KrTxGNN／MyTxGNN 同名檔，
規則是本站英文句型；Au 同為給付清單，句型最接近）。

正文裡「two New Zealand licences」「3 Pharmac Schedule items」「the 5 listings」這類 Pharmac 品項張數敘述，由程式依許可證快照
（snapshots/nz_licenses.json.gz 的 facts：total／valid／cancelled）改寫，每週資料更新後跟著改，不逐筆寫查核紀錄（同 Au／Kr／My／Tw：
程式改寫的張數敘述是程式產生的正文）。本站的本地資料是 Pharmac Pharmaceutical Schedule（社區用藥給付清單）的品項，不是 Medsafe 許可證：
事實層的 total＝本藥的 Pharmac 品項數（Pharmacode），valid＝仍在清單上的品項（現行全部＝total）；頁面只講筆數、不講有效。

只改「句型明確、數字確定是本藥 Pharmac 品項張數」的地方，其餘一律不動：
  - 數字（阿拉伯數字或 one～twenty）後面緊接（可隔 current／listed／New Zealand／Pharmac 等形容詞）名詞 item(s)／licence(s)／
    listing(s)／entry(ies)／Pharmacode(s)，或「N of M …」的 M；句子沒有試驗、文獻、病人、劑量、年份等字樣（NOT_LICENSE）。
  - 跳過：查核框（<!-- review -->）、程式許可證區塊（<!-- licenses -->）、表格列、標題、front matter；
    查核紀錄 replacement／anchor 所在的句子（改了紀錄會對不上）。
  - 整句不動（skipped）：有保留（at least、partial、may be incomplete…）、講證據包或查詢（Evidence Pack、query、returned、found…）、
    相對數（other、further、remaining、additional、another…）、劑型、廠商或單方／複方子集（single-ingredient、combination、tablet、eye drop、
    brand…）、fully subsidised 子集、其他國家或 Medsafe 口徑；講表格顯示筆數（table、above、below、shown…，reason＝display）；
    事實層 partial 或總數 0 的頁整頁不動。
角色：數字前面的形容詞或後面的狀態字有 current／valid／listed／active（not「no longer listed」）→有效；
  no longer listed／delisted／cancelled／not current→註銷；「N of M」的 M＝總數、N 看後面狀態字；其餘＝總數。
同一行只要有一句含品項數字而被略過，整行都不動。
數字已等於該角色的任一合理讀法（沒有限定詞的總數＝總數或有效數）就不動；不等於時兩種讀法相同才改，不同列為 ambiguous。
不改、列出（要改請寫查核紀錄）：原數字 0（zero-premise）、新數字 0（zero-result）、1 跟非 1 之間的改動（名詞與動詞單複數要跟著改，
grammar）、both（grammar）、句子說「all … valid／current」或「none … no longer listed」而事實不符（premise）、說只有一張（only／the sole，premise）。
"""
from __future__ import annotations

import re

SENT_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z*\[(>-])")
WORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
         "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17,
         "eighteen": 18, "nineteen": 19, "twenty": 20}
W_OUT = {v: k for k, v in WORDS.items()}
NOUN = r"(?:(?:Pharmac(?:eutical)?\s+)?(?:Schedule\s+)?items?|licen[cs]es?|listings?|entr(?:y|ies)|presentations?|Pharmacodes?)"
NUM = re.compile(r"(?<![\w.,/-])(\d{1,4}|" + "|".join(sorted(WORDS, key=len, reverse=True)) + r"|both)(?![\w%/-])", re.I)
LICENSE = re.compile(r"\bPharmac\b|\bSchedule\b|\blicen[cs]es?\b|\blistings?\b|\bPharmacodes?\b", re.I)
NOT_LICENSE = re.compile(r"\btrials?\b|\bstud(?:y|ies)\b|patients?|PMID|NCT|publication|articles?|case reports?|\bmg\b|\bml\b|%|"
                         r"\byears?\b|\bmonths?\b|\bweeks?\b|\bdoses?\b|score|interactions?|predict|candidates?|indications?|"
                         r"\b(?:19|20)\d\d\b|known uses?|ranked|rank\b", re.I)
HEDGED = re.compile(r"at least|\bpartial|incomplete|not complete|possibly|probably|may (?:be|not)|"
                    r"Evidence Pack|\bquer|returned|retrieved|\bfound\b|identified|\bsearch|"
                    r"\bother\b|\bfurther\b|remaining|additional|another|\brest\b|\bincluding\b|\bincludes?\b|\bamong\b|\bexcept\b|"
                    r"single[- ]ingredient|single[- ]agent|combination|combined|co-?formulat|co-?packaged|fixed-dose|\bwith\b|"
                    r"tablets?|capsules?|injections?|injectable|infusion|solution|suspension|powder|syringe|\bpens?\b|drops?|"
                    r"cream|ointment|gel\b|inhal|patch|implant|spray|oral|topical|ophthalmic|intravenous|subcutaneous|vaginal|rectal|"
                    r"Section 29|veterinar|Medsafe|\bTGA\b|\bFDA\b|\bUS\b|United States|Australia|another country|other countries|"
                    r"subsidi[sz]ed|funded|special authority|hospital|"
                    r"manufacturer|supplier|sponsor|brand|generic|strengths?", re.I)
DISPLAY = re.compile(r"\btable\b|\babove\b|\bbelow\b|\bshown\b|\bshows\b|\bfirst\b|\bdisplayed\b|\blisted here\b|\bnamed\b", re.I)
VALID = r"(?:current(?:ly)?|valid|active|listed|marketed)"
CANC = r"(?:no longer (?:listed|current|on)|delisted|de-listed|cancell?ed|not current(?:ly listed)?|not listed|discontinued|withdrawn)"
ADJ = r"(?:(?:current|valid|active|listed|registered|recorded|New\s+Zealand|NZ|Pharmac-listed|Pharmac|Schedule|community)\s+)*"
BLOCK = re.compile(r"<!-- (review|licenses):begin.*?<!-- \1:end[^>]*-->", re.S)


def _val(tok):
    t = tok.lower()
    if t.isdigit():
        return int(t)
    if t == "both":
        return 2
    return WORDS[t]


def _fmt(tok, v):
    if tok.isdigit() or v not in W_OUT:
        return str(v)
    w = W_OUT[v]
    return w.capitalize() if tok[:1].isupper() else w


def _role(s, a, b):
    left, right = s[max(0, a - 40):a], s[b:b + 80]
    # 「N of M Pharmac items」的 M＝總數（前面是數字或 N valid of M）
    if re.search(r"\bof\s+(?:the\s+)?(?:total\s+)?$", left, re.I):
        pre = re.sub(r"\bof\s+(?:the\s+)?(?:total\s+)?$", "", left, flags=re.I)
        if re.search(r"(?:\d+|" + "|".join(WORDS) + r")\s*(?:" + ADJ + NOUN + r"\s*)?$", pre, re.I) \
                and re.match(r"\s+" + ADJ + NOUN + r"\b", right, re.I):
            return "total"
        return None
    # 「N of M …」的 N：看後面的狀態字
    m = re.match(r"\s+of\s+(?:the\s+)?(?:\d+|\w+)\s+" + ADJ + NOUN + r"\s*(?:\([^)]*\)\s*)?(?:(?:are|is|remain|were)\s+)?(?:(?:still|currently|now|also)\s+)?(" + CANC + "|" + VALID + ")", right, re.I)
    if m:
        return "cancelled" if re.match(CANC, m.group(1), re.I) else "valid"
    if re.match(r"\s+of\s+", right, re.I):
        return None
    m = re.match(r"\s+(" + ADJ + r")" + NOUN + r"\b([^.;:]{0,40})", right, re.I)
    if m:
        adj, tail = m.group(1).lower(), m.group(2)
        if re.search(r"\b(?:current|valid|active|listed|pharmac-listed)\b", adj):
            return "valid"
        if re.match(r"\s*(?:are|is|were|was|have been|has been)\s+(?:all\s+|now\s+|also\s+)?" + CANC, tail, re.I):
            return "cancelled"
        if re.match(r"\s*(?:are|is|remain)\s+(?:all\s+|still\s+)?(?:currently\s+)?(?:valid|current|active)\b", tail, re.I):
            return "valid"
        return "total"
    # 「(8 listed, 1 no longer listed)」「7 valid」
    if re.match(r"\s+" + CANC + r"\b", right, re.I):
        return "cancelled"
    if re.match(r"\s+(?:valid|current|currently listed|listed|active)\b(?!\s+(?:on|in|under|as|for|by)\b)", right, re.I) \
            and re.search(r"[(,;:]\s*(?:\w+\s+)?$|\band\s+$", left):
        return "valid"
    return None


def _readings(role, f, explicit):
    t, v, c = f.get("total"), f.get("valid"), f.get("cancelled")
    if role == "cancelled":
        return {c}, c
    if role == "valid":
        return {v}, v
    if explicit:
        return {t}, t
    return {t, v}, (t if t == v else None)


def _premise_ok(sent, f):
    lo = sent.lower()
    if re.search(r"\b(all|every|each)\b[^.;]{0,60}\b(valid|current|listed|active)\b", lo) and not re.search(CANC, lo) \
            and f.get("valid") != f.get("total"):
        return False
    if re.search(r"\b(none|no)\b[^.;]{0,40}" + CANC, lo) and f.get("cancelled"):
        return False
    if re.search(r"\b(only|sole|single)\b", lo):
        return False
    return True


def rewrite_sentence(sent, f):
    """回傳 (新句子, 跳過理由或 None)。"""
    if not LICENSE.search(sent) or not NUM.search(sent):
        return sent, None
    found = [(m, _role(sent, m.start(), m.end())) for m in NUM.finditer(sent)]
    found = [(m, r) for m, r in found if r]
    if not found:
        return sent, None
    if NOT_LICENSE.search(sent):
        return sent, None
    if HEDGED.search(sent):
        return sent, "hedged/subset"
    explicit = any(r in ("valid", "cancelled") for _, r in found)
    if DISPLAY.search(sent):  # 講表格顯示筆數：不改；數字跟事實對不上時列出（reason＝display）
        bad = any(_val(m.group(1)) not in _readings(r, f, explicit)[0] for m, r in found)
        return sent, ("display" if bad else "display-ok")
    subs = []
    for m, role in found:
        old = _val(m.group(1))
        ok, v = _readings(role, f, explicit)
        if old in ok:
            continue
        if v is None:
            return sent, "ambiguous"
        if old == 0:
            return sent, "zero-premise"
        if v == 0:
            return sent, "zero-result"
        if (old == 1) != (v == 1) or m.group(1).lower() == "both":
            return sent, "grammar"
        subs.append((m, v))
    if not _premise_ok(sent, f) and (subs or re.search(r"\b(all|every|each|none|no)\b", sent, re.I)):
        return sent, "premise"  # 張數對、但「all … listed」「none … no longer listed」已不成立：也列出（不改）
    if not subs:
        return sent, None
    out, pos = [], 0
    for m, v in subs:
        out.append(sent[pos:m.start(1)])
        out.append(_fmt(m.group(1), v))
        pos = m.end(1)
    out.append(sent[pos:])
    return "".join(out), None


def _line(ln, f, protect, skipped):
    if ln.startswith(("|", "#", "{:", ">")) or not ln.strip():
        return ln
    parts = SENT_SPLIT.split(ln)
    seps = SENT_SPLIT.findall(ln)
    new, held = [], False
    for s in parts:
        if any(p and (p in s or s in p) for p in protect):
            new.append(s)
            continue
        ns, why = rewrite_sentence(s, f)
        if why and why != "display-ok":
            skipped.append((why, s.strip()))
            held = True
        new.append(ns)
    if held:  # 同一行有句子判斷不了：整行不動
        return ln
    out = new[0]
    for sep, s in zip(seps, new[1:]):
        out += sep + s
    return out


def rewrite(text, facts, protect=()):
    """text：整頁 markdown。facts：快照的 facts。回傳 (新內容, skipped)。front matter、查核框、程式區塊、表格不動。"""
    skipped = []
    if not facts or facts.get("partial") or not facts.get("total"):
        return text, skipped
    protect = [p.strip() for p in protect if p and len(p.strip()) >= 6]
    fm_end = 0
    if text.startswith("---"):
        j = text.find("\n---", 3)
        fm_end = j + 4 if j > 0 else 0
    body = text[fm_end:]
    keep = []

    def hold(m):
        keep.append(m.group(0))
        return f"\x00{len(keep) - 1}\x00"
    body = BLOCK.sub(hold, body)
    lines = [ln if "\x00" in ln else _line(ln, facts, protect, skipped) for ln in body.split("\n")]
    body = re.sub(r"\x00(\d+)\x00", lambda m: keep[int(m.group(1))], "\n".join(lines))
    return text[:fm_end] + body, skipped

#!/usr/bin/env python3
"""藥物頁「作用機轉資料不可得」句改成附出處的作用機轉（2026-11-19 建）。

為什麼要有這支：
  藥師報告的「Why is This Prediction Reasonable?」段，在證據包沒有 DrugBank 作用機轉文字時（drugbank.source＝vocab_only），
  AI 寫成「Detailed mechanism of action data is not available in the data compiled for this report.」。
  2026-11-19 量測：461 個公開藥物頁有 409 頁這樣寫；搜尋「<藥名> mechanism of action」的人點進來看到的是「沒有資料」。
  作用機轉是藥物本身的基本藥理事實（不是模型預測），所以照查核機制補：每一段都摘自官方仿單、附可點的出處與取用日期，
  不自己寫作用機轉。

資料來源（選擇理由見 CLAUDE.md「作用機轉段的資料來源與查核機制」）：
  1. 美國 FDA 核准仿單（openFDA drug label API；連結到 NLM DailyMed 同一份 SPL）：PLR 格式的「12.1 Mechanism of Action」
     （mechanism_of_action 欄），舊格式仿單沒有這欄時用「Clinical Pharmacology」段開頭的藥理說明。
     只用單一成分、主成分名（去鹽類）跟本頁相同的處方藥仿單；多份時取有 12.1 欄、版本最新的。
     文字照仿單原句節錄 2～4 句（拿掉「[see …]」交互參照與段落編號），不改寫。
  2. 美國沒有仿單的藥：ChEMBL 的 drug mechanism 表（結構化：作用機轉、標靶、作用類型；CC BY-SA 3.0，附出處）。
  3. 兩者都沒有：不補，寫進 docs/_data/drug_reviews_uncertain.json（source＝mechanism of action），列原因。
  Medsafe Data Sheet（紐西蘭主管機關仿單）最適合，但 Medsafe 的 Data Sheet 查詢（/DbSearch/InfoSearch）是 Blazor Server 互動頁、
  沒有可下載的清單或 API，2026-11-19 從雲端無法依藥名逐一取得 PDF 網址，所以沒用；之後能取得時應優先換成 Medsafe。

紀錄：docs/_data/drug_reviews.json 的 action＝mechanism（id：nz-moa-<查核日>-<藥>-1）。claim＝頁面上「不可得」那句
  （或句子裡「不可得」子句加連接詞，句子後半是 AI 的推論，保留、首字母大寫），replacement＝附出處的作用機轉＋保留的後半句。
  scripts/apply_drug_reviews.py 套用（可重複執行）；頁面「Review Log」表列原句與出處。模型預測與其他正文不動。

用法：
  python3 scripts/build_mechanism_reviews.py --cache <目錄>            # 抓來源、寫紀錄與待查清單（已有紀錄的頁不重抓）
  python3 scripts/build_mechanism_reviews.py --cache <目錄> --dry-run  # 只印結果
  python3 scripts/build_mechanism_reviews.py --list                    # 只列出公開頁仍寫「不可得」的句子（不連網）
"""
import argparse
import datetime as dt
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DRUGS = ROOT / "docs" / "_drugs"
REVIEWS = ROOT / "docs" / "_data" / "drug_reviews.json"
UNCERTAIN = ROOT / "docs" / "_data" / "drug_reviews_uncertain.json"
BUNDLES = ROOT / "data" / "bundles"
SECTION = "## Why is This Prediction Reasonable?"
UNCERTAIN_SOURCE = "mechanism of action"

# 「不可得」子句：Detailed mechanism(-)of(-)action data/text/entry … is/are/was/were not available …（到句號、逗號＋連接詞或分號為止）
NA_CLAUSE = (r"(?:Currently, )?(?:[Dd]etailed|A detailed) mechanism[- ]of[- ]action (?:data|text|entry|information)"
             r"[^.;\n]*?\b(?:is|are|was|were) (?:currently )?not available[^.;,\n]*")
NA_SENTENCE = re.compile(r"(?:(?<=\n)|(?<=\. )|(?<=\*\* )|^)(" + NA_CLAUSE + r")(\.|, (?:so|but|and) |; )", re.M)
NA_ANY = re.compile(r"mechanism[- ]of[- ]action[^.\n|]*?\bnot available", re.I)

SALTS = {"hydrochloride", "hcl", "dihydrochloride", "sodium", "potassium", "calcium", "magnesium", "maleate", "mesylate",
         "mesilate", "besylate", "besilate", "sulfate", "sulphate", "acetate", "citrate", "tartrate", "bitartrate",
         "fumarate", "succinate", "phosphate", "bromide", "chloride", "hydrobromide", "monohydrate", "dihydrate",
         "trihydrate", "hemihydrate", "anhydrous", "disodium", "dipropionate", "propionate", "valerate", "furoate",
         "lactate", "gluconate", "nitrate", "tosylate", "hyclate", "malate", "pamoate", "decanoate", "enanthate",
         "cypionate", "embonate", "xinafoate", "trometamol", "tromethamine", "meglumine", "lysine", "arginine",
         "hydrogen", "dimesylate", "oxalate", "salicylate", "camsylate", "napsylate", "base", "usp", "acetonide", "hexacetonide"}
# 酯類是不同的藥（fluticasone furoate ≠ fluticasone propionate）：頁名有酯類字樣時，仿單要有同一個字樣
ESTERS = {"propionate", "dipropionate", "furoate", "valerate", "acetonide", "hexacetonide", "decanoate", "enanthate", "cypionate"}


def ester_ok(page_name, label_name):
    pe = {t for t in re.split(r"[^a-z]+", page_name.lower()) if t in ESTERS}
    le = {t for t in re.split(r"[^a-z]+", label_name.lower()) if t in ESTERS}
    return not pe or pe == le


def norm_base(name):
    s = re.sub(r"-[a-z]{4}\b", "", name.lower())  # 生物相似藥的 4 字母字尾（insulin glargine-yfgn）
    toks = [t for t in re.split(r"[^a-z0-9]+", s) if t and t not in SALTS]
    return " ".join(toks)


def public_pages():
    for p in sorted(DRUGS.glob("*.md")):
        t = p.read_text(encoding="utf-8")
        fm = t.split("---", 2)[1] if t.startswith("---") else ""
        if re.search(r"^layout:\s*id_redirect|^noindex:\s*true", fm, re.M) or 'http-equiv="refresh"' in t[:3000]:
            continue
        yield p, t, fm


def section_body(text):
    i = text.find(SECTION)
    if i < 0:
        return None, -1
    j = text.find("\n## ", i + len(SECTION))
    return text[i:j if j > 0 else len(text)], i


def find_claim(text):
    """回傳 (claim, tail)：claim 是要換掉的原文（不可得子句＋句號或連接詞），tail 是 replacement 要接回的後半句開頭（空＝整句）。"""
    body, _ = section_body(text)
    if body is None:
        return None, None
    blocks = [m.span() for m in re.finditer(r"<!-- review:begin (\S+) -->.*?<!-- review:end \1 -->", body, re.S)]
    for m in NA_SENTENCE.finditer(body):
        if any(a <= m.start() < b for a, b in blocks):
            continue
        clause, conn = m.group(1), m.group(2)
        if conn == ".":
            return clause + ".", ""
        nxt = body[m.end():m.end() + 1]
        return clause + conn + nxt, nxt.upper()
    return None, None


def front(fm, key):
    m = re.search(rf'^{key}:\s*"?([^"\n]+)"?', fm, re.M)
    return m.group(1).strip() if m else ""


def http_json(url, cache, key, tries=4):
    f = cache / (re.sub(r"[^\w.-]+", "_", key)[:150] + ".json")
    if f.exists():
        return json.loads(f.read_text(encoding="utf-8"))
    for k in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "txgnn-site-review/1.0", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=60) as r:
                data = json.loads(r.read().decode("utf-8"))
            break
        except urllib.error.HTTPError as e:
            if e.code == 404:
                data = {}
                break
            if k == tries - 1:
                raise
            time.sleep(5 * (k + 1))
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(5 * (k + 1))
    f.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    time.sleep(0.3)
    return data


LEAD_HEAD = re.compile(r"^(?:(?i:Pharmacologic Properties|ACTIONS?|Mechanisms? of action)\s*|\d+(?:\.\d+)*\.?\s*|CLINICAL PHARMACOLOGY\s*|Clinical Pharmacology\s*|Mechanisms? of (?:\w+ )?Action\s*|"
                       r"MECHANISM OF ACTION\s*|Pharmacodynamics\s*|General\s*|Action\s*|Actions\s*|[:;,.\-–]\s*|[a-z]\s+(?=[A-Z]))*")


def clean_label(s):
    s = re.sub(r"\[\s*see [^\]]*\]", "", s, flags=re.I)
    s = re.sub(r"\(\s*\d+(?:\.\d+)*\s*\)", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = LEAD_HEAD.sub("", s)  # 段落編號與小標（「12.1 Mechanism of Action」「Pharmacodynamics」）
    s = re.sub(r"\s+([.,;:])", r"\1", s)
    return s.strip()


ABBR = re.compile(r"(?:\b(?:e\.g|i\.e|approx|ca|vs|Fig|al|No|U\.S|Inc|Ltd)\.|\b[A-Z]\.)$")


def sentences(s):
    out, cur = [], ""
    for part in re.split(r"(?<=[.!?])\s+(?=[A-Z(\[0-9])", s):
        cur = f"{cur} {part}".strip() if cur else part
        if ABBR.search(cur):
            continue
        out.append(cur)
        cur = ""
    if cur:
        out.append(cur)
    return out


MOA_WORDS = re.compile(r"inhibit|block|bind|agonist|antagonist|receptor|enzyme|mechanism|acts? |suppress|stimulat|"
                       r"interfer|chelat|analog|reduc|increas|decreas|depolari|alkylat|crosslink|cross-link|synthesis|activity|mode of action|"
                       r"neurotransmission|neuronal transmission|anticholinergic|antihistamin|vasodilat|fungicid|bactericid", re.I)
MOA_STRICT = re.compile(r"inhibit|block|binds?\b|binding|agonist|antagonist|receptor|enzyme|mechanism|acts? |suppress|stimulat|"
                        r"interfer|chelat", re.I)
START_BAD = re.compile(r"\bdo(?:es)? not\b|\bresistan|^In clinical (?:studies|trials)", re.I)
PK_WORDS = re.compile(r"absorb|absorption|half-life|bioavailab|plasma concentration|peak|volume of distribution|excret|"
                      r"following (?:oral|intravenous) administration|after (?:oral|intravenous) administration|metaboli[sz]ed in|metabolism|serum levels|blood levels|radioactivity", re.I)
STOP_HEAD = re.compile(r"\b(?:Pharmacodynamics|Pharmacokinetics|Absorption|Distribution|Metabolism|Elimination|"
                       r"Microbiology|Mechanism of Resistance|Cardiac Electrophysiology|Resistance(?= [A-Z])|Cross[- ][Rr]esistance|"
                       r"Antiviral Activity|Antibacterial Activity|Antimicrobial Activity|Antifungal Activity|Activity in [Cc]ell)\b")


def excerpt(text, from_cp=False, key=""):
    s = re.sub(r"\s+", " ", text)
    if from_cp:
        # Clinical Pharmacology 段：有「Mechanism of Action」小標（標題大小寫、不是句中的 mechanism of action）就從那裡開始
        m = re.search(r"(?:^|(?<=[.:)] )|(?<=\d ))Mechanisms? of Action\s*:?\s*(?=[A-Z])", s)
        headed = bool(m)
        if m:
            s = s[m.end():]
    s = clean_label(s)
    # 到下一個小標（Pharmacokinetics、12.3 …）為止
    m = re.search(r"(?:\b\d+(?:\.\d+)+\s+[A-Z]|" + STOP_HEAD.pattern + ")", s)
    if m and m.start() > 0:
        s = s[:m.start()].strip()
    sents = [x for x in sentences(s) if len(x) > 15]
    if from_cp:
        # 舊格式 Clinical Pharmacology 段常先講藥動學：沒有「Mechanism of Action」小標時，第一句要講作用、不講藥動學，
        # 否則不用；節錄到第一句藥動學句為止（只取連續的句子，不跳著挑）
        if not headed:
            # 沒有小標：從前 12 句裡第一句講作用、不講藥動學的句子開始（連續節錄，不跳著挑）
            start = next((i for i, x in enumerate(sents[:12]) if (MOA_WORDS if i == 0 else MOA_STRICT).search(x)
                          and not PK_WORDS.search(x) and not START_BAD.search(x)
                          and (not key or key in x.lower() or i == 0)), None)
            if start is None:
                return ""
            sents = sents[start:]
        cut = next((i for i, x in enumerate(sents) if PK_WORDS.search(x)), len(sents))
        sents = sents[:cut]
    picked = []
    for x in sents[:4]:
        if picked and len(" ".join(picked + [x])) > 650:
            break
        picked.append(x)
        if len(picked) >= 2 and len(" ".join(picked)) >= 220:
            break
    # 節錄要講到本藥：前面幾句只講生理背景（例 candesartan 先講 angiotensin II）時，接上第一句提到本藥的句子（中間以「[…]」標示省略）
    if key and not any(key in x.lower() for x in picked):
        later = next((x for x in sents[len(picked):8] if key in x.lower()), None)
        if later:
            picked = picked[:2] + ["[…]", later]
    txt = " ".join(picked).strip()
    if not txt or not txt.endswith((".", ")")) or len(txt) < 20:
        return ""
    if re.search(r"homeopath", txt, re.I):
        return ""
    return txt


# 紐西蘭用 INN／英式寫法，美國仿單用 USAN：只收同一個藥的寫法（查仿單時用，頁面上的藥名不改）
US_NAME = {"clomifene": "clomiphene", "paracetamol": "acetaminophen", "salbutamol": "albuterol", "adrenaline": "epinephrine",
           "noradrenaline": "norepinephrine", "glibenclamide": "glyburide", "frusemide": "furosemide", "ciclosporin": "cyclosporine",
           "lignocaine": "lidocaine", "levothyroxine": "levothyroxine", "thyroxine": "levothyroxine", "rifampicin": "rifampin",
           "cefalexin": "cephalexin", "amoxicillin clavulanate": "amoxicillin clavulanate", "oestradiol": "estradiol",
           "colecalciferol": "cholecalciferol", "ergocalciferol": "ergocalciferol", "dosulepin": "dothiepin",
           "mesalazine": "mesalamine", "sulfasalazine": "sulfasalazine", "aciclovir": "acyclovir", "valaciclovir": "valacyclovir",
           "ganciclovir": "ganciclovir", "bendroflumethiazide": "bendroflumethiazide", "chlorphenamine": "chlorpheniramine",
           "trimeprazine": "trimeprazine", "pethidine": "meperidine", "methylthioninium": "methylene blue",
           "isoprenaline": "isoproterenol", "orciprenaline": "metaproterenol", "dexamfetamine": "dextroamphetamine",
           "glyceryl trinitrate": "nitroglycerin", "beclometasone": "beclomethasone", "flucloxacillin": "flucloxacillin",
           "sodium cromoglicate": "cromolyn", "cromoglicate": "cromolyn", "sodium cromoglycate": "cromolyn",
           "tretinoin": "tretinoin", "busulfan": "busulfan", "ciclopirox": "ciclopirox", "phenobarbitone": "phenobarbital",
           "procaine benzylpenicillin": "penicillin g procaine", "benzylpenicillin": "penicillin g", "phenoxymethylpenicillin": "penicillin v",
           "amylobarbitone": "amobarbital", "thiopentone": "thiopental", "suxamethonium": "succinylcholine",
           "levomepromazine": "methotrimeprazine", "pizotifen": "pizotyline", "hydroxocobalamin": "hydroxocobalamin",
           "ipratropium": "ipratropium", "tiotropium": "tiotropium", "zuclopenthixol": "zuclopenthixol",
           "metamizole": "dipyrone", "cinacalcet": "cinacalcet", "sodium valproate": "valproate", "valproic acid": "valproic acid",
           "dimeticone": "dimethicone", "povidone iodine": "povidone-iodine", "liquid paraffin": "mineral oil",
           "chlormethine": "mechlorethamine", "mitozantrone": "mitoxantrone", "teicoplanin": "teicoplanin",
           "calcitriol": "calcitriol", "alfacalcidol": "alfacalcidol", "lysine acetylsalicylate": "aspirin", "benzatropine": "benztropine",
           "trastuzumab deruxtecan": "fam trastuzumab deruxtecan", "enfortumab vedotin": "enfortumab vedotin",
           "acetylsalicylic acid": "aspirin", "nicotinic acid": "niacin", "nicotinamide": "niacinamide"}


def label_source(inn, cache):
    src, why = _label_source(inn, cache)
    us = US_NAME.get(norm_base(inn))
    if not src and us and us != norm_base(inn):
        src, why2 = _label_source(us, cache)
        why = why if src is None else ""
        if src:
            src["us_name"] = us
    return src, why


def _label_source(inn, cache):
    base = norm_base(inn)
    if not base:
        return None, "no INN"
    q = " AND ".join(f'openfda.generic_name:"{t}"' for t in base.split())
    url = "https://api.fda.gov/drug/label.json?" + urllib.parse.urlencode({"search": q, "limit": 100})
    d = http_json(url, cache, "fda_" + base)
    cands = []
    for r in d.get("results", []):
        o = r.get("openfda", {})
        gens = o.get("generic_name") or []
        subs = o.get("substance_name") or []
        if len(gens) != 1 or len(subs) > 1 or re.search(r",| AND |\band\b", gens[0], re.I):
            continue
        if norm_base(gens[0]) != base or not ester_ok(inn, gens[0]):
            continue
        if "HUMAN PRESCRIPTION DRUG" not in (o.get("product_type") or []):
            continue
        moa = (r.get("mechanism_of_action") or [""])[0]
        key = base.split()[0]
        ex, field = (excerpt(moa, key=key), "12.1 Mechanism of Action") if moa else ("", "")
        if not ex:
            ex, field = excerpt((r.get("clinical_pharmacology") or [""])[0], from_cp=True, key=key), "Clinical Pharmacology"
        # 抗感染藥的 12.1 常只有一句「X is an antibacterial drug」，作用機轉寫在 12.4 Microbiology 的「Mechanism of Action」小標下
        # （有的仿單 Microbiology 是獨立欄，有的併在 Clinical Pharmacology 段裡）
        micro = re.sub(r"\s+", " ", (r.get("microbiology") or [""])[0])
        if not micro:
            cp = re.sub(r"\s+", " ", (r.get("clinical_pharmacology") or [""])[0])
            mi = re.search(r"\bMicrobiology\b", cp)
            micro = cp[mi.end():] if mi else ""
        mm = re.search(r"Mechanism of (?:\w+ )?Action\s*:?\s*(?=[A-Z])", micro)
        if not ex and mm:
            ex, field = excerpt(micro[mm.end():], key=key), "Clinical Pharmacology; Microbiology"
        if not ex:
            continue
        if len(ex) < 120 and mm and not field.endswith("Microbiology"):
            ex2 = excerpt(micro[mm.end():], key=key)
            if ex2:
                ex, field = f"{ex} […] {ex2}", ("12.1 Mechanism of Action; 12.4 Microbiology" if field.startswith("12.1")
                                                 else "Clinical Pharmacology; Microbiology")
        if len(ex) < 40:
            continue
        brand = (o.get("brand_name") or [""])[0]
        branded = 1 if brand and norm_base(brand) != base else 0  # 有品牌名的多半是原廠仿單
        routes = set(o.get("route") or [])
        systemic = 1 if routes & {"ORAL", "INTRAVENOUS", "INTRAMUSCULAR", "SUBCUTANEOUS", "SUBLINGUAL", "BUCCAL"} else 0
        # 排序：全身給藥優先（外用仿單常寫「X 治療痤瘡的機轉不明」，不是藥物本身的作用機轉）→ 有 12.1 → 原廠 → 版本新
        cands.append((systemic, 1 if len(ex) >= 120 else 0, 1 if field.startswith("12.1") else 0, branded,
                      r.get("effective_time", ""), r, ex, field))
    combo = False
    if not cands:
        # 美國只有複方仿單（例 glecaprevir／pibrentasvir、sacubitril／valsartan）：只節錄 12.1 裡講本藥成分的句子
        key = base.split()[0]
        for r in d.get("results", []):
            o = r.get("openfda", {})
            gens = o.get("generic_name") or []
            if len(gens) != 1 or "HUMAN PRESCRIPTION DRUG" not in (o.get("product_type") or []):
                continue
            parts = [norm_base(x) for x in re.split(r",|\band\b|/", gens[0], flags=re.I)]
            if base not in parts or len(parts) < 2:
                continue
            moa = (r.get("mechanism_of_action") or [""])[0]
            sents = [x for x in sentences(clean_label(moa)) if re.search(rf"\b{re.escape(key)}\b", x, re.I) and len(x) > 30
                     and MOA_WORDS.search(x)
                     and not re.search(r"described below|individual components|combination product containing|metabolism of|plasma levels", x, re.I)]
            if not sents:
                continue
            ex = " ".join(sents[:2]) if len(" ".join(sents[:2])) <= 650 else sents[0]
            if not ex.endswith((".", ")")):
                continue
            brand = (o.get("brand_name") or [""])[0]
            cands.append((1, 1, 1, 1 if brand else 0, r.get("effective_time", ""), r, ex, "12.1 Mechanism of Action"))
        combo = bool(cands)
    if not cands:
        return None, f"no single-ingredient US prescription label for '{base}' with a mechanism or clinical pharmacology section in openFDA"
    cands.sort(key=lambda c: c[:5], reverse=True)
    eff, r, ex, field = cands[0][4:]
    o = r["openfda"]
    brand = (o.get("brand_name") or [""])[0]
    gen = o["generic_name"][0].lower()
    mfr = (o.get("manufacturer_name") or [""])[0]
    title = f"DailyMed: {brand.title() + ' (' + gen + ')' if brand and brand.lower() != gen else gen} prescribing information" + (f", {mfr}" if mfr else "")
    return {
        "kind": "us_label",
        "title": title,
        "url": f"https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid={r['set_id']}",
        "set_id": r["set_id"],
        "label_version": f"{eff[:4]}-{eff[4:6]}-{eff[6:8]}" if len(eff) == 8 else eff,
        "section": field,
        "excerpt": ex,
        "via": "openFDA drug label API (https://open.fda.gov/apis/drug/label/)",
        "combination": gen if combo else "",
    }, ""


def chembl_source(inn, cache):
    base = norm_base(inn)
    url = "https://www.ebi.ac.uk/chembl/api/data/molecule.json?" + urllib.parse.urlencode({"pref_name__iexact": base, "limit": 10})
    d = http_json(url, cache, "chembl_pref_" + base)
    mol = None
    for m in d.get("molecules", []):
        names = {norm_base(m.get("pref_name") or "")} | {norm_base(s.get("molecule_synonym", "")) for s in m.get("molecule_synonyms") or []}
        if base in names and str(m.get("max_phase")) in ("4", "4.0"):
            mol = m
            break
    if not mol:
        return None, f"no approved ChEMBL molecule named '{base}'"
    cid = mol["molecule_chembl_id"]
    mech = http_json("https://www.ebi.ac.uk/chembl/api/data/mechanism.json?" + urllib.parse.urlencode(
        {"molecule_chembl_id": cid, "limit": 20}), cache, "chembl_mech_" + cid).get("mechanisms", [])
    rows = []
    for m in mech:
        moa = (m.get("mechanism_of_action") or "").strip()
        if moa and moa not in rows:
            rows.append(moa)
    if not rows:
        return None, f"ChEMBL {cid} has no mechanism entry"
    return {
        "kind": "chembl",
        "title": f"ChEMBL {cid} ({mol.get('pref_name', '').lower()}): drug mechanism",
        "url": f"https://www.ebi.ac.uk/chembl/explore/compound/{cid}",
        "chembl_id": cid,
        "excerpt": "; ".join(rows[:4]),
        "via": "ChEMBL web services (https://www.ebi.ac.uk/chembl/api/data/docs), CC BY-SA 3.0",
    }, ""


def md_safe(t):
    """仿單原文放進 markdown 段落：避免被當成 HTML、強調或表格（只換符號，不改字）。"""
    return t.replace("<", "&lt;").replace("*", "\\*").replace("|", "&#124;").replace('"', "'")


def replacement_text(name, src, accessed):
    src = dict(src, excerpt=md_safe(src["excerpt"]))
    if src["kind"] == "us_label":
        sec = {"12.1 Mechanism of Action": "Mechanism of Action section",
               "12.1 Mechanism of Action; 12.4 Microbiology": "Mechanism of Action and Microbiology sections",
               "Clinical Pharmacology; Microbiology": "Clinical Pharmacology and Microbiology sections"}.get(src["section"], "Clinical Pharmacology section")
        if src.get("combination"):
            return (f"According to the {sec} of the US FDA-approved prescribing information for {src['combination']} "
                    f"(a combination product containing {name.lower()}): "
                    f"\"{src['excerpt']}\" ([{src['title']}]({src['url']}), label version {src['label_version']}; accessed {accessed}.)")
        return (f"According to the {sec} of the US FDA-approved prescribing information for {name.lower()}: "
                f"\"{src['excerpt']}\" ([{src['title']}]({src['url']}), label version {src['label_version']}; accessed {accessed}.)")
    return (f"ChEMBL (EMBL-EBI) records the mechanism of action of {name.lower()} as: {src['excerpt']} "
            f"([{src['title']}]({src['url']}), CC BY-SA 3.0; accessed {accessed}.)")


def gate_errors():
    """給 scripts/check_seo_docs.py：公開頁仍寫「作用機轉資料不可得」、卻沒有 mechanism 紀錄也沒有列在待查清單（說明為什麼補不到）的頁。
    有紀錄卻沒套上的頁由 apply_drug_reviews.check_errors 擋。"""
    rev = load_json(REVIEWS, {"records": []})["records"]
    unc = load_json(UNCERTAIN, {"items": []})["items"]
    covered = {r["file"] for r in rev if r.get("action") == "mechanism"}
    covered |= {i["file"] for i in unc if i.get("source") == UNCERTAIN_SOURCE and i.get("status") in ("open", "unresolvable")}
    bad = [p.name for p, t, _ in public_pages() if p.name not in covered and find_claim(t)[0]]
    if not bad:
        return []
    return [f"公開藥物頁寫「作用機轉資料不可得」、沒有附出處的作用機轉紀錄也沒列待查 {len(bad)} 頁：{'、'.join(bad[:8])}"
            "（跑 python3 scripts/build_mechanism_reviews.py --cache <目錄> 再跑 scripts/apply_drug_reviews.py）"]


def load_json(p, default):
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else default


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--chembl-down", metavar="說明",
                    help="ChEMBL API 連不上時：不查 ChEMBL，補不到的頁照樣列進待查清單，原因寫這段說明（例「ChEMBL API HTTP 500／逾時」）")
    ap.add_argument("--out", help="--dry-run 時把全部新紀錄寫到這個檔（人工逐筆看節錄用）")
    ap.add_argument("--no-chembl", action="store_true", help="只查美國仿單（ChEMBL API 從雲端很慢時先跑這個，再對補不到的頁跑一次不加這個參數）")
    ap.add_argument("--date", default=dt.date.today().isoformat())
    ap.add_argument("drugs", nargs="*")
    a = ap.parse_args()
    targets = []
    for p, t, fm in public_pages():
        if a.drugs and p.stem not in a.drugs:
            continue
        claim, tail = find_claim(t)
        if claim:
            targets.append((p, fm, claim, tail))
    if a.list:
        for p, _, claim, _ in targets:
            print(f"{p.name}\t{claim}")
        print(f"公開頁仍寫「作用機轉資料不可得」：{len(targets)} 頁")
        return 0
    if not a.cache:
        sys.exit("要給 --cache <目錄>（API 回應快取；當不受信任資料）")
    cache = Path(a.cache)
    cache.mkdir(parents=True, exist_ok=True)
    rev = load_json(REVIEWS, {"records": []})
    unc = load_json(UNCERTAIN, {"items": []})
    have = {r["file"] for r in rev["records"] if r.get("action") == "mechanism"}
    added, missing = [], []
    for p, fm, claim, tail in targets:
        if p.name in have:
            continue
        name = front(fm, "title") or p.stem.replace("_", " ")
        inn = front(fm, "monitor_inn") or name
        try:
            src, why = label_source(inn, cache)
        except Exception as e:  # 單一藥的來源抓取失敗不擋其他藥；下次重跑會補（快取只存成功的回應）
            src, why = None, f"openFDA fetch failed ({e})"
        why2 = ("ChEMBL not queried in this run" if a.no_chembl else
                f"ChEMBL not queried: {a.chembl_down}" if a.chembl_down else "")
        if not src and not a.no_chembl and not a.chembl_down:
            try:
                src, why2 = chembl_source(inn, cache)
            except Exception as e:
                src, why2 = None, f"ChEMBL fetch failed ({e})"
        if not src:
            missing.append({"file": p.name, "drug": p.stem, "reason": f"{why}; {why2}"})
            continue
        rep = replacement_text(name, src, a.date) + (" " + tail if tail else "")
        rec = {
            "id": f"nz-moa-{a.date.replace('-', '')}-{p.stem}-1",
            "drug": p.stem,
            "file": p.name,
            "action": "mechanism",
            "claim": claim,
            "replacement": rep,
            "summary": f"\"{claim.rstrip('.,;: ').strip()}\" replaced with a sourced mechanism-of-action statement",
            "finding": ("頁面寫作用機轉資料不可得（證據包 drugbank.source＝vocab_only）；作用機轉是藥物本身的藥理事實，"
                        f"依 {'美國 FDA 仿單 ' + src['section'] if src['kind'] == 'us_label' else 'ChEMBL drug mechanism 表'} 原文節錄補上，模型預測與其他正文不動。"),
            "sources": [{k: v for k, v in {
                "title": src["title"], "url": src["url"], "accessed": a.date, "excerpt": src["excerpt"],
                "label_version": src.get("label_version"), "section": src.get("section"), "via": src["via"],
                "set_id": src.get("set_id"), "chembl_id": src.get("chembl_id")}.items() if v}],
            "checked": a.date,
            "method": "scripts/build_mechanism_reviews.py（2026-11-19 建）",
        }
        added.append(rec)
    print(f"目標 {len(targets)} 頁；新紀錄 {len(added)}（仿單 {sum(r['sources'][0].get('set_id') is not None for r in added)}、"
          f"ChEMBL {sum('chembl_id' in r['sources'][0] for r in added)}）；找不到來源 {len(missing)}")
    for m in missing:
        print("  ✗", m["file"], m["reason"])
    if a.dry_run:
        if a.out:
            Path(a.out).write_text(json.dumps({"added": added, "missing": missing}, ensure_ascii=False, indent=1), encoding="utf-8")
        for r in added[:5]:
            print(json.dumps(r, ensure_ascii=False, indent=1))
        return 0
    rev["records"].extend(added)
    REVIEWS.write_text(json.dumps(rev, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    open_items = {(i.get("file"), i.get("source")) for i in unc["items"]}
    for m in missing:
        if a.no_chembl:
            continue  # 還沒查 ChEMBL 的不列待查
        if (m["file"], UNCERTAIN_SOURCE) in open_items:
            continue
        unc["items"].append({"file": m["file"], "drug": m["drug"], "source": UNCERTAIN_SOURCE, "status": "open",
                             "added": a.date, "issue": "Page says mechanism-of-action data is not available; no citable source found",
                             "searched": ["openFDA drug label API (US prescribing information)"] +
                                         ([] if a.chembl_down else ["ChEMBL drug mechanism"]),
                             "reason": m["reason"]})
    UNCERTAIN.write_text(json.dumps(unc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())

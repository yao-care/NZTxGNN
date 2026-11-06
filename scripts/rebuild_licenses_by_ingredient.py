"""NZ 依成分重建證據包的「本地資料」紀錄（2026-11-06，用戶核准；全程不用 AI；做法比照 Za／Us／Sg／Eu／Uk）。

NZ 站的本地資料是 Pharmac 的藥品給付清單（Pharmaceutical Schedule，社區用藥表），不是 Medsafe 許可證；這裡只依成分重建清單筆數，
頁面上一律寫成「列在 Pharmac 給付清單」、不含 Medsafe 許可狀態（check_drug_page.SOURCE_STATUS／NO_CENTRAL）。
狀態欄「Fully subsidised」（Yes／空白）是給付狀態，照原文寫進 status（scripts/license_adapter.py 每週更新同一欄）。

1. 來源：https://schedule.pharmac.govt.nz/latest/CPSReporting.xlsx（會轉址到當期檔，免金鑰；與 license_adapter 同一份），
   工作表「Community Medicines」，表頭列含「Pharmacode」。重建前重新下載，存 data/raw/CPSReporting.xlsx（--skip-download 用已存的檔）。
2. 比對：「Chemical」欄是成分名（鹽類寫在名稱裡），複方寫成「A with B」「A, B and C」。成分名拿掉括號內容、鹽類／水合物字樣，
   得到有效成分；英美名統一（paracetamol＝acetaminophen、ciclosporin＝cyclosporine…）。頁的 drug.inn 同樣處理，有效成分相同就收（含複方）。
   同一個 Pharmacode 只算一筆（任一列 Fully subsidised＝Yes 即 Yes，與 license_adapter.status_index 相同）。
3. 寫回證據包 tfda：records＝全部比對到的 Pharmacode（單方在前），total_matches＝筆數；原紀錄整批換掉。比對結果是空的、但原紀錄的
   Pharmacode 仍在現行清單 → 不改這個證據包、列進報告（疑似比對漏掉，要人看）。
用法（repo 根目錄）：python3 scripts/rebuild_licenses_by_ingredient.py [--skip-download] [--dry-run] [--report out.tsv] [頁 ...]
"""
import io
import json
import re
import sys
import time
from pathlib import Path

import openpyxl
import requests

ROOT = Path(__file__).resolve().parents[1]
BUNDLES = ROOT / "data/bundles"
RAW = ROOT / "data/raw"
XLSX = RAW / "CPSReporting.xlsx"
URL = "https://schedule.pharmac.govt.nz/latest/CPSReporting.xlsx"
SHEET = "Community Medicines"
CONFIRMED_EMPTY = {}

SALT = (r"hydrochloride|dihydrochloride|hcl|hydrobromide|besilate|besylate|maleate|mesilate|mesylate|tartrate|bitartrate|fumarate|"
        r"succinate|citrate|acetate|diacetate|propionate|dipropionate|valerate|butyrate|phosphate|sulfate|sulphate|nitrate|lactate|gluconate|"
        r"palmitate|sodium|disodium|potassium|calcium|magnesium|bromide|chloride|iodide|tosilate|tosylate|pamoate|embonate|stearate|"
        r"mandelate|xinafoate|furoate|hyclate|trometamol|meglumine|olamine|erbumine|benzathine|esilate|isethionate|mononitrate|"
        r"oxalate|dihydrogen|hydrogen|decanoate|enanthate|cipionate|undecanoate|pivalate|axetil|cilexetil|medoxomil|dimesylate|"
        r"butylbromide|hippurate|acetonide|hexacetonide|monohydrate|dihydrate|trihydrate|hemihydrate|anhydrous|hydrate|base")
SALT_RX = re.compile(r"\b(" + SALT + r")\b")
CANON = [(r"\bparacetamol\b", "acetaminophen"), (r"\bciclosporin\b", "cyclosporine"), (r"\bfrusemide\b", "furosemide"),
         (r"\badrenaline\b", "epinephrine"), (r"\bnoradrenaline\b", "norepinephrine"), (r"\bsalbutamol\b", "albuterol"),
         (r"\bglibenclamide\b", "glyburide"), (r"\blignocaine\b", "lidocaine"), (r"\brifampin\b", "rifampicin"),
         (r"\bacetylsalicylic acid\b", "aspirin"), (r"\bcolecalciferol\b", "cholecalciferol"), (r"\baluminium\b", "aluminum"),
         (r"\bsimethicone\b", "simeticone"), (r"\bthyroxine\b", "levothyroxine"), (r"\bhyoscine\b", "scopolamine"),
         (r"\bphenobarbitone\b", "phenobarbital"), (r"\bglyceryl trinitrate\b", "nitroglycerin"), (r"\boestr", "estr"), (r"\bsulph", "sulf"),
         (r"\bcyclosporin\b", "cyclosporine"), (r"\bamoxycillin\b", "amoxicillin"), (r"\bcephalexin\b", "cefalexin"),
         (r"\baciclovir\b", "acyclovir"), (r"\bchlortalidone\b", "chlorthalidone"), (r"\bindometacin\b", "indomethacin"),
         (r"\bmeclozine\b", "meclizine"), (r"\bphytomenadione\b", "phylloquinone"), (r"\bhydroxycarbamide\b", "hydroxyurea"),
         (r"\bnicotinic acid\b", "niacin"), (r"\bpolyethylene glycol\b", "macrogol"), (r"\bdextrose\b", "glucose"),
         (r"\bchlorpheniramine\b", "chlorphenamine"), (r"\bethinyloestradiol\b", "ethinylestradiol"), (r"\bethinyl estradiol\b", "ethinylestradiol"),
         # 2026-11-06 實測：Pharmac 用舊英國名
         (r"\bflupenthixol\b", "flupentixol"), (r"\bmitozantrone\b", "mitoxantrone"), (r"\bpericyazine\b", "periciazine"),
         (r"\bthioguanine\b", "tioguanine"), (r"\bmitomycin c\b", "mitomycin"), (r"\bzuclopenthixol\b", "zuclopentixol")]


def norm(x):
    t = str(x or "").lower()
    while re.search(r"\([^()]*\)|\[[^\[\]]*\]", t):
        t = re.sub(r"\([^()]*\)|\[[^\[\]]*\]", " ", t)
    t = " ".join(re.sub(r"[^a-z0-9]+", " ", t).split())
    for a, b in CANON:
        t = re.sub(a, b, t)
    return " ".join(t.split())


def base(t):
    t = re.sub(r"^(l|d|dl)\s+(?=[a-z]{3})", "", t)
    t = re.sub(r"\s+(k\s*)?\d+$", "", t)  # Macrogol 3350
    return " ".join(SALT_RX.sub(" ", t).split()) or t


def components(chem):
    ps = re.split(r"\bwith\b|,|\band\b|/|\+", str(chem or ""), flags=re.I)
    return [p for p in (base(norm(x)) for x in ps) if len(p) >= 3]


def _get(url):
    for attempt in range(4):
        try:
            r = requests.get(url, timeout=180, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0 (compatible; TxGNN/1.0; research)"})
            r.raise_for_status()
            if r.content[:2] != b"PK":
                raise RuntimeError("回傳的不是 xlsx")
            return r
        except Exception:
            if attempt == 3:
                raise
            time.sleep(15 * (attempt + 1))


def load_cps(download):
    if download:
        RAW.mkdir(parents=True, exist_ok=True)
        XLSX.write_bytes(_get(URL).content)
    wb = openpyxl.load_workbook(io.BytesIO(XLSX.read_bytes()), read_only=True)
    if SHEET not in wb.sheetnames:
        raise SystemExit(f"找不到工作表 {SHEET}：{wb.sheetnames}")
    head, rows = None, []
    for row in wb[SHEET].iter_rows(values_only=True):
        if head is None:
            if row and "Pharmacode" in row:
                head = [" ".join(str(x or "").split()) for x in row]
            continue
        rec = dict(zip(head, row))
        if str(rec.get("Pharmacode") or "").strip():
            rows.append(rec)
    if head is None or not {"Chemical", "Presentation", "Brand", "Pharmacode", "Fully subsidised"} <= set(head):
        raise SystemExit(f"Pharmac 表頭改變：{head}")
    if len(rows) < 1500:
        raise SystemExit(f"Pharmac 只解析出 {len(rows)} 筆，疑似格式改變")
    return rows


def main(argv):
    dry = "--dry-run" in argv
    rep = argv[argv.index("--report") + 1] if "--report" in argv else None
    only = [a for a in argv if not a.startswith("--") and a != rep]
    rows = load_cps("--skip-download" not in argv)
    prods, by_base = {}, {}
    for r in rows:
        code = str(r.get("Pharmacode") or "").strip()
        cs = components(r.get("Chemical"))
        if not cs:
            continue
        fs = "Yes" if str(r.get("Fully subsidised") or "").strip() == "Yes" or (prods.get(code) or {}).get("fs") == "Yes" else ""
        prods[code] = {"r": prods.get(code, {}).get("r") or r, "n": len(cs), "fs": fs}
        for c in cs:
            by_base.setdefault(c, set()).add(code)
    out, held, n_changed, n_found, n_empty = [], [], 0, 0, 0
    for f in sorted(BUNDLES.glob("*/drug_bundle.json")):
        page = f.parent.name
        if only and page not in only:
            continue
        b = json.loads(f.read_text(encoding="utf-8"))
        reg = b.get("tfda") or {}
        inn = (b.get("drug") or {}).get("inn") or page.replace("_", " ")
        key = base(norm(inn))
        hit = set(by_base.get(key, set())) if len(key) >= 3 else set()
        old_ids = sorted({str(x.get("license_id") or "").strip() for x in reg.get("records") or []} - {""})
        old_cur = sorted(set(old_ids) & set(prods))
        if not hit and old_cur and page not in CONFIRMED_EMPTY:
            held.append(page)
            out.append(f"{page}\t{key}\t{reg.get('total_matches')}\t{len(old_ids)}\t-\tHELD\t{';'.join(old_cur[:10])}\t")
            continue
        recs = []
        for code in sorted(hit, key=lambda k: (prods[k]["n"] > 1, str(prods[k]["r"].get("Chemical") or ""), str(prods[k]["r"].get("Presentation") or ""), k)):
            r = prods[code]["r"]
            recs.append({"license_id": code, "brand_name_zh": str(r.get("Brand") or ""), "brand_name_en": str(r.get("Brand") or ""),
                         "ingredients": str(r.get("Chemical") or ""), "indication": "", "dosage_form": str(r.get("Presentation") or ""),
                         "manufacturer": "", "license_holder": "", "approval_date": "", "expiry_date": "", "status": prods[code]["fs"],
                         "kind": "single" if prods[code]["n"] == 1 else "combo",
                         "source": "Pharmac Pharmaceutical Schedule, Community Medicines (funding list; not Medsafe licences)"})
        new = dict(reg, found=bool(recs), records=recs, total_matches=len(recs),
                   rebuilt="2026-11-06 by ingredient (Pharmac Pharmaceutical Schedule, Community Medicines)")
        new_ids = [x["license_id"] for x in recs]
        dropped = sorted(set(old_ids) - set(new_ids))
        out.append(f"{page}\t{key}\t{reg.get('total_matches')}\t{len(old_ids)}\t{len(new_ids)}\t{len(dropped)}\t"
                   f"{';'.join(dropped[:15])}{' (not in current list: ' + ';'.join(sorted(set(dropped) - set(prods))) + ')' if set(dropped) - set(prods) else ''}\t"
                   f"{';'.join(sorted(set(new_ids) - set(old_ids))[:10])}")
        n_found += bool(recs)
        n_empty += not recs
        if new != reg:
            n_changed += 1
            b["tfda"] = new
            if not dry:
                f.write_text(json.dumps(b, ensure_ascii=False, indent=2), encoding="utf-8")
    if rep:
        Path(rep).write_text("page\tmatch\told_total_matches\told_records\tnew_records\tdropped\tdropped_ids\tadded_ids\n" + "\n".join(out) + "\n",
                             encoding="utf-8")
    print(f"Pharmac 社區用藥 {len(prods)} 個 Pharmacode；改 {n_changed} 個證據包（有紀錄 {n_found}、查無 {n_empty}、比對落空但原 Pharmacode 仍在現行清單而保留 {len(held)}）")
    if held:
        print("保留（要人看）：" + ", ".join(held[:40]))


if __name__ == "__main__":
    main(sys.argv[1:])

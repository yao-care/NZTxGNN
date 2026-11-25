#!/usr/bin/env python3
"""證據包收錯品項移出（2026-11-25 第五輪，用戶決定）：依 config/bundle_record_exclusions.json，
把 data/bundles/*/drug_bundle.json 裡成分欄沒有本藥（drug_mapping 把別的成分對到本藥 DrugBank ID）的紀錄從 tfda.records 移到
tfda.excluded_records（附 reason、basis、excluded＝第一次排除日），total_matches 扣掉移出的筆數。
只移不刪、可重複執行（已移出的不再扣）。每週 license-refresh.yml 在 license_refresh 前先跑；改完要重算事實層與快照。
用法（repo 根目錄，先還原 data/）：python3 scripts/exclude_mismatched_records.py [--dry-run]
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from nztxgnn.regulatory import record_exclusions as RX  # noqa: E402


def run(dry=False):
    today = time.strftime("%Y-%m-%d")
    moved, touched = 0, []
    for f in sorted((ROOT / "data" / "bundles").glob("*/drug_bundle.json")):
        name = f.parent.name
        if RX.bundle_key(name) not in RX.rules():
            continue
        b = json.loads(f.read_text(encoding="utf-8"))
        t = b.get("tfda") or {}
        keep, out = [], []
        for r in t.get("records") or []:
            rule = RX.match(name, r)
            if rule:
                out.append(dict(r, excluded=today, excluded_reason=rule["reason"], excluded_basis=rule["basis"]))
            else:
                keep.append(r)
        if not out:
            continue
        for r in out:
            print(f"  {name}: 移出 {r.get('license_id')} {str(r.get('brand_name_en'))[:50]}（{r.get('ingredients')[:60]}）")
        moved += len(out)
        touched.append(name)
        if dry:
            continue
        t["records"] = keep
        t["excluded_records"] = (t.get("excluded_records") or []) + out
        t["total_matches"] = max(len(keep), int(t.get("total_matches") or 0) - len(out))
        if not keep:
            t["found"] = False
        b["tfda"] = t
        f.write_text(json.dumps(b, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"證據包收錯品項：移出 {moved} 筆（{len(touched)} 個證據包）{'（試跑，沒寫檔）' if dry else ''}")
    return touched


if __name__ == "__main__":
    run(dry="--dry-run" in sys.argv[1:])

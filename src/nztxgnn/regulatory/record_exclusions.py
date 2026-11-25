"""證據包收錯品項的排除規則（2026-11-25 第五輪，用戶決定；規則與依據在 config/bundle_record_exclusions.json）。

scripts/rebuild_licenses_by_ingredient.py（2026-11-06 依成分重建）比對前把鹽類／酯類字樣拿掉（base），ferrous fumarate 與 ferrous sulfate
都變成「ferrous」、fluticasone furoate 與 propionate 都變成「fluticasone」，別的鹽類或酯類的品項就被收進本藥證據包。
規則：證據包名在規則的 bundles 裡，紀錄成分欄（Pharmac Chemical，「with」「,」「and」分隔）有 require 時沒有一段符合 require、
或有一段符合 forbid → 排除。scripts/exclude_mismatched_records.py、rebuild_licenses_by_ingredient.py 與 collectors/nzfda.py 共用。
"""
from __future__ import annotations

import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

CONF = Path(__file__).resolve().parents[3] / "config" / "bundle_record_exclusions.json"


def _norm(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", s).strip()


def bundle_key(name) -> str:
    return re.sub(r"[^a-z0-9]+", "_", _norm(name)).strip("_")


@lru_cache(maxsize=None)
def rules() -> dict:
    """證據包名 → [規則]"""
    out = {}
    for r in json.loads(CONF.read_text(encoding="utf-8")).get("rules") or []:
        for b in r["bundles"]:
            out.setdefault(bundle_key(b), []).append(r)
    return out


def components(ingredients) -> list:
    return [_norm(c) for c in re.split(r"\bwith\b|,|\band\b|;|\+", str(ingredients or ""), flags=re.I) if c.strip()]


def match_ingredients(bundle, ingredients):
    """成分欄依規則該排除就回傳規則，否則 None"""
    comps = components(ingredients)
    for r in rules().get(bundle_key(bundle), ()):
        if r.get("require") and not any(re.search(r["require"], c) for c in comps):
            return r
        if r.get("forbid") and any(re.search(r["forbid"], c) for c in comps):
            return r
    return None


def match(bundle, rec):
    return match_ingredients(bundle, rec.get("ingredients"))

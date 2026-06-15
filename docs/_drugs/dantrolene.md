---
layout: default
title: Dantrolene
parent: 僅模型預測 (L5)
nav_order: 99
evidence_level: L5
indication_count: 9
---

# Dantrolene
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Dantrolene: Drug Repurposing Evaluation — Insufficient Data for Assessment

## One-Sentence Summary

Dantrolene (DrugBank: DB01219) is currently under data collection review for drug repurposing analysis.
This Evidence Pack contains **no TxGNN predicted indications**, and critical data gaps remain in mechanism of action and safety profiling — a complete data refresh is required before any repurposing evaluation can proceed.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not available in current Evidence Pack |
| Predicted New Indication | No TxGNN prediction available |
| TxGNN Prediction Score | — |
| Evidence Level | Not applicable — no prediction received |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Data Completeness Assessment

This Evidence Pack cannot support a standard drug repurposing evaluation. The table below summarises the critical gaps identified during data collection:

| Data Item | Query Status | Impact |
|-----------|-------------|--------|
| TxGNN Predicted Indications | Not received (`predicted_indications: []`) | Cannot identify or evaluate any repurposing direction |
| Mechanism of Action (MOA) | DrugBank queried successfully, but MOA not extracted | Cannot assess mechanistic plausibility for any candidate indication |
| Key Warnings / Contraindications | TFDA package insert queried successfully, but data not extracted | Cannot complete safety pre-screening |
| Original Approved Indications | No records found in TFDA database | Cannot anchor repurposing logic to known clinical use |
| Drug–Drug Interactions | Not found | Cannot assess interaction risk |

> Note: The DrugBank query (Log ID 3) and TFDA package insert query (Log ID 4) both returned `result_status: success`, which suggests the underlying data exists but was not surfaced into the Evidence Pack. Re-running the extraction pipeline is likely sufficient to resolve most gaps.

---

## New Zealand Market Information

Dantrolene currently holds **no product authorisations** in New Zealand. There are no licensed products, dosage forms, or approved indications on record in this dataset.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No TxGNN predicted indication is present in this Evidence Pack, and the absence of MOA, safety warnings, and approved indication data makes it impossible to perform even a preliminary plausibility or risk assessment. Proceeding without this information would not meet the minimum standard for a drug repurposing evaluation.

**To proceed, the following is needed:**

- **Re-run TxGNN prediction pipeline** to obtain scored predicted indications for Dantrolene (DB01219)
- **Extract MOA from DrugBank** — the query succeeded (Log ID 3), so the data is available; re-run the extraction step
- **Extract key warnings and contraindications from TFDA package insert** — the query succeeded (Log ID 4); re-run the parsing step
- **Confirm original approved indications** from DrugBank or international regulatory sources (e.g., FDA, EMA), since TFDA holds no records
- **Re-check DDI database** with alternate search terms or identifiers (e.g., DrugBank ID rather than INN) if the INN query continues to return no results
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


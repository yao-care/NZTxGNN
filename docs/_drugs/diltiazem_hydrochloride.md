---
layout: default
title: Diltiazem Hydrochloride
parent: 僅模型預測 (L5)
nav_order: 117
evidence_level: L5
indication_count: 0
---

# Diltiazem Hydrochloride
{: .fs-9 }

證據等級: **L5** | 預測適應症: **0** 個
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

# Diltiazem Hydrochloride: Repurposing Evaluation — Insufficient Data for Full Analysis

## One-Sentence Summary

Diltiazem Hydrochloride is a widely used cardiovascular agent, but the current Evidence Pack contains neither original indication records nor TxGNN-predicted new indications for this compound. Without prediction outputs or supporting evidence, a complete drug repurposing evaluation cannot be produced at this stage — a data remediation step is required before proceeding.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not available in current dataset |
| Predicted New Indication | Not available — TxGNN output not retrieved |
| TxGNN Prediction Score | Not available |
| Evidence Level | Cannot be determined |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why a Full Analysis Cannot Be Completed

The Evidence Pack for Diltiazem Hydrochloride is missing three critical inputs that the standard repurposing workflow requires:

**No TxGNN predicted indications.** The `predicted_indications` array is empty, meaning the TxGNN model either was not run for this compound or its output was not captured in this Evidence Pack. Without a predicted indication, there is no repurposing hypothesis to evaluate, no mechanistic link to assess, and no evidence to tabulate.

**No original indication or MOA data.** The `original_indications` list is empty and mechanism of action is recorded as unavailable. The query log confirms DrugBank was queried on 2026-03-29 and returned one result — this data exists but was not parsed into the Evidence Pack. Similarly, the package insert query returned one result on the same date but was also not incorporated.

**No safety information.** Warnings and contraindications are absent. The drug–drug interaction query returned no results.

---

## New Zealand Market Information

Diltiazem Hydrochloride currently has **zero registered authorizations** in New Zealand based on data retrieved on 2026-03-29. No product names, dosage forms, or approved indications are on record.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This Evidence Pack is structurally incomplete — no TxGNN predictions, no indication data, no MOA, and no safety records were incorporated, making it impossible to evaluate any repurposing candidate or assign an evidence level.

**To proceed, the following is needed:**

- **Run TxGNN prediction** for DILTIAZEM HYDROCHLORIDE and populate `predicted_indications` before re-generating this report
- **Parse the DrugBank result** already retrieved on 2026-03-29 (query log ID 3, result count = 1) — extract MOA, categories, and toxicity fields and add to Evidence Pack
- **Parse the package insert** already retrieved on 2026-03-29 (query log ID 4, result count = 1) — extract original indications, warnings, and contraindications
- **Re-query market registration** in New Zealand (Medsafe) if a different registry is intended, as the TFDA query returned 0 results
- **Re-generate this Evidence Pack** once the four items above are resolved, then re-run report generation

> Note: The query log indicates that DrugBank and package insert sources both returned data — the bottleneck is parsing, not data availability. Resolving the parsing step may resolve most of the current data gaps without additional external queries.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


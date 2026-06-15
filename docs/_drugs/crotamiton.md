---
layout: default
title: Crotamiton
parent: 僅模型預測 (L5)
nav_order: 88
evidence_level: L5
indication_count: 0
---

# Crotamiton
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

# CROTAMITON: Antiparasitic Drug — TxGNN Prediction Data Unavailable

## One-Sentence Summary

Crotamiton is a topical antiparasitic and antipruritic agent, traditionally used for the treatment of scabies and relief of pruritus (itching).
However, the current Evidence Pack contains **no TxGNN-predicted new indications**, which means a standard repurposing evaluation cannot be completed at this stage.
This report documents the data gaps and recommends remediation steps before further evaluation.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Scabies treatment; antipruritic (topical) |
| Predicted New Indication | — (No TxGNN predictions available) |
| TxGNN Prediction Score | — |
| Evidence Level | L5 (Model prediction unavailable — no supporting studies in this pack) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Safety Considerations

No safety data was available in this Evidence Pack. All key warnings, contraindications, and drug interaction data were flagged as data gaps.

> Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Evidence Pack for Crotamiton (DB00265) is incomplete — no TxGNN-predicted indications were returned, no MOA data is available, the drug is not registered in New Zealand, and all safety fields are empty. There is currently insufficient evidence to evaluate any repurposing candidate.

**To proceed, the following is needed:**

- **Run TxGNN pipeline for Crotamiton** to obtain disease-score predictions (`predicted_indications` array is currently empty)
- **Retrieve MOA data from DrugBank** — DrugBank query was successful (result\_count: 1) but MOA was not extracted into this pack; re-run extraction
- **Retrieve safety data** — download and parse the package insert PDF from the regulatory authority to populate `key_warnings` and `contraindications`
- **Check DDI database** with alternative search terms (e.g., brand name Eurax) as the current query returned no results
- **Re-generate this Evidence Pack** (suggest upgrading from v4 to v5) after filling the above data gaps, then re-run evaluation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


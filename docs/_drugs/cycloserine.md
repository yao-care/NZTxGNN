---
layout: default
title: Cycloserine
parent: 僅模型預測 (L5)
nav_order: 93
evidence_level: L5
indication_count: 7
---

# Cycloserine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **7** 個
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

# Cycloserine: Evaluation Pending — No TxGNN Predictions Available

## One-Sentence Summary

Cycloserine (DB00260) is a broad-spectrum antibiotic known for use in drug-resistant tuberculosis management.
The current Evidence Pack contains **no TxGNN-predicted indications** for this compound, and critical data fields — original indication, mechanism of action, and safety profile — are absent from the pipeline output.
A full repurposing evaluation cannot be completed until these gaps are resolved.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not recorded in Evidence Pack |
| Predicted New Indication | No predictions available |
| TxGNN Prediction Score | N/A |
| Evidence Level | L5 — No predictions generated |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Since the `predicted_indications` array is empty in this Evidence Pack, no mechanistic repurposing rationale can be presented. The following data gaps directly block analysis:

**Original indication** is not recorded in the Evidence Pack. The TFDA query returned zero results (query log ID 1), indicating Cycloserine has no regulatory footprint in the queried database. Without a confirmed base indication, the "From X to Y" repurposing framework cannot be applied.

**Mechanism of action** is not available. The DrugBank query returned a result (query log ID 3), yet MOA data was not populated — suggesting an extraction failure downstream. Without MOA, it is not possible to assess pharmacological overlap with any candidate new indication.

**Safety profile** is entirely absent. Warnings and contraindications both returned as data gaps despite a successful package insert query (query log ID 4). For a compound with known CNS toxicity risks, this constitutes a blocking issue for any safety-gated repurposing decision.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Cycloserine is not currently marketed in New Zealand. No product authorizations are on record.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Evidence Pack for Cycloserine is structurally incomplete — no TxGNN predictions were generated, and three blocking data gaps (original indication, MOA, safety warnings) prevent entry into the standard evaluation workflow.

**To proceed, the following is needed:**

- **Run TxGNN prediction pipeline** for Cycloserine (DB00260) to generate candidate repurposing indications
- **Extract MOA from DrugBank** — query log confirms a successful DrugBank hit (ID 3); re-run extraction to populate `original_moa`
- **Parse package insert safety data** — query log confirms a successful TFDA insert retrieval (ID 4); re-run parsing to populate `key_warnings` and `contraindications`
- **Confirm original indication** from DrugBank categories or international label (e.g., WHO, FDA) to anchor the "From X" axis of repurposing analysis
- **Re-generate Evidence Pack** after gaps are resolved, then re-submit for full evaluation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


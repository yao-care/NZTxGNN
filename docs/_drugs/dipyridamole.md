---
layout: default
title: Dipyridamole
parent: 僅模型預測 (L5)
nav_order: 49
evidence_level: L5
indication_count: 10
---

# Dipyridamole
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
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

# Dipyridamole: Evidence Pack Incomplete — Repurposing Evaluation Cannot Proceed

## One-Sentence Summary

Dipyridamole (DrugBank: DB00975) is a cardiovascular agent with established antiplatelet and vasodilatory properties, with broad clinical use in stroke prevention and cardiac stress imaging.
However, the current Evidence Pack contains **no TxGNN predicted new indications**, and key data fields — including mechanism of action, safety warnings, and contraindications — are absent, making a complete repurposing evaluation impossible at this stage.
No further evidence review can be conducted until the pipeline gaps identified below are resolved.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not available in this Evidence Pack |
| Predicted New Indication | No TxGNN predictions present |
| TxGNN Prediction Score | N/A |
| Evidence Level | N/A |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Evidence Pack contains no TxGNN predicted indications, and all critical upstream data — mechanism of action, regulatory safety warnings, and drug–drug interactions — are unresolved. There is no evidence base on which to build a repurposing evaluation.

**To proceed, the following is needed:**

- **Re-run TxGNN prediction** for Dipyridamole (DB00975) to generate repurposing candidate indications with confidence scores
- **Retrieve mechanism of action** from DrugBank API (DG002 — High severity)
- **Parse TFDA package insert** to extract warnings and contraindications (DG001 — Blocking severity); the query log confirms a PDF was retrieved on 2026-03-29 but content was not ingested into the Evidence Pack
- **Query drug–drug interaction database** — current result status is `not_found`; confirm whether this reflects a true absence of known interactions or a query failure
- Once regulatory data is available, verify whether Dipyridamole has any market authorisation in target markets outside New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


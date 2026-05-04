---
layout: default
title: Dabigatran
parent: 僅模型預測 (L5)
nav_order: 25
evidence_level: L5
indication_count: 0
---

# Dabigatran
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

# Dabigatran: Insufficient Data for Drug Repurposing Evaluation

## One-Sentence Summary

Dabigatran (DrugBank ID: DB14726) is a direct thrombin inhibitor anticoagulant, widely known under the brand name Pradaxa (dabigatran etexilate prodrug form).
However, **this Evidence Pack contains no TxGNN-predicted new indications**, and critical data fields — including mechanism of action, safety warnings, and regulatory authorizations — are all missing.
At this stage, a complete repurposing evaluation **cannot be generated**; a "Hold" decision is recommended pending data remediation.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not recorded in this Evidence Pack |
| Predicted New Indication | None — TxGNN prediction data unavailable |
| TxGNN Prediction Score | N/A |
| Evidence Level | L5 (Model prediction only — not yet reached) |
| New Zealand Market Status | Not marketed (per Evidence Pack) |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

> **⚠️ Data Quality Notice:** DB14726 may correspond to the active metabolite form of dabigatran rather than the clinically prescribed prodrug dabigatran etexilate (DB06695). The "未上市" (not marketed) status and absence of indications may reflect this distinction rather than a true absence from the market. Before proceeding, verify which DrugBank entry is the intended target.

---

## Why is This Prediction Reasonable?

No TxGNN prediction is available for this Evidence Pack (`predicted_indications` is empty). Therefore, a mechanistic rationale for a new indication **cannot be constructed at this time**.

From general pharmaceutical knowledge, dabigatran is a direct competitive inhibitor of thrombin — a key serine protease in the coagulation cascade. It is established for stroke prevention in non-valvular atrial fibrillation and for treatment and prevention of venous thromboembolism (DVT/PE). Its potential repurposing directions — such as roles in cancer-associated coagulopathy, post-operative prophylaxis, or inflammatory conditions — have been explored in the literature, but **none of these can be formally evaluated without TxGNN prediction data**.

Mechanism of action data is listed as a data gap (DG002, severity: High). Until MOA data is retrieved from DrugBank and TxGNN predictions are generated, this section cannot be completed per evidence standards.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for a predicted new indication (no prediction available).

---

## Literature Evidence

Currently no related literature available (no predicted indication to search against).

---

## New Zealand Market Information

No regulatory authorizations found in this Evidence Pack. Total licenses recorded: **0**.

> If the intended drug is dabigatran etexilate (Pradaxa, DB06695), this would be a data retrieval error — Pradaxa has regulatory approvals in multiple jurisdictions including New Zealand. Please verify the DrugBank ID used for the query.

---

## Safety Considerations

Please refer to the package insert for safety information.

> All safety fields in this Evidence Pack — key warnings, contraindications, and drug-drug interactions — returned either `[Data Gap]` or `not_found`. No substantive safety data can be reported. This is classified as a Blocking gap (DG001) that prevents entry into safety pre-screening (S1).

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This Evidence Pack is critically incomplete — there are no TxGNN-predicted indications, no mechanism of action data, no safety information, and no regulatory authorizations recorded. A drug repurposing evaluation cannot be responsibly produced under these conditions.

**To proceed, the following is needed:**

- **[Blocking — DG001]** Retrieve and parse Taiwan FDA package insert PDF to extract approved warnings and contraindications; required before safety pre-screening (S1) can begin
- **[High — DG002]** Query DrugBank API to retrieve mechanism of action (MOA) for DB14726; required for mechanistic rationale
- **[Critical]** Verify whether DB14726 (dabigatran active form) is the correct target, or whether DB06695 (dabigatran etexilate, the marketed prodrug) should be used instead
- **[Critical]** Re-run TxGNN prediction pipeline for this drug to generate `predicted_indications`; without predictions, no repurposing evaluation is possible
- **[High]** Re-query Taiwan FDA regulatory database for dabigatran etexilate (if DB06695 is confirmed as the correct target) to retrieve license and market status information
- **[Medium]** Re-query DDI database after confirming the correct drug identity
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


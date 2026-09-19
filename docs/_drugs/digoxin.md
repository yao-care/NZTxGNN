---
layout: default
title: Digoxin
parent: Model Prediction Only (L5)
nav_order: 115
evidence_level: L5
indication_count: 6
---

# Digoxin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Digoxin: Evidence Pack Incomplete — Drug Repurposing Assessment Pending Supplementation

## One-Sentence Summary

Digoxin (DrugBank: DB00390) is a drug included in the current Evidence Pack and is not currently marketed in the New Zealand market.
The TxGNN model **produced no new indication predictions** in this analysis,
and critical data gaps exist in both mechanism of action (MOA) and safety data, preventing completion of a standard drug repurposing assessment.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | No data in current Evidence Pack |
| Predicted New Indication | No prediction results generated |
| TxGNN Prediction Score | N/A |
| Evidence Level | Cannot be determined (predicted_indications is empty) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why This Evaluation Cannot Proceed

The current Evidence Pack contains the following three fundamental gaps that prevent drug repurposing assessment from proceeding:

**1. TxGNN produces no prediction output**
The `predicted_indications` array is empty, indicating that the TxGNN model generated no new indication candidates for Digoxin. This may result from insufficient knowledge graph node connectivity or model confidence scores below the output threshold.

**2. Mechanism of action (MOA) is missing**
`drug.original_moa` is empty, preventing mechanism-relatedness analysis and precluding assessment of pharmacological similarity between new and original indications.

**3. Safety data is completely absent**
Key warnings, contraindications, and DDI data are unavailable, preventing completion of baseline safety initial assessment (S1 assessment phase).

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN model produced no indication predictions for Digoxin, and three Blocking/High priority data gaps exist simultaneously, meaning the minimum data conditions necessary to conduct drug repurposing assessment are not currently met.

**To proceed, the following is needed:**

- **\[Priority 1 — Blocking\]** Obtain TFDA package insert PDF and extract Key Warnings and Contraindications (Gap DG001)
- **\[Priority 2 — High\]** Complete mechanism of action (MOA) data through DrugBank API (Gap DG002)
- **\[Priority 3\]** Re-execute TxGNN prediction workflow to confirm correct mapping of Digoxin node to the knowledge graph
- **\[Priority 4\]** Supplement DDI query data (current status: not\_found, need to confirm query parameters are correct)
- Regenerate complete Evidence Pack once the above gaps are filled (current version: v4, data\_cutoff: 2026-04-20)

---

> **Note:** This report cannot output standard sections (Clinical Trial Evidence, Literature Evidence, New Zealand Market Information, Safety Considerations) due to incomplete Evidence Pack data. All missing sections will be fully presented in the next version of the report after data gaps are filled.

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


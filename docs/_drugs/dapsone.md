---
layout: default
title: Dapsone
parent: Model Prediction Only (L5)
nav_order: 100
evidence_level: L5
indication_count: 1
---

# Dapsone
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

Analysis of the Evidence Pack situation:

- `predicted_indications`: empty array → no TxGNN prediction results
- `original_indications`: empty array → no original indication data
- `original_moa`: [Data Gap]
- Market status: Not marketed, 0 authorizations
- Safety data: all [Data Gap]
- Data gaps: DG001 (Blocking), DG002 (High)

Because the Evidence Pack data is severely incomplete, the following is an assessment report that accurately reflects the current situation:

---

# DAPSONE: Incomplete Evidence Pack Data—Cannot Perform Drug Repurposing Assessment

## One-Sentence Summary

DAPSONE (DrugBank ID: DB00250) currently has severely insufficient Evidence Pack data, making it impossible to complete a comprehensive drug repurposing assessment. The TxGNN model has not yet generated any new indication predictions, and key fields such as original indications, mechanism of action, and safety warnings all contain data gaps. The formal assessment process cannot be entered until these data gaps are remediated.

---

## Quick Overview

| Item | Content |
|------|---------|
| Predicted New Indication | No TxGNN prediction available |
| Evidence Level | L5 — No predictions or studies available |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Safety Considerations

Please refer to the package insert for safety information.

> ⚠️ **Data Gap DG001 \[Blocking\]**: Package insert warnings and contraindications have not been retrieved. This blocks entry into the safety pre-screening stage (S1). Remediation required: download and parse the official package insert PDF.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Evidence Pack for DAPSONE contains no TxGNN-predicted indications and two unresolved data gaps of Blocking and High severity. A repurposing assessment cannot be meaningfully conducted until these gaps are remediated.

**To proceed, the following is needed:**

- **\[Blocking — DG001\]** Retrieve package insert from the official regulatory authority website, extract warnings and contraindications, and re-run the safety pre-screening (S1) gate
- **\[High — DG002\]** Query DrugBank API for mechanism of action (MOA) to enable mechanism-relevance analysis for any future predicted indications
- **\[Required\]** Run TxGNN prediction pipeline for DB00250 to generate candidate new indications; without predictions, the repurposing rationale cannot be constructed
- **\[Required\]** Confirm original approved indications from authoritative sources (DrugBank, WHO, or approved regulatory filings) to establish the baseline for repurposing comparison
- **\[Optional\]** Conduct a drug interaction (DDI) query once the above data gaps are closed

---

> **Note:** This report reflects the state of the Evidence Pack as of 2026-04-20. The absence of data does not indicate that DAPSONE lacks repurposing potential — only that the current data pipeline has not yet produced the inputs required for evaluation. Please re-run the evidence collection pipeline and regenerate this report once remediation steps are completed.

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


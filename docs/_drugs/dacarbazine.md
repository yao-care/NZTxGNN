---
layout: default
title: Dacarbazine
parent: 僅模型預測 (L5)
nav_order: 27
evidence_level: L5
indication_count: 1
---

# Dacarbazine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Dacarbazine: Evaluation Report — Insufficient Data for Repurposing Analysis

## One-Sentence Summary

Dacarbazine (DTIC) is a well-known antineoplastic agent used in oncology.
However, the current Evidence Pack contains **no TxGNN predicted indications**, **no approved indication records in New Zealand**, and critical data gaps in both mechanism of action and safety profiles.
This report serves as a **data gap summary** and cannot proceed to full repurposing evaluation without remediation.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | No approved indication records available in New Zealand |
| Predicted New Indication | No TxGNN predictions available |
| TxGNN Prediction Score | N/A |
| Evidence Level | N/A — no predictions or supporting studies loaded |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for Dacarbazine in this Evidence Pack.
Based on known information, Dacarbazine is a classic alkylating antineoplastic agent; its efficacy in melanoma and Hodgkin's lymphoma has been established in global clinical practice.
However, because the `predicted_indications` array is empty and `original_moa` is marked as a data gap, no mechanistic connection to a new indication can be evaluated at this stage.

No repurposing rationale can be constructed until the TxGNN prediction pipeline is run and MOA data is retrieved from DrugBank.

---

## Clinical Trial Evidence

Currently no TxGNN-predicted indication is available, so no targeted clinical trial evidence can be presented.

---

## Literature Evidence

Currently no TxGNN-predicted indication is available, so no targeted literature evidence can be presented.

---

## New Zealand Market Information

Dacarbazine has **no registered product licences** in New Zealand as of the data cutoff (2026-04-20).

---

## Cytotoxicity

Dacarbazine belongs to the alkylating agent class of conventional cytotoxic chemotherapy. The following is based on established pharmacological classification, as specific toxicity data was not included in this Evidence Pack.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic (Alkylating agent — triazene class) |
| Myelosuppression Risk | High (leukopenia and thrombocytopenia are dose-limiting; nadir typically at 3–4 weeks) |
| Emetogenicity Classification | High (classified as highly emetogenic; prophylactic antiemetics required) |
| Monitoring Items | CBC with differential (before each cycle and at nadir), liver function tests, renal function |
| Handling Protection | Must follow cytotoxic drug handling regulations; avoid skin/eye contact; dedicated disposal pathway required |

---

## Safety Considerations

Please refer to the package insert for safety information. Key warnings, contraindications, and drug interaction data were not available in this Evidence Pack and require retrieval from the TFDA package insert PDF and DrugBank API before safety evaluation can proceed.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Evidence Pack is missing all three components required for a repurposing evaluation: TxGNN predictions, mechanism of action data, and safety/warning data. No repurposing recommendation can be issued in the current state.

**To proceed, the following is needed:**

- **[Blocking — DG001]** Retrieve TFDA package insert PDF and extract key warnings and contraindications to enable safety pre-screening (S1 gate)
- **[High — DG002]** Query DrugBank API to populate `original_moa` and confirm drug categories (required for mechanistic relevance analysis and cytotoxicity classification)
- **[Required]** Run TxGNN prediction pipeline for Dacarbazine to populate `predicted_indications`; without predictions, no repurposing target exists
- **[Follow-up]** Once predictions are loaded, retrieve clinical trial and literature evidence for the top-ranked indication
- **[Follow-up]** Confirm New Zealand licensing pathway (Medsafe) — current status is unregistered, which affects regulatory feasibility of any repurposing application
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


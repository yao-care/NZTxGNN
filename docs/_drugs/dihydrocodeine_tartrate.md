---
layout: default
title: Dihydrocodeine Tartrate
parent: 僅模型預測 (L5)
nav_order: 45
evidence_level: L5
indication_count: 0
---

# Dihydrocodeine Tartrate
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

# DIHYDROCODEINE TARTRATE: Repurposing Analysis — Insufficient Evidence for Evaluation

## One-Sentence Summary

Dihydrocodeine Tartrate is an opioid-class analgesic and antitussive agent whose regulatory and pharmacological data could not be fully retrieved in this analysis cycle.
The TxGNN model returned **no predicted new indications** for this compound, and the drug is **not marketed in New Zealand**, leaving critical data gaps that prevent a complete repurposing evaluation.
Without predicted indications, clinical trial linkage, or confirmed mechanism-of-action data, this candidate cannot proceed past the intake stage.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not available in this Evidence Pack |
| Predicted New Indication | None — TxGNN returned no predictions |
| TxGNN Prediction Score | N/A |
| Evidence Level | L5 (model prediction only — and none present) |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for this Evidence Pack.
Based on general pharmacological knowledge, Dihydrocodeine Tartrate is a semi-synthetic opioid derived from codeine. It binds to µ-opioid receptors in the central nervous system to produce analgesic and antitussive effects, and is typically indicated for moderate-to-severe pain and non-productive cough.

However, because `original_moa` is flagged as a data gap (DG002) and `original_indications` was not populated in this submission, this evaluation cannot formally verify the mechanistic basis for any proposed repurposing direction.

The TxGNN model produced **zero predicted indications**, which may reflect insufficient graph coverage for this compound or that the drug was not represented in the training knowledge graph under this name/salt form. This is a critical issue requiring upstream investigation before proceeding.

---

## Clinical Trial Evidence

Currently no related clinical trials registered under this Evidence Pack's predicted indications (no indications returned by TxGNN).

---

## Literature Evidence

Currently no related literature available within this Evidence Pack (no indications returned by TxGNN to anchor a literature search).

---

## New Zealand Market Information

This drug is **not marketed in New Zealand**. No authorizations on record.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** Both `key_warnings` and `contraindications` were returned as data gaps (DG001). The DDI query returned no interactions. As Dihydrocodeine Tartrate is an opioid, standard opioid-class warnings (respiratory depression, dependence liability, CNS depression interactions) should be assumed until formal package insert data is retrieved.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN model returned no predicted indications for this compound, and two blocking/high-severity data gaps (package insert warnings and mechanism of action) remain unresolved. There is no evidence base on which to conduct a repurposing evaluation at this time.

**To proceed, the following is needed:**

- **DG001 (Blocking):** Retrieve TFDA package insert PDF and extract warnings and contraindications — this is a prerequisite for safety screening
- **DG002 (High):** Query DrugBank API to confirm mechanism of action and pharmacological class
- **TxGNN re-run:** Verify whether the compound is indexed in the TxGNN knowledge graph under its INN or an alternative identifier (e.g., "dihydrocodeine" without the salt suffix); re-run prediction pipeline if necessary
- **Regulatory baseline:** Confirm whether any international regulatory bodies (EMA, FDA, PMDA) hold approvals that can serve as a proxy indication profile
- **DrugBank ID resolution:** The `drugbank_id` field is null — this must be resolved to enable DDI lookups and pharmacological class assignment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


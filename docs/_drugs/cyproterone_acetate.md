---
layout: default
title: Cyproterone Acetate
parent: 僅模型預測 (L5)
nav_order: 94
evidence_level: L5
indication_count: 10
---

# Cyproterone Acetate
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

# Cyproterone Acetate: Drug Repurposing Evaluation (Pending TxGNN Predictions)

## One-Sentence Summary

Cyproterone Acetate (DB04839) is a synthetic antiandrogen and progestogen with established clinical use internationally for conditions including prostate cancer, androgen-dependent disorders, and hormone therapy.
However, the current Evidence Pack contains **no TxGNN-predicted new indications** for this compound, and the drug is **not registered in New Zealand**, meaning evaluation cannot proceed beyond the preliminary stage.
A full repurposing assessment requires pipeline rerun with TxGNN predictions and retrieval of missing MOA and safety data.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not available in current dataset |
| Predicted New Indication | No TxGNN predictions generated |
| TxGNN Prediction Score | N/A |
| Evidence Level | L5 — Model prediction not yet generated |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why is This Prediction Reasonable?

No TxGNN repurposing predictions are present in this Evidence Pack (`predicted_indications: []`). As a result, no mechanistic bridging analysis can be performed at this stage.

Currently, detailed mechanism of action data is also not available in the Evidence Pack. Based on publicly known pharmacological class, Cyproterone Acetate is a synthetic steroidal antiandrogen and progestogen. Its established clinical roles internationally include suppression of androgen-dependent conditions (such as prostate cancer and severe acne/hirsutism) and use in gender-affirming hormone therapy. These mechanisms — androgen receptor antagonism and progestogenic activity — may have relevance to conditions driven by androgen excess or hormone-dependent cell proliferation, but this hypothesis cannot be formally evaluated until TxGNN predictions are available.

Once predictions are generated, the mechanistic bridge between Cyproterone Acetate's antiandrogen pathway and candidate new indications should be assessed using updated DrugBank MOA data (Data Gap DG002).

---

## Clinical Trial Evidence

Currently no related clinical trials registered in the Evidence Pack.

> **Note:** This reflects the absence of `predicted_indications` in the current dataset, not the global clinical trial landscape. Once a target indication is identified via TxGNN, a dedicated ClinicalTrials.gov search should be conducted.

---

## Literature Evidence

Currently no related literature available in the Evidence Pack.

> **Note:** Same as above — literature evidence is indication-specific and cannot be retrieved until TxGNN predictions define the candidate repurposing target.

---

## New Zealand Market Information

Cyproterone Acetate is **not currently registered** in New Zealand. No product authorizations were found in the regulatory query (0 licenses, query date 2026-03-29).

> This does not preclude future registration or off-label use discussion, but any repurposing pathway would require full regulatory submission from the outset.

---

## Safety Considerations

Please refer to the package insert for safety information.

> The TFDA package insert query (query ID 4) returned one result on 2026-03-29, but the structured safety fields (key warnings, contraindications) could not be parsed into the Evidence Pack. Retrieval and parsing of the package insert PDF is listed as a Blocking data gap (DG001) and must be resolved before any safety screening can proceed.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Evidence Pack for Cyproterone Acetate (DB04839) is incomplete at two blocking points: no TxGNN predictions have been generated, and critical safety data (package insert warnings and contraindications) has not been structured into the pipeline. Evaluation cannot proceed responsibly without these inputs.

**To proceed, the following is needed:**

- **[Blocking — DG001]** Parse and structure the TFDA package insert PDF to extract warnings, contraindications, and dosing restrictions — required before safety screening (S1 gate)
- **[High — DG002]** Query DrugBank API to retrieve mechanism of action (MOA) data for DB04839 — required for mechanistic bridging analysis
- **[Pipeline]** Re-run TxGNN prediction pipeline for DB04839 to generate `predicted_indications` with scores, clinical trial links, and literature evidence
- **[Regulatory]** Confirm whether Cyproterone Acetate has been evaluated or rejected by Medsafe (New Zealand) previously, as the absence of registration may reflect a prior regulatory decision rather than a data gap
- **[Evidence]** Once a target indication is identified, conduct structured ClinicalTrials.gov and PubMed searches specific to that indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


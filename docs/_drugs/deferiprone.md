---
layout: default
title: Deferiprone
parent: 僅模型預測 (L5)
nav_order: 105
evidence_level: L5
indication_count: 9
---

# Deferiprone
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
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

# Deferiprone: Evaluation Report — Insufficient Data for Full Assessment

## One-Sentence Summary

Deferiprone (DB08826) is an oral iron chelator approved in multiple international markets for treating iron overload in patients with thalassaemia who cannot tolerate standard therapy.
However, **this Evidence Pack contains no TxGNN-predicted indications**, and Deferiprone holds **no New Zealand market authorization** — a complete repurposing evaluation cannot be conducted at this stage without first resolving the identified data gaps.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Iron overload in thalassaemia *(sourced from published pharmacology; not present in Evidence Pack)* |
| Predicted New Indication | None — no TxGNN predictions returned |
| TxGNN Prediction Score | N/A |
| Evidence Level | L5 (prediction stage not reached) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why is This Prediction Reasonable?

Not applicable — the TxGNN model has not returned any repurposing candidate for Deferiprone in this pipeline run. Without a predicted target indication, no mechanistic bridge can be assessed.

Currently, detailed mechanism of action data is not available in this Evidence Pack. Based on published pharmacology, Deferiprone belongs to the 3-hydroxy-4-pyridinone class of iron chelators: it binds trivalent iron (Fe³⁺) and promotes its urinary excretion, thereby preventing iron-mediated oxidative organ damage in chronic transfusion-dependent conditions such as beta-thalassaemia major. It is approved by the FDA (brand name **Ferriprox**) and EMA as an alternative to deferoxamine when deferoxamine therapy is inadequate or contraindicated.

Until TxGNN predictions are generated and MOA data is formally retrieved from DrugBank, the mechanistic rationale for any new indication remains speculative.

---

## Clinical Trial Evidence

No TxGNN-predicted indication is available; therefore, no indication-specific clinical trial evidence can be extracted at this stage.

---

## Literature Evidence

No TxGNN-predicted indication is available; therefore, no indication-specific literature evidence can be extracted at this stage.

---

## New Zealand Market Information

Deferiprone holds no current New Zealand Medsafe market authorizations. The TFDA query returned zero records; no approved product licenses exist in this jurisdiction.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** Both key warnings and contraindications were listed as data gaps in this Evidence Pack. The TFDA package insert query returned a result (query log ID 4, status: success), but the extracted content was not propagated into the safety fields. This should be resolved before any safety assessment is attempted.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN pipeline did not return any predicted indications for Deferiprone, and the drug is entirely absent from the New Zealand market. There is currently no basis — mechanistic, regulatory, or evidential — on which to recommend a repurposing pathway.

**To proceed, the following is needed:**

- **Re-run TxGNN pipeline** to generate predicted indications for Deferiprone (DB08826); verify input graph connectivity for this node
- **Retrieve MOA data** from DrugBank API (Data Gap DG002) to enable mechanistic analysis
- **Parse TFDA package insert** (query log ID 4 returned success — extract warnings and contraindications from the downloaded PDF to resolve Data Gap DG001)
- **Cross-reference FDA Ferriprox label** as a regulatory reference, given New Zealand's absence of local authorization
- **Re-submit Evidence Pack** once data gaps are resolved before any safety or efficacy evaluation proceeds
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


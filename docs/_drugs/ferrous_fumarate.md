---
layout: default
title: Ferrous Fumarate
parent: 僅模型預測 (L5)
nav_order: 150
evidence_level: L5
indication_count: 1
---

# Ferrous Fumarate
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

# Ferrous Fumarate: From Iron Deficiency (Indication Data Not on File) to Non-syndromic Esophageal Malformation

## One-Sentence Summary

Ferrous fumarate is an iron salt commonly used for iron supplementation, though this evidence pack does not contain formally recorded original indication or mechanism-of-action data for the drug. The TxGNN model predicts a possible association with **non-syndromic esophageal malformation**, but this prediction is currently supported by **zero clinical trials** and **zero publications**, and the accompanying mechanistic analysis explicitly finds no plausible biological link between iron supplementation and a congenital structural anomaly.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — no licensed indication text on file (drug is not marketed in New Zealand) |
| Predicted New Indication | Non-syndromic esophageal malformation |
| TxGNN Prediction Score | 99.49% |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for ferrous fumarate in this evidence pack. No original indication record was returned either, so a direct pharmacological comparison between the original and predicted indications cannot be constructed from the available data.

More importantly, the model's own repurposing rationale flags this specific prediction as biologically implausible: non-syndromic esophageal malformation is a congenital anatomical defect arising from failure of embryonic tracheo-esophageal septation, and it is managed surgically rather than pharmacologically. Iron salts act on iron-ion supplementation and hematopoietic support — a mechanism with no known connection to structural embryogenesis. There is therefore no credible mechanistic bridge between the drug's presumed pharmacology and this predicted indication.

Given the combination of missing drug-level data (MOA, original indication, safety label) and an explicit lack of mechanistic plausibility noted in the source rationale, this candidate should be treated as a pure model-output signal rather than a scientifically grounded repurposing hypothesis at this stage.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Ferrous fumarate is not currently marketed in New Zealand under this evidence pack, and no product authorizations are on file.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This prediction is supported only by a raw TxGNN model score (L5), with no clinical trials, no literature, and no plausible mechanistic rationale — the source analysis itself concludes there is no credible biological link between iron supplementation and a congenital esophageal structural defect. Core drug-level data (MOA, original indication, TFDA/NZ label warnings and contraindications) are also missing, which blocks even a preliminary safety assessment (S1 stage).

**To proceed, the following is needed:**
- Original indication and confirmed drug label data (currently missing — Blocking gap DG001)
- Mechanism of action data from DrugBank or equivalent source (High-priority gap DG002)
- An independent clinical/biological plausibility review of the non-syndromic esophageal malformation prediction before any further evidence search is warranted
- Any preclinical, case-report, or mechanistic literature that could establish a rationale, should one emerge
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


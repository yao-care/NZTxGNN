---
layout: default
title: Isotretinoin
parent: 僅模型預測 (L5)
nav_order: 183
evidence_level: L5
indication_count: 2
---

# Isotretinoin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
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

# Isotretinoin: From Unspecified Original Indication to Malignant Renovascular Hypertension

## One-Sentence Summary

> The evidence pack does not document Isotretinoin's original approved indication or mechanism of action (both flagged as data gaps).
> The TxGNN model predicts potential efficacy for **Malignant Renovascular Hypertension**,
> but this prediction is currently supported by **0 clinical trials** and **0 publications** — it is a model-only signal.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — evidence pack contains no license or indication records for this drug |
| Predicted New Indication | Malignant Renovascular Hypertension |
| TxGNN Prediction Score | 99.01% |
| Evidence Level | L5 (model prediction only, no clinical trials, no literature) |
| New Zealand Market Status | Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data for Isotretinoin is not currently available in this evidence pack (flagged as DG002, High severity). Isotretinoin is known to be a retinoic acid derivative (RAR/RXR agonist class); the repurposing rationale in the evidence pack notes that retinoic acid signaling has been indirectly linked in animal models to renin-angiotensin system (RAAS) regulation, vascular smooth muscle differentiation, and renal fibrosis.

However, this mechanistic connection to malignant renovascular hypertension is explicitly characterized in the evidence pack as **speculative** — there is no direct mechanistic evidence, no clinical trial, and no literature support for this specific link. The prediction rests entirely on the TxGNN model's statistical output (score 0.990, rank 7466 among all drug-disease pairs), not on a validated pharmacological rationale.

A second, closely related prediction — **Malignant Hypertensive Renal Disease** (score 0.990, rank 7467) — shares the identical mechanistic hypothesis and the same lack of supporting evidence, suggesting the model is capturing a general "retinoid–renal/vascular" signal rather than indication-specific pharmacology.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Isotretinoin currently holds no marketing authorization on record (0 licenses, market status: Not Marketed). No product-level information is available to summarize.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: TFDA package insert warnings/contraindications are flagged in the evidence pack as a Blocking data gap — DG001 — meaning a formal safety pre-assessment (S1) cannot proceed until this is resolved.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This candidate is at Evidence Level L5 — a TxGNN model prediction with no corroborating clinical trials or literature, and no verified mechanistic pathway. Combined with two Blocking/High-severity data gaps (missing TFDA label safety data and missing MOA), there is insufficient basis to advance beyond initial screening.

**To proceed, the following is needed:**
- TFDA package insert warnings and contraindications (DG001, Blocking — required before any safety pre-assessment)
- Confirmed mechanism of action from DrugBank or primary literature (DG002, High)
- Original approved indication data (currently absent from the evidence pack) to properly assess indication-to-indication plausibility
- Preclinical or mechanistic studies specifically testing the retinoid–RAAS/renal fibrosis hypothesis in the context of malignant hypertension, before considering any clinical investigation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Insulin Lispro
parent: 僅模型預測 (L5)
nav_order: 177
evidence_level: L5
indication_count: 9
---

# Insulin Lispro
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

# Insulin Lispro: From Diabetes Mellitus to Autoimmune Oophoritis

## One-Sentence Summary

Insulin lispro is a rapid-acting insulin analog originally used to achieve glycemic control in diabetes mellitus. The TxGNN model predicts a possible link to **Autoimmune Oophoritis**, but this pairing currently has **zero clinical trials** and **zero publications** supporting it.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Diabetes mellitus (glycemic control) — general drug-class knowledge; no New Zealand license text is available to confirm the exact approved wording |
| Predicted New Indication | Autoimmune Oophoritis |
| TxGNN Prediction Score | 99.78% |
| Evidence Level | L5 (model prediction only, no supporting studies) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, insulin lispro is a rapid-acting insulin analog used for insulin replacement therapy, and its efficacy in diabetes mellitus is well established; mechanistically, insulin/IGF signaling could in theory intersect with ovarian follicular and immune biology, which is the likely basis for the model's association.

However, this same evidence pack flags a cluster of adjacent low-confidence predictions for this drug — classic stiff person syndrome, focal stiff limb syndrome, centrifugal lipodystrophy, and pressure-induced localized lipoatrophy — as probable **false positives caused by the knowledge graph confusing disease co-morbidity with a direct treatment relationship** (e.g., insulin is used to manage co-existing type 1 diabetes in stiff-person-syndrome patients, not to treat the syndrome itself; lipodystrophy/lipoatrophy are actually known adverse effects of insulin injection, not indications for it). Autoimmune oophoritis shares the same risk profile: it is an autoimmune endocrine disease that can co-occur with autoimmune (type 1) diabetes, so the high score plausibly reflects shared patient population/co-morbidity rather than a genuine pharmacological effect of insulin on ovarian autoimmunity. No mechanistic or clinical data currently distinguish these possibilities.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

Insulin lispro is not currently marketed in New Zealand (Medsafe status: not marketed); no authorization records are available.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted pairing has no clinical trial or literature support (L5, model prediction only), the drug is not marketed in New Zealand, and safety/label data are entirely unavailable (a Blocking data gap that prevents even an initial S1 safety screen). There is no basis to advance this candidate at this time.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert — warnings, contraindications (DG001, Blocking; currently prevents S1 safety evaluation)
- Verified mechanism of action data (DG002, High)
- Any preclinical or mechanistic evidence linking insulin signaling to autoimmune oophoritis specifically (not just diabetes co-morbidity)
- Confirmation of whether the TxGNN signal reflects a true treatment effect versus disease co-occurrence bias, given the same pattern was flagged as likely false-positive for related predictions in this drug's own evidence pack
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


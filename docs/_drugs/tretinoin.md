---
layout: default
title: Tretinoin
parent: 僅模型預測 (L5)
nav_order: 352
evidence_level: L5
indication_count: 10
---

# Tretinoin
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

# Tretinoin: From Acne Vulgaris / Acute Promyelocytic Leukemia to Rheumatoid Nodulosis

## One-Sentence Summary

Tretinoin (all-trans retinoic acid) is a retinoid best known for treating acne vulgaris (topical) and acute promyelocytic leukemia (oral differentiation therapy). The TxGNN model predicts it may be effective for **Rheumatoid Nodulosis**, but this direction is currently supported by **0 clinical trials** and **0 publications** — it is a model-only signal with no corroborating evidence.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Acne vulgaris / Acute promyelocytic leukemia (APL) — based on well-known drug information; not specified in evidence pack |
| Predicted New Indication | Rheumatoid Nodulosis |
| TxGNN Prediction Score | 99.84% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (blocking data gap). Based on known information, tretinoin is a retinoid that activates nuclear retinoic acid receptors (RAR/RXR), driving cell differentiation — this underlies its established efficacy in acne (keratinocyte differentiation) and APL (differentiation of malignant promyelocytes). Retinoid signaling is also known more broadly to modulate immune cell differentiation and inflammatory gene expression, which is the theoretical bridge to rheumatologic conditions.

However, for this specific prediction the evidence pack's own rationale is explicit: *"僅TxGNN模型高分預測，無已知機轉文獻或臨床證據支持tretinoin與此適應症之關聯"* — i.e., there is no mechanistic literature or clinical evidence directly linking tretinoin to rheumatoid nodulosis. The plausibility above is a general pharmacological argument, not evidence specific to this indication, and should be treated as a hypothesis only.

Notably, among the 10 TxGNN-predicted indications in this pack, only two carry any literature support: **osteoarthritis** (rank 7, 20 publications, L4) and **Quinquaud's folliculitis decalvans** (rank 10, 1 case report, L4). Rheumatoid nodulosis (rank 1) has neither, despite the highest raw model score — a reminder that TxGNN rank alone does not correlate with evidence availability.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for rheumatoid nodulosis rests entirely on the TxGNN model score, with zero clinical trials, zero literature, and no mechanism-of-action data to support biological plausibility. Combined with a blocking data gap on TFDA/Medsafe safety labeling, this candidate cannot proceed past S0.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — blocking gap (DG001)
- DrugBank mechanism-of-action data — high-priority gap (DG002)
- Targeted literature/preclinical search specifically on retinoic acid signaling in rheumatoid nodulosis or related autoimmune nodule formation
- Consider re-scoping evaluation toward the two evidence-supported candidates in this pack (osteoarthritis, folliculitis decalvans) rather than the top-ranked but evidence-free indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


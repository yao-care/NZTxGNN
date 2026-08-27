---
layout: default
title: Pegfilgrastim
parent: 僅模型預測 (L5)
nav_order: 270
evidence_level: L5
indication_count: 2
---

# Pegfilgrastim
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

# Pegfilgrastim: From Unspecified Original Indication to Severe Nonproliferative Diabetic Retinopathy

## One-Sentence Summary

The original approved indication and mechanism of action for pegfilgrastim are not available in the current evidence pack, and the drug is not currently marketed in New Zealand.
The TxGNN model predicts a possible signal for **Severe Nonproliferative Diabetic Retinopathy**, but this is a **model-prediction-only signal (L5)** with **zero clinical trials and zero publications** currently supporting it, and the mechanistic rationale itself raises a safety concern rather than an efficacy rationale.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in current evidence pack |
| Predicted New Indication | Severe Nonproliferative Diabetic Retinopathy |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for pegfilgrastim in this evidence pack, and its original indication(s) are also not recorded. Based on general pharmacological class knowledge referenced in the evidence's own rationale, pegfilgrastim is a pegylated, long-acting analogue of G-CSF (granulocyte colony-stimulating factor), which stimulates proliferation and differentiation of granulocyte precursors and activates neutrophil release and function.

This mechanism has **no established direct therapeutic link** to the pathology of diabetic retinopathy (microvascular basement membrane thickening, pericyte loss, VEGF-driven abnormal angiogenesis, ischemia-reperfusion injury). Notably, the evidence pack's own repurposing rationale flags that endogenous G-CSF/neutrophil activation has been associated in the literature with *increased* disease activity in proliferative diabetic retinopathy — potentially by promoting pathological angiogenesis or leukocyte-mediated microvascular occlusion. This suggests the mechanistic relationship may point toward a **risk signal rather than a treatment signal**, and the high TxGNN score likely reflects co-occurrence of "neutrophil/inflammation/angiogenesis" graph nodes rather than a causal therapeutic relationship. This same caution applies to the closely related second-ranked prediction, "diabetic retinopathy" (score 99.73%, rank 2935), which shares an identical mechanistic concern.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## New Zealand Market Information

Pegfilgrastim is not currently marketed in New Zealand (0 authorizations on file), so no product-level licensing information is available.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Both predicted indications (severe nonproliferative diabetic retinopathy and diabetic retinopathy) are supported only by TxGNN model output (L5) with no clinical trials or literature, and the drug's own mechanistic rationale suggests a plausible safety concern (worsening of pathological angiogenesis) rather than a treatment benefit — the opposite direction from what the prediction implies. This does not meet the threshold to advance past S0.

**To proceed, the following is needed:**
- TFDA/regulatory package insert warnings and contraindications (currently a blocking data gap — required before any S1 safety screening)
- Verified mechanism of action data for pegfilgrastim
- Original approved indication(s) confirmed from a regulatory source
- Preclinical or mechanistic studies directly evaluating G-CSF/neutrophil pathway effects on diabetic retinopathy progression (given the directionality concern raised above)
- Any emerging clinical trial or literature evidence specific to these two indications
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


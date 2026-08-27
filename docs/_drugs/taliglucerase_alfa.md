---
layout: default
title: Taliglucerase Alfa
parent: 僅模型預測 (L5)
nav_order: 330
evidence_level: L5
indication_count: 5
---

# Taliglucerase Alfa
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

# Taliglucerase Alfa: From Gaucher Disease to Hurler Syndrome

## One-Sentence Summary

Taliglucerase alfa is a recombinant human glucocerebrosidase used as enzyme replacement therapy for Gaucher disease (GBA gene deficiency). The TxGNN model predicts it may be effective for **Hurler syndrome (MPS I)**, but currently **no clinical trials** and **no published literature** support this direction — the prediction rests on model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Gaucher disease (per drug classification; New Zealand regulatory data unavailable — not marketed) |
| Predicted New Indication | Hurler syndrome |
| TxGNN Prediction Score | 99.52% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for taliglucerase alfa is not available in the current evidence pack. Based on known drug classification, taliglucerase alfa is a recombinant human glucocerebrosidase, used exclusively as enzyme replacement therapy to correct the GBA enzyme deficiency underlying Gaucher disease.

Hurler syndrome (MPS I) is caused by deficiency of a different lysosomal enzyme, alpha-L-iduronidase, acting on a different substrate (glycosaminoglycans rather than glucocerebroside). Although Gaucher disease and Hurler syndrome are both lysosomal storage diseases, they do not share a common enzyme target or metabolic pathway.

The mechanistic rationale accompanying this prediction explicitly flags this as a weak link, more likely reflecting TxGNN's disease-similarity clustering (grouping diseases of the same broad category) than a genuine pharmacological mechanism transferable from glucocerebrosidase replacement to alpha-L-iduronidase deficiency. This should be treated as a hypothesis-generating signal only, not mechanistically validated.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

Taliglucerase alfa is not currently marketed in New Zealand (0 product authorizations on file), so no product-level licensing information is available.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high, but there are zero supporting clinical trials, zero literature citations, and the drug's own mechanistic rationale describes the enzyme-target overlap with Hurler syndrome as biologically weak (glucocerebrosidase vs. alpha-L-iduronidase, distinct substrates). This is a pure model-prediction signal (L5) with no corroborating evidence.

**To proceed, the following is needed:**
- TFDA/regulatory package insert data (currently a blocking data gap — DG001)
- Confirmed, sourced mechanism-of-action documentation for taliglucerase alfa (DG002)
- Preclinical or mechanistic studies testing any cross-pathway effect on alpha-L-iduronidase activity or glycosaminoglycan accumulation
- If pursued, safety and dosing data specific to an MPS I population, since none currently exists for this drug
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


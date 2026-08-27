---
layout: default
title: Travoprost
parent: 僅模型預測 (L5)
nav_order: 351
evidence_level: L5
indication_count: 10
---

# Travoprost
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

# Travoprost: From Glaucoma to Visceral Calciphylaxis

## One-Sentence Summary

Travoprost is a prostaglandin F2α (FP) receptor agonist eye drop used to lower intraocular pressure in open-angle glaucoma and ocular hypertension. The TxGNN model's top prediction for this drug is **Visceral Calciphylaxis**, but this direction is currently supported by **0 clinical trials** and **0 publications**, with no known mechanistic link identified.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Open-angle glaucoma / Ocular hypertension (inferred from trial evidence; no formal NZ license record exists) |
| Predicted New Indication | Visceral Calciphylaxis |
| TxGNN Prediction Score | 99.9998% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for Travoprost in this evidence pack. Based on known pharmacology, Travoprost is a topical FP-receptor agonist prodrug used to reduce intraocular pressure by increasing uveoscleral outflow — its efficacy in open-angle glaucoma and ocular hypertension is well established through numerous Phase 3/4 trials.

However, for the top-ranked prediction (Visceral Calciphylaxis), no mechanistic or clinical rationale connects FP-receptor agonism to the pathophysiology of vascular/visceral calcification. The evidence pack's own analysis states explicitly that there is no known pathway linking FP-receptor activity to calciphylaxis prevention or treatment — this ranking reflects the TxGNN network's prediction score alone, without any supporting trial or literature evidence.

Notably, a lower-ranked prediction in the same evidence pack — "vascular disease" (rank 5) — does have some supporting signal: a Phase 4 study (NCT00308945) examined Travoprost's effect on retinal vascular diameter and choroidal blood flow, suggesting a possible systemic vasoactive property. This is a mechanistically more plausible, evidence-backed direction than the top-ranked Visceral Calciphylaxis prediction, though it too is confounded mostly by glaucoma-indication trial noise (see Conclusion).

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

Travoprost currently holds **0 authorizations** and is **not marketed** in New Zealand, per the available regulatory data. No product license records are available to summarize.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Despite a near-maximal TxGNN prediction score, Visceral Calciphylaxis has zero clinical trials, zero literature, and no identified mechanistic link — this is a pure model-prediction signal (L5) with no corroborating evidence, so it does not meet the bar to advance.

**To proceed, the following is needed:**
- Confirmed mechanism of action (MOA) data for Travoprost (currently a data gap)
- Any preclinical or in vitro evidence connecting FP-receptor agonism to vascular/visceral calcification pathways
- TFDA/Medsafe package insert (warnings, contraindications) — currently a blocking data gap for safety review
- Consider evaluating **"vascular disease" (rank 5)** instead as a lower-score but evidence-bearing alternative direction (1 Phase 4 mechanistic trial on choroidal blood flow), while noting most of its 15 trials/20 papers reflect original glaucoma-indication data rather than true repurposing signal
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


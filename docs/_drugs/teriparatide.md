---
layout: default
title: Teriparatide
parent: Model Prediction Only (L5)
nav_order: 445
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Teriparatide: predicted new indication Duodenal ulcer (evidence level L5). Approved use: Postmenopausal osteoporosis / osteoporosis"
monitor_inn: "TERIPARATIDE"
predicted_indications: ["duodenal ulcer (disease)", "non-syndromic esophageal malformation", "duodenal obstruction", "duodenogastric reflux", "esophageal disease", "Worth syndrome", "autosomal dominant neovascular inflammatory vitreoretinopathy", "succinyl-CoA:3-ketoacid CoA transferase deficiency", "amenorrhea (disease)"]
---

# Teriparatide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Teriparatide: From Postmenopausal Osteoporosis to Duodenal Ulcer

## One-Sentence Summary

Teriparatide is a parathyroid hormone fragment (PTH 1-34) that was originally used to treat osteoporosis in patients at high risk of fracture.
The TxGNN model predicts it may be effective for **duodenal ulcer**, but there are **0 clinical trials** and **0 publications** supporting this prediction.
It is a model-only signal with no plausible mechanistic link, so we recommend **Hold**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Postmenopausal osteoporosis / osteoporosis (from the knowledge graph; the New Zealand licence records contain no indication text) |
| Predicted New Indication | Duodenal ulcer |
| TxGNN Prediction Score | 99.86% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action text is not available. Pharmacology data show that teriparatide is a peptide agonist of the **PTH1 receptor (PTH1R)** and also binds the PTH2 receptor. By activating PTH1R it stimulates osteoblast-mediated bone formation, which is why it is used for osteoporosis in patients at high fracture risk.

The prediction is **not well supported**. Bone formation through PTH1R has no known role in healing the gastroduodenal mucosa. The high TxGNN score (99.86%) is a graph-based association with no clinical or literature backing. It most likely reflects shared network annotations in the knowledge graph rather than a true pharmacological connection. Osteoporosis and duodenal ulcer are unrelated diseases, so the original indication gives no mechanistic reason to expect efficacy here.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Status | Approved Indication |
|---------|------|------|------|-----------|
| 2724324 | Forteo | Injection 250 mcg per ml, 2.4 ml | Cancelled | Not stated in the data compiled |
| 2670399 | Teriparatide - Teva | Injection 250 mcg per ml, 2.4 ml | Cancelled | Not stated in the data compiled |

Both licences are for single-ingredient injectable products, and neither is currently valid. Any future use would therefore depend on obtaining a product through other channels.

---

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (these come from graph edges, not from package insert text): hypotensive disorder, hypercalcaemia, nephrocalcinosis, phosphorus metabolism disease, bone osteosarcoma, bone Paget disease, juvenile Paget disease, osteomalacia, rickets, urolithiasis, renal osteodystrophy, osteitis fibrosa, and hyperparathyroidism.
- **Drug interactions** (8 records, including 2 receptor-target entries):
  - Moderate: topical calcitriol, topical calcipotriol, digoxin, digitoxin
  - Minor: hydrochlorothiazide, furosemide

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The duodenal ulcer prediction is supported only by a model score. It has no clinical trials, no literature, and no plausible mechanism, and teriparatide is not currently marketed in New Zealand. It should not be pursued further.

**Other predicted indications (for context):**
- **Pregnancy-associated osteoporosis (rank 8, evidence level L3, Proceed with Guardrails):** This is the best-supported lead. It extends the approved bone-formation mechanism and has about 20 publications, including systematic reviews and teriparatide cohort studies. The evidence is observational, with no RCTs.
- **Amenorrhoea (rank 10, evidence level L4):** The link is indirect, through bone loss in functional hypothalamic amenorrhoea. Teriparatide would not treat the amenorrhoea itself.
- **Worth syndrome (rank 6):** Teriparatide may be counterproductive because the condition already involves high bone mass.

**To proceed, the following is needed:**
- Evaluate pregnancy-associated osteoporosis as a separate candidate, with specialist guardrails (lactation stopped, postpartum use only, duration limits).
- Obtain the Medsafe package insert to complete the safety screening, which is currently blocked.
- Confirm product availability, since both New Zealand licences are cancelled.
- Add detailed mechanism-of-action data from DrugBank.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


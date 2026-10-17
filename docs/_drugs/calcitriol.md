---
layout: default
title: Calcitriol
parent: Model Prediction Only (L5)
nav_order: 80
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Calcitriol: predicted new indication Obsolete vitamin D deficiency (evidence level L5). Approved use: Hypercalcaemia, nephrocalcinosis, phosphorus metabolism disease…"
monitor_inn: "CALCITRIOL"
predicted_indications: ["obsolete vitamin D deficiency", "renal tubular acidosis", "acromesomelic dysplasia, Campailla Martinelli type", "craniofacial conodysplasia", "Dahlberg-Borer-Newcomer syndrome", "hereditary hypophosphatemic rickets"]
---

# Calcitriol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

# Calcitriol: From Calcium and Phosphate Metabolism Disorders to Obsolete Vitamin D Deficiency

## One-Sentence Summary

Calcitriol is the active form of vitamin D. It is used in disorders of calcium and phosphate balance such as hypoparathyroidism and renal osteodystrophy.
The TxGNN model predicts it may be effective for **obsolete vitamin D deficiency**, a legacy ontology term, with a very high score.
There are **0 clinical trials** and **0 publications** supporting this specific prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypercalcaemia, nephrocalcinosis, phosphorus metabolism disease, hypophosphataemia, hypoparathyroidism, renal osteodystrophy (from the knowledge graph; the New Zealand licence records contain no indication text) |
| Predicted New Indication | Obsolete vitamin D deficiency |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Based on known information, calcitriol is the hormonally active form of vitamin D (1,25-dihydroxyvitamin D3). It is already used in conditions where the body cannot make enough active vitamin D, such as hypoparathyroidism and kidney-related bone disease. Mechanistically, it may be applicable to vitamin D deficiency.

The predicted disease entry is marked "obsolete", so it appears to be a legacy ontology term rather than a current diagnosis. The high score therefore reflects a graph-based prediction, with no trials or literature behind it. Any real-world relevance would map to current vitamin D deficiency or rickets entries, which should be assessed separately.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

All four authorizations are cancelled, so no calcitriol product is currently marketed in New Zealand. The data compiled for this report contain no approved-indication text for these licences.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2691663 | Calcitriol XL | Capsule 0.5 mcg | Not stated (cancelled) |
| 2091011 | Calcitriol-AFT | Capsule 0.5 mcg | Not stated (cancelled) |
| 2091038 | Calcitriol-AFT | Capsule 0.25 mcg | Not stated (cancelled) |
| 2695154 | Calcitriol XL | Capsule 0.25 mcg | Not stated (cancelled) |

## Safety Considerations

- **Disease associations flagged as contraindications (knowledge graph, not label text)**: kidney disease, pyoureter, sarcoidosis, hypercalcaemia, nephrocalcinosis, phosphorus metabolism disease, nephrolithiasis (including susceptibility caused by SLC26A1), acute urate nephropathy, hyperphosphataemia, hypervitaminosis, arteriosclerosis obliterans. These come from a computational resource and must be verified against the official package insert.
- **Drug Interactions**: 100 interactions were identified.
  - **Major**: cholecalciferol, calcifediol and burosumab, mainly because of additive vitamin D activity or interference with phosphate regulation.
  - **Moderate**: thiazide diuretics (hydrochlorothiazide, chlorothiazide, chlorthalidone and others), anticonvulsants and barbiturates (carbamazepine, fosphenytoin, amobarbital, butalbital, butabarbital), bile-acid sequestrants (cholestyramine, colesevelam, colestipol), digoxin and digitoxin, aluminium hydroxide, and topical calcipotriol.

Please refer to the package insert for official warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the model score (L5). The target disease is an obsolete ontology term, and no trials or publications address it. All New Zealand licences are cancelled, which further limits practical availability.

**To proceed, the following is needed:**
- Re-map the prediction to current vitamin D deficiency or rickets entries and assess them separately.
- Mechanism of action data and the official package insert (warnings, contraindications, approved indications).
- Hypercalcaemia and nephrocalcinosis risk assessment, given the knowledge-graph contraindication signals.
- Prioritisation of a stronger candidate from the same analysis: **hereditary hypophosphataemic rickets** has two relevant trials (NCT03748966, a completed early Phase 1 study of calcitriol monotherapy in XLH, and NCT03820518, a Phase 4 dosing trial) and supporting reviews. It reaches only moderate evidence, but is much better supported than this prediction.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Travoprost
parent: Model Prediction Only (L5)
nav_order: 465
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-28
description: "Travoprost: predicted new indication Visceral calciphylaxis (evidence level L5). Approved use: Open-angle glaucoma"
monitor_inn: "TRAVOPROST"
predicted_indications: ["visceral calciphylaxis", "arterial thoracic outlet syndrome", "venous thoracic outlet syndrome", "neurogenic thoracic outlet syndrome", "vascular disease", "angiodysplasia of stomach", "blue toe syndrome", "idiopathic spontaneous coronary artery dissection", "lymphangiectasis", "hemangioendothelioma"]
---

# Travoprost
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Travoprost: From Open-Angle Glaucoma to Visceral Calciphylaxis
{: .text-alpha }

## One-Sentence Summary

Travoprost is a topical prostaglandin F2-alpha analogue, originally used to lower eye pressure in open-angle glaucoma and ocular hypertension.
The TxGNN model predicts it may be effective for **visceral calciphylaxis**, but **no clinical trials and no publications** support this prediction.
It is a computational prediction only and is not backed by any real-world evidence.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Open-angle glaucoma; ocular hypertension |
| Predicted New Indication | Visceral calciphylaxis |
| TxGNN Prediction Score | 1.0000 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known information, travoprost is a prostaglandin F2-alpha analogue that acts on the FP receptor. Its efficacy in lowering intraocular pressure in glaucoma is well established, but no established mechanistic route to visceral calciphylaxis has been shown.

The original and predicted indications are not closely related. Glaucoma is an eye disease treated with eye drops. Visceral calciphylaxis is a systemic vascular calcification disorder. Systemic exposure after topical eye dosing is minimal, so it is hard to see how the drug could reach the relevant tissues at useful levels. The very high TxGNN score (about 1.0) is not corroborated by any trial or publication, and the mechanism cannot be cross-checked.

Other top-ranked predictions are also computational only, with no supporting evidence and no evident pharmacological rationale. They include arterial, venous and neurogenic thoracic outlet syndrome, angiodysplasia of stomach, blue toe syndrome, spontaneous coronary artery dissection and lymphangiectasis.

The "vascular disease" prediction (rank 5) did return trials and publications. However, all concern glaucoma or ocular hypertension, such as IOP lowering and conjunctival hyperemia as a side effect. They were retrieved through vascular-related terms and do not test a vascular disease indication. The only item with a vascular endpoint, NCT00308945 (n=20), is a physiological measurement of retinal vessel diameter and choroidal blood flow in glaucoma patients.

## Clinical Trial Evidence

Currently no related clinical trials registered for visceral calciphylaxis.

## Literature Evidence

Currently no related literature available for visceral calciphylaxis.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2550091 (cancelled) | Travatan | Eye drops 0.004% | Not recorded |

## Safety Considerations

- **Knowledge-graph contraindication signals** (from the TxGNN knowledge graph, not from label text): uveitis, iris disease, ciliary body disease, panuveitis, iritis, cystoid macular edema and macular retinal edema.
- **Drug Interactions**: 49 interactions were recorded, all with an unclassified severity level. Examples include calcitriol, pantoprazole, sucralfate, morphine, metformin, omeprazole, prednisone, simvastatin, acetylsalicylic acid and glyburide.
- **Safety signal from the literature**: a case report (PMID 19107053) describes uveal effusion with exudative retinal detachment induced by topical travoprost in a patient with Sturge-Weber-Krabbe syndrome, a vascular malformation disorder.

Please also refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on model output alone (evidence level L5). There are no trials or publications for visceral calciphylaxis, and no plausible mechanism links a topical ocular prostaglandin analogue to this systemic condition. The only New Zealand authorization has been cancelled.

**To proceed, the following is needed:**
- Mechanism of action data, to test whether any link to calcification or vascular pathology exists
- Package insert warnings and contraindications, which are required for safety screening
- Preclinical or mechanistic studies showing relevance to visceral calciphylaxis
- A review of route compatibility, since the drug is currently available only as an ophthalmic product
- Re-evaluation of the vascular-related predictions only if new, indication-specific evidence emerges
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


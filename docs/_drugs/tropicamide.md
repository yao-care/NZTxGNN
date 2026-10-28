---
layout: default
title: Tropicamide
parent: Model Prediction Only (L5)
nav_order: 469
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-15
monitor_inn: "TROPICAMIDE"
predicted_indications: ["cauda equina syndrome", "obsolete neurogenic bladder (disease)", "irritable bowel syndrome"]
description: "Tropicamide: predicted new indication Cauda equina syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Tropicamide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

## Tropicamide: Cauda equina syndrome
{: .text-alpha }

## One-Sentence Summary

Tropicamide is a short-acting antimuscarinic eye drop, originally used to produce cycloplegia and pupil dilation for eye examinations.
The TxGNN model predicts it may be effective for **cauda equina syndrome**, but there are currently **no clinical trials and no publications** supporting this direction, so it is a model prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Cauda equina syndrome |
| TxGNN Prediction Score | 0.9953 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on the known pharmacology of the drug and the receptor data compiled, tropicamide is a muscarinic antagonist that acts on the M2, M3, M4 and M5 receptors. Its established use is as a short-acting mydriatic to allow better examination of the lens, vitreous humor and retina.

Cauda equina syndrome causes neurogenic bladder and bowel dysfunction, so antimuscarinic action could in principle reduce detrusor overactivity. However, this link is weak:
- Tropicamide is formulated only as an ophthalmic product with minimal systemic exposure.
- Cauda equina syndrome is caused by nerve root compression and requires surgical decompression, not drug therapy alone.
- The high score most likely reflects a shared antimuscarinic class association with bladder drugs rather than a validated therapeutic link.

Other predictions with the same weakness were also generated for this drug:
- **Neurogenic bladder (obsolete ontology term)**, score 0.9913: antimuscarinics such as oxybutynin and tolterodine are established for this use, but approved alternatives already exist and the obsolete term weakens the mapping.
- **Irritable bowel syndrome**, score 0.9912: antispasmodics such as hyoscine and dicyclomine are used for it, but tropicamide has no oral or systemic formulation.

All three predictions are L5 (model prediction only).

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 254002 | Mydriacyl (valid) | Eye drops 1% | Not recorded |
| 253995 | Mydriacyl (valid) | Eye drops 0.5% | Not recorded |


## Safety Considerations

- **Contraindication signals from the knowledge graph (not label text)**: the following conditions are linked to tropicamide as contraindications:
  - Eye: open-angle glaucoma, angle-closure (closed-angle) glaucoma, ocular hypertension
  - Cardiovascular: hypertension and hypertensive disorder, myocardial ischemia, coronary artery disease, sudden cardiac arrest, heart conduction disease
  - Metabolic and endocrine: diabetes mellitus, diabetic ketoacidosis, hyperthyroidism, Graves disease, thyrotoxicosis
- **Drug interactions**: 10 interacting drugs were identified, but their severity levels are unclassified. These are atropine, potassium chloride, vancomycin, simvastatin, prednisolone, famotidine, warfarin, lidocaine, fluconazole and brimonidine. Atropine is another antimuscarinic, so additive anticholinergic effects are plausible.
- **Receptor targets**: M2, M3, M4 and M5 muscarinic receptors.

Package insert warnings and contraindications were not available. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on model scores and class-level plausibility only, with no trials or publications.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data
- Evidence that a systemically relevant formulation and route could deliver adequate exposure for neurogenic bladder or bowel symptoms
- Preclinical or clinical data supporting any non-ophthalmic use
- Comparison against approved antimuscarinic alternatives for neurogenic bladder and irritable bowel syndrome, which are more plausible targets than cauda equina syndrome
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


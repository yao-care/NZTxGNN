---
layout: default
title: Octreotide
parent: Model Prediction Only (L5)
nav_order: 333
evidence_level: L5
indication_count: 2
last_modified_at: 2026-11-12
monitor_inn: "OCTREOTIDE"
predicted_indications: ["vulvar inverted follicular keratosis", "seborrheic keratosis"]
description: "Octreotide: predicted new indication Vulvar inverted follicular keratosis (evidence level L5). Approved use: Not recorded in the local data"
---

# Octreotide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

## Octreotide: Vulvar inverted follicular keratosis
{: .text-alpha }

## One-Sentence Summary

Octreotide is a somatostatin analogue, originally used to treat acromegaly, hyperpituitarism and carcinoid syndrome.
The TxGNN model predicts it may be effective for **vulvar inverted follicular keratosis**, but there are **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Vulvar inverted follicular keratosis |
| TxGNN Prediction Score | 0.9958 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (11 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 11 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, octreotide is a somatostatin analogue that acts mainly on somatostatin receptors (SSTR2 and SSTR5). Its efficacy in acromegaly and carcinoid syndrome is established, which is consistent with suppressing hormone secretion and cell growth signalling.

A link to vulvar inverted follicular keratosis, a benign follicular keratinocyte lesion, is speculative. It would depend on somatostatin receptor expression in skin or follicular epithelium and on antiproliferative or anti-IGF-1 effects. Neither is supported by the data compiled for this report. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

A second predicted indication, **seborrheic keratosis** (score 0.9955), has the same problem. It also has no trials or literature, and it is a common benign lesion with low-risk standard treatments such as cryotherapy and curettage. A systemic peptide drug would have an unfavourable risk-benefit balance without supporting evidence. The two scores are nearly identical. Both come from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not independent biological evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

The table lists 5 of them.

<!-- review:begin nz-lic-20261106-octreotide-1 -->

> **Review correction (2026-11-06)**: Originally stated: "All 11 licences are valid, and no approved indication text is recorded. The table lists 5 of them." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 11 listings for this drug, 11 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-octreotide-1 -->

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2595885 | Octreotide GH | Octreotide | Inj 100 mcg per ml, 1 ml ampoule | Yes |
| 2629518 | Max Health | Octreotide | Inj 100 mcg per ml, 1 ml ampoule | Yes |
| 2669382 | Sun Pharma | Octreotide | Inj 100 mcg per ml, 1 ml ampoule | Yes |
| 2687275 | Omega | Octreotide | Inj 100 mcg per ml, 1 ml vial | Yes |
| 2595907 | Octreotide GH | Octreotide | Inj 50 mcg per ml, 1 ml ampoule | Yes |
| 2629534 | Max Health | Octreotide | Inj 50 mcg per ml, 1 ml ampoule | Yes |
| 2695375 | Omega | Octreotide | Inj 50 mcg per ml, 1 ml vial | Yes |
| 2595893 | Octreotide GH | Octreotide | Inj 500 mcg per ml, 1 ml ampoule | Yes |
| 2629526 | Max Health | Octreotide | Inj 500 mcg per ml, 1 ml ampoule | Yes |
| 2669390 | Sun Pharma | Octreotide | Inj 500 mcg per ml, 1 ml ampoule | Yes |
| 2694433 | Omega | Octreotide | Inj 500 mcg per ml, 1 ml vial | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Contraindication-related conditions (knowledge graph, not label text)**: The knowledge graph links octreotide to caution or contraindication in these conditions:
  - Metabolic: hyperglycaemia, hypoglycaemia, diabetes mellitus, diabetic ketoacidosis, vitamin B12 deficiency
  - Thyroid: hypothyroidism, myxedema, congenital hypothyroidism
  - Liver, biliary and gastrointestinal: cirrhosis of the liver, liver disease, gallbladder disease, cholelithiasis, cholecystolithiasis, choledocholithiasis, ileus, intestinal obstruction
  - Cardiac: sudden cardiac arrest, heart conduction disease
  - Renal: kidney failure, uremia
- **Drug Interactions**: 100 interactions were found, and the 20 returned are all rated Moderate. They are mainly glucose-lowering drugs, which fits octreotide's effect on blood glucose. Examples include:
  - Insulins (aspart, degludec, detemir, glargine, glulisine, human)
  - Metformin
  - Sulfonylureas (chlorpropamide, glimepiride)
  - GLP-1 agonists (albiglutide, dulaglutide, semaglutide)
  - SGLT2 inhibitors (canagliflozin, dapagliflozin, empagliflozin)
  - DPP-4 inhibitors (alogliptin, linagliptin)
  - Acarbose

Please refer to the package insert for warnings and formal contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only support for either predicted indication is the model score. There are no clinical trials or publications, the mechanism of action is not available, and both indications are benign conditions with low-risk standard care.

<!-- review:begin nz-lic-20261106-octreotide-2 -->

> **Review correction (2026-11-06)**: Originally stated: "There are no clinical trials or publications, the mechanism of action is not available, and both indications are benign conditions with low-risk standard care. Octreotide is marketed in New Zealand, with 11 of 11 licences valid." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 11 listings for this drug, 11 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-octreotide-2 -->

**To proceed, the following is needed:**
- Mechanism of action data (for example from DrugBank) and an assessment of somatostatin receptor expression in vulvar or follicular epithelium
- Package insert warnings and contraindications from Medsafe, which are required before safety screening
- Preclinical or case-level evidence of benefit in inverted follicular keratosis or seborrheic keratosis
- Confirmation of current New Zealand supply
- Route and formulation compatibility assessment (for example, whether a topical approach is plausible instead of an injection)

<!-- review:begin nz-lic-20261106-octreotide-3 -->

> **Review correction (2026-11-06)**: Originally stated: "Confirmation of current New Zealand supply (11 of 11 licences are valid)" Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 11 listings for this drug, 11 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-octreotide-3 -->

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "All 11 licences are valid, and no approved indication text is recorded" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "Octreotide is marketed in New Zealand, with 11 of 11 licences valid" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "(11 of 11 licences are valid)" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


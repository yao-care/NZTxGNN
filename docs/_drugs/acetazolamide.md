---
layout: default
title: Acetazolamide
parent: Model Prediction Only (L5)
nav_order: 15
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Acetazolamide: predicted new indication Exercise-induced malignant hyperthermia (evidence level L5). Approved use: Open-angle glaucoma, angle-closure glaucoma, altitude sickness…"
monitor_inn: "ACETAZOLAMIDE"
predicted_indications: ["exercise-induced malignant hyperthermia", "hypertrophic cardiomyopathy", "congenital myopathy with excess of thin filaments", "hypertrophic cardiomyopathy due to intensive athletic training", "distal myopathy, Tateyama type", "cirrhotic cardiomyopathy", "cardiomyopathy", "intestinal obstruction", "glycogen storage disease due to acid maltase deficiency, late-onset", "unclassified intestinal pseudoobstruction"]
---

# Acetazolamide
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

# Acetazolamide: From Glaucoma and Altitude Sickness to Exercise-Induced Malignant Hyperthermia

## One-Sentence Summary

Acetazolamide is a carbonic anhydrase inhibitor originally used for absence epilepsy, congestive heart failure, open-angle and angle-closure glaucoma, and altitude sickness.
The TxGNN model predicts it may be effective for **exercise-induced malignant hyperthermia**,
but there are **no clinical trials and no publications** supporting this prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Open-angle glaucoma, angle-closure glaucoma, altitude sickness, congestive heart failure, absence epilepsy (from the knowledge graph; no approved indication text is available for the New Zealand licence) |
| Predicted New Indication | Exercise-induced malignant hyperthermia |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, acetazolamide is a carbonic anhydrase inhibitor that alters acid-base balance and fluid handling. It has proven use in glaucoma, altitude sickness and heart failure congestion, and it is also used in some channelopathies such as periodic paralysis.

A link to malignant hyperthermia, which involves dysfunction of ryanodine receptor and calcium handling, is speculative. The data compiled for this report contain nothing that directly supports it. The very high TxGNN score (99.95%, model rank 737) reflects patterns in the knowledge graph, not demonstrated biology or clinical benefit.

Other top-ranked predictions are equally unsupported. Hypertrophic cardiomyopathy, cardiomyopathy and several myopathies have only indirect or tangential evidence, and for intestinal obstruction and pseudo-obstruction the literature points toward harm.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for exercise-induced malignant hyperthermia.

For context, the next-ranked predictions have only indirect literature. Examples are a case report of acetazolamide for hypochloremia in advanced heart failure with hypertrophic cardiomyopathy ([30279861](https://pubmed.ncbi.nlm.nih.gov/30279861/), 2018) and a case of non-cardiogenic pulmonary edema after intravenous acetazolamide ([29123889](https://pubmed.ncbi.nlm.nih.gov/29123889/), 2017). Case reports of acetazolamide-induced adynamic ileus ([19653068](https://pubmed.ncbi.nlm.nih.gov/19653068/), 2009) point toward harm.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2670801 (cancelled) | Medsurge | Tab 250 mg | Not available |

## Safety Considerations

Package insert warnings and contraindications were not available for this report, so please refer to the package insert for label safety information. The following signals come from other data sources:

- **Knowledge-graph contraindications** (not label text): liver failure, liver disease, kidney disease, renal tubular acidosis, hematologic disease, gallbladder disease, gout, type 1 and type 2 diabetes mellitus, diabetic ketoacidosis, respiratory failure, and adrenal insufficiency conditions (including Addison disease and congenital adrenal hyperplasia).
- **Drug interactions**: 100 interactions were identified. Major ones include acetylsalicylic acid and cisapride. Moderate ones include antidiabetic agents (acarbose, acetohexamide, alogliptin, canagliflozin, dapagliflozin, chlorpropamide, and GLP-1 agonists such as albiglutide and dulaglutide), corticosteroids (dexamethasone, betamethasone, beclomethasone), amphotericin B formulations, and laxatives (bisacodyl, castor oil).
- **Literature signals**: reports of adynamic ileus and non-cardiogenic pulmonary edema after intravenous use. Cirrhosis carries a risk of hyperammonemia and encephalopathy. Diuresis could be harmful in obstructive hypertrophic cardiomyopathy.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone, with no trials, no literature and no plausible mechanistic support. The only licence in New Zealand is cancelled, and the safety signals for several neighbouring predictions are unfavourable.

**To proceed, the following is needed:**
- Mechanism of action data, and any preclinical evidence linking carbonic anhydrase inhibition to ryanodine receptor or calcium handling in malignant hyperthermia
- Package insert warnings and contraindications (from Medsafe) for a formal safety screen
- Any clinical or case-level evidence in exercise-induced malignant hyperthermia
- Clarification of the regulatory pathway, since there is no valid New Zealand licence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


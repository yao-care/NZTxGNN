---
layout: default
title: Salbutamol
parent: Model Prediction Only (L5)
nav_order: 418
evidence_level: L5
indication_count: 8
last_modified_at: 2026-11-12
monitor_inn: "SALBUTAMOL"
predicted_indications: ["papillary conjunctivitis", "nasal cavity disease", "pharyngitis", "acute laryngopharyngitis", "anaphylaxis", "common cold", "atopic conjunctivitis", "anorectal stricture"]
description: "Salbutamol: predicted new indication Papillary conjunctivitis (evidence level L5). Approved use: Not recorded in the local data"
---

# Salbutamol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
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

## Salbutamol: Papillary conjunctivitis
{: .text-alpha }

## One-Sentence Summary

Salbutamol is a short-acting beta-2 adrenergic agonist that is marketed in New Zealand in inhaled, oral, nebulised and injectable forms.
The TxGNN model predicts it may be useful for **papillary conjunctivitis**, with a score of **0.9999**.
However, **no clinical trials and no publications** were found for this disease, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Papillary conjunctivitis |
| TxGNN Prediction Score | 1.0000 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (13 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 13 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available for this drug. Salbutamol is generally known as a beta-2 adrenergic agonist. Beta-2 stimulation can reduce the release of inflammatory mediators from mast cells, which is the main reason this prediction is conceivable in an allergic eye condition.

This link is speculative. Papillary conjunctivitis is often driven by contact lenses or mechanical irritation rather than by classic allergic mechanisms, so mast-cell stabilisation may not address the main cause. No studies were retrieved that test salbutamol in this condition, and the model's score alone cannot establish benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2026414 | Ventolin | Salbutamol | Aerosol inhaler, 100 mcg per dose CFC free | No |
| 2404516 | SalAir | Salbutamol | Aerosol inhaler, 100 mcg per dose CFC free | Yes |
| 257923 | Ventolin | Salbutamol | Infusion 1 mg per ml, 5 ml | Yes |
| 257915 | Ventolin | Salbutamol | Inj 500 mcg per ml, 1 ml | Yes |
| 2154315 | Asthalin | Salbutamol | Nebuliser soln, 1 mg per ml, 2.5 ml ampoule | Yes |
| 2710811 | UK Cipla | Salbutamol | Nebuliser soln, 1 mg per ml, 2.5 ml ampoule | Yes |
| 2154323 | Asthalin | Salbutamol | Nebuliser soln, 2 mg per ml, 2.5 ml ampoule | Yes |
| 2712199 | UK Cipla | Salbutamol | Nebuliser soln, 2 mg per ml, 2.5 ml ampoule | Yes |
| 2140543 | Ventolin | Salbutamol | Oral liq 400 mcg per ml | Yes |
| 2349337 | Duolin HFA | Salbutamol with ipratropium bromide | Aerosol inhaler, 100 mcg with ipratropium bromide… | Yes |
| 2039281 | Duolin | Salbutamol with ipratropium bromide | Nebuliser soln, 2.5 mg with ipratropium bromide 0… | Yes |
| 2729512 | Combiprasal | Salbutamol with ipratropium bromide | Nebuliser soln, 2.5 mg with ipratropium bromide 0… | Yes |
| 2719959 | Cipla | Salbutamol with ipratropium bromide | Nebuliser soln, 2.5 mg with ipratropium bromide 0… | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

Other forms include an infusion (1 mg per ml, 5 ml) and an injection (500 mcg per ml, 1 ml).

<!-- review:begin nz-lic-20261106-salbutamol-1 -->

> **Review correction (2026-11-06)**: Originally stated: "These are 5 of the 9 valid licences. Other forms include an infusion (1 mg per ml, 5 ml) and an injection (500 mcg per ml, 1 ml)." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 13 listings for this drug, 13 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-salbutamol-1 -->

## Safety Considerations

- **Drug Interactions:** 100 interactions are recorded. Moderate-level examples include epinephrine, clarithromycin, cisapride, and several antidiabetic drugs (acarbose, alogliptin, canagliflozin, dapagliflozin, chlorpropamide). Minor-level examples include corticosteroids such as hydrocortisone, budesonide, and dexamethasone.
- **Conditions requiring caution:** The data compiled for this report flag the following conditions as contraindication-related: hypertension, long QT syndrome, epilepsy, potassium deficiency, diabetes mellitus and diabetic ketoacidosis, metabolic acidosis, benign prostatic hyperplasia, bladder neck obstruction, angle-closure glaucoma and ocular hypertension, hyperthyroidism, myocardial ischaemia and coronary artery disease, sudden cardiac arrest, and heart conduction disease. These are not taken from the product label, so the package insert should be checked.

Package insert warnings and contraindications for the New Zealand products have not yet been reviewed.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is very high, but there are no trials or publications on salbutamol in papillary conjunctivitis, and the proposed mechanism is speculative for a condition often caused by lens wear or mechanical irritation. The evidence level is L5.

**To proceed, the following is needed:**
- Mechanism of action data and a clearer rationale for use in papillary conjunctivitis
- Preclinical or early human data in this condition, ideally with a topical ocular formulation
- Review of the Medsafe package insert for warnings and contraindications
- Assessment of ocular safety, given that angle-closure glaucoma and ocular hypertension appear among the flagged conditions

<!-- review:begin nz-lic-20261106-salbutamol-2 -->

> **Review correction (2026-11-06)**: Originally stated: "Preclinical or early human data in this condition, ideally with a topical ocular formulation (no ocular product is currently licensed in New Zealand)" Removed: it conflicted with the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 13 listings for this drug, 13 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-salbutamol-2 -->

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "These are 5 of the 9 valid licences" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "(no ocular product is currently licensed in New Zealand)" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


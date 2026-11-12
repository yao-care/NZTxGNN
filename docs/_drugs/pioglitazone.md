---
layout: default
title: Pioglitazone
parent: Model Prediction Only (L5)
nav_order: 368
evidence_level: L5
indication_count: 9
last_modified_at: 2026-11-12
monitor_inn: "PIOGLITAZONE"
predicted_indications: ["opsismodysplasia", "focal stiff limb syndrome", "classic stiff person syndrome", "thiamine-responsive dysfunction syndrome", "drug-induced localized lipodystrophy", "centrifugal lipodystrophy", "pressure-induced localized lipoatrophy", "idiopathic localized lipodystrophy", "pancreatic agenesis"]
description: "Pioglitazone: predicted new indication Opsismodysplasia (evidence level L5). Approved use: Not recorded in the local data"
---

# Pioglitazone
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

## Pioglitazone: Opsismodysplasia
{: .text-alpha }

## One-Sentence Summary

Pioglitazone is a PPAR-gamma agonist insulin sensitizer, originally used to treat type 2 diabetes mellitus.
The TxGNN model predicts it may be effective for **Opsismodysplasia**, a rare skeletal dysplasia,
but there are **0 clinical trials** and **0 publications** supporting this prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Opsismodysplasia |
| TxGNN Prediction Score | 0.9959 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (5 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 5 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, pioglitazone is a PPAR-gamma agonist (thiazolidinedione class). It improves insulin sensitivity, and its efficacy in type 2 diabetes is well established.

Opsismodysplasia is a rare skeletal dysplasia caused by mutations in INPPL1 (SHIP2), a gene that modulates PI3K/AKT signalling. Pioglitazone acts on insulin-related pathways, so a loose link through insulin/PI3K signalling is conceivable. However, no direct mechanistic or clinical evidence supports this connection. The high TxGNN score is a model prediction only.

Other top-ranked predictions are also unsupported by trials or publications. They include focal stiff limb syndrome, classic stiff person syndrome, thiamine-responsive dysfunction syndrome, and several localized lipodystrophies. The lipodystrophy predictions have a more plausible mechanism, since PPAR-gamma drives adipocyte differentiation, but there are still no data. Pancreatic agenesis is the only prediction with retrieved literature. That literature consists of general type 2 diabetes and thiazolidinedione reviews that do not address the condition, and the mechanism is unsuitable because pioglitazone needs residual insulin or beta-cell function to work.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for opsismodysplasia.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2479435 | Vexazone | Pioglitazone | Tab 15 mg | Yes |
| 2739917 | Rising | Pioglitazone | Tab 15 mg | Yes |
| 2479443 | Vexazone | Pioglitazone | Tab 30 mg | Yes |
| 2739925 | Rising | Pioglitazone | Tab 30 mg | Yes |
| 2479451 | Vexazone | Pioglitazone | Tab 45 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Contraindications (from knowledge graph, not label text)**: kidney disease, liver disease, gallbladder disease, congestive heart failure, adrenocortical insufficiency / Addison disease, thymus gland disease, infectious disease, pyoureter, and multiple pituitary hormone deficiency and hypogonadotropic hypogonadism-related conditions (including Kallmann syndrome and Sheehan syndrome).
- **Drug Interactions**: 100 interactions were identified. Those shown are mostly Moderate (some Minor). Main ones include:
  - Moderate: abiraterone, abametapir (topical), phenylephrine, pseudoephedrine, epinephrine, salbutamol, formoterol, acetazolamide, hydrocortisone, hydrochlorothiazide, alpelisib, ethanol, aloe vera leaf
  - Minor: nifedipine, ethinylestradiol, lipoic acid, topical corticosteroids (hydrocortisone, alclometasone, amcinonide), aminoglutethimide

Package insert warnings and contraindications were not available. Please refer to the package insert for the full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no supporting clinical trials or publications (evidence level L5), the mechanistic link to opsismodysplasia is speculative.

<!-- review:begin nz-lic-20261106-pioglitazone-2 -->

> **Review correction (2026-11-06)**: Originally stated: "The prediction has no supporting clinical trials or publications (evidence level L5), the mechanistic link to opsismodysplasia is speculative, and pioglitazone is marketed in New Zealand with 3 valid licences of 3." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 5 listings for this drug, 5 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-pioglitazone-2 -->

**To proceed, the following is needed:**
- Mechanistic evidence linking PPAR-gamma or insulin/PI3K signalling to INPPL1-related skeletal dysplasia (preclinical or in vitro studies)
- Detailed mechanism of action data
- Package insert warnings and contraindications for a formal safety screen
- Review of the knowledge-graph contraindications (heart failure, liver and kidney disease) against the likely paediatric target population

<!-- review:begin nz-lic-20261106-pioglitazone-1 -->

> **Review correction (2026-11-06)**: Originally stated: "Review of the knowledge-graph contraindications (heart failure, liver and kidney disease) against the likely paediatric target population / Clarification of the regulatory pathway given the cancelled New Zealand licences" Removed: it said the Schedule listings were cancelled, discontinued, or not current, which did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 5 listings for this drug, 5 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-pioglitazone-1 -->

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "Clarification of the regulatory pathway given the cancelled New Zealand licences" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "and pioglitazone is marketed in New Zealand with 3 valid licences of 3" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


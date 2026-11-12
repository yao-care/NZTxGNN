---
layout: default
title: Amantadine
parent: Model Prediction Only (L5)
nav_order: 30
evidence_level: L5
indication_count: 4
monitor_inn: "Amantadine"
predicted_indications: ["Rasmussen subacute encephalitis", "transaldolase deficiency", "myelitis", "PLA2G6-associated neurodegeneration"]
last_modified_at: 2026-11-12
description: "Amantadine: predicted new indication Rasmussen subacute encephalitis (evidence level L5). Approved use: Not recorded in the local data"
---

# Amantadine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

## Amantadine: NMDA receptor antagonist and dopaminergic modulator → Rasmussen subacute encephalitis
{: .text-alpha }

## One-Sentence Summary

Amantadine is a drug with NMDA receptor activity that has been used in movement disorders such as Parkinson disease.
The TxGNN model predicts it may be relevant for **Rasmussen subacute encephalitis**,
but **no clinical trials and no publications** currently support this specific prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Rasmussen subacute encephalitis |
| TxGNN Prediction Score | 0.9948 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. The pharmacology records show that amantadine acts on NMDA receptor subunits GluN2A, GluN2B, GluN2C and GluN2D (genes GRIN2A, GRIN2B, GRIN2C, GRIN2D).

Rasmussen encephalitis is an inflammatory brain disease in which glutamate-mediated excitotoxicity and neuroinflammation are thought to play a part. Blocking NMDA receptors could therefore plausibly be relevant. This link is a hypothesis based on general pharmacology. It has not been tested in any study supplied here, and it should be treated as unverified.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for Rasmussen subacute encephalitis.

<!-- review:begin nz-lic-20261106-amantadine-1 -->

> **Review correction (2026-11-06)**: Originally stated: "The local data lists two licences for Symmetrel (single-ingredient capsule, 100 mg). The licence records carry no indication text." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 2 listings for this drug, 2 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-amantadine-1 -->

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 204749 | Symmetrel | Amantadine hydrochloride | Cap 100 mg | Yes |
| 788589 | Symmetrel | Amantadine hydrochloride | Cap 100 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Drug Interactions**: The interaction search completed and returned four pharmacology records. They describe amantadine's binding to NMDA receptor subunits (GluN2A, GluN2B, GluN2C, GluN2D), not drug–drug interactions with specific medicines. Concurrent use with other agents acting on the central nervous system should be reviewed with the package insert.

Please refer to the package insert for further safety information, including warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is very high (0.9948), but no clinical trial or publication studies amantadine in Rasmussen encephalitis, so the evidence level is L5. The proposed NMDA-related mechanism is plausible but unverified.

**To proceed, the following is needed:**
- Targeted literature review of NMDA antagonism and glutamate excitotoxicity in Rasmussen encephalitis
- Preclinical or case-level evidence of amantadine in this disease
- Mechanism of action data for amantadine
- Package insert warnings and contraindications from Medsafe, to allow a safety assessment
- Confirmation of current licence status with the regulator

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "The local data lists two licences for Symmetrel (single-ingredient capsule, 100 mg). The licence records carry no indication text" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


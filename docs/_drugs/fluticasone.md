---
layout: default
title: Fluticasone
parent: Model Prediction Only (L5)
nav_order: 211
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-30
description: "Fluticasone: predicted new indication Migraine disorder (evidence level L5). Approved use: Not available"
monitor_inn: "FLUTICASONE"
predicted_indications: ["migraine disorder", "Prinzmetal angina", "migraine with brainstem aura"]
---

# Fluticasone
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

## Fluticasone: From Inhaled Respiratory Therapy to Migraine Disorder
{: .text-alpha }

## One-Sentence Summary

Fluticasone is an inhaled glucocorticoid, marketed in New Zealand as Flixotide aerosol inhalers and Accuhaler powder inhalers, and all 6 of its New Zealand licences are valid.
The TxGNN model predicts it may be effective for **migraine disorder**, but there are currently **0 clinical trials** and only **1 publication**, and that publication addresses adverse events rather than benefit.
This prediction rests on the model alone and is not yet backed by clinical evidence.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available (no approved indication text in the New Zealand licence records) |
| Predicted New Indication | Migraine disorder |
| TxGNN Prediction Score | 0.9920 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (6 of 6 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 6 (valid: 6) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this drug. Fluticasone is known to be a glucocorticoid with anti-inflammatory activity. Neurogenic inflammation and sensitization of the trigeminovascular system are thought to be involved in migraine, which gives a plausible but unverified link.

The TxGNN score is very high (0.9920), but it comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. The data compiled for this report contain no mechanistic, preclinical or clinical study supporting a benefit in migraine.

The only retrieved publication is a review of neuropsychiatric disturbances reported with intranasal corticosteroids. It deals with adverse events, not therapeutic benefit. It does not support efficacy in migraine and may point to a neurological safety concern.

Two other predictions were also generated, and neither has any clinical trial or literature support:
- **Prinzmetal angina** (score 0.9919): any link would be speculative, via anti-inflammatory effects on vascular tone.
- **Migraine with brainstem aura** (score 0.9909): a rare migraine subtype. The score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [18681087](https://pubmed.ncbi.nlm.nih.gov/18681087/) | 2008 | Review | Annals of Allergy, Asthma & Immunology | Review of WHO pharmacovigilance reports of neuropsychiatric disturbances during intranasal corticosteroid use. It concerns adverse events, not efficacy in migraine. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2004887 | Flixotide | Aerosol inhaler, 50 mcg per dose | Not stated (valid) |
| 472867 | Flixotide | Aerosol inhaler, 250 mcg per dose | Not stated (valid) |
| 402958 | Flixotide Accuhaler | Powder for inhalation, 50 mcg per dose | Not stated (valid) |
| 402974 | Flixotide Accuhaler | Powder for inhalation, 100 mcg per dose | Not stated (valid) |
| 402990 | Flixotide Accuhaler | Powder for inhalation, 250 mcg per dose | Not stated (valid) |

One further cancelled authorization (472859, Flixotide aerosol inhaler, 125 mcg per dose) is not shown. All 6 authorizations are single-ingredient products and are cancelled.

## Safety Considerations

- **Drug Interactions**: 100 interactions were identified. Those with a graded severity are:
  - **Clarithromycin**: Major
  - **Aprepitant**: Moderate
  - **Inhaled rapid-acting human insulin**: Moderate

  Other listed interacting drugs have no severity grade, including triamcinolone, prednisone, simvastatin, omeprazole, morphine and doxycycline.
- **Neuropsychiatric signal**: The only retrieved publication reports neuropsychiatric disturbances with intranasal corticosteroids. This is relevant when considering a neurological indication.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the model score (L5). There are no clinical trials, and the single publication concerns adverse events rather than benefit. All 6 New Zealand licences are valid, but package insert safety data and mechanism data are missing.

**To proceed, the following is needed:**
- Mechanism of action data, to assess whether a link to migraine pathophysiology is credible
- Package insert warnings and contraindications from Medsafe, which is a blocking gap for safety screening
- Preclinical or clinical evidence of benefit in migraine
- An assessment of the neuropsychiatric safety signal
- An assessment of route compatibility, since only inhaled forms were licensed
- A decision on whether any product would be re-registered, given the cancelled market status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


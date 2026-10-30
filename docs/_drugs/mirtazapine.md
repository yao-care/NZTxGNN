---
layout: default
title: Mirtazapine
parent: Model Prediction Only (L5)
nav_order: 304
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-30
monitor_inn: "MIRTAZAPINE"
predicted_indications: ["Ohdo syndrome and variants", "blepharophimosis - intellectual disability syndrome, Ohdo type", "benign paroxysmal torticollis of infancy"]
description: "Mirtazapine: predicted new indication Ohdo syndrome and variants (evidence level L5). Approved use: Not recorded in the local data"
---

# Mirtazapine
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

## Mirtazapine: Ohdo syndrome and variants
{: .text-alpha }

## One-Sentence Summary

Mirtazapine is an antidepressant, originally used for unipolar depression, major depressive disorder and anxiety disorder.
The TxGNN model predicts it may be effective for **Ohdo syndrome and variants**,
but **no clinical trials and no publications** currently support this direction, so it is a model prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Ohdo syndrome and variants |
| TxGNN Prediction Score | 0.9942 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on general knowledge, mirtazapine is an antidepressant whose efficacy in depression and anxiety is established. However, the data compiled for this report do not show a mechanistic link to Ohdo syndrome.

Ohdo syndrome is a rare genetic disorder (KAT6B-related) with neurodevelopmental features. No plausible pharmacological rationale linking mirtazapine to its underlying pathology is evident from the available data. The very high TxGNN score (0.9942) reflects a model prediction without clinical corroboration.

Two other predictions show the same pattern:
- **Blepharophimosis - intellectual disability syndrome, Ohdo type** (score 0.9911): likely driven by overlap with the Ohdo syndrome entry, so it is not an independent signal. Any symptomatic benefit (for example for sleep or mood) would be speculative and would not address the underlying disease.
- **Benign paroxysmal torticollis of infancy** (score 0.9911): a self-limiting episodic movement disorder of infancy. Mirtazapine is not established for paediatric use, which is a further concern.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2678802 | Noumed | Tab 30 mg | Not available (licence valid) |
| 2678810 | Noumed | Tab 45 mg | Not available (licence valid) |

## Safety Considerations

- **Contraindications / disease associations (knowledge graph, not label text)**: The knowledge graph links mirtazapine to the following conditions as contraindications:
  - Neurological and psychiatric: epilepsy, manic bipolar affective disorder, drug-induced akathisia, serotonin syndrome
  - Blood pressure and autonomic: orthostatic hypotension, hypotensive disorder, postural orthostatic tachycardia syndrome, multiple system atrophy
  - Cardiovascular and cerebrovascular: myocardial infarction, coronary thrombosis, stroke, cerebrovascular disorder, cerebral infarction, susceptibility to ischemic stroke
  - Haematological: neutropenia, agranulocytosis, hemoglobinopathy
  - Metabolic: hypertriglyceridemia, obesity disorder, monogenic obesity
  - Organ function: liver disease, gallbladder disease, impaired renal function disease
- **Drug Interactions**: The interaction query returned 100 records. Of the 20 listed, the **Major** interactions are:
  - Bupropion
  - Lorcaserin
  - Dolasetron
  - Palonosetron

  The remaining listed interactions are Moderate, including famotidine, loperamide, aprepitant, morphine, dexamethasone, cimetidine, clarithromycin and levofloxacin.

Package insert warnings were not available for this report. Please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
All three predicted indications rest on model scores alone (Evidence Level L5), with no clinical trials or literature.

**To proceed, the following is needed:**
- Mechanism of action data for mirtazapine, and a pharmacological rationale linking it to the predicted disease
- Package insert warnings and contraindications from Medsafe
- Preclinical or clinical evidence (trials or literature) for the predicted indication
- Paediatric safety assessment, particularly for the infant-onset condition
- Clarification of the regulatory pathway, since both New Zealand licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Metyrapone
parent: Model Prediction Only (L5)
nav_order: 298
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-30
description: "Metyrapone: predicted new indication Exercise-induced malignant hyperthermia (evidence level L5). Approved use: Not stated in the New Zealand license record (clinical use: diagnosti…"
monitor_inn: "METYRAPONE"
predicted_indications: ["exercise-induced malignant hyperthermia", "King-Denborough syndrome", "central core myopathy", "moderate multiminicore disease with hand involvement", "congenital multicore myopathy with external ophthalmoplegia", "myopathy, centronuclear", "familial periodic paralysis", "renal tubular acidosis", "malignant hyperthermia of anesthesia"]
---

# Metyrapone
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

## Metyrapone: From Cortisol Synthesis Inhibition to Exercise-Induced Malignant Hyperthermia
{: .text-alpha }

## One-Sentence Summary

Metyrapone is an 11-beta-hydroxylase inhibitor, used as a diagnostic tool for hypothalamic-pituitary function and occasionally to treat Cushing's syndrome.
The TxGNN model predicts it may be effective for **exercise-induced malignant hyperthermia**,
but there are currently **0 clinical trials** and **0 publications** supporting this prediction. It is a model-based prediction only (TxGNN, deep learning).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the New Zealand license record (clinical use: diagnostic testing of hypothalamic-pituitary function; occasional treatment of Cushing's syndrome) |
| Predicted New Indication | Exercise-induced malignant hyperthermia |
| TxGNN Prediction Score | 0.9995 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. From the pharmacology data, metyrapone targets CYP11B1 (11-beta-hydroxylase) and CYP11B2 (aldosterone synthase), reducing cortisol synthesis. This is the basis for its use in testing pituitary-adrenal function and in Cushing's syndrome.

The predicted indication has no established link to this mechanism. Malignant hyperthermia is driven mainly by dysregulated calcium release through the skeletal muscle ryanodine receptor (RYR1). The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

The other top predictions form a cluster of RYR1-related muscle disorders:
- Malignant hyperthermia susceptibility
- King-Denborough syndrome
- Central core myopathy
- Multiminicore disease
- Centronuclear myopathy
- Malignant hyperthermia of anaesthesia

None has any supporting trial or literature. Two other predictions are speculative: renal tubular acidosis (via accumulation of mineralocorticoid precursors) and familial periodic paralysis (via steroid influence on potassium handling).

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

For the top prediction, no related literature is available.

The only related publication across the top 10 predictions concerns a different predicted condition, familial periodic paralysis (rank 8):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [4322666](https://pubmed.ncbi.nlm.nih.gov/4322666/) | 1971 | Mechanistic/physiological study (type not confirmed) | The Journal of Clinical Investigation | In a patient with hyperkalaemic periodic paralysis, attacks were induced by potassium chloride, corticotropin-gel and glucocorticoids, but not by mineralocorticoids or by ACTH plus metyrapone. This is indirect evidence at best, not a therapeutic trial. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 388130 | Metopirone (valid) | Cap 250 mg | Not stated |

## Safety Considerations

- **Contraindications (from the knowledge graph, not label text)**: Metyrapone is linked to contraindications in adrenal insufficiency conditions, including Addison disease, chronic primary adrenal insufficiency, adrenal insufficiency, adrenocortical insufficiency, congenital adrenal hyperplasia, familial glucocorticoid deficiency, X-linked adrenal hypoplasia congenita, familial adrenal hypoplasia with absent pituitary luteinizing hormone, and inherited isolated adrenal insufficiency due to partial CYP11A1 deficiency.
- **Drug Interactions**: 10 moderate-level interactions were identified:
  - Estrogens: ethinylestradiol, estradiol (including topical), conjugated estrogens (including topical), mestranol, esterified estrogens, and estrone sulfate (including topical)
  - Cyproheptadine

  In addition, pharmacology data confirm that metyrapone targets CYP11B1 and CYP11B2.

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the TxGNN model score (L5). There are no clinical trials or publications for the top prediction, and no plausible mechanistic link between 11-beta-hydroxylase inhibition and RYR1-mediated calcium dysregulation. The drug is marketed in New Zealand (1 valid licence of 1), and the contraindications involve adrenal insufficiency.

**To proceed, the following is needed:**
- Mechanistic evidence connecting steroidogenesis inhibition to malignant hyperthermia pathophysiology, or preclinical studies in a relevant model
- Detailed mechanism of action data (MOA)
- The Medsafe package insert, to check warnings and contraindications
- Confirmation of the original approved indication and the regulatory status in New Zealand
- A review of whether any better-supported prediction should be prioritised over the RYR1-related cluster
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


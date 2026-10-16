---
layout: default
title: Adapalene
parent: Model Prediction Only (L5)
nav_order: 22
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Adapalene: predicted new indication Zinc, elevated plasma (evidence level L5). Approved use: Acne"
monitor_inn: "ADAPALENE"
predicted_indications: ["zinc, elevated plasma"]
---

# Adapalene
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

# Adapalene: From Acne to Elevated Plasma Zinc

## One-Sentence Summary

Adapalene is a topical retinoid originally used to treat acne.
The TxGNN model predicts it may be relevant to **elevated plasma zinc**, but **no clinical trials and no publications** currently support this direction. The prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Acne (the New Zealand licence record contains no approved indication text; acne comes from the knowledge graph indication data) |
| Predicted New Indication | Zinc, elevated plasma |
| TxGNN Prediction Score | 99.51% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the compiled data. Adapalene is generally described as a third-generation topical retinoid that selectively binds retinoic acid receptors (RAR-beta and RAR-gamma). It modulates keratinocyte differentiation and inflammation, which is consistent with its established use in acne.

The data compiled for this report show no link between retinoid receptor signalling and systemic zinc handling, such as zinc transporters, metallothionein, or albumin binding. The high TxGNN score reflects a pattern in the knowledge graph only and is not supported by any trial or publication. Elevated plasma zinc is also a laboratory finding rather than a well-defined disease. It is therefore a weak therapeutic target for a topical dermatological agent. Overall, the prediction is **not mechanistically supported** at this stage.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 456160 (cancelled) | Differin | Gel 0.1% (topical) | Not recorded |

## Safety Considerations

- **Disease-level contraindications (knowledge graph, not label text)**: The knowledge graph links adapalene to photoparoxysmal response, photosensitivity disease, exfoliative dermatitis and dermatitis as conditions where use is contraindicated or cautioned.
- **Drug Interactions**: The interaction screen returned 58 potential interactions. The severity level is unclassified for all of the ones shown. Examples include:
  - Tetracycline-class antibiotics: doxycycline, minocycline, tetracycline
  - Corticosteroids: prednisone, triamcinolone, budesonide
  - Acid suppressants: omeprazole, pantoprazole, rabeprazole, ranitidine
  - Antihistamines: cetirizine, loratadine, fexofenadine
  - Others: metronidazole, fluconazole, ibuprofen, clopidogrel, bupropion, loperamide, scopolamine

Please refer to the package insert for warnings and label contraindications, as these were not available in the compiled data.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no supporting trials or literature (L5). No mechanistic link to zinc homeostasis is apparent, and the indication is a laboratory finding rather than a disease. The only New Zealand licence for adapalene is cancelled, and it is a topical product that is unlikely to affect systemic zinc.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications, which are a blocking gap for safety screening
- Mechanism of action data for adapalene (for example, from DrugBank)
- Any biological evidence that retinoid signalling affects plasma zinc
- Clarification of whether "elevated plasma zinc" is a clinically meaningful treatment target
- An assessment of route compatibility, since adapalene is available only as a topical gel and the predicted indication is systemic
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


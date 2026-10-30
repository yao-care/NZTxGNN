---
layout: default
title: Fexofenadine
parent: Model Prediction Only (L5)
nav_order: 199
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-30
description: "Fexofenadine: predicted new indication Rosacea conjunctivitis (evidence level L5). Approved use: Allergic rhinitis (also vasomotor rhinitis, atopic…"
monitor_inn: "Fexofenadine"
predicted_indications: ["rosacea conjunctivitis"]
---

# Fexofenadine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Fexofenadine: From Allergic Rhinitis to Rosacea Conjunctivitis
{: .text-alpha }

## One-Sentence Summary

Fexofenadine is an antihistamine originally used for allergic rhinitis, allergic and other forms of conjunctivitis, and idiopathic urticaria.
The TxGNN model predicts it may be effective for **rosacea conjunctivitis**,
but **no clinical trials and no publications** currently support this direction, so it rests on model prediction alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Allergic rhinitis (also vasomotor rhinitis, atopic, papillary and vernal conjunctivitis, idiopathic urticaria) |
| Predicted New Indication | Rosacea conjunctivitis |
| TxGNN Prediction Score | 0.9985 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 3 (valid: 3) |
| Recommended Decision | Hold |

*Note: The approved indication text was not available in the licence records. The original indications above come from the TxGNN model (deep learning).*

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Pharmacology data show that fexofenadine targets the histamine H1 receptor (HRH1). It is a selective peripheral H1 antagonist, which is used for seasonal allergies and other histamine-dependent conditions.

Fexofenadine is already used for several types of conjunctivitis (allergic, atopic, papillary, vernal). By blocking histamine, it may reduce itching, redness and vasodilation of the ocular surface. This closeness to known conjunctivitis indications is the plausible link to rosacea conjunctivitis.

The link is weak, however. Ocular rosacea is driven mainly by meibomian gland dysfunction, microbial or Demodex involvement, and neurovascular and innate-immune dysregulation. Histamine plays a limited role. Any benefit would probably be symptomatic (itching, redness) rather than disease-modifying. The high prediction score (0.9985) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, such as related conjunctivitis indications, not biological evidence, and should not be read as independent evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2701405 | Telfast | Tab 60 mg | Not stated in the available records (valid) |
| 2697092 | Fexaclear | Tab 180 mg | Not stated in the available records (valid) |
| 2711222 | Fexaclear | Tab 120 mg | Not stated in the available records (valid) |

Only the Telfast 60 mg licence is currently valid. All three products are oral tablets, and no topical or ophthalmic formulation is listed.

## Safety Considerations

- **Conditions flagged in the knowledge graph (not label text)**: The knowledge graph links fexofenadine with caution in these conditions. These links should be checked against the official package insert before any use:
  - Cardiovascular: hypertension and hypertensive disorder, coronary artery disease, coronary atherosclerosis, arteriosclerosis, atherosclerosis
  - Metabolic and endocrine: diabetes mellitus, diabetic ketoacidosis, hyperthyroidism, Graves disease, thyrotoxicosis
  - Renal and urinary: kidney disease, pyoureter, benign prostatic hyperplasia
  - Ocular: angle-closure (closed-angle) glaucoma
- **Drug Interactions**: The interaction query completed with no drug-drug interactions listed. The only entry is the pharmacological target, the H1 receptor (HRH1).

Official warnings and contraindications from the package insert were not available. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported by model score alone (L5), with no clinical trials or literature. The histamine mechanism has limited relevance to the main drivers of ocular rosacea, so any benefit would likely be symptomatic at most. The available product is an oral tablet, and route compatibility for an ocular indication has not been assessed.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- A literature and trial search for fexofenadine or antihistamines in ocular rosacea
- Assessment of route and formulation suitability for ocular use
- Review of the knowledge-graph contraindications against the official label
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Empagliflozin
parent: Model Prediction Only (L5)
nav_order: 175
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-15
description: "Empagliflozin: predicted new indication Classic stiff person syndrome (evidence level L5). Approved use: Type 2 diabetes mellitus"
monitor_inn: "EMPAGLIFLOZIN"
predicted_indications: ["classic stiff person syndrome", "focal stiff limb syndrome", "opsismodysplasia"]
---

# Empagliflozin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

# Empagliflozin: From Type 2 Diabetes Mellitus to Classic Stiff Person Syndrome

## One-Sentence Summary

Empagliflozin is an SGLT2 inhibitor originally used to treat type 2 diabetes mellitus.
The TxGNN model predicts it may be effective for **classic stiff person syndrome**, but this is a model-only prediction, with **0 clinical trials** and **0 publications** currently supporting it.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Type 2 diabetes mellitus (from the knowledge graph; no approved indication text available in the New Zealand licence records) |
| Predicted New Indication | Classic stiff person syndrome |
| TxGNN Prediction Score | 99.06% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Empagliflozin is known as an SGLT2 inhibitor that lowers renal glucose reabsorption, and its efficacy in type 2 diabetes is established. However, this general pharmacology has not been linked to the predicted indication by any supplied data.

Stiff person syndrome is an autoimmune neurological disorder, typically associated with anti-GAD65 antibodies and impaired GABAergic signalling. A loose connection may exist through the overlap between GAD65 autoimmunity and autoimmune diabetes, but this is speculative and does not support a therapeutic effect of empagliflozin.

The high score (0.991) is a graph-based prediction only. Notably, focal stiff limb syndrome (rank 2) received an identical score, suggesting the model treats these closely related conditions as near-duplicates rather than providing independent evidence. The third prediction, opsismodysplasia (score 0.990), also has no supporting mechanistic link: it is a rare skeletal dysplasia caused by INPPL1 (SHIP2) mutations, and SGLT2 inhibition does not act on that pathway. Bone-safety concerns reported for SGLT2 inhibitors would also need careful evaluation in a paediatric skeletal disorder.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2599708 | Jardiance | Tab 25 mg | Not available (licence cancelled) |
| 2599694 | Jardiance | Tab 10 mg | Not available (licence cancelled) |

## Safety Considerations

- **Drug Interactions**: 100 potential interactions were identified in the interaction screening. Most of those listed are Moderate, with a few Minor. Examples include:
  - Moderate: phenylephrine, acebutolol, acetazolamide, formoterol, pseudoephedrine, nifedipine, epinephrine, ethinylestradiol, hydrocortisone, salbutamol, ethanol, fosinopril, fosphenytoin, furosemide, gemifloxacin
  - Minor: topical hydrocortisone, topical alclometasone

Please refer to the package insert for warnings and contraindications, as this information was not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests solely on a computational score, with no clinical trials, no literature, and no supported mechanistic link to stiff person syndrome. The drug also has no currently valid New Zealand licence.

**To proceed, the following is needed:**
- Mechanism of action data, and a plausible biological rationale linking SGLT2 inhibition to GABAergic or autoimmune pathways in stiff person syndrome
- Preclinical or observational evidence supporting the prediction
- Package insert warnings and contraindications from Medsafe
- Clarification of the regulatory status of empagliflozin in New Zealand, given that both Jardiance licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


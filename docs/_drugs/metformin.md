---
layout: default
title: Metformin
parent: Model Prediction Only (L5)
nav_order: 288
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-28
monitor_inn: "Metformin"
predicted_indications: ["focal stiff limb syndrome", "classic stiff person syndrome", "opsismodysplasia", "thiamine-responsive dysfunction syndrome", "drug-induced localized lipodystrophy"]
description: "Metformin: predicted new indication Focal stiff limb syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Metformin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

## Metformin: Focal stiff limb syndrome
{: .text-alpha }

## One-Sentence Summary

Metformin was originally used to treat type 2 diabetes mellitus.
The TxGNN model predicts it may be effective for **focal stiff limb syndrome**,
but **no clinical trials and no publications** currently support this prediction, so it is a model-based prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Focal stiff limb syndrome |
| TxGNN Prediction Score | 0.9945 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, metformin is a biguanide whose efficacy in type 2 diabetes is well established. Its commonly described actions include AMPK activation, reduced hepatic gluconeogenesis and possible anti-inflammatory effects.

Focal stiff limb syndrome belongs to the stiff person spectrum and is characterised by impaired GABAergic inhibition, often with an autoimmune component. No established link connects metformin's known actions to this pathophysiology. The high score (0.994) is a TxGNN model (deep learning) prediction only. The closely related prediction, classic stiff person syndrome, has an identical score. This suggests the model is propagating from a shared disease-ontology node rather than from independent evidence.

Stiff person syndrome is also associated with autoimmune diabetes. This co-occurrence may confound the TxGNN model (deep learning) score, which may reflect similarities the model learned between related diseases, not biological evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## Other Predicted Indications (for Reference)

All four of the following predictions also have no trials or literature (Evidence Level L5, Hold).

| Rank | Predicted Indication | TxGNN Score | Comment |
|------|------|------|------|
| 2 | Classic stiff person syndrome | 0.9945 | Same score as rank 1, likely driven by a shared ontology node. Anti-GAD65 autoimmunity is the typical driver. Any immunomodulatory role for metformin is speculative. |
| 3 | Opsismodysplasia | 0.9940 | A rare skeletal dysplasia caused by INPPL1 mutations. A theoretical link exists through insulin/PI3K-AKT signalling, but there is no clinical support and the paediatric population raises feasibility and safety concerns. |
| 4 | Thiamine-responsive dysfunction syndrome | 0.9940 | Treated with thiamine supplementation. The score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence, possibly driven by the diabetes phenotype. |
| 5 | Drug-induced localized lipodystrophy | 0.9906 | Metformin improves insulin sensitivity in other lipodystrophy settings, but this is indirect and does not address local tissue reactions. |

## New Zealand Market Information

Both authorisations are cancelled. No approved indication text is recorded for either.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2628031 | Metformin Viatris | Tablet, immediate-release 500 mg | Valid |
| 2628058 | Metformin Viatris | Tablet, immediate-release 850 mg | Valid |

## Safety Considerations

- **Contraindication-related conditions (knowledge graph, not label text)**: The knowledge graph links metformin to contraindications in several disease groups:
  - Liver failure and liver disease
  - Kidney disease
  - Pancreatitis
  - Gallbladder disease
  - Congestive heart failure
  - Pyoureter
  - Adrenocortical insufficiency
  - Thymus gland disease
  - A large group of pituitary and hypothalamic-pituitary disorders (for example hypopituitarism, growth hormone deficiency, Sheehan syndrome, Kallmann syndrome and hypogonadotropic hypogonadism)
- **Drug Interactions**: The data compiled for this report list 100 interactions. The main ones among those reviewed:
  - **Major**: acetazolamide, ethanol
  - **Moderate**: dolutegravir, abemaciclib, phenylephrine, ranitidine, formoterol, pseudoephedrine, ketorolac, nifedipine, epinephrine, ibuprofen, ethinylestradiol, hydrocortisone, salbutamol, hydrochlorothiazide, acetohexamide
  - **Minor**: acarbose, topical hydrocortisone, topical alclometasone
- **Additional consideration**: Metformin is associated with altered vitamin B12 absorption. Any effect on thiamine transport is a safety consideration if it were used in thiamine-related disorders.

Package insert warnings and contraindications were not available. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a TxGNN model (deep learning) score alone. The identical score for classic stiff person syndrome suggests a model artefact.

**To proceed, the following is needed:**
- Mechanism of action data for metformin, to allow a proper mechanistic analysis
- Package insert warnings and contraindications from Medsafe, as safety screening cannot proceed without them
- Literature and trial searches (including case reports and preclinical studies) for metformin in stiff person spectrum disorders
- Clarification of the cancelled regulatory status, and of whether other metformin products are marketed in New Zealand
- Review of the other predicted indications, as none currently has supporting evidence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


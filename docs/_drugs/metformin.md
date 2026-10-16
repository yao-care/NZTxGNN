---
layout: default
title: Metformin
parent: Model Prediction Only (L5)
nav_order: 288
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Metformin: predicted new indication Focal stiff limb syndrome (evidence level L5). Approved use: Type 2 diabetes mellitus"
monitor_inn: "Metformin"
predicted_indications: ["focal stiff limb syndrome", "classic stiff person syndrome", "opsismodysplasia", "thiamine-responsive dysfunction syndrome", "drug-induced localized lipodystrophy"]
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

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Metformin: From Type 2 Diabetes Mellitus to Focal Stiff Limb Syndrome

## One-Sentence Summary

Metformin was originally used to treat type 2 diabetes mellitus.
The TxGNN model predicts it may be effective for **focal stiff limb syndrome**,
but **no clinical trials and no publications** currently support this prediction, so it is a model-based prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Type 2 diabetes mellitus (from the knowledge graph; the New Zealand licence records contain no indication text) |
| Predicted New Indication | Focal stiff limb syndrome |
| TxGNN Prediction Score | 99.45% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorisation) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, metformin is a biguanide whose efficacy in type 2 diabetes is well established. Its commonly described actions include AMPK activation, reduced hepatic gluconeogenesis and possible anti-inflammatory effects.

Focal stiff limb syndrome belongs to the stiff person spectrum and is characterised by impaired GABAergic inhibition, often with an autoimmune component. No established link connects metformin's known actions to this pathophysiology. The high score (0.994) is a knowledge-graph prediction only. The closely related prediction, classic stiff person syndrome, has an identical score. This suggests the model is propagating from a shared disease-ontology node rather than from independent evidence.

Stiff person syndrome is also associated with autoimmune diabetes. This co-occurrence may confound the graph-based association.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## Other Predicted Indications (for Reference)

All four of the following predictions also have no trials or literature (Evidence Level L5, Hold).

| Rank | Predicted Indication | TxGNN Score | Comment |
|------|------|------|------|
| 2 | Classic stiff person syndrome | 99.45% | Same score as rank 1, likely driven by a shared ontology node. Anti-GAD65 autoimmunity is the typical driver. Any immunomodulatory role for metformin is speculative. |
| 3 | Opsismodysplasia | 99.40% | A rare skeletal dysplasia caused by INPPL1 mutations. A theoretical link exists through insulin/PI3K-AKT signalling, but there is no clinical support and the paediatric population raises feasibility and safety concerns. |
| 4 | Thiamine-responsive dysfunction syndrome | 99.40% | Treated with thiamine supplementation. The link is likely a graph artefact driven by the diabetes phenotype. |
| 5 | Drug-induced localized lipodystrophy | 99.06% | Metformin improves insulin sensitivity in other lipodystrophy settings, but this is indirect and does not address local tissue reactions. |

## New Zealand Market Information

Both authorisations are cancelled. No approved indication text is recorded for either.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2628031 | Metformin Viatris | Tablet, immediate-release 500 mg | Cancelled |
| 2628058 | Metformin Viatris | Tablet, immediate-release 850 mg | Cancelled |

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
The prediction rests on a knowledge-graph score alone. There are no clinical trials or publications, no supported mechanistic link to focal stiff limb syndrome, and no currently valid New Zealand authorisation. The identical score for classic stiff person syndrome suggests a model artefact.

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


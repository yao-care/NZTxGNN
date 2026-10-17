---
layout: default
title: Risdiplam
parent: Model Prediction Only (L5)
nav_order: 406
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Risdiplam: predicted new indication Acne (evidence level L5). Approved use: Not stated in the New Zealand licence data (the drug's known use is s…"
monitor_inn: "RISDIPLAM"
predicted_indications: ["acne (disease)"]
---

# Risdiplam
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

# Risdiplam: From Spinal Muscular Atrophy to Acne

## One-Sentence Summary

Risdiplam is an oral SMN2 splicing modifier, a drug class used for spinal muscular atrophy (SMA).
The TxGNN model predicts it may be effective for **acne**, but **no clinical trials and no publications** currently support this direction.
The prediction rests only on a knowledge-graph score.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the New Zealand licence data (the drug's known use is spinal muscular atrophy) |
| Predicted New Indication | Acne |
| TxGNN Prediction Score | 99.45% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on general knowledge, risdiplam is an SMN2 splicing modifier developed for spinal muscular atrophy, where it increases production of functional SMN protein.

No documented biological link connects SMN2 splicing modulation to the processes that drive acne: sebum production, follicular hyperkeratinization, *C. acnes* colonization and inflammation. The high TxGNN score may reflect knowledge-graph topology artifacts, such as shared neighbours through off-target splicing or retinoid-related pathways, rather than a true pharmacological relationship.

The prediction should therefore be treated as a hypothesis only. Its mechanistic plausibility is currently low and unproven.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2649403 | Evrysdi (cancelled) | Powder for oral solution, 750 mcg per mL, 60 mg per bottle | Not recorded |

The only New Zealand authorization has been cancelled, so no valid product is currently marketed. The available formulation is oral only.

## Safety Considerations

- **Drug Interactions**: Two moderate-level interactions were identified:
  - Metformin
  - Methylnaltrexone
- **Other Safety Concerns**: Preclinical retinal toxicity and effects on epithelial tissue have been reported for risdiplam. These would need careful weighing against a non-life-threatening condition like acne.

Please refer to the package insert for warnings and contraindications, as this information is not available in the data compiled for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only support for this prediction is a high TxGNN score, with no clinical trials, no literature and no plausible mechanism (Evidence Level L5). The drug's New Zealand licence is cancelled, and its preclinical retinal and epithelial toxicity signals are difficult to justify for acne.

**To proceed, the following is needed:**
- Mechanism of action data and a plausible biological link between SMN2 splicing modulation and acne pathogenesis
- Package insert warnings and contraindications (for example from the Medsafe website)
- Preclinical or early clinical evidence of benefit in acne
- A benefit-risk assessment addressing retinal and epithelial toxicity
- Clarification of the regulatory status of the cancelled licence and of route compatibility (oral versus the topical or systemic routes typically used for acne)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


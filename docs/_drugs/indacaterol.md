---
layout: default
title: Indacaterol
parent: Model Prediction Only (L5)
nav_order: 234
evidence_level: L5
indication_count: 10
---

# Indacaterol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Indacaterol: From Obstructive Airway Disease (Asthma/COPD) to Nephrogenic Syndrome of Inappropriate Antidiuresis

## One-Sentence Summary

Indacaterol is an ultra-long-acting inhaled beta2-agonist bronchodilator, used for obstructive airway disease.
The TxGNN model predicts it may be effective for **nephrogenic syndrome of inappropriate antidiuresis (NSIAD)**,
but **no clinical trials and no publications** currently support this prediction, so it rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Asthma and COPD (bronchodilator use, based on the literature; the approved indication text is not stated in the local licence records) |
| Predicted New Indication | Nephrogenic syndrome of inappropriate antidiuresis |
| TxGNN Prediction Score | 99.54% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for indacaterol is not available in the data compiled for this report. Based on its known drug class, indacaterol is a beta2-adrenergic agonist. It raises cAMP in airway smooth muscle, which produces sustained bronchodilation.

NSIAD is caused by gain-of-function variants in the vasopressin V2 receptor (AVPR2), a Gs/cAMP-coupled receptor. Beta2 agonism also raises cAMP, which may explain why the graph model links the two. However, raising cAMP does not correct the underlying receptor defect and could plausibly be unhelpful.

The mechanistic link is therefore weak and speculative. No supporting trials or literature were found, so this remains a computational prediction only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication | Status |
|---------|------|------|-----------|------|
| 2465930 | Onbrez Breezhaler | Powder for inhalation 300 mcg | Not specified | Cancelled |
| 2465922 | Onbrez Breezhaler | Powder for inhalation 150 mcg | Not specified | Cancelled |

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: hypertension and hypertensive disorder, long QT syndrome (including the familial form), sudden cardiac arrest, heart conduction disease, coronary artery disease, epilepsy, hyperglycemia, diabetes mellitus, diabetic ketoacidosis, potassium deficiency disease, thyrotoxicosis, hyperthyroidism, and Graves disease.
- **Drug Interactions** (100 interactions recorded; main ones shown):
  - *Moderate*:
    - Antidiabetic agents: acarbose, alogliptin, saxagliptin, metformin, pioglitazone, canagliflozin, dapagliflozin, chlorpropamide, albiglutide
    - Epinephrine
    - Isometheptene
    - Famotidine
    - Clarithromycin
  - *Minor*:
    - Corticosteroids: hydrocortisone, dexamethasone, triamcinolone, beclomethasone, betamethasone, budesonide
    - Aprepitant

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for the full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The NSIAD prediction has a high model score but no trials, no literature, and a mechanistic link that is weak and possibly counterproductive. In addition, the product is no longer marketed in New Zealand (both licences cancelled).

**To proceed, the following is needed:**
- Detailed mechanism-of-action data and a mechanistic assessment of beta2 agonism in AVPR2 gain-of-function disease
- Preclinical or case-level evidence of any benefit in NSIAD
- Package insert warnings and contraindications, and verification of current availability and formulation in New Zealand
- A cardiovascular and metabolic safety review, given the knowledge-graph contraindication signals

**Note on other predictions:** The best-supported prediction for indacaterol is **bronchial disease** (rank 7), with 24 registered trials and 20 publications, including Phase 3 evidence (Evidence Level L1, Proceed with Guardrails). This is essentially the drug's established respiratory use rather than a new repurposing. Most of the Phase 3 evidence is for the indacaterol/glycopyrronium/mometasone combination rather than indacaterol alone.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


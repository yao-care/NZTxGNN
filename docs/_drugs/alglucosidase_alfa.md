---
layout: default
title: Alglucosidase Alfa
parent: Model Prediction Only (L5)
nav_order: 27
evidence_level: L5
indication_count: 10
---

# Alglucosidase Alfa
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

# Alglucosidase Alfa: From Glycogen Storage Disease to Adult Polyglucosan Body Disease

## One-Sentence Summary

Alglucosidase alfa is a recombinant human enzyme originally used to treat glycogen storage disease (Pompe disease, GSD II).
The TxGNN model predicts it may be effective for **adult polyglucosan body disease**, but there are currently **0 clinical trials** and **0 publications** supporting this direction.
The prediction rests on model output alone, and the biological rationale is weak.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Glycogen storage disease (Pompe disease). The New Zealand licence record contains no approved indication text. |
| Predicted New Indication | Adult polyglucosan body disease |
| TxGNN Prediction Score | 99.47% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the data compiled for this report. Based on known information, alglucosidase alfa is a recombinant human acid alpha-glucosidase (GAA). It breaks down glycogen inside lysosomes and is an established enzyme replacement therapy for Pompe disease.

Adult polyglucosan body disease is caused by a deficiency of the glycogen branching enzyme (GBE1). This produces poorly branched, insoluble polyglucosan that accumulates mainly in the cytosol of neurons and other cells. The shared theme of abnormal glycogen metabolism likely explains the high TxGNN score.

The mechanistic link is nonetheless weak and speculative:
- Lysosomal GAA does not correct the GBE1 defect.
- The enzyme does not readily cross the blood-brain barrier.
- It does not readily reach polyglucosan stored in the cytosol.

The other top-ranked predictions follow the same pattern:
- **Glycogen branching enzyme deficiency (fatal perinatal and congenital neuromuscular forms)**: these are also GBE1 disorders, and the prediction likely reflects proximity to the original indication in the knowledge graph. GAA cannot replace branching enzyme activity.
- **Eyelid and ocular conditions** (congenital entropion, congenital ectropion, epiblepharon, congenital Horner syndrome, ptosis-related syndromes, and others): no plausible mechanism links them to enzyme replacement for glycogen storage, so these predictions are probably knowledge-graph artifacts.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2500701 | Myozyme (status: cancelled) | Inj 50 mg vial | Not recorded |

---

## Safety Considerations

- **Drug Interactions**: No interaction records were found for this drug.

Please refer to the package insert for other safety information, including warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the model score, with no clinical trials or literature. The mechanistic link is weak because lysosomal GAA does not address the GBE1 defect or reach cytosolic polyglucosan. The only New Zealand licence has been cancelled, so there is no current local supply pathway.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, as safety screening cannot proceed without them
- Detailed mechanism-of-action data from DrugBank
- Preclinical evidence on whether GAA can reach and degrade polyglucosan in relevant cells or models
- A review of current supply and regulatory status in New Zealand, since the only licence is cancelled
- Evaluation of whether a clinical question exists for adult polyglucosan body disease, since the other nine predictions lack credible biological support
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


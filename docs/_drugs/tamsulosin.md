---
layout: default
title: Tamsulosin
parent: Model Prediction Only (L5)
nav_order: 438
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Tamsulosin: predicted new indication Ambras type hypertrichosis universalis congenita (evidence level L5). Approved use: Benign prostatic hyperplasia"
---

# Tamsulosin
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

# Tamsulosin: From Benign Prostatic Hyperplasia to Ambras Type Hypertrichosis Universalis Congenita

## One-Sentence Summary

Tamsulosin is an alpha-1 adrenergic receptor antagonist, originally used for symptomatic relief of benign prostatic hyperplasia (BPH).
The TxGNN model predicts it may be effective for **Ambras type hypertrichosis universalis congenita**, but there are **0 clinical trials** and **0 publications** supporting this direction. The prediction rests on the model alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Benign prostatic hyperplasia |
| Predicted New Indication | Ambras type hypertrichosis universalis congenita |
| TxGNN Prediction Score | 99.996% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorization) |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, tamsulosin is an antagonist of the alpha-1A, alpha-1B and alpha-1D adrenoceptors. Its efficacy in BPH is established, where it relaxes smooth muscle in the prostate and bladder neck to relieve urinary symptoms.

For the predicted indication, the mechanistic link is weak. Ambras type hypertrichosis universalis congenita is a rare congenital condition caused by genomic rearrangements affecting regulation of the *TRPS1* gene. No known mechanism connects alpha-1 adrenergic blockade to this pathway. The very high score (0.99996) most likely reflects graph-neighbourhood similarity among hair-related disease nodes in the knowledge graph rather than biological rationale.

Other top-ranked predictions show the same pattern. They are mostly rare hair disorders (hypertrichosis, hypotrichosis, hair shaft abnormalities, alopecia areata) and developmental malformation syndromes, and none has supporting trials or literature. The rank 1 prediction is therefore best viewed as a hypothesis-generating output, not as evidence of a therapeutic opportunity.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available for the predicted indication.

For context, two papers were retrieved for the related prediction "alopecia" (rank 9). Neither shows a benefit of tamsulosin:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [33175100](https://pubmed.ncbi.nlm.nih.gov/33175100/) | 2021 | Cohort/Pharmacovigilance | JAMA Dermatology | Investigates suicidality and psychological adverse events with finasteride, a drug used for alopecia and BPH. It does not concern tamsulosin. |
| [32826647](https://pubmed.ncbi.nlm.nih.gov/32826647/) | 2021 | Cohort | Cornea | Examines meibomian gland changes in men with BPH. It is an indirect association with no evidence of efficacy. |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2438526 | Tamsulosin-Rex (status: cancelled) | Cap 400 mcg | Not recorded |

---

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: multiple system atrophy, postural orthostatic tachycardia syndrome, orthostatic hypotension, liver disease, gallbladder disease, priapism, and intraoperative floppy iris syndrome.
- **Drug Interactions**: The interaction query returned only pharmacological target data, not drug-drug interactions. Tamsulosin binds the alpha-1A, alpha-1B and alpha-1D adrenoceptors. No specific interacting drugs were identified.

Please refer to the package insert for full warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the TxGNN score. There are no clinical trials or publications, and no plausible mechanistic link exists between alpha-1 blockade and a congenital hypertrichosis driven by *TRPS1* regulatory changes. In addition, the only New Zealand authorization has been cancelled, and the package insert safety data have not been retrieved.

**To proceed, the following is needed:**
- Mechanism of action data, to allow a proper mechanistic-link assessment
- Package insert warnings and contraindications from Medsafe, to enable safety screening
- Any preclinical or biological evidence connecting alpha-1 adrenergic signalling to hair follicle regulation or *TRPS1* pathways
- Review of the other top-ranked predictions, none of which currently has supporting evidence either
- Assessment of route compatibility and similarity to the original indication, both still pending
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


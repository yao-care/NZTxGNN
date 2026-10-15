---
layout: default
title: Vancomycin
parent: Model Prediction Only (L5)
nav_order: 476
evidence_level: L5
indication_count: 10
---

# Vancomycin
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

# Vancomycin: From Clostridium difficile Colitis to Diffuse Scleroderma

## One-Sentence Summary

Vancomycin is a glycopeptide antibiotic, originally used for Gram-positive bacterial infections such as *Clostridium difficile* colitis, staphylococcal infections and endocarditis.
The TxGNN model predicts it may be effective for **diffuse scleroderma**, but there are **0 clinical trials** and only **1 unrelated case report** behind this prediction, so it is most likely a graph artifact.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Clostridium difficile colitis, staphylococcal infections, endocarditis, bacterial arthritis, staphylococcal pneumonia (the regulatory record contains no approved indication text) |
| Predicted New Indication | Diffuse scleroderma |
| TxGNN Prediction Score | 99.92% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only |
| Number of Authorizations | 1 (0 currently valid) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Vancomycin is known to be a glycopeptide antibiotic that inhibits bacterial cell wall synthesis by binding D-Ala-D-Ala. Its efficacy in Gram-positive infections is well established.

Diffuse scleroderma is an autoimmune, fibrotic disease, not a bacterial infection. No plausible mechanistic link between vancomycin and this condition could be identified. The only retrieved paper is a case report of erythroderma with sepsis and eosinophilia, which is not about scleroderma treatment. The high TxGNN score is likely a knowledge-graph artifact rather than a true therapeutic signal.

For context, among the other top-ranked predictions only **streptococcal pneumonia** (rank 9) is biologically plausible, because the pathogen is Gram-positive. Vancomycin is already used for penicillin-resistant pneumococcal infection, so this is closer to an existing use than to true repurposing. The Gram-negative targets (paratyphoid fever, typhoid fever, salmonellosis) are implausible because vancomycin cannot penetrate the Gram-negative outer membrane. The remaining predictions (congenital analbuminemia, polyclonal hyperviscosity syndrome, hyperamylasemia, blood group incompatibility, premalignant hematological system disease) have no mechanistic rationale.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31541072](https://pubmed.ncbi.nlm.nih.gov/31541072/) | 2019 | Case report | The American Journal of Case Reports | A 56-year-old man with diffuse exfoliative rash, sepsis and eosinophilia (erythroderma). It does not address scleroderma treatment. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2696282 | Vancomycin Viatris (cancelled) | Inj 500 mg vial | Not recorded |

## Safety Considerations

- **Contraindication-related diseases (from the knowledge graph, not package insert text)**: kidney disease, pyoureter, neutropenia, agranulocytosis, hearing disorder, auditory system disease.
- **Drug Interactions**: 100 interactions were identified, and 20 are shown in the compiled data.
  - **Major**: human immunoglobulin G (intravenous), human botulinum neurotoxin A/B immune globulin, and Vibrio cholerae CVD 103-HgR live vaccine.
  - **Moderate (examples)**: ketorolac, ibuprofen, diclofenac, celecoxib, amphotericin B, amikacin, capreomycin, carboplatin, bacitracin, metformin, estradiol, ethinylestradiol, mesalazine, balsalazide, ibandronate and cholestyramine. Many of these combinations involve additive nephrotoxicity or ototoxicity, which is consistent with the kidney and hearing concerns above.

Package insert warnings and contraindications were not available. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for diffuse scleroderma rests on a model score alone. There are no clinical trials, the single paper is an unrelated case report, and there is no plausible mechanism. The only authorization in New Zealand has been cancelled.

**To proceed, the following is needed:**
- Mechanism of action data and a credible rationale linking vancomycin to fibrotic or autoimmune pathways (none is currently apparent)
- Package insert warnings and contraindications from Medsafe
- Any preclinical or clinical evidence in scleroderma
- If an infection-related use is of interest, evaluate **streptococcal pneumonia** as a separate, more plausible research question. It is closer to an existing use, and the current market status would need to be addressed.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


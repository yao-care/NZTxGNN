---
layout: default
title: Fosfomycin
parent: Model Prediction Only (L5)
nav_order: 214
evidence_level: L5
indication_count: 8
monitor_inn: "FOSFOMYCIN"
predicted_indications: ["uterine inflammatory disease", "xanthogranulomatous pyelonephritis", "epiglottitis", "urogenital tuberculosis", "laryngitis", "hyperamylasemia", "polyclonal hyperviscosity syndrome", "pyelitis"]
last_modified_at: 2026-10-28
description: "Fosfomycin: predicted new indication Uterine inflammatory disease (evidence level L5). Approved use: Urinary tract infection"
---

# Fosfomycin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
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

## Fosfomycin: From Urinary Tract Infection to Uterine Inflammatory Disease
{: .text-alpha }

## One-Sentence Summary

Fosfomycin is an antibiotic. The TxGNN model (deep learning) predicts it may be useful for **uterine inflammatory disease** (score 0.9998). This score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. The evidence is thin: **2 clinical trials** and **1 publication** were retrieved, and none of them directly studies this disease.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Urinary tract infection (relation recorded in the knowledge graph; the local licence record contains no indication text) |
| Predicted New Indication | Uterine inflammatory disease |
| TxGNN Prediction Score | 0.9998 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Licences | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for fosfomycin are not available in the data compiled for this report. It is generally described as a broad-spectrum bactericidal antibiotic that inhibits MurA, an enzyme in bacterial cell wall (peptidoglycan) synthesis. It is active against many Gram-negative and Gram-positive bacteria.

Uterine inflammatory disease is often caused by mixed bacterial infection. An antibiotic with broad activity and a record in urinary tract infection could plausibly be considered for it. This link is only theoretical. No data on fosfomycin penetration into uterine tissue were found, and no study tested it in this condition. The high TxGNN score is a computational prediction and not clinical proof.

## Clinical Trial Evidence

Both registered trials are broad paediatric pharmacokinetic studies. Neither tests efficacy in uterine inflammatory disease, and both were graded as having low relevance.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01431326](https://clinicaltrials.gov/study/NCT01431326) | N/A | Completed | 3,520 | Pharmacokinetics of understudied drugs given to children as standard of care. It does not address uterine inflammation. |
| [NCT04278404](https://clinicaltrials.gov/study/NCT04278404) | N/A | Recruiting | 5,000 | Pharmacokinetics, pharmacodynamics and safety of understudied drugs in children and young adults. It has no indication-specific efficacy data. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35653789](https://pubmed.ncbi.nlm.nih.gov/35653789/) | 2022 | Cohort | Journal of Obstetrics and Gynaecology | Prospective study of 145 women after laparoscopic surgery for endometriosis. It compared acute cystitis symptoms and bacteriuria between dienogest and combined oral contraceptives. It does not test fosfomycin in uterine inflammatory disease. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2538210 | UroFos | Powder for oral solution, 3 g sachet | Not stated in the licence record |

The only licence is a valid single-ingredient product. It is an oral formulation.

## Safety Considerations

- **Contraindication-type relations in the knowledge graph** (not taken from label text): kidney disease, pyoureter, *Clostridium difficile* colitis and perinatal necrotizing enterocolitis.
- **Drug Interactions** (3 found):
  - Balsalazide: Moderate
  - Picosulfuric acid: Moderate
  - Metoclopramide: Minor

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high, but no retrieved trial or publication studies fosfomycin in uterine inflammatory disease. The evidence therefore stays at L5. The only trials are general paediatric pharmacokinetic studies, and the one publication concerns cystitis after endometriosis surgery.

**To proceed, the following is needed:**
- Clinical or microbiological data on fosfomycin use in uterine or pelvic infections, including tissue penetration
- Package insert warnings and contraindications from the Medsafe record
- Confirmed mechanism-of-action data
- A check of whether the oral 3 g sachet formulation is suitable for this condition
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


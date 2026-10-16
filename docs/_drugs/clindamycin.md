---
layout: default
title: Clindamycin
parent: Model Prediction Only (L5)
nav_order: 105
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-15
description: "Clindamycin: predicted new indication Punctate epithelial keratoconjunctivitis (evidence level L5). Approved use: Bacterial infections (streptococcal, staphylococcal, anaerobic), acne…"
---

# Clindamycin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

# Clindamycin: From Bacterial Infections and Acne to Punctate Epithelial Keratoconjunctivitis

## One-Sentence Summary

Clindamycin is a lincosamide antibiotic originally used for bacterial infections (streptococcal and staphylococcal infections, anaerobic infections, bacterial vaginosis) and acne.
The TxGNN model predicts it may be effective for **punctate epithelial keratoconjunctivitis**,
but there are **no clinical trials** and **no publications** supporting this prediction, so it rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Bacterial infections (streptococcal, staphylococcal, anaerobic), acne, bacterial vaginosis (listed in the knowledge graph; no approved indication text for the New Zealand licences) |
| Predicted New Indication | Punctate epithelial keratoconjunctivitis |
| TxGNN Prediction Score | 99.97% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, clindamycin is a lincosamide antibacterial that inhibits the bacterial 50S ribosomal subunit. Its efficacy in bacterial infections is established, and mechanistically it could only be applicable to the new indication if a bacterial component were present.

Punctate epithelial keratoconjunctivitis is often viral or inflammatory in origin, so an antibacterial mechanism has no clear link to the disease itself. Any benefit would probably be limited to bacterial superinfection. The high model score is therefore best treated as a statistical signal from the knowledge graph rather than a mechanistically grounded prediction.

Other predictions in the model output show the same pattern:
- **Exposure keratitis** (score 99.80%): the supplied papers describe bacterial keratitis (*S. aureus*, *B. cereus*), which clindamycin may cover. However, exposure keratitis is mainly a mechanical or desiccation injury, no paper studies clindamycin in it, and ocular use of clindamycin is not an established route. Evidence level L4.
- **Neurotrophic keratopathy** (99.49%), **epidemic keratoconjunctivitis** (99.49%, typically adenoviral), **postmenopausal atrophic vaginitis** (99.03%, driven by estrogen deficiency) and **non-human animal disease** (99.69%, a non-specific category): all have weak or no mechanistic links. For the animal disease category, the papers are *C. difficile* epidemiology studies, and clindamycin is a known risk factor for *C. difficile* infection, which suggests potential harm rather than benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the top predicted indication, punctate epithelial keratoconjunctivitis.

For reference, the second-ranked prediction (exposure keratitis) has indirect literature only, none of which studies clindamycin:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [24244625](https://pubmed.ncbi.nlm.nih.gov/24244625/) | 2013 | Retrospective cohort | PLoS One | Review of hospital cases of *S. aureus* keratitis, comparing MRSA and MSSA in demographics, clinical features, antibiotic susceptibility and outcomes |
| [22880135](https://pubmed.ncbi.nlm.nih.gov/22880135/) | 2012 | Retrospective cohort | PLoS One | Prevalence, clinical features and antibiotic susceptibility of ocular MRSA vs MSSA infections |
| [11581057](https://pubmed.ncbi.nlm.nih.gov/11581057/) | 2001 | Case report | Ophthalmology | Contact lens-related *B. cereus* keratitis with contamination of the lens case |
| [42763505](https://pubmed.ncbi.nlm.nih.gov/42763505/) | 2026 | Case report | IDCases | Ocular toxoplasmosis involving the fovea, diagnosed by intraocular fluid mNGS |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2387131 (cancelled) | Dalacin C | Injection 150 mg per ml, 4 ml ampoule | Not available |
| 2576392 (cancelled) | Dalacin C | Capsule (hydrochloride) 150 mg | Not available |

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: liver failure, sunburn, Crohn's disease, Crohn's colitis, inflammatory bowel disease, ulcerative colitis, atopic eczema/atopic dermatitis, exfoliative dermatitis, dermatitis, diarrhoeal disease, *Clostridium difficile* colitis, perinatal necrotising enterocolitis, vulvovaginal candidiasis.
- **Drug Interactions**: 100 interactions were found in total. Moderate-level interactions include balsalazide, clarithromycin, picosulfuric acid, attapulgite and kaolin. Other listed interactions are of unknown severity, including amphotericin B, acetylsalicylic acid, amoxicillin, bupropion, pantoprazole, glimepiride, doxycycline, clotrimazole, morphine, metformin, omeprazole, sucralfate, rosiglitazone, lansoprazole and vancomycin.

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on the model score alone, with no clinical trials or literature, and the antibacterial mechanism has no clear link to a largely viral or inflammatory condition. Clindamycin also has no currently valid New Zealand licence, and ocular use is not an established route.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Evidence that a bacterial component exists in punctate epithelial keratoconjunctivitis, plus any clinical or preclinical data on ocular clindamycin
- An assessment of ocular formulation and route feasibility, as none is currently established
- Review of the other predicted indications, which currently show no better support (exposure keratitis is the only one with indirect literature, at L4)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


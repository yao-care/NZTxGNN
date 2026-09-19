---
layout: default
title: Metronidazole
parent: Model Prediction Only (L5)
nav_order: 223
evidence_level: L5
indication_count: 10
---

# Metronidazole
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

# metronidazole: From anaerobic bacterial and protozoal infection to Pneumocystosis

## Summary in one sentence

metronidazole (metronidazole, DB00916) is a nitroimidazole-class antimicrobial drug; the evidence package mentions its original indications encompass anaerobic bacterial infections, amoebiasis, trichomoniasis, etc. (complete package insert data and original MOA are data gaps). The TxGNN model predicts with an **extremely high score of 99.99%** that it may be effective for **Pneumocystosis (PCP)**, but after systematic review of **24 clinical trials** and **10 literature papers** on an individual basis, **no direct supporting evidence was found**, and the mechanistic rationale is not established (*Pneumocystis jirovecii*, the causative organism of PCP, is a fungus, not within the anaerobic bacterial/protozoal scope of metronidazole).

---

## Overview

| Item | Content |
|------|---------|
| Original indication | Complete package insert data not provided (evidence package internal literature mentions anaerobic bacterial infections, amoebiasis, trichomoniasis, etc.; MOA is also a data gap) |
| Predicted new indication | Pneumocystosis (PCP) |
| TxGNN prediction score | 99.99% (rank 250) |
| Evidence level | L5 (model prediction only, no substantive supporting evidence) |
| New Zealand market status | Not marketed |
| Number of permits | 0 |
| Recommended decision | **Hold** |

---

## Why this prediction warrants review (and why current evidence does not support it)

metronidazole's mechanism of action is as a prodrug that requires reduction and activation in anaerobic or microaerophilic environments, generating free radicals that damage the pathogen's DNA, and thus has bactericidal/protozoal activity against anaerobic bacteria and certain protozoa (such as *Entamoeba histolytica*, *Trichomonas vaginalis*). In the evidence package, other candidate indications (such as vulvar ulceration, cap polyposis) indeed cite this mechanism as theoretical basis.

However, the target organism of this prediction, *Pneumocystis jirovecii* (the causative agent of pneumocystosis), is a **fungus**, not an anaerobic bacterium or protozoan; metronidazole's nitroimidazole reduction and activation mechanism lacks a mechanistic basis against this pathogen. Literature PMID 7355683 (1980, *American Family Physician*) clearly indicates that the standard therapy for PCP is **trimethoprim-sulfamethoxazole (TMP-SMX)**, while metronidazole is applicable only to amoebic colitis, trichomoniasis, and other anaerobic/protozoal infections; the therapeutic scope of the two does not overlap. Therefore, although this prediction has an extremely high model score, **the mechanistic relevance is not established**, making it a potential false positive case for TxGNN.

---

## Clinical trial evidence

The systematic search identified 24 registered trials; after individual relevance grading, all 6 trials that have been scored were classified as "not relevant (Grade C)"; the remaining 18 trials are pending grading, but their abstract topics (behavioral interventions, diabetes education, advance directives, etc.) also show no apparent association with PCP/metronidazole treatment. The following lists trials that have completed relevance assessment:

| Trial number | Phase | Status | Enrollment | Key findings |
|---------|------|------|------|---------|
| [NCT04463914](https://clinicaltrials.gov/study/NCT04463914) | N/A | Completed | 649 | Mobile app intervention for depression trial, unrelated to PCP/metronidazole |
| [NCT02208947](https://clinicaltrials.gov/study/NCT02208947) | Phase 3 | Terminated | 77 | Advance directive financial incentive trial, unrelated to this indication |
| [NCT05892666](https://clinicaltrials.gov/study/NCT05892666) | N/A | Recruiting | 4000 | Comparative study of value of emergency triage in ambulatory acute/critical settings, unrelated to this indication |
| [NCT03466866](https://clinicaltrials.gov/study/NCT03466866) | Phase 3 | Completed | 156 | Diabetes emergency reduction education trial, unrelated to this indication |
| [NCT05340426](https://clinicaltrials.gov/study/NCT05340426) | Phase 1 | Withdrawn | 0 | Pig kidney xenotransplantation trial, unrelated to this indication |

The remaining 19 trials (such as NCT06597123, NCT01909076, etc.) have topics encompassing AI clinical decision support training, opioid risk management, diabetes care models, etc., and are unrelated to the hypothesis of metronidazole treatment for PCP, so they are not listed individually.

**Conclusion: Currently, no registered clinical trial directly evaluates metronidazole for the treatment of pneumocystosis.**

---

## Literature evidence

| PMID | Year | Type | Journal | Key findings |
|------|-----|------|------|---------|
| [1545596](https://pubmed.ncbi.nlm.nih.gov/1545596/) | 1992 | Review | Mayo Clinic Proceedings | Comprehensive review of antiparasitic drugs; does not mention metronidazole use in PCP |
| [26518395](https://pubmed.ncbi.nlm.nih.gov/26518395/) | 2015 | Review | Topics in Antiviral Medicine | Overview of current HIV-related opportunistic infections; does not specifically support the role of metronidazole in PCP |
| [2996829](https://pubmed.ncbi.nlm.nih.gov/2996829/) | 1985 | Review | Clinical Pharmacy | Review of AIDS-related complications treatment; standard therapy for PCP is TMP-SMX, not metronidazole |
| [7355683](https://pubmed.ncbi.nlm.nih.gov/7355683/) | 1980 | Unclassified | American Family Physician | Clearly indicates that the first-line agent for PCP is **trimethoprim-sulfamethoxazole (TMP-SMX)**; metronidazole is only indicated for amoebic colitis, trichomoniasis, and other anaerobic/protozoal infections; the therapeutic scope of the two does not overlap |
| [1782741](https://pubmed.ncbi.nlm.nih.gov/1782741/) | 1991 | Unclassified | Clinical Pharmacokinetics | Comprehensive review of pharmacokinetics of antiprotozoal drugs; does not specifically link metronidazole to PCP |
| [6771863](https://pubmed.ncbi.nlm.nih.gov/6771863/) | 1980 | Unclassified | Reviews of Infectious Diseases | Commentary on prophylactic antibiotic therapy; not related to PCP treatment |
| [16496064](https://pubmed.ncbi.nlm.nih.gov/16496064/) | 2005 | Unclassified | J Formosan Medical Association | Case report of colon perforation from CMV and amoebiasis in an AIDS patient; metronidazole used for amoebiasis infection, not PCP |
| [6282154](https://pubmed.ncbi.nlm.nih.gov/6282154/) | 1982 | Case report | American Review of Respiratory Disease | Case report of PCP with CMV infection; patient previously received metronidazole for diarrhea, not for treatment of PCP itself |
| [2338506](https://pubmed.ncbi.nlm.nih.gov/2338506/) | 1990 | Case report | Journal of Infectious Diseases (Japan) | AIDS patient treated with metronidazole for amoebiasis dysentery subsequently developed PCP; metronidazole not used for PCP treatment |
| [2280469](https://pubmed.ncbi.nlm.nih.gov/2280469/) | 1990 | Unclassified | Nihon Rinsho | Comprehensive review of antiprotozoal drugs; no abstract available for assessment |

**Conclusion: None of the 10 papers found in the current literature demonstrate therapeutic efficacy of metronidazole against pneumocystosis; multiple papers explicitly state that standard therapy for PCP is TMP-SMX, with metronidazole appearing only in the treatment context of other concurrent infections (amoebiasis) in the same patients, representing coincidental comorbidity rather than evidence of efficacy.**

---

## New Zealand market information

metronidazole is currently **not marketed in New Zealand**; no drug permit registration data was found.

---

## Safety considerations

Please refer to the drug package insert for safety information (Medsafe package insert warnings/contraindications, DDI data are key data gaps to be completed).

---

## Conclusion and follow-up recommendations

**Decision: Hold**

**Rationale:**
- Mechanistic rationale not established: *Pneumocystis jirovecii* is a fungus; metronidazole's anaerobic bacterial/protozoal reduction and activation mechanism lacks a mechanistic basis; standard therapy for PCP is TMP-SMX.
- After individual review of 24 clinical trials and 10 papers in the literature, **no direct supporting evidence was found**; the high TxGNN score should be viewed as a potential false positive.
- The drug is not marketed in New Zealand; safety data (package insert warnings, contraindications, DDI) are also data gaps, and S1 safety initial assessment cannot be performed.

**If continuation is desired, the following must be supplemented:**
- Complete package insert warnings and contraindications data from TFDA/manufacturer (DG001, Blocking)
- Complete metronidazole mechanism of action (MOA) data (DG002, High)
- Should this pairing be pursued, alternative mechanistic hypotheses or in vitro/animal experimental evidence concerning PCP pathogenesis must be sought.

**Note:** Among candidate indications in this batch, rank 9 (cap polyposis) and rank 10 (vulvar ulceration) have more explicit mechanistic discussion and indirect literature support (reaching L4/Research Question stage, respectively); these are recommended for priority in subsequent evidence inventory.

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


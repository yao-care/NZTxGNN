---
layout: default
title: Zidovudine
parent: Model Prediction Only (L5)
nav_order: 485
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-15
monitor_inn: "ZIDOVUDINE"
predicted_indications: ["feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter", "obsolete familial combined hyperlipidemia"]
description: "Zidovudine: predicted new indication Feline acquired immunodeficiency syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Zidovudine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

## Zidovudine: Feline acquired immunodeficiency syndrome
{: .text-alpha }

## One-Sentence Summary

Zidovudine is a nucleoside reverse transcriptase inhibitor, originally used to treat HIV infection and AIDS.
The TxGNN model's top-ranked prediction is **feline acquired immunodeficiency syndrome**, a veterinary indication supported by **19 publications** (all animal or veterinary studies) and **no registered clinical trials**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Feline acquired immunodeficiency syndrome |
| TxGNN Prediction Score | 0.9996 |
| Evidence Level | L5 (animal and veterinary studies only; no human trials for this indication) |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold (rank 1 is a veterinary research question; see the Conclusion for the human-relevant predictions) |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, zidovudine is a thymidine analogue that must be phosphorylated in lymphocytes before it acts. It inhibits the reverse transcriptase of HIV by chain termination, and its efficacy in HIV infection and AIDS is well established.

Feline immunodeficiency virus (FIV) is a lentivirus related to HIV and carries a reverse transcriptase that zidovudine can inhibit. In experimentally and naturally infected cats, zidovudine lowered plasma virus titres and in some studies produced clinical benefit. It did not prevent infection when given prophylactically. Toxicity, mainly anaemia, has also been documented in cats. The same logic applies to the second-ranked prediction, simian immunodeficiency virus infection, where macaque studies show reduced viral load and prolonged survival.

Both are animal models or veterinary conditions, so they are not directly applicable to human drug repurposing. The other two predictions are probably model artefacts: a rare neurodevelopmental disorder and an obsolete familial combined hyperlipidaemia term. Neither has any literature, clinical trials or plausible mechanism.

---

## Clinical Trial Evidence

The top-ranked prediction (feline AIDS) has no registered clinical trials.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00001011](https://clinicaltrials.gov/study/NCT00001011) | Phase 3 | Completed | 538 | Safety and efficacy of zidovudine in early AIDS related complex; evaluates HIV suppression and prevention of progression to AIDS |
| [NCT00000736](https://clinicaltrials.gov/study/NCT00000736) | Phase 3 | Completed | 3200 | Zidovudine in asymptomatic HIV infection; delay or prevention of AIDS/ARC, and comparison of a lower dose with the dose used in AIDS and severe ARC |
| [NCT00001022](https://clinicaltrials.gov/study/NCT00001022) | Phase 3 | Completed | 1200 | AZT alone vs AZT plus didanosine vs AZT plus zalcitabine for delaying AIDS-related conditions |
| [NCT00002124](https://clinicaltrials.gov/study/NCT00002124) | Phase 3 | Completed | 1250 | Three doses of delavirdine plus zidovudine vs zidovudine alone in HIV-1 infection |
| [NCT00000625](https://clinicaltrials.gov/study/NCT00000625) | Phase 2 | Completed | 2100 | Monotherapy vs combination nucleoside analogue therapy (AZT, ddI, ddC) in disease progression |
| [NCT00000803](https://clinicaltrials.gov/study/NCT00000803) | Phase 2 | Completed | 471 | Delavirdine with AZT and/or ddI vs AZT/ddI; safety and anti-HIV activity |
| [NCT00002238](https://clinicaltrials.gov/study/NCT00002238) | Phase 3 | Completed | Not reported | Interferon beta (Betaseron) in AIDS and advanced ARC patients on reduced-dose AZT |
| [NCT00002326](https://clinicaltrials.gov/study/NCT00002326) | Phase 1 | Completed | 20 | Safety, tolerance and pharmacokinetics of AZT plus adefovir (PMEA) |
| [NCT00000960](https://clinicaltrials.gov/study/NCT00000960) | Phase 3 | Completed | 1496 | Oral zidovudine in pregnant HIV-infected women and their infants to reduce mother-to-child transmission (relevant to the congenital HIV prediction) |
| [NCT00002117](https://clinicaltrials.gov/study/NCT00002117) | Phase 3 | Completed | 528 | Alternative ddC and AZT combination schedules in HIV infection |

---

## Literature Evidence

**Feline AIDS (rank 1, veterinary):**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7688949](https://pubmed.ncbi.nlm.nih.gov/7688949/) | 1993 | Animal study | Archives of Virology | Zidovudine lowered plasma FIV titres at 2 weeks but did not prevent infection and the effect was not maintained |
| [22816032](https://pubmed.ncbi.nlm.nih.gov/22816032/) | 2012 | Veterinary clinical study | Viruses | Zidovudine alone or combined with interferon, lamivudine or valproic acid in naturally FIV-infected cats over one year |
| [8381867](https://pubmed.ncbi.nlm.nih.gov/8381867/) | 1993 | Animal study | J Acquir Immune Defic Syndr | Prophylactic zidovudine prevented early viraemia and lymphocyte decline but not primary infection |
| [2178336](https://pubmed.ncbi.nlm.nih.gov/2178336/) | 1990 | Animal study | Antimicrob Agents Chemother | Interferon alpha plus zidovudine vs zidovudine alone in presymptomatic FeLV-induced immunodeficiency |
| [2163339](https://pubmed.ncbi.nlm.nih.gov/2163339/) | 1990 | Animal toxicity study | Fundam Appl Toxicol | Dose-dependent zidovudine toxicity in FeLV-infected cats |
| [8820595](https://pubmed.ncbi.nlm.nih.gov/8820595/) | 1995 | Review | Semin Vet Med Surg (Small Anim) | Management of retrovirus-infected cats, including zidovudine |


| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [2677429](https://pubmed.ncbi.nlm.nih.gov/2677429/) | 1989 | Clinical study | JAMA | Prolonged zidovudine therapy in 229 AIDS/ARC patients; 12-month survival 84.5% in the original treatment group |
| [1777174](https://pubmed.ncbi.nlm.nih.gov/1777174/) | 1991 | Double-blind trial | AIDS | Zidovudine with or without acyclovir in 199 ARC patients |
| [2191113](https://pubmed.ncbi.nlm.nih.gov/2191113/) | 1990 | Placebo-controlled trial | J Acquir Immune Defic Syndr | Quality of life; at 1 year, 1 death in the zidovudine group vs 10 in the placebo group |
| [7693435](https://pubmed.ncbi.nlm.nih.gov/7693435/) | 1993 | Review | Drugs | Zidovudine delays progression to AIDS and ARC, reduces opportunistic infections and improves survival |
| [2201071](https://pubmed.ncbi.nlm.nih.gov/2201071/) | 1990 | Review | Rev Infect Dis | Zidovudine intolerance, mainly time- and dose-dependent haematologic toxicity |

**Congenital HIV (rank 6, human):**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7913986](https://pubmed.ncbi.nlm.nih.gov/7913986/) | 1994 | Guideline | MMWR Recomm Rep | USPHS recommendations: zidovudine in pregnant women and newborns reduced perinatal transmission by about two-thirds |
| [28537936](https://pubmed.ncbi.nlm.nih.gov/28537936/) | 2017 | Cohort/safety study | AIDS | Inconsistent evidence that zidovudine in pregnancy increases congenital malformations |
| [29912896](https://pubmed.ncbi.nlm.nih.gov/29912896/) | 2018 | Systematic review and network meta-analysis | PLoS One | Comparative safety and effectiveness of perinatal antiretroviral therapies |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 818682 | Retrovir | Oral liquid 10 mg per ml | Not recorded (valid) |
| 552666 | Retrovir | Capsule 100 mg | Not recorded (valid) |


---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Not an antineoplastic; antiretroviral nucleoside analogue with known haematologic and mitochondrial toxicity |
| Myelosuppression Risk | Medium to high; anaemia and neutropenia are documented and are dose- and time-dependent |
| Emetogenicity Classification | Low to medium |
| Monitoring Items | Full blood count with differential, liver and renal function, creatine kinase, lactate |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Contraindications (associations only, not label text)**: liver failure, liver disease, fatty liver disease, non-alcoholic steatohepatitis, kidney disease, pancreatitis, gallbladder disease, myopathy and other muscle disease (including myositis and dermatomyositis), neutropenia, agranulocytosis, anaemia (including macrocytic anaemia), lactic acidosis, hypertriglyceridemia, obesity disorders, familial periodic paralysis, pyoureter.
- **Drug Interactions**: 100 interactions are recorded. The main ones listed are:
  - Moderate: amphotericin B (including the cholesteryl sulfate and lipid complex forms), corticosteroids (betamethasone, budesonide, dexamethasone, hydrocortisone, prednisolone, prednisone, triamcinolone), naltrexone, rosuvastatin, simvastatin, orlistat.
  - Minor: cimetidine, clarithromycin.
  - Unclassified: pantoprazole, morphine, metformin, omeprazole.
- **Evidence of toxicity in the literature**: haematologic toxicity (anaemia, leukopenia), and in cats, dose-dependent toxicity. For pregnancy, malformation evidence is inconsistent.

Package insert warnings and contraindications are not available in the data compiled for this report. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold** for the top-ranked prediction (feline AIDS) and for the unsupported predictions (rank 3 and rank 4). **Proceed with Guardrails** applies only to ranks 5 and 6, which are confirmations of the established HIV use.

**Rationale:**
- The top prediction is a veterinary indication with animal evidence only, so it does not support human repurposing. The neurodevelopmental disorder and the obsolete hyperlipidaemia term have no evidence and no plausible mechanism, and zidovudine is more often linked to dyslipidaemia than to its treatment.
- Zidovudine is now used within combination regimens, and it is marketed in New Zealand with 2 valid licences out of 2.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (from Medsafe) to complete safety screening
- Detailed mechanism of action data (from DrugBank)
- Verification of current availability in New Zealand, given the cancelled licences
- For any HIV-related use: a monitoring plan for haematologic toxicity, myopathy and lactic acidosis, and a review of the 100 recorded interactions
- For a veterinary pathway, if pursued: a separate veterinary regulatory and toxicity assessment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


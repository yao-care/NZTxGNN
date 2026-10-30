---
layout: default
title: Itraconazole
parent: Model Prediction Only (L5)
nav_order: 248
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-30
monitor_inn: "ITRACONAZOLE"
predicted_indications: ["pneumocystosis"]
description: "Itraconazole: predicted new indication Pneumocystosis (evidence level L5). Approved use: Not recorded in the local data"
---

# Itraconazole
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

## Itraconazole: Pneumocystosis
{: .text-alpha }

## One-Sentence Summary

Itraconazole is an azole antifungal originally used for systemic fungal infections such as histoplasmosis, aspergillosis, candidiasis, blastomycosis and sporotrichosis.
The TxGNN model predicts it may be effective for **pneumocystosis**, but the data compiled for this report contain **no registered clinical trials** and **no publications that directly test itraconazole against Pneumocystis**, so the prediction rests mainly on the model score.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Pneumocystosis |
| TxGNN Prediction Score | 0.9934 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 3 (valid: 3) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. From general pharmacological knowledge, itraconazole inhibits fungal lanosterol 14-alpha-demethylase (CYP51), which blocks ergosterol synthesis in the fungal cell membrane. Its efficacy against many systemic fungal infections is established.

*Pneumocystis jirovecii* is classified as a fungus, which is the likely reason the model links it to itraconazole's approved indications. However, its cell membrane contains little or no ergosterol, with cholesterol as the main sterol. The azole mechanism therefore has **limited biological plausibility** against this organism.

The high score (0.9934) most likely reflects network proximity to approved antifungal indications such as histoplasmosis, aspergillosis and candidiasis, not a validated mechanism. The retrieved literature mostly covers prophylaxis and treatment of opportunistic infections in HIV, transplant and neutropenic patients. In those papers itraconazole is typically discussed for other fungi, not for Pneumocystis. This prediction should be treated as a hypothesis only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

None of the retrieved papers shows itraconazole being tested for pneumocystosis. The most relevant are listed below (reviews and cohorts prioritised, plus one randomised trial of fungal prophylaxis).

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [11737382](https://pubmed.ncbi.nlm.nih.gov/11737382/) | 2001 | Randomised placebo-controlled trial (phase III) | HIV Medicine | Itraconazole capsules for prevention of deep fungal infections in HIV-infected, immunodeficient patients; the abstract excerpt does not report Pneumocystis-specific results |
| [2121456](https://pubmed.ncbi.nlm.nih.gov/2121456/) | 1990 | Review | Drugs | Therapy and prophylaxis of systemic protozoan infections, including *P. carinii* |
| [21418688](https://pubmed.ncbi.nlm.nih.gov/21418688/) | 2010 | Review | BMJ Clinical Evidence | Primary and secondary prophylaxis of opportunistic infections in HIV |
| [15250025](https://pubmed.ncbi.nlm.nih.gov/15250025/) | 2004 | Review | Clinical Infectious Diseases | Antimicrobial prophylaxis in febrile neutropenia; trimethoprim-sulfamethoxazole is highly effective against *P. carinii* pneumonia |
| [8397916](https://pubmed.ncbi.nlm.nih.gov/8397916/) | 1993 | Review | Current Clinical Topics in Infectious Diseases | Prophylaxis and treatment of infection in bone marrow transplant recipients |
| [8016481](https://pubmed.ncbi.nlm.nih.gov/8016481/) | 1993 | Review | Seminars in Respiratory Infections | Infection after lung transplantation |
| [21973267](https://pubmed.ncbi.nlm.nih.gov/21973267/) | 2011 | Review | Clinical Pharmacokinetics | Penetration of antifungal and other anti-infective agents into pulmonary epithelial lining fluid |
| [26036497](https://pubmed.ncbi.nlm.nih.gov/26036497/) | 2015 | Cohort | Transplantation Proceedings | Invasive fungal infections after kidney transplantation (single centre) |
| [2233233](https://pubmed.ncbi.nlm.nih.gov/2233233/) | 1990 | Cohort | Medicine | Disseminated histoplasmosis in AIDS: clinical findings, diagnosis and treatment |
| [7877856](https://pubmed.ncbi.nlm.nih.gov/7877856/) | 1994 | Not classified | Pathologie-Biologie | Aspergillosis in AIDS; notes previous pulmonary infection, especially pneumocystosis, as a context |

## New Zealand Market Information

All three licences are valid, and no approved indication text is recorded for them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2354322 | Itrazole | Capsule 100 mg | Valid |
| 2705109 | Itraconazole Cresent | Capsule 100 mg | Valid |
| 2693364 | Itraconazole Kent | Oral liquid 10 mg/ml | Valid |

## Safety Considerations

Package insert warnings and contraindications were not available in the compiled data, so please refer to the package insert for these.

- **Conditions associated with contraindication in the knowledge graph** (these come from a knowledge graph, not label text): long QT syndrome, torsades de pointes, congestive heart failure and other cardiomyopathies, myocardial ischaemia, coronary artery disease, kidney disease and impaired renal function, liver disease and drug-induced liver injury, chronic obstructive pulmonary disease, peripheral neuropathy, potassium deficiency, hearing loss, vision disorders, and others. Cardiac, hepatic and renal conditions stand out.
- **Drug interactions**: 100 interactions were found in total. Examples of those rated **Major** among the 20 returned: loperamide, triamcinolone and budesonide. Examples rated **Moderate**: famotidine, ranitidine, omeprazole, rabeprazole, cimetidine, dexamethasone, hydrocortisone, betamethasone, aprepitant and alosetron. Metformin, pioglitazone, chlorpropamide and amphotericin B are rated **Minor**. Acid-suppressing drugs are notable because they can reduce absorption of some itraconazole formulations.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no registered clinical trials and no literature directly supporting itraconazole for pneumocystosis (Evidence Level L5). The mechanism is also biologically doubtful because *Pneumocystis* has little ergosterol.

**To proceed, the following is needed:**
- Mechanism of action data and a mechanistic assessment of azole activity against *Pneumocystis*
- Preclinical or in vitro evidence of itraconazole activity against *P. jirovecii*
- Package insert warnings and contraindications from Medsafe, to allow safety screening
- Review of the cardiac, hepatic and renal safety signals and the major drug interactions
- Clarification of regulatory status, since 3 of 3 New Zealand licences are valid
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


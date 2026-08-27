---
layout: default
title: Itraconazole
parent: 僅模型預測 (L5)
nav_order: 184
evidence_level: L5
indication_count: 1
---

# Itraconazole
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Itraconazole: From Fungal Infections to Pneumocystosis

## One-Sentence Summary

Itraconazole is a broad-spectrum triazole antifungal traditionally used to treat and prevent systemic fungal infections in immunocompromised patients. The TxGNN model predicts it may be effective for **Pneumocystosis** (Pneumocystis jirovecii pneumonia), with a very high prediction score but **no dedicated clinical trials** and **20 supporting publications**, most of which address fungal-infection prophylaxis in immunocompromised populations rather than Pneumocystis treatment specifically.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Systemic fungal infections (e.g., aspergillosis, candidiasis, histoplasmosis) — based on general pharmacological knowledge; no formal indication text available in this dataset |
| Predicted New Indication | Pneumocystosis |
| TxGNN Prediction Score | 99.34% |
| Evidence Level | L3 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known pharmacology, itraconazole is a triazole antifungal that inhibits fungal cytochrome P450 14-alpha-demethylase (lanosterol demethylase), blocking ergosterol synthesis and disrupting fungal cell membrane integrity. This mechanism underlies its established efficacy against a broad range of fungal pathogens.

Pneumocystis jirovecii, though phylogenetically classified as a fungus, has atypical membrane sterol biology — its trophic form relies substantially on cholesterol (including host-scavenged cholesterol) rather than ergosterol, which is why classic azole antifungals are generally considered to have limited direct activity against it, unlike Aspergillus or Candida species. This is an important caveat: it may partly explain why the literature evidence base consists largely of case reports and reviews describing itraconazole used for **prophylaxis of other fungal infections in the same at-risk populations** (HIV/AIDS, transplant recipients, chronic granulomatous disease) where pneumocystosis also occurs as a co-morbid opportunistic infection — rather than direct trials of itraconazole treating Pneumocystis itself.

The TxGNN association is therefore plausible at the population/epidemiological level (both conditions cluster in similarly immunocompromised patients managed with prophylactic antifungal strategies) but is mechanistically less certain at the pharmacological level. This distinction should be weighed carefully before advancing the candidate.

---

## Clinical Trial Evidence

Currently no related clinical trials registered (0 hits on ClinicalTrials.gov and ICTRP for itraconazole + pneumocystosis).

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [11737382](https://pubmed.ncbi.nlm.nih.gov/11737382/) | 2001 | RCT | HIV Medicine | Randomized, double-blind, placebo-controlled Phase III trial of itraconazole capsules for prevention of deep fungal infections in HIV-infected patients |
| [2121456](https://pubmed.ncbi.nlm.nih.gov/2121456/) | 1990 | Review | Drugs | Reviews therapy/prophylaxis for systemic protozoan and opportunistic infections including Pneumocystis carinii |
| [8397916](https://pubmed.ncbi.nlm.nih.gov/8397916/) | 1993 | Review | Current Clinical Topics in Infectious Diseases | Prophylaxis and treatment strategies for infections in bone marrow transplant recipients |
| [21418688](https://pubmed.ncbi.nlm.nih.gov/21418688/) | 2010 | Review | BMJ Clinical Evidence | Primary/secondary prophylaxis for opportunistic infections in HIV patients |
| [8016481](https://pubmed.ncbi.nlm.nih.gov/8016481/) | 1993 | Review | Seminars in Respiratory Infections | Infection (including fungal) as major cause of morbidity/mortality after lung transplantation |
| [21973267](https://pubmed.ncbi.nlm.nih.gov/21973267/) | 2011 | Review | Clinical Pharmacokinetics | Pulmonary epithelial lining fluid penetration of antifungal agents including itraconazole |
| [7877856](https://pubmed.ncbi.nlm.nih.gov/7877856/) | 1994 | Review | Pathologie-biologie | Aspergillosis in AIDS patients, noting prior pneumocystosis as a predisposing factor |
| [2233233](https://pubmed.ncbi.nlm.nih.gov/2233233/) | 1990 | Review | Medicine | Disseminated histoplasmosis in AIDS: clinical findings, diagnosis, treatment, and literature review |
| [26036497](https://pubmed.ncbi.nlm.nih.gov/26036497/) | 2015 | Observational | Transplantation Proceedings | Single-center experience of invasive fungal infections after kidney transplantation |
| [30429396](https://pubmed.ncbi.nlm.nih.gov/30429396/) | 2018 | Observational | Indian Journal of Medical Microbiology | Profile and susceptibility of respiratory fungal pathogens in immunocompetent vs. immunocompromised hosts, correlated with CD4+ counts |

---

## New Zealand Market Information

Itraconazole is currently not marketed in New Zealand; no product authorizations are on file in this dataset.

---

## Safety Considerations

Please refer to the package insert for safety information. (Note: TFDA/Medsafe package insert data, warnings, contraindications, and drug interaction data are flagged as a **blocking data gap** in this evidence pack — see Conclusion below.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high, but no clinical trials specifically target pneumocystosis, the supporting literature is largely indirect (fungal-infection prophylaxis in the same at-risk populations rather than direct anti-Pneumocystis evidence), and there is a plausible mechanistic caveat given Pneumocystis's atypical sterol biology. Combined with the blocking data gap on regulatory safety information and the drug's current non-marketed status in New Zealand, the evidence does not yet support progression beyond Hold.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data (warnings, contraindications, DDI) — currently a blocking gap
- Confirmed mechanism of action data (DrugBank) to properly assess pharmacological plausibility against Pneumocystis jirovecii's sterol pathway
- Direct clinical evidence (trial or well-classified literature) evaluating itraconazole specifically for pneumocystosis treatment or prophylaxis, rather than general fungal-infection prophylaxis
- A regulatory pathway/market entry assessment for New Zealand, given the drug is not currently marketed there
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Raltegravir
parent: Model Prediction Only (L5)
nav_order: 397
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-28
monitor_inn: "Raltegravir"
predicted_indications: ["feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter"]
description: "Raltegravir: predicted new indication Feline acquired immunodeficiency syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Raltegravir
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

## Raltegravir: Feline acquired immunodeficiency syndrome
{: .text-alpha }

## One-Sentence Summary

Raltegravir is an HIV-1 integrase strand transfer inhibitor, originally used to treat HIV infection and AIDS.
The TxGNN model predicts it may be effective for **feline acquired immunodeficiency syndrome**, but **no clinical trials and no publications** directly support this prediction.
The top-ranked prediction is a veterinary indication with no direct evidence; the second-ranked prediction (simian immunodeficiency virus infection) has **16 preclinical publications**, but it is mainly a research model for HIV.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Feline acquired immunodeficiency syndrome |
| TxGNN Prediction Score | 0.9978 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, raltegravir is an HIV-1 integrase strand transfer inhibitor, its efficacy in HIV/AIDS has been established, and mechanistically it may be applicable to other lentiviral infections.

Feline immunodeficiency virus (FIV) is a lentivirus that has its own integrase enzyme, structurally related to HIV-1 integrase. A class-level mechanistic rationale therefore exists. However, the high TxGNN score most likely reflects network proximity to the original HIV/AIDS indications rather than independent evidence. No trials or literature support activity against FIV, and because this is a veterinary indication, its relevance to human clinical development is limited.

Two other predictions were also reviewed:
- **Simian immunodeficiency virus (SIV) infection** (score 0.9978): Preclinical work in macaque models shows raltegravir-containing regimens act against SIV. This supports the mechanism, but SIV infection is a research model for HIV rather than a human disease, so it does not support a new human indication. Evidence level L4, categorised as a research question.
- **Neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter** (score 0.9977): No mechanistic link was identified. This rare genetic disorder has no apparent viral or integrase-related pathophysiology. The score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. It should not be pursued.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

For the top-ranked prediction (feline acquired immunodeficiency syndrome), currently no related literature is available.

For the related prediction of SIV infection, the data compiled for this report lists 16 publications, of which 10 are shown below. All are preclinical (nonhuman or in vitro). Study types are inferred from titles and should be verified against full records.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [20233398](https://pubmed.ncbi.nlm.nih.gov/20233398/) | 2010 | Preclinical | Retrovirology | SIVmac251-infected macaques treated with two nucleoside/nucleotide RT inhibitors plus raltegravir; basis for a simian AIDS treatment model |
| [22737073](https://pubmed.ncbi.nlm.nih.gov/22737073/) | 2012 | Preclinical | PLoS Pathog | Highly intensified multidrug ART induced long-term viral suppression and restricted the viral reservoir in a simian AIDS model |
| [29643246](https://pubmed.ncbi.nlm.nih.gov/29643246/) | 2018 | Preclinical | J Virol | Dynamics of SIV 2-LTR circles (products of failed integration) in macaques treated with an integrase inhibitor, with and without CD8+ cells |
| [29466356](https://pubmed.ncbi.nlm.nih.gov/29466356/) | 2018 | Preclinical | PLoS One | Resistance mutations emerged in two macaques given non-suppressive tenofovir/emtricitabine with raltegravir intensification |
| [24622515](https://pubmed.ncbi.nlm.nih.gov/24622515/) | 2014 | Preclinical | Sci Transl Med | Topical integrase inhibitors protected macaques from vaginal SHIV infection when given post-exposure |
| [24920794](https://pubmed.ncbi.nlm.nih.gov/24920794/) | 2014 | Preclinical | J Virol | HIV integrase resistance mutations introduced into SIVmac239 and their effect on INSTI susceptibility |
| [25583721](https://pubmed.ncbi.nlm.nih.gov/25583721/) | 2015 | Preclinical | Antimicrob Agents Chemother | Simian-tropic HIV as a model for studying integrase inhibitor resistance |
| [26378179](https://pubmed.ncbi.nlm.nih.gov/26378179/) | 2015 | Preclinical | J Virol | Resistance profiles of INSTIs in SIVmac239 tissue culture |
| [32166319](https://pubmed.ncbi.nlm.nih.gov/32166319/) | 2020 | Preclinical | Clin Infect Dis | Dolutegravir and raltegravir showed proadipogenic and profibrotic effects and induced insulin resistance in human/simian adipose tissue and adipocytes |
| [34903055](https://pubmed.ncbi.nlm.nih.gov/34903055/) | 2021 | Preclinical | mBio | Lentiviral infection persisted in the brain despite effective ART |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2562960 | Isentress HD (valid) | Tab 600 mg | Not available |
| 2324059 | Isentress (valid) | Tab 400 mg | Not available |

## Safety Considerations

- **Contraindications (from the knowledge graph, not label text)**: The knowledge graph links raltegravir to myopathy, muscular disease, skeletal muscle disease, familial periodic paralysis, rhabdomyolysis and nodular fasciitis. These come from graph edges and should be verified against the product label.
- **Drug Interactions**: 36 interactions were found in total; 20 are listed in the data compiled for this report.
  - **Major**: zinc sulfate, zinc acetate, aluminium hydroxide, calcium carbonate
  - **Moderate**: magnesium oxide, magnesium sulfate, magnesium chloride, orlistat
  - **Minor**: acid-suppressing drugs (famotidine, ranitidine, cimetidine, nizatidine, omeprazole, esomeprazole, lansoprazole, dexlansoprazole, pantoprazole, rabeprazole), pioglitazone, dexamethasone

  The major and moderate interactions involve polyvalent metal cations and antacids, which are known to chelate integrase inhibitors and may reduce absorption.

Please refer to the package insert for further safety information, including warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction is a veterinary indication with no trials or publications, and the high TxGNN score appears to reflect proximity to the original HIV/AIDS indications rather than independent evidence. The SIV literature is preclinical and validates the mechanism but does not support a new human indication.

**To proceed, the following is needed:**
- Mechanism of action data
- Package insert warnings and contraindications, to complete safety screening
- Direct evidence of raltegravir activity against FIV, if the veterinary indication is to be pursued
- Confirmation of regulatory status, since both New Zealand licences are cancelled
- Verification of study types of the SIV publications against full records
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


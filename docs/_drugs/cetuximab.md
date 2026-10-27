---
layout: default
title: Cetuximab
parent: Model Prediction Only (L5)
nav_order: 93
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Cetuximab: predicted new indication Non-seminomatous lesion (evidence level L5). Approved use: Colorectal cancer"
monitor_inn: "CETUXIMAB"
predicted_indications: ["non-seminomatous lesion", "chondroid hamartoma", "ductal or ductular proliferation", "bronchial adenomas/carcinoids childhood", "tumor of testis and paratestis", "odontogenic cyst", "thyroglossal duct cyst", "epiglottis neoplasm", "cystic neoplasm", "pre-malignant neoplasm"]
---

# Cetuximab
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

## Cetuximab: From Colorectal and Head and Neck Cancer to Non-Seminomatous Lesion
{: .text-alpha }

## One-Sentence Summary

Cetuximab is an anti-EGFR monoclonal antibody, originally used for EGFR-expressing colorectal cancer and squamous cell carcinoma of the head and neck.
The TxGNN model's top-ranked new prediction is **non-seminomatous lesion**, but it has **0 clinical trials** and **0 publications** behind it, so it is a model prediction only.
Two lower-ranked predictions, **pre-malignant neoplasm** and **cystic neoplasm**, have some supporting trials and literature and are discussed below.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Colorectal cancer; squamous cell carcinoma, including pharyngeal and laryngeal (from the knowledge graph; no approved indication text is available in the New Zealand licence records) |
| Predicted New Indication | Non-seminomatous lesion |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Cetuximab binds the epidermal growth factor receptor (EGFR) and blocks its signalling. It is used for EGFR-expressing metastatic colorectal cancer and, with radiotherapy, for squamous cell cancers of the head and neck. A detailed mechanism-of-action record is not available in the data compiled for this report, but the target is well established.

For the top-ranked prediction, **non-seminomatous lesion**, no mechanistic link has been established. The high score (0.9995) is a model output with no trial or literature support for EGFR-pathway involvement in this condition. It should not be treated as a repurposing signal on its own.

Two lower-ranked predictions have a more plausible rationale:

- **Pre-malignant neoplasm (L2):** EGFR is overexpressed early in head and neck carcinogenesis, which supports EGFR-targeted chemoprevention. A completed Phase 2 trial tested single-agent cetuximab in high-risk pre-malignant upper aerodigestive lesions, but its results are not provided.
- **Cystic neoplasm (L3):** EGFR is expressed in adenoid cystic carcinoma and other salivary gland tumours. Evidence consists of early-phase combination studies and one Phase II salivary gland study.

Other predictions in the top 10 are weaker:

- **Epiglottis neoplasm:** mostly laryngeal squamous cell carcinoma, already an original indication, so not true repurposing.
- **Ductal or ductular proliferation:** the literature is mostly preclinical pancreatic cancer work. KRAS mutation, common in that setting, is a recognised predictor of resistance to anti-EGFR antibodies.
- **Odontogenic cyst:** two case reports only, both about carcinoma arising in such a cyst.

---

## Clinical Trial Evidence

Currently no related clinical trials are registered for the top-ranked prediction, non-seminomatous lesion.

The table below shows the most relevant trials for the better-supported predictions. None of them has results provided here.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00524017](https://clinicaltrials.gov/study/NCT00524017) | Phase 2 | Completed | 35 | Single-agent cetuximab in high-risk pre-malignant upper aerodigestive lesions (pre-malignant neoplasm); the most directly relevant trial |
| [NCT01192087](https://clinicaltrials.gov/study/NCT01192087) | Phase 1/2 | Unknown | 49 | Cetuximab with IMRT and carbon-ion boost in adenoid cystic carcinoma (cystic neoplasm); combined with radiotherapy |
| [NCT00482222](https://clinicaltrials.gov/study/NCT00482222) | Phase 3 | Unknown | 340 | Oxaliplatin/fluoropyrimidine with or without cetuximab in resectable colorectal liver metastases; an established cancer setting, not pre-malignant disease |
| [NCT00397384](https://clinicaltrials.gov/study/NCT00397384) | Phase 1 | Completed | 43 | Erlotinib plus cetuximab dual EGFR blockade in advanced solid tumours; safety study, not specific to cystic neoplasms |
| [NCT00101348](https://clinicaltrials.gov/study/NCT00101348) | Phase 1/2 | Completed | 66 | Erlotinib, cetuximab and bevacizumab in metastatic solid tumours; multi-agent combination |
| [NCT01637194](https://clinicaltrials.gov/study/NCT01637194) | Phase 1 | Completed | 12 | Cetuximab plus everolimus in solid tumours; small safety study |
| [NCT00719199](https://clinicaltrials.gov/study/NCT00719199) | Phase 1 | Terminated | 21 | FOLFIRI, cetuximab and IMO-2055 in colorectal cancer; terminated |
| [NCT00537823](https://clinicaltrials.gov/study/NCT00537823) | Phase 2 | Terminated | 9 | Short-duration neoadjuvant FOLFOX before liver resection for colorectal metastases; terminated, not pre-malignant disease |

---

## Literature Evidence

Currently no related literature is available for the top-ranked prediction, non-seminomatous lesion.

The most relevant publications for the better-supported predictions are:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [18804410](https://pubmed.ncbi.nlm.nih.gov/18804410/) | 2009 | Phase II trial | Oral Oncology | Cetuximab in recurrent or metastatic salivary gland carcinomas (30 patients, mostly adenoid cystic); clinical benefit rate was the primary measure |
| [24412287](https://pubmed.ncbi.nlm.nih.gov/24412287/) | 2014 | Review | Oral Oncology | Targeting EGFR for head and neck cancer chemoprevention; pre-malignant oral lesions show increased EGFR expression |
| [33243986](https://pubmed.ncbi.nlm.nih.gov/33243986/) | 2020 | Review | Nature Reviews Disease Primers | Overview of head and neck squamous cell carcinoma |
| [35300246](https://pubmed.ncbi.nlm.nih.gov/35300246/) | 2022 | Randomized Phase II | Clin Transl Radiat Oncol | Long-term results of cetuximab, gemcitabine and radiotherapy in locally advanced pancreatic cancer (PARC trial) |
| [21552097](https://pubmed.ncbi.nlm.nih.gov/21552097/) | 2012 | Phase II | Am J Clin Oncol | Gemcitabine, oxaliplatin and cetuximab in advanced pancreatic cancer |
| [27797376](https://pubmed.ncbi.nlm.nih.gov/27797376/) | 2017 | Preclinical | Oncogene | Cetuximab resistance in pancreatic ductal adenocarcinoma driven by integrin β1–Src–Akt bypass signalling |
| [30141310](https://pubmed.ncbi.nlm.nih.gov/30141310/) | 2018 | Cohort | Asian Pac J Cancer Prev | Skin disorders and tumour location as prognostic factors in metastatic colorectal cancer treated with cetuximab |
| [32518035](https://pubmed.ncbi.nlm.nih.gov/32518035/) | 2020 | Case report | Oral Oncology | Cetuximab monotherapy for relapsing high-grade mucoepidermoid carcinoma |
| [41546361](https://pubmed.ncbi.nlm.nih.gov/41546361/) | 2025 | Case report | Gan to Kagaku Ryoho | Primary intraosseous carcinoma arising in an odontogenic cyst of the mandible |

---

## New Zealand Market Information

All three licences are cancelled, and no approved indication text is recorded for them.

| Authorization Number | Product Name | Dosage Form |
|---------|------|------|
| 2304457 | Erbitux | Inj 5 mg per ml, 20 ml vial |
| 2304465 | Erbitux | Inj 5 mg per ml, 100 ml vial |
| 2537958 | Baxter | Inj 1 mg for ECP |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (anti-EGFR monoclonal antibody) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Drug Interactions:** 19 drug–drug interaction entries were found.
  - **Major:** aminolevulinic acid and thalidomide.
  - **Moderate:**
    - Proton pump inhibitors: rabeprazole, omeprazole, dexlansoprazole, esomeprazole, lansoprazole and pantoprazole.
    - Platinum agents: carboplatin, cisplatin and oxaliplatin.
    - Fluoropyrimidines: capecitabine and fluorouracil.
    - Others: methoxsalen, topical aminolevulinic acid, idelalisib, topical methyl aminolevulinate, omacetaxine mepesuccinate and verteporfin.
- The case report in the literature (PMID 39415301) notes that cetuximab can cause severe infusion reactions.

Please refer to the package insert for key warnings and contraindications, which are not available in the data compiled for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction, non-seminomatous lesion, rests on a model score alone, with no trials, no literature and no mechanistic link (L5). Cetuximab is also no longer actively licensed in New Zealand, since all three licences are cancelled. The best-supported alternatives, pre-malignant upper aerodigestive lesions (L2) and cystic or salivary gland tumours (L3), are research questions rather than ready candidates. Their key trial results are not available, and most of their evidence comes from combination or early-phase studies.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (a blocking gap for safety screening).
- Detailed mechanism-of-action data.
- Published results of NCT00524017 (pre-malignant upper aerodigestive lesions) and of the salivary gland carcinoma Phase II study.
- A review of whether the non-seminomatous lesion prediction has any biological basis for EGFR involvement.
- Clarification of the current regulatory and supply status of cetuximab in New Zealand.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


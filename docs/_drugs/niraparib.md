---
layout: default
title: Niraparib
parent: Model Prediction Only (L5)
nav_order: 323
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Niraparib: predicted new indication Cystic neoplasm (evidence level L5). Approved use: Peritoneal cancer, primary peritoneal carcinoma…"
monitor_inn: "NIRAPARIB"
predicted_indications: ["epiglottis neoplasm", "cystic neoplasm", "benign neoplasm of hypopharynx", "benign neoplasm of tongue", "benign neoplasm of floor of mouth", "cervical neuroblastoma", "tumor of testis and paratestis", "inner ear neoplasm", "pre-malignant neoplasm", "schwannoma of jugular foramen"]
---

# Niraparib
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

## Niraparib: From Ovarian and Peritoneal Cancers to Cystic Neoplasm
{: .text-alpha }

## One-Sentence Summary

Niraparib is a PARP inhibitor originally used for ovarian and primary peritoneal cancers.
The TxGNN model predicts it may be effective for **cystic neoplasm**, but this is a prediction with limited support: **3 clinical trials** were retrieved (none with results, and only one recruiting Phase 2 study of real relevance), along with **10 publications**, most of them preclinical or observational.
The evidence relates mainly to serous carcinomas (notably endometrial serous carcinoma) rather than cystic neoplasms as such.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Peritoneal cancer, primary peritoneal carcinoma, primary peritoneal serous/papillary carcinoma, desmoplastic small round cell tumour, malignant epithelial tumour of the ovary (licence indication text is not available) |
| Predicted New Indication | Cystic neoplasm |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L2 (weak: the only substantive study is an ongoing Phase 2 trial with no results) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold (research question) |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, niraparib inhibits PARP1/2 enzymes. In tumours with homologous recombination deficiency (HRD), such as those with BRCA1/2 alterations or RAD51C methylation, this causes synthetic lethality and tumour cell death.

The link to "cystic neoplasm" is probably an ontology-mapping artefact arising from serous and ovarian disease terms. Ovarian cancer is already within the original indication scope, so it is not a true repurposing signal. The genuinely new angle in the retrieved evidence is **endometrial/uterine serous carcinoma**, which shares molecular features with high-grade serous ovarian carcinoma (chromosomal instability, copy number changes, similar mutations) and is treated using the same model. Extrapolating to cystic neoplasms in general is not supported by the data.

The scores across the top-ranked predictions are nearly identical (about 0.9998 to 0.9999), which suggests a saturated, poorly discriminating model output. The score alone should not be treated as strong evidence.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04716686](https://clinicaltrials.gov/study/NCT04716686) | Phase 2 | Recruiting | 83 | Niraparib monotherapy as maintenance and recurrent treatment in endometrial serous carcinoma; no results yet |
| [NCT04159155](https://clinicaltrials.gov/study/NCT04159155) | Phase 2/3 | Terminated | 11 | Canadian multi-arm trial of front-line and maintenance treatment in serous or p53-mutant endometrial cancer; too small to interpret |
| [NCT05289648](https://clinicaltrials.gov/study/NCT05289648) | Early Phase 1 | Withdrawn | 0 | Preoperative niraparib in high-grade endometrial cancer; no participants enrolled, so no evidence |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [40473279](https://pubmed.ncbi.nlm.nih.gov/40473279/) | 2025 | Phase 2 protocol | BMJ Open | Protocol for maintenance niraparib in stage III/IV, chemo-naïve recurrent or platinum-sensitive recurrent uterine serous carcinoma |
| [41323499](https://pubmed.ncbi.nlm.nih.gov/41323499/) | 2025 | Cohort | Pathol Oncol Res | Comprehensive genomic profiling for HRD guides PARP inhibitor recommendations in ovarian cancer |
| [42157333](https://pubmed.ncbi.nlm.nih.gov/42157333/) | 2026 | Cohort/Nomogram | Clin Epigenetics | HRR gene promoter methylation nomogram predicts PARP inhibitor efficacy in high-grade serous ovarian cancer |
| [42442041](https://pubmed.ncbi.nlm.nih.gov/42442041/) | 2026 | Real-world cohort | Gynecol Oncol | HRD testing in BRCA1/2 wild-type high-grade serous carcinoma; MYC amplification as a possible surrogate marker |
| [41520277](https://pubmed.ncbi.nlm.nih.gov/41520277/) | 2026 | Preclinical | Cancer Biol Ther | Carboplatin and PARP inhibitor combination tested in high-grade serous carcinoma spheroids and organoids |
| [41465250](https://pubmed.ncbi.nlm.nih.gov/41465250/) | 2025 | Preclinical | Int J Mol Sci | Proteomic analysis of the poly-pharmacological effects of PARP inhibitors in HGSOC cells |
| [34321239](https://pubmed.ncbi.nlm.nih.gov/34321239/) | 2021 | Preclinical | Cancer Res | Loss of RAD51C promoter methylation causes PARP inhibitor resistance in HGSC |
| [41214101](https://pubmed.ncbi.nlm.nih.gov/41214101/) | 2025 | Preclinical | Sci Rep | Claudin-4 regulates genome stability and immune evasion in HGSC |
| [31466953](https://pubmed.ncbi.nlm.nih.gov/31466953/) | 2019 | Case report | BMJ Case Rep | Niraparib maintenance in a patient with ovarian cancer and brain metastases |
| [40702505](https://pubmed.ncbi.nlm.nih.gov/40702505/) | 2025 | Computational | J Ovarian Res | Stem-cell-based subtypes and prognostic model in HGSOC |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2690691 | Zejula | Tab 100 mg | Not available (cancelled) |
| 2690683 | Zejula | Tab 100 mg | Not available (cancelled) |
| 2635232 | Zejula | Cap 100 mg | Not available (cancelled) |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (PARP inhibitor) |
| Myelosuppression Risk | Medium to high (thrombocytopenia, anaemia and neutropenia are known class effects; secondary MDS/AML is a recognised risk with long-term exposure) |
| Emetogenicity Classification | Low to medium |
| Monitoring Items | CBC (regular monitoring), blood pressure, liver and renal function |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Drug Interactions**: The interaction screen returned 100 interacting drugs in total. Examples include:
  - **Major**: deferiprone, samarium (153Sm) lexidronam
  - **Moderate**: acetylsalicylic acid, rolapitant, apixaban, dipyridamole, cilostazol, abciximab, alteplase, bivalirudin, cangrelor, caplacizumab, betrixaban, antithrombin III, and several radiopharmaceuticals (iobenguane I-131, ibritumomab tiuxetan, tositumomab, strontium-89 chloride)
  - Many of these are anticoagulants or antiplatelet agents, which is relevant given the thrombocytopenia risk with niraparib.

Please refer to the package insert for warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The "cystic neoplasm" prediction rests on a model score that is nearly identical across candidates, and the retrieved evidence concerns serous carcinomas rather than cystic neoplasms. The only meaningful signal is a recruiting Phase 2 trial in endometrial serous carcinoma with no results. In addition, all New Zealand licences are cancelled, so there is currently no marketed product.

**To proceed, the following is needed:**
- Results from NCT04716686 and the uterine serous carcinoma Phase 2 study (PMID 40473279)
- Clarification of what "cystic neoplasm" refers to, and whether the prediction reflects an ontology artefact
- Mechanism of action data from DrugBank
- Package insert warnings and contraindications from Medsafe, needed before any safety screening
- A decision on whether to pursue endometrial serous carcinoma as a more clearly defined repurposing target
- Clarification of New Zealand supply or licensing status, since all current licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


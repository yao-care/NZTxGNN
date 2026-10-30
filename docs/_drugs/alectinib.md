---
layout: default
title: Alectinib
parent: Model Prediction Only (L5)
nav_order: 25
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-30
description: "Alectinib: predicted new indication Fibromatosis, gingival (evidence level L5). Approved use: Non-small cell lung cancer / lung cancer"
monitor_inn: "ALECTINIB"
predicted_indications: ["fibromatosis, gingival", "fibroma of lung", "hamartoma of lung", "lung benign neoplasm", "pulmonary sulcus neoplasm", "lung germ cell tumor", "Leukomelanoderma-infantilism-intellectual disability-hypodontia-hypotrichosis syndrome", "inclusion body myopathy with early-onset Paget disease with or without frontotemporal dementia", "junctional epidermolysis bullosa"]
---

# Alectinib
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

## Alectinib: From Non-Small Cell Lung Cancer to Fibromatosis, Gingival
{: .text-alpha }

## One-Sentence Summary

Alectinib is an ALK inhibitor originally used to treat ALK-positive non-small cell lung cancer.
The TxGNN model's top-ranked prediction is **gingival fibromatosis**, but this is a pure model prediction with **0 clinical trials** and **0 publications** supporting it.
The strong evidence in the compiled data (phase 3 RCTs such as ALEX, J-ALEX and ALINA) relates to the drug's original lung cancer indication, not to the predicted one.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Non-small cell lung cancer / lung cancer (approved indication text not available in the New Zealand licence record) |
| Predicted New Indication | Fibromatosis, gingival |
| TxGNN Prediction Score | 0.9997 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Based on known information, alectinib is a selective ALK tyrosine kinase inhibitor. Its efficacy in ALK-positive non-small cell lung cancer is well established, including activity against central nervous system disease.

For gingival fibromatosis, no ALK-related mechanism has been established. The very high TxGNN score (0.9997) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. No trials or publications support this prediction, so it should be treated as a hypothesis only.

The other top-ranked predictions show the same pattern. Most are benign or unrelated conditions (lung fibroma, lung hamartoma, junctional epidermolysis bullosa and others) with no supporting evidence. A few merit a brief note:

- **Lung hilum carcinoma and pulmonary sulcus neoplasm** are presentations of lung cancer, so they are not true repurposing. There is no indication-specific evidence for the pulmonary sulcus tumour.
- **Lung germ cell tumour** has one terminated phase 2 trial (1 participant) and case reports of ALK-rearranged rare tumours responding to ALK inhibitors. This suggests a biomarker-driven (ALK fusion) rationale rather than a histology-driven one. It is the only prediction flagged as a research question.
- **Lung benign neoplasm** is graded L1, but all the retrieved literature concerns malignant ALK-positive NSCLC. This appears to be an ontology mismatch, so the grade does not support the predicted benign indication.

## Clinical Trial Evidence

For the top-ranked prediction (gingival fibromatosis), currently no related clinical trials are registered.

For reference, the only trial found among the top 10 predictions is for lung germ cell tumour:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04644315](https://clinicaltrials.gov/study/NCT04644315) | Phase 2 | Terminated | 1 | Single-arm, home-based study of alectinib in ALK-positive solid tumours other than lung cancer. Enrolled only 1 participant, so no usable efficacy data. |

## Literature Evidence

For the top-ranked prediction (gingival fibromatosis), currently no related literature is available.

The literature below relates to other predictions and mainly supports the original NSCLC indication:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38598794](https://pubmed.ncbi.nlm.nih.gov/38598794/) | 2024 | RCT (ALINA, phase 3) | N Engl J Med | Adjuvant alectinib versus chemotherapy in resected ALK-positive NSCLC |
| [28586279](https://pubmed.ncbi.nlm.nih.gov/28586279/) | 2017 | RCT (ALEX, phase 3) | N Engl J Med | Alectinib versus crizotinib in untreated ALK-positive NSCLC |
| [28501140](https://pubmed.ncbi.nlm.nih.gov/28501140/) | 2017 | RCT (J-ALEX, phase 3) | Lancet | Alectinib versus crizotinib in Japanese ALK-positive NSCLC patients |
| [41110693](https://pubmed.ncbi.nlm.nih.gov/41110693/) | 2026 | RCT | Ann Oncol | Final overall survival analysis of the phase 3 ALEX study |
| [32418886](https://pubmed.ncbi.nlm.nih.gov/32418886/) | 2020 | RCT follow-up | Ann Oncol | Updated overall survival and final progression-free survival data from ALEX |
| [39368244](https://pubmed.ncbi.nlm.nih.gov/39368244/) | 2024 | Systematic review / network meta-analysis | Lung Cancer | Lorlatinib compared with other ALK inhibitors as first-line treatment |
| [30069772](https://pubmed.ncbi.nlm.nih.gov/30069772/) | 2018 | Drug review | Recent Results Cancer Res | Overview of alectinib as a second-generation ALK inhibitor |
| [32054410](https://pubmed.ncbi.nlm.nih.gov/32054410/) | 2020 | Case report | J Oncol Pharm Pract | Alectinib-related hypertriglyceridaemia-induced pancreatitis (safety report, not efficacy) |
| [37031440](https://pubmed.ncbi.nlm.nih.gov/37031440/) | 2023 | Case report | Orv Hetil | Alectinib in mixed large cell neuroendocrine carcinoma of the lung |
| [35200571](https://pubmed.ncbi.nlm.nih.gov/35200571/) | 2022 | Case report | Curr Oncol | Partial response to alectinib in ALK-rearranged combined adenocarcinoma and neuroendocrine carcinoma |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2574152 | Alecensa (valid) | Cap 150 mg | Not available |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (ALK tyrosine kinase inhibitor), not a conventional cytotoxic agent |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions. The literature notes anaemia, including a case of haemolytic anaemia |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert. Published reports also point to blood counts, lipids, liver function and cardiac parameters (ECG) |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Drug Interactions**: 92 interactions were identified, of which the first 20 returned include one Major and the rest Moderate. The Major interaction is with aminolevulinic acid. Moderate interactions include dolasetron, naltrexone, mefloquine, quinine, methoxsalen, bedaquiline, atazanavir, and several beta-blockers (acebutolol, atenolol, betaxolol, bisoprolol), as well as cholinesterase inhibitors (edrophonium, physostigmine, ambenonium) and asparaginase products.
- **Adverse events reported in the literature** (case reports): hypertriglyceridaemia-induced pancreatitis, haemolytic anaemia, DRESS syndrome, erythema multiforme and weight gain.

Package insert warnings and contraindications were not available for this report; please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication, gingival fibromatosis, has no trials, no literature and no known mechanistic link to ALK inhibition, so the high TxGNN score alone is not sufficient. In addition, the only New Zealand licence for alectinib is cancelled, which limits near-term local access.

**To proceed, the following is needed:**
- Detailed mechanism of action data and a biological rationale linking ALK (or related kinase) signalling to gingival fibromatosis
- Package insert warnings and contraindications from Medsafe to allow safety screening
- Clarification of the current New Zealand regulatory status of alectinib, since the only licence is cancelled
- Consideration of whether a biomarker-driven (ALK fusion) approach in rare tumours, such as the lung germ cell tumour signal, is a more promising direction than the top-ranked prediction
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


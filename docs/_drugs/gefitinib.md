---
layout: default
title: Gefitinib
parent: Model Prediction Only (L5)
nav_order: 217
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-30
monitor_inn: "GEFITINIB"
predicted_indications: ["fibromatosis, gingival", "fibroma of lung", "inclusion body myopathy with early-onset Paget disease with or without frontotemporal dementia", "hamartoma of lung", "lung benign neoplasm", "Leukomelanoderma-infantilism-intellectual disability-hypodontia-hypotrichosis syndrome", "pulmonary sulcus neoplasm", "lung germ cell tumor", "junctional epidermolysis bullosa"]
description: "Gefitinib: predicted new indication Fibromatosis, gingival (evidence level L5). Approved use: Not recorded in the local data"
---

# Gefitinib
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

## Gefitinib: Fibromatosis, gingival
{: .text-alpha }

## One-Sentence Summary

Gefitinib is an EGFR tyrosine kinase inhibitor, originally used to treat non-small cell lung cancer (NSCLC).
The TxGNN model's top-ranked prediction is **gingival fibromatosis**, but there are **0 clinical trials** and **0 publications** supporting this direction, so it rests on computational prediction alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Fibromatosis, gingival |
| TxGNN Prediction Score | 0.9989 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, gefitinib is a small-molecule EGFR tyrosine kinase inhibitor, and its efficacy in EGFR-mutant NSCLC is well established in the literature compiled for this report.

A speculative link is EGFR/ErbB signalling in fibroblast proliferation, which could in principle relate to gingival overgrowth. Nothing in the data compiled for this report supports this link, and no trials or publications were retrieved for this disease. The score should be treated as a hypothesis-generating signal only.

Other high-scoring predictions follow a similar pattern. Several are lung neoplasm subtypes (lung hilum carcinoma, pulmonary sulcus neoplasm, lung benign neoplasm), where the high scores probably reflect network proximity to NSCLC rather than disease-specific biology. Others (inclusion body myopathy with Paget disease, junctional epidermolysis bullosa) have no established EGFR-inhibition rationale.

## Clinical Trial Evidence

Currently no related clinical trials registered for gingival fibromatosis.

For reference, the only trial retrieved among the top 10 predictions was for a lower-ranked prediction, lung germ cell tumour. It is only indirectly relevant:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00068497](https://clinicaltrials.gov/study/NCT00068497) | Phase NA (described as phase I) | Completed | 40 | Single-agent gefitinib safety study in advanced head and neck carcinoma or NSCLC in patients aged 75+ (and a cohort aged 50 or younger). No germ cell tumour patients enrolled. |

## Literature Evidence

Currently no related literature available for gingival fibromatosis.

For reference, the literature retrieved for the lower-ranked "lung hilum carcinoma" and "lung benign neoplasm" predictions concerns gefitinib in malignant NSCLC, not these specific conditions:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [22688581](https://pubmed.ncbi.nlm.nih.gov/22688581/) | 2012 | Case report | Gen Thorac Cardiovasc Surg | Salvage surgery after a gefitinib super-response in EGFR-mutant advanced lung adenocarcinoma |
| [15118125](https://pubmed.ncbi.nlm.nih.gov/15118125/) | 2004 | Cohort | Science | EGFR mutations correlate with clinical response to gefitinib in NSCLC |
| [27212579](https://pubmed.ncbi.nlm.nih.gov/27212579/) | 2016 | Review | Expert Opin Drug Saf | Safety of gefitinib in NSCLC; recommended first-line for EGFR-mutant disease |
| [12381224](https://pubmed.ncbi.nlm.nih.gov/12381224/) | 2002 | Review | Drugs | Gefitinib pharmacology and early clinical profile in NSCLC |
| [15549594](https://pubmed.ncbi.nlm.nih.gov/15549594/) | 2004 | Case report | Int J Clin Oncol | Fatal acute gefitinib-induced pneumonitis/interstitial lung disease |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2146541 | Iressa | Tab 250 mg | Status: valid; approved indication text not available |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (EGFR tyrosine kinase inhibitor) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Liver function and respiratory symptoms (see safety section); otherwise please refer to the package insert |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Disease-level cautions (knowledge graph, not label text)**: Contraindication links exist with liver disease, gallbladder disease, interstitial lung disease, pulmonary fibrosis and postinflammatory pulmonary fibrosis. This is consistent with the published case of fatal gefitinib-induced pneumonitis.
- **Other signals in the retrieved literature**: Cutaneous toxicity (acneiform eruption, paronychia, xerosis), QT prolongation and cardiovascular events have been reported. Skin toxicity is a tolerability concern for skin-fragility disorders such as junctional epidermolysis bullosa.
- **Drug Interactions**: 100 interactions were identified, of which the 20 listed are mostly Moderate severity.
  - Acid-suppressing agents: famotidine, ranitidine, cimetidine, nizatidine, omeprazole, esomeprazole, lansoprazole, dexlansoprazole, pantoprazole, rabeprazole.
  - CYP-related agents: clarithromycin, miconazole, clotrimazole, aprepitant, dexamethasone, troglitazone, naltrexone.
  - Level "Unknown": calcitriol, glimepiride.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction (gingival fibromatosis) is supported only by a model score, with no trials, no literature and no established mechanistic link. All ten top predictions are at L4–L5 with a Hold recommendation.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking data gap for safety screening)
- Mechanism of action data for gefitinib (e.g. from DrugBank)
- Preclinical or mechanistic evidence for EGFR/ErbB involvement in gingival fibroblast overgrowth
- Clarification of the cancelled Iressa authorization and any current supply route
- A systematic literature search specific to gingival fibromatosis, as no publications were retrieved
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


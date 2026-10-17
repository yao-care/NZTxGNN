---
layout: default
title: Imatinib
parent: Model Prediction Only (L5)
nav_order: 231
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Imatinib: predicted new indication Heart fibrosarcoma (evidence level L5). Approved use: Systemic mastocytosis, gastrointestinal stromal tumour…"
monitor_inn: "imatinib"
predicted_indications: ["heart fibrosarcoma", "fibroblastic neoplasm", "conventional fibrosarcoma", "kidney fibrosarcoma", "low grade fibromyxoid sarcoma", "liposarcoma", "liver fibrosarcoma", "autosomal recessive familial Mediterranean fever", "ovarian myxoid liposarcoma", "familial rhabdoid tumor"]
---

# Imatinib
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

# Imatinib: From Systemic Mastocytosis and Chronic Myeloid Leukaemia to Heart Fibrosarcoma

## One-Sentence Summary

Imatinib is a tyrosine kinase inhibitor originally used for blood cancers (such as BCR-ABL1-positive chronic myeloid leukaemia) and gastrointestinal stromal tumour.
The TxGNN model predicts it may be effective for **heart fibrosarcoma** (the top-ranked prediction), but there are **0 clinical trials** and only **1 publication** (a general review) behind this specific prediction, so the evidence is essentially model-based.
Among the other top-10 predictions, the best-supported are fibroblastic neoplasm (largely dermatofibrosarcoma protuberans, DFSP) and liposarcoma (three completed early-phase sarcoma trials).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Systemic mastocytosis, gastrointestinal stromal tumour, BCR-ABL1-positive chronic myeloid leukaemia, chronic eosinophilic leukaemia, dermatofibrosarcoma protuberans, hypereosinophilic syndrome (from the knowledge graph; no approved-indication text is available for the New Zealand licences) |
| Predicted New Indication | Heart fibrosarcoma |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, imatinib inhibits the ABL, KIT and PDGFR kinases. Its efficacy in chronic myeloid leukaemia, gastrointestinal stromal tumour and DFSP is established.

DFSP is driven by a COL1A1-PDGFB fusion that constitutively activates PDGFRB, which imatinib blocks. Fibrosarcoma-like tumours, including fibrosarcomatous transformation of DFSP, may therefore respond through the same pathway. For heart fibrosarcoma specifically, no cardiac data exist, and the link is inferred only from the sarcoma class.

The strength of the rationale varies across the top-10 predictions:

- **Fibroblastic neoplasm (rank 2):** Strong mechanistic rationale, because DFSP is already a labelled indication. The evidence is mostly reviews and guidelines, and no trials were retrieved.
- **Conventional fibrosarcoma (rank 3):** Indirect. It lacks the COL1A1-PDGFB driver, and the literature is almost entirely DFSP-focused.
- **Liposarcoma (rank 6):** Limited. It is mainly driven by MDM2/CDK4 amplification, not KIT/PDGFR, although it was included in mixed-sarcoma imatinib trials.
- **Low grade fibromyxoid sarcoma, myxoid liposarcoma, familial rhabdoid tumour, familial Mediterranean fever:** Prediction only, with weak or no mechanistic support.

---

## Clinical Trial Evidence

No clinical trials are registered for the top-ranked prediction (heart fibrosarcoma). The most relevant trials found across the top-10 predictions are below. They are mixed-sarcoma or basket studies, and no disease-specific results are shown.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00031915](https://clinicaltrials.gov/study/NCT00031915) | Phase 2 | Completed | Not reported | Imatinib in metastatic or unresectable soft tissue and bone sarcoma (liposarcoma a possible subgroup; no subgroup results) |
| [NCT00006357](https://clinicaltrials.gov/study/NCT00006357) | Phase 1/2 | Completed | 91 | Dose-finding and efficacy in advanced soft tissue sarcoma (may include liposarcoma; no subgroup data) |
| [NCT00154388](https://clinicaltrials.gov/study/NCT00154388) | Phase 2 | Completed | 185 | Basket study of imatinib in life-threatening rare malignancies; not specific to kidney fibrosarcoma or liposarcoma |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [18623899](https://pubmed.ncbi.nlm.nih.gov/18623899/) | 2008 | Review | Prescrire international | Imatinib's expanding indications, but "not robust evidence" (heart fibrosarcoma prediction) |
| [39904126](https://pubmed.ncbi.nlm.nih.gov/39904126/) | 2025 | Guideline | Eur J Cancer | European interdisciplinary guideline update on DFSP diagnosis and treatment (fibroblastic neoplasm) |
| [42062942](https://pubmed.ncbi.nlm.nih.gov/42062942/) | 2026 | Cohort | BMC Cancer | 153 Chinese DFSP patients: COL1A1::PDGFB fusion and outcomes of Mohs surgery with postoperative imatinib |
| [19451433](https://pubmed.ncbi.nlm.nih.gov/19451433/) | 2009 | Phase 2 trial | J Clin Oncol | Imatinib in 10 sarcoma subtypes using a Bayesian hierarchical model (liposarcoma included) |
| [25852058](https://pubmed.ncbi.nlm.nih.gov/25852058/) | 2015 | Translational | Mol Cancer Ther | CDKN2A/p16 loss implicates CDK4 as a target in imatinib-resistant DFSP; imatinib benefits about 50% of unresectable or metastatic DFSP |
| [37610680](https://pubmed.ncbi.nlm.nih.gov/37610680/) | 2023 | Preclinical | Human Cell | Patient-derived model of imatinib-resistant DFSP with fibrosarcomatous transformation |
| [40558511](https://pubmed.ncbi.nlm.nih.gov/40558511/) | 2025 | Preclinical | Cells | Patient-derived cell lines predict imatinib sensitivity in DFSP; resistance arises in 10-50% of cases |
| [30901500](https://pubmed.ncbi.nlm.nih.gov/30901500/) | 2019 | Preclinical | Drug Dev Res | Targeted liposomes and nanocochleates with imatinib plus dexketoprofen against fibrosarcoma |
| [12702540](https://pubmed.ncbi.nlm.nih.gov/12702540/) | 2003 | Report | Ann Oncol | Potential use of imatinib in c-KIT-expressing ocular melanoma and liposarcoma |
| [30096127](https://pubmed.ncbi.nlm.nih.gov/30096127/) | 2018 | Case report | Anti-Cancer Drugs | Imatinib-induced DRESS in a DFSP patient, managed with desensitization (a safety signal) |

The only publication linked to heart fibrosarcoma is a general review. The remaining entries support related predictions and are mostly DFSP-focused.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2594315 | Imatinib-Rex | Cap 100 mg | Not available (status: cancelled) |
| 2594323 | Imatinib-Rex | Cap 400 mg | Not available (status: cancelled) |

Both licences are single-ingredient products and are cancelled, so there is no currently valid imatinib licence in the data compiled for this report.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (tyrosine kinase inhibitor), not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions. The knowledge graph flags neutropenia, thrombocytopenia, agranulocytosis and anaemia as related conditions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Complete blood count, liver function, renal function, fluid status and cardiac status, based on the knowledge-graph contraindication signals |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report. The following signals come from other sources:

- **Contraindication signals (knowledge graph, not label text):** Liver failure and liver disease, kidney disease, thrombocytopenia, neutropenia, agranulocytosis, anaemia, congestive heart failure and heart failure, coronary artery disease, heart valve disease, pericardial effusion, pulmonary edema, brain edema, hypertension, diabetes mellitus, diabetic ketoacidosis, hyperbilirubinaemia (including Gilbert syndrome), active peptic ulcer disease, hypophosphataemia, obesity and others. These should be verified against the actual label.
- **Drug Interactions:** 675 interactions were recorded. The major-level ones in the displayed subset are **eliglustat** and **naloxegol**. Moderate-level examples include corticosteroids (hydrocortisone, dexamethasone, betamethasone, triamcinolone, budesonide), clarithromycin, cimetidine, aprepitant, morphine, pioglitazone, glimepiride, saxagliptin, lansoprazole and dexlansoprazole, naltrexone, naldemedine and metreleptin.
- **Case-report signal:** Imatinib-induced DRESS (drug reaction with eosinophilia and systemic symptoms) has been reported in a DFSP patient.

Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction, heart fibrosarcoma, has a very high model score but no trials, one general review, and no cardiac-specific data, so it is L5. The best-supported direction is the DFSP/fibroblastic neoplasm group, where imatinib is already an established indication, and mixed-sarcoma trials give limited support for liposarcoma. Both remain research questions rather than actionable repurposing. The New Zealand licences are cancelled, and package insert safety data are missing, a blocking gap for safety screening.

**To proceed, the following is needed:**
- Download and review the Medsafe package insert for warnings and contraindications (blocking gap).
- Mechanism of action data (for example from DrugBank).
- Disease-specific outcome data for heart fibrosarcoma and conventional fibrosarcoma, or a decision to redirect effort to the DFSP/fibroblastic neoplasm group.
- Subgroup results from the mixed-sarcoma trials (NCT00031915, NCT00006357, NCT00154388) for liposarcoma and fibrosarcoma.
- Clarification of why the licences were cancelled and whether any imatinib product is currently supplied in New Zealand.
- A safety monitoring plan covering cardiac, hepatic, haematological and fluid-retention risks, plus a drug-interaction review.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


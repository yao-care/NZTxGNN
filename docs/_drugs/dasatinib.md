---
layout: default
title: Dasatinib
parent: Model Prediction Only (L5)
nav_order: 136
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Dasatinib: predicted new indication Ewing sarcoma (evidence level L5). Approved use: Chronic myelogenous leukaemia…"
monitor_inn: "DASATINIB"
predicted_indications: ["Ewing sarcoma", "myeloid leukemia", "liposarcoma", "fibromatosis, gingival", "dermatofibrosarcoma protuberans", "ovarian myxoid liposarcoma", "ganglioneuroblastoma (disease)", "vertebral anomalies and variable endocrine and T-cell dysfunction", "inclusion body myopathy with early-onset Paget disease with or without frontotemporal dementia", "hamartoma of lung"]
---

# Dasatinib
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

# Dasatinib: From BCR-ABL1-Positive Chronic Myeloid Leukaemia to Ewing Sarcoma

## One-Sentence Summary

Dasatinib is a multi-targeted tyrosine kinase inhibitor originally used for BCR-ABL1-positive chronic myeloid leukaemia (CML), including its blast phase.
The TxGNN model predicts it may be effective for **Ewing sarcoma**, but only **2 clinical trials** (one terminated after 7 patients, one broad sarcoma study) and mostly preclinical literature support this direction.
The evidence is therefore early-stage and exploratory.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Chronic myelogenous leukaemia, BCR-ABL1 positive (including blast phase) |
| Predicted New Indication | Ewing sarcoma |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L2 (see note below) |
| New Zealand Market Status | Cancelled only (no currently valid authorisations) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

*Note on evidence level:* The data compiled for this report assigns L2, but no completed Phase 2/3 trial in this data shows efficacy in Ewing sarcoma. The completed Phase 2 study covers mixed sarcomas, and a review notes dasatinib failed as a single agent in Ewing sarcoma and rhabdomyosarcoma. A conservative reading is closer to L3–L4.

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available in the data compiled for this report. Based on known information, dasatinib inhibits BCR-ABL and SRC-family kinases (and also c-KIT and PDGFR). Its efficacy in BCR-ABL1-positive CML is established, and mechanistically it may be applicable to Ewing sarcoma.

In Ewing sarcoma cell lines, SRC activation drives invadopodia formation, migration and invasion under microenvironmental stress. Laboratory studies show dasatinib has antiproliferative and antimigratory activity in Ewing sarcoma and neuroblastoma cells, and induces apoptosis in bone sarcoma cells that depend on SRC. This gives a plausible biological link to SRC/FAK signalling.

The link is mechanistic only. The original indication (a BCR-ABL-driven leukaemia) and Ewing sarcoma (an EWS-FLI1-driven bone and soft tissue tumour) are biologically different diseases. A review reports that single-agent dasatinib failed in a phase 2 sarcoma study that included Ewing sarcoma, so any benefit would likely require combination approaches.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00788125](https://clinicaltrials.gov/study/NCT00788125) | Phase 1/2 | Terminated | 7 | Dasatinib with ifosfamide, carboplatin and etoposide in paediatric patients. Terminated early, so it offers only minimal safety/feasibility information and no efficacy conclusion. |
| [NCT00464620](https://clinicaltrials.gov/study/NCT00464620) | Phase 2 | Completed | 366 | Dasatinib in advanced sarcomas, assessing response rate and 6-month progression-free survival. It covers multiple subtypes, and Ewing-specific results cannot be confirmed from the available data. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35655525](https://pubmed.ncbi.nlm.nih.gov/35655525/) | 2022 | Review | Sarcoma | Reviews FAK-Src targeting in DSRCT, Ewing sarcoma and rhabdomyosarcoma. Single-agent dasatinib failed in these subtypes in a phase 2 study. |
| [42804591](https://pubmed.ncbi.nlm.nih.gov/42804591/) | 2026 | Case series | J Pediatr Hematol Oncol | Descriptive single-centre experience with the RIST regimen (rapamycin, irinotecan, dasatinib, temozolomide) in relapsed Ewing sarcoma. |
| [26170970](https://pubmed.ncbi.nlm.nih.gov/26170970/) | 2015 | Review | Oncol Lett | Src signalling in sarcoma and its feasibility as a drug target. |
| [18202781](https://pubmed.ncbi.nlm.nih.gov/18202781/) | 2008 | In vitro | Oncol Rep | Dasatinib showed antiproliferative and antimigratory activity in neuroblastoma and Ewing sarcoma cell lines. |
| [17363602](https://pubmed.ncbi.nlm.nih.gov/17363602/) | 2007 | In vitro | Cancer Res | Dasatinib inhibited migration and invasion in sarcoma cell lines and induced apoptosis in SRC-dependent bone sarcoma cells. |
| [27566104](https://pubmed.ncbi.nlm.nih.gov/27566104/) | 2016 | In vitro | Neoplasia | Microenvironmental stress induces Src-dependent invadopodia and migration in Ewing sarcoma. |
| [31521948](https://pubmed.ncbi.nlm.nih.gov/31521948/) | 2019 | In vitro | Neoplasia | Tenascin C and Src cooperate to promote invadopodia formation in Ewing sarcoma. |

## New Zealand Market Information

All three authorisations are cancelled, and no valid authorisations currently exist. The data compiled for this report does not include approved indication text for them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2679620 | Dasatinib-Teva | Tab 50 mg | Cancelled |
| 2679612 | Dasatinib-Teva | Tab 20 mg | Cancelled |
| 2679639 | Dasatinib-Teva | Tab 70 mg | Cancelled |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (multi-kinase inhibitor) |
| Myelosuppression Risk | Medium to High. Knowledge-graph contraindication links include neutropenia, thrombocytopenia, anaemia and agranulocytosis. |
| Emetogenicity Classification | Low |
| Monitoring Items | Complete blood count with differential, liver function, cardiac status (QT interval, heart failure signs), signs of pleural/pericardial effusion and bleeding |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Knowledge-graph contraindication signals** (from drug-disease links, not label text): long QT syndrome (including familial), torsades de pointes, liver disease, gallbladder disease, thrombocytopenia, neutropenia, agranulocytosis, anaemia, hemoglobinopathy, congestive heart failure, heart failure, myocardial infarction, coronary thrombosis, active peptic ulcer disease, intracerebral haemorrhage, pericardial effusion and haemopericardium.
- **Drug Interactions** (100 interactions recorded; examples shown):
  - **Major:** famotidine, ranitidine, cimetidine, omeprazole, rabeprazole (acid-suppressing drugs, which can reduce dasatinib absorption), dexamethasone, acetylsalicylic acid, clarithromycin.
  - **Moderate:** hydrocortisone, budesonide, loperamide, aprepitant, bisacodyl, magnesium oxide, aluminium hydroxide, calcium carbonate, saxagliptin, castor oil, polyethylene glycol.

Please refer to the package insert for label warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic link (SRC/FAK signalling) is plausible and supported by laboratory data, but clinical evidence is thin. The only Ewing-specific trial was terminated with 7 patients, the broader sarcoma phase 2 study is not subtype-specific, and a review reports single-agent failure in Ewing sarcoma. Dasatinib also has no currently valid New Zealand authorisation, and label safety data are missing.

**To proceed, the following is needed:**
- Subtype-specific results for Ewing sarcoma from the advanced sarcoma Phase 2 study (NCT00464620)
- Further data on combination regimens such as RIST, ideally from prospective trials
- Detailed mechanism of action data
- Package insert warnings and contraindications (Medsafe), which are currently missing and block safety screening
- Clarification of the regulatory pathway in New Zealand, since all licences are cancelled
- A cardiac, haematological and fluid-retention monitoring plan for paediatric and young-adult patients

---

**Other predicted indications (for reference):**
- **Myeloid leukaemia** (score 99.68%, L1, Proceed with Guardrails): strong clinical evidence, including the DASISION Phase 3 trial and a 2026 Phase 3 trial in core-binding factor AML. However, this largely overlaps the labelled CML use and is not a true repurposing signal.
- **Liposarcoma, dermatofibrosarcoma protuberans** (L4, Hold): preclinical or indirect evidence only, with no clinical trials.
- **Six other predictions** (L5, Hold): fibromatosis gingival, ovarian myxoid liposarcoma, ganglioneuroblastoma, vertebral anomalies with endocrine and T-cell dysfunction, inclusion body myopathy with Paget disease, and hamartoma of lung. These rest on prediction scores alone, with no supporting trials or literature.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


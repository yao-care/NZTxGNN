---
layout: default
title: Nintedanib
parent: Moderate Evidence (L3-L4)
nav_order: 322
evidence_level: L4
indication_count: 3
last_modified_at: 2026-10-15
description: "Nintedanib: predicted new indication Dermatofibrosarcoma protuberans (evidence level L4). Approved use: Idiopathic pulmonary fibrosis, non-small cell lung cancer"
monitor_inn: "NINTEDANIB"
predicted_indications: ["dermatofibrosarcoma protuberans", "liposarcoma", "ovarian myxoid liposarcoma"]
---

# Nintedanib
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **3** 
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

## Nintedanib: From Idiopathic Pulmonary Fibrosis and Lung Cancer to Dermatofibrosarcoma Protuberans
{: .text-alpha }

## One-Sentence Summary

Nintedanib was originally used to treat idiopathic pulmonary fibrosis and non-small cell lung cancer.
The TxGNN model predicts it may be effective for **dermatofibrosarcoma protuberans (DFSP)**,
but there are currently **no registered clinical trials** and only **1 publication** (a class-level review) supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Idiopathic pulmonary fibrosis, non-small cell lung cancer (from the knowledge graph; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Dermatofibrosarcoma protuberans |
| TxGNN Prediction Score | 99.15% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data for nintedanib is not available in the data compiled for this report. Based on general pharmacological knowledge (not verified against the compiled data), nintedanib is a multi-target tyrosine kinase inhibitor acting on PDGFR alpha/beta, VEGFR 1-3 and FGFR 1-3.

DFSP is driven by the COL1A1-PDGFB fusion, which causes constitutive PDGFR-beta activation. Inhibiting PDGFR is therefore a biologically plausible approach. The one retrieved publication is a review of small-molecule PDGFR inhibitors in neoplastic disorders. It offers indirect, class-level support and contains no nintedanib-specific clinical data.

Imatinib is the established PDGFR-directed therapy for DFSP, so nintedanib would need to show a rationale beyond it, such as activity after imatinib resistance.

Two other predictions, liposarcoma (score 99.13%) and ovarian myxoid liposarcoma (score 99.12%), have no trials or literature behind them. They are computational predictions only, and the second likely reflects proximity to the liposarcoma node in the knowledge graph. Both are at evidence level L5 with a recommendation of Hold.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [29408302](https://pubmed.ncbi.nlm.nih.gov/29408302/) | 2018 | Review | Pharmacological Research | Reviews the role of small-molecule PDGFR inhibitors in neoplastic disorders; indirect class-level support only, no nintedanib-specific data |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2553708 | Ofev | Cap 150 mg | Not available (licence cancelled) |
| 2553694 | Ofev | Cap 100 mg | Not available (licence cancelled) |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (tyrosine kinase inhibitor), based on general pharmacological knowledge |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Drug Interactions**: 100 interactions were identified in the DDI database. The entries listed in the data compiled for this report are mostly Moderate, and one is Major:
  - **Major**: Dexamethasone
  - **Moderate, bleeding-related (antiplatelet/anticoagulant/thrombolytic agents)**: acetylsalicylic acid, clopidogrel, cilostazol, dipyridamole, cangrelor, abciximab, apixaban, betrixaban, dalteparin, bivalirudin, desirudin, antithrombin III, alteplase, caplacizumab, defibrotide
  - **Moderate, other**: clarithromycin, cobicistat, naltrexone, rolapitant

For other safety information (warnings and contraindications), please refer to the package insert. It could not be retrieved for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high, and PDGFR-beta activation in DFSP gives a plausible mechanism. However, there are no clinical trials and no nintedanib-specific publications, and imatinib is already the established targeted therapy. In addition, all New Zealand licences for nintedanib are cancelled.

**To proceed, the following is needed:**
- Nintedanib-specific preclinical or clinical data in DFSP, especially in imatinib-resistant disease
- Verified mechanism of action data from DrugBank
- The Medsafe package insert (warnings and contraindications), which is required before safety screening
- Clarification of the New Zealand supply and regulatory pathway, since there are no valid licences
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


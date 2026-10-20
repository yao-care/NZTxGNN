---
layout: default
title: Crizotinib
parent: Model Prediction Only (L5)
nav_order: 118
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Crizotinib: predicted new indication Fibromatosis, gingival (evidence level L5). Approved use: Non-small cell lung carcinoma"
monitor_inn: "CRIZOTINIB"
predicted_indications: ["fibromatosis, gingival", "fibroma of lung", "hamartoma of lung", "lung benign neoplasm", "pulmonary sulcus neoplasm", "lung germ cell tumor", "inclusion body myopathy with early-onset Paget disease with or without frontotemporal dementia", "junctional epidermolysis bullosa", "Leukomelanoderma-infantilism-intellectual disability-hypodontia-hypotrichosis syndrome"]
---

# Crizotinib
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

# Crizotinib: From Non-Small Cell Lung Cancer to Fibromatosis, Gingival

## One-Sentence Summary

Crizotinib is a targeted kinase inhibitor originally used to treat non-small cell lung cancer (NSCLC) and other lung cancers.
The TxGNN model ranks **gingival fibromatosis** as its top predicted new indication, but **no clinical trials and no publications** support this prediction, so it rests on the computational score alone.
Among the other top-10 predictions, only **lung germ cell tumor** has any trial-level evidence (2 trials, both non-specific basket or tumour-agnostic studies).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Non-small cell lung carcinoma; lung cancer |
| Predicted New Indication | Fibromatosis, gingival |
| TxGNN Prediction Score | 99.81% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Published literature describes crizotinib as an ATP-competitive inhibitor of the receptor tyrosine kinases ALK, ROS1 and c-MET. It is effective in NSCLC driven by ALK or ROS1 rearrangements.

For gingival fibromatosis, **no mechanistic link is supported by the data**. The high TxGNN score (0.998) is a computational prediction only. A speculative link could exist through kinase-driven fibroblastic proliferation, but nothing in the data compiled for this report supports it.

The same applies to most other high-scoring predictions (fibroma of lung, pulmonary sulcus neoplasm, inclusion body myopathy with Paget disease, junctional epidermolysis bullosa): they are prediction-only with no supporting trials or literature. Several are rare genetic disorders with no evident connection to ALK, ROS1 or MET inhibition.

Two other predictions show some indirect signals:
- **Lung hilum carcinoma:** Case reports describe crizotinib in ROS1-positive and ALK-positive lung adenocarcinoma. This largely overlaps with the original NSCLC indication and is not true repurposing.
- **Lung germ cell tumor:** A biomarker-selected, tumour-agnostic approach (for tumours carrying ALK, ROS1 or MET alterations) is the only plausible path. Such alterations are rare in germ cell tumours.

---

## Clinical Trial Evidence

The top-ranked prediction (gingival fibromatosis) has no registered clinical trials. The only trials linked to the top-10 predictions are for **lung germ cell tumor** (rank 7):

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01121588](https://clinicaltrials.gov/study/NCT01121588) | Phase 1 | Terminated | 44 | Safety and activity of crizotinib in ALK-positive tumours other than NSCLC; no result specific to lung germ cell tumours |
| [NCT02465060](https://clinicaltrials.gov/study/NCT02465060) | Phase 2 | Active, not recruiting | 6452 | NCI-MATCH genetically guided basket study with a crizotinib arm; no indication-level result shown |

---

## Literature Evidence

The top-ranked prediction (gingival fibromatosis) has no related literature. The literature below belongs to other top-10 predictions and is indirect evidence only.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28586279](https://pubmed.ncbi.nlm.nih.gov/28586279/) | 2017 | RCT | N Engl J Med | Alectinib vs crizotinib in untreated ALK-positive NSCLC (original indication; lung benign neoplasm entry) |
| [25470694](https://pubmed.ncbi.nlm.nih.gov/25470694/) | 2014 | RCT | N Engl J Med | First-line crizotinib vs chemotherapy in ALK-positive lung cancer |
| [30280657](https://pubmed.ncbi.nlm.nih.gov/30280657/) | 2018 | RCT | N Engl J Med | Brigatinib vs crizotinib in ALK-positive NSCLC |
| [29596029](https://pubmed.ncbi.nlm.nih.gov/29596029/) | 2018 | Phase 2 | J Clin Oncol | Crizotinib in East Asian patients with ROS1-positive advanced NSCLC |
| [39716454](https://pubmed.ncbi.nlm.nih.gov/39716454/) | 2024 | Case report | Am J Case Rep | Pathological complete response to neoadjuvant crizotinib plus chemotherapy in ROS1-positive lung adenocarcinoma |
| [34011083](https://pubmed.ncbi.nlm.nih.gov/34011083/) | 2021 | Case report | Medicine | ALK-negative lung inflammatory myofibroblastic tumour; weak, indirect relevance to hamartoma of lung |
| [33568345](https://pubmed.ncbi.nlm.nih.gov/33568345/) | 2021 | Trial | Clin Cancer Res | Crizotinib activity in ALK-aberrant relapsed/refractory neuroblastoma (COG ADVL0912) |
| [26198834](https://pubmed.ncbi.nlm.nih.gov/26198834/) | 2015 | Review | Future Oncol | Crizotinib-induced cardiotoxicity and the need for proactive monitoring |

These are all in malignant ALK/ROS1-positive NSCLC (the original indication) or other cancers. None supports benign or fibroblastic conditions such as gingival fibromatosis.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2467607 | Xalkori | Cap 250 mg | Not available (cancelled) |
| 2467593 | Xalkori | Cap 200 mg | Not available (cancelled) |

Both licences are cancelled, so crizotinib currently has no valid authorization in New Zealand.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (ALK/ROS1/MET tyrosine kinase inhibitor) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Liver function (case reports of severe hepatotoxicity), ECG/heart rate (QT prolongation, bradycardia), and routine haematology |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Drug Interactions:** 100 interactions were identified. Those listed below are the Major-level interactions among the 20 entries shown in the data compiled for this report:
  - Clarithromycin
  - Cisapride
  - Dolasetron
  - Eliglustat
  - Granisetron

  Many Moderate interactions were also listed (for example famotidine, metformin, loperamide, bupropion, aprepitant, dexamethasone, budesonide and clotrimazole).
- **Safety signals from the literature (case reports and reviews):**
  - Fatal fulminant liver failure
  - Multiple cardiac toxicities (bradycardia, QT prolongation, sick sinus syndrome)
  - Erythema multiforme
  - Erosive esophagitis in a paediatric patient

Please refer to the package insert for warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication, gingival fibromatosis, has a high model score but no trials, no literature and no supported mechanism (evidence level L5). Crizotinib also carries notable hepatic and cardiac risks, and it has no valid New Zealand licence at present.

**To proceed, the following is needed:**
- Mechanism of action data to assess whether ALK/ROS1/MET inhibition is relevant to gingival fibromatosis
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Any preclinical or clinical evidence in gingival fibromatosis or related fibroblastic conditions
- For lung germ cell tumor, which has the most trial-level signal, evidence of actionable ALK, ROS1 or MET alterations in that tumour type, ideally through a biomarker-selected study
- Clarification of New Zealand regulatory status, since both licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


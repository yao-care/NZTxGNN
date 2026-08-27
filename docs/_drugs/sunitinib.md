---
layout: default
title: Sunitinib
parent: 僅模型預測 (L5)
nav_order: 328
evidence_level: L5
indication_count: 10
---

# Sunitinib
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
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

# Sunitinib: From Renal Cell Carcinoma to Liposarcoma

## One-Sentence Summary

Sunitinib is a multitargeted tyrosine kinase inhibitor whose established, mechanistically-founded indication is renal cell carcinoma (confirmed within this evidence pack via its role as a direct comparator/standard-of-care arm across the RCC trial set).
The TxGNN model predicts it may also be effective for **Liposarcoma**,
with **3 clinical trials** and **9 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Renal Cell Carcinoma (inferred from evidence-pack rationale for the RCC prediction, which states this reflects sunitinib's originally approved MOA, not a repurposing hypothesis; formal TFDA label text is a **blocking data gap**, DG001) |
| Predicted New Indication | Liposarcoma |
| TxGNN Prediction Score | 99.87% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Research Question |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (flagged as a High-severity data gap, DG002 — DrugBank MOA lookup pending). Based on the information available in this evidence pack, sunitinib is a multitargeted receptor tyrosine kinase inhibitor (VEGFR1-3, PDGFR-α/β, KIT, RET), and its efficacy in renal cell carcinoma — which is highly dependent on VEGF-driven tumor angiogenesis — is well established; this is corroborated by dozens of RCC trials in the evidence pack where sunitinib serves as the active comparator/standard-of-care arm (see Rank 9: "renal carcinoma").

Liposarcoma, particularly the dedifferentiated and myxoid/round-cell subtypes, frequently shows activated PDGFR-β signaling and angiogenesis dependence — mechanistically within reach of sunitinib's anti-VEGFR/PDGFR activity. Two Phase 2 sarcoma trials in the evidence pack (NCT00400569, n=48; NCT00474994, n=53) directly tested sunitinib in metastatic/unresectable soft-tissue sarcoma populations that included liposarcoma, and case-level literature reports durable clinical benefit in heavily pretreated metastatic liposarcoma. However, response rates vary substantially by histological subtype, and no liposarcoma-specific randomized trial has been completed, which is why the evidence level remains L2 with a "Research Question" recommendation rather than a stronger decision.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00400569](https://clinicaltrials.gov/study/NCT00400569) | Phase 2 | Completed | 48 | Open-label Phase II trial of sunitinib malate in adult patients with metastatic/unresectable soft-tissue sarcoma, including liposarcoma, leiomyosarcoma, fibrosarcoma, and MFH |
| [NCT00474994](https://clinicaltrials.gov/study/NCT00474994) | Phase 2 | Completed | 53 | Multicenter continuous-dosing sunitinib trial in non-GIST sarcomas, covering liposarcoma subtypes |
| [NCT02048371](https://clinicaltrials.gov/study/NCT02048371) | Phase 2 | Completed | 131 | SARC024: primarily evaluated regorafenib (not sunitinib) across sarcoma subtypes; only indirectly relevant as precedent for TKI activity in soft-tissue sarcoma |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [21154746](https://pubmed.ncbi.nlm.nih.gov/21154746/) | 2011 | Phase 2 (non-RCT) | International Journal of Cancer | Phase II study of sunitinib malate in relapsed/refractory soft-tissue sarcoma, focused on leiomyosarcoma, liposarcoma, and MFH |
| [23482782](https://pubmed.ncbi.nlm.nih.gov/23482782/) | 2013 | Case report | Anticancer Research | Long-lasting clinical benefit of sunitinib in a heavily pretreated metastatic liposarcoma case |
| [38254762](https://pubmed.ncbi.nlm.nih.gov/38254762/) | 2024 | Review/Genomic | Cancers | Genetic, epigenetic, and transcriptomic alterations in liposarcoma relevant to target-therapy selection |
| [22987955](https://pubmed.ncbi.nlm.nih.gov/22987955/) | 2012 | Review | Annals of Oncology | Histology- and non-histology-driven therapy for soft-tissue sarcomas, including subtype-specific drug sensitivities |
| [24555529](https://pubmed.ncbi.nlm.nih.gov/24555529/) | 2014 | Review | Expert Review of Anticancer Therapy | Emerging therapies for adult soft-tissue sarcoma |
| [24712007](https://pubmed.ncbi.nlm.nih.gov/24712007/) | 2014 | Review | Magyar Onkologia | Medical treatment of soft-tissue sarcomas based on histological subtype |
| [28423517](https://pubmed.ncbi.nlm.nih.gov/28423517/) | 2017 | Genomic/Translational | Oncotarget | Next-generation sequencing of extraskeletal myxoid chondrosarcoma, evaluating predictors of sunitinib benefit |
| [25884155](https://pubmed.ncbi.nlm.nih.gov/25884155/) | 2015 | Trial protocol | BMC Cancer | REGOSARC trial protocol (regorafenib in advanced soft-tissue sarcoma); background context on TKI activity in sarcoma |
| [38717131](https://pubmed.ncbi.nlm.nih.gov/38717131/) | 2024 | Case series | American Journal of Surgical Pathology | Clinicopathologic analysis of myxoid inflammatory myofibroblastic sarcoma (25 cases); background sarcoma-classification context, not sunitinib-specific |

---

## New Zealand Market Information

Sunitinib currently holds no product authorizations in New Zealand (market status: Not Marketed, 0 licenses on record).

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (multitargeted receptor tyrosine kinase inhibitor) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Research Question**

**Rationale:**
Two Phase 2 trials and case-level literature show mechanistic plausibility and some clinical benefit for sunitinib in liposarcoma, but no liposarcoma-specific randomized trial exists, and response is subtype-dependent — evidence supports a defined research question rather than a Go/Hold decision at this time.

**To proceed, the following is needed:**
- TFDA/regulatory package insert data (blocking gap, DG001) to complete the S1 safety pre-screen
- Formal DrugBank mechanism-of-action data (DG002) to strengthen the mechanistic-link analysis
- A liposarcoma-subtype-stratified prospective or retrospective study to clarify which histological subtypes (e.g., dedifferentiated, myxoid/round-cell) respond
- Drug interaction and myelosuppression/emetogenicity data specific to New Zealand labeling, since the product is not currently marketed there
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


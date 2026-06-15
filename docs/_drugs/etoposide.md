---
layout: default
title: Etoposide
parent: 僅模型預測 (L5)
nav_order: 141
evidence_level: L5
indication_count: 10
---

# Etoposide
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

# Etoposide: From Germ Cell Tumors & Lymphoma to Well-Differentiated Fetal Adenocarcinoma of the Lung

## One-Sentence Summary

Etoposide (VP-16) is a topoisomerase II inhibitor classically used in the treatment of germ cell tumors, lymphomas, and small cell lung cancer, though it is not currently registered in New Zealand.
The TxGNN model predicts it may be effective for **Well-Differentiated Fetal Adenocarcinoma of the Lung (WDFA)**,
with **no registered clinical trials** and **1 case report** currently supporting this direction within the pulmonary blastoma spectrum.
Evidence at this stage is preliminary; this indication requires significant preclinical and clinical development before translation.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Germ cell tumors, lymphoma, small cell lung cancer (inferred from literature; no New Zealand regulatory registration on file) |
| Predicted New Indication | Well-Differentiated Fetal Adenocarcinoma of the Lung (WDFA) |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L4 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action (MOA) data is not available in this Evidence Pack. Based on established pharmacology, Etoposide is a semisynthetic epipodophyllotoxin that inhibits topoisomerase IIα (Topo IIα), trapping the enzyme–DNA cleavage complex and causing irreversible double-strand breaks during the S and G2 phases of the cell cycle. This mechanism underlies its efficacy across rapidly proliferating tumors including germ cell tumors, aggressive lymphomas, and small cell lung cancer (PMID 1984834).

Well-Differentiated Fetal Adenocarcinoma (WDFA) belongs to the pulmonary blastoma spectrum — a heterogeneous group of rare lung tumors characterised by activated WNT/β-catenin signalling and primitive epithelial components that histologically resemble 6–10 week fetal lung. Given the high proliferative index of these tumors, Topo IIα expression is biologically plausible, which would in theory confer sensitivity to Etoposide. The mechanistic link, however, remains entirely speculative: no direct preclinical validation in WDFA-specific cell lines or patient-derived xenograft (PDX) models has been reported.

WDFA is an ultra-rare tumor with fewer than 50 new cases diagnosed globally per year, making dedicated clinical trial design practically infeasible. The TxGNN score likely reflects shared genomic and pathway-level features between WDFA and other Etoposide-sensitive solid tumors within the pulmonary blastoma spectrum rather than WDFA-specific empirical evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [33107372](https://pubmed.ncbi.nlm.nih.gov/33107372/) | 2020 | Case Report + Review | The Journal of International Medical Research | Classic biphasic pulmonary blastoma (spectrum includes WDFA) after right upper lobe resection; adjuvant chemotherapy with nedaplatin + paclitaxel administered. After disease recurrence, further chemotherapy was attempted. Authors note complete absence of standard treatment guidelines for pulmonary blastoma due to extreme rarity. No specific Etoposide response data for the WDFA subtype. |

---

## Cytotoxicity

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic — Epipodophyllotoxin class (Topoisomerase II inhibitor) |
| Myelosuppression Risk | High — leukopenia and thrombocytopenia are the primary dose-limiting toxicities; febrile neutropenia is common and typically requires G-CSF support in combination regimens |
| Emetogenicity Classification | Low to moderate |
| Monitoring Items | CBC with differential (before each cycle and at nadir), liver function tests, renal function (creatinine/eGFR), blood pressure during IV infusion (hypotension risk) |
| Handling Protection | Must follow cytotoxic drug handling regulations — preparation in a biological safety cabinet (BSC), appropriate PPE (gloves, gown, eye protection) required throughout preparation and administration |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
WDFA is an ultra-rare tumor with no dedicated clinical trial evidence for Etoposide, only indirect case-level data through the broader pulmonary blastoma category, and a mechanistic link that is biologically plausible but preclinically unvalidated. Proceeding without further foundational evidence would not represent responsible drug development.

**To proceed, the following is needed:**
- **Preclinical validation:** Topo IIα expression profiling in WDFA tumour specimens; in vitro or PDX model studies documenting Etoposide sensitivity in this specific histotype
- **Systematic literature review:** Comprehensive mapping of all published WDFA and pulmonary blastoma case reports to identify any chemotherapy response patterns involving epipodophyllotoxins
- **MOA data:** Complete mechanism of action characterisation from DrugBank (currently unavailable in this Evidence Pack)
- **Safety data:** Medsafe package insert or TFDA prescribing information to characterise contraindications and drug interaction profile
- **Regulatory feasibility assessment:** Given zero New Zealand authorisations, a full regulatory pathway analysis would be required before any clinical development

> **Note on higher-priority candidates:** Multiple other TxGNN predicted indications within this same Evidence Pack carry substantially stronger evidence than WDFA. In particular, **Ewing Sarcoma** (Rank 4, L1 evidence — multiple completed Phase 3 RCTs including EE2012, AEWS0031, and COG AEWS1221 establishing VDC/IE as the global standard of care) and **Rhabdomyosarcoma** (Rank 6, L1 evidence — IRSG-IV RCT directly demonstrating IE superiority over VM in metastatic RMS, PMID 11846301) represent far more actionable repurposing candidates and should be prioritised in any subsequent evaluation workflow.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


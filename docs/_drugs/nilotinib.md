---
layout: default
title: Nilotinib
parent: 僅模型預測 (L5)
nav_order: 242
evidence_level: L5
indication_count: 1
---

# Nilotinib
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Nilotinib: From Chronic Myeloid Leukemia to Dermatofibrosarcoma Protuberans

## One-Sentence Summary

Nilotinib is a second-generation BCR-ABL/PDGFR/KIT tyrosine kinase inhibitor, publicly known for its approved use in Philadelphia-chromosome-positive chronic myeloid leukemia (CML); however, the Evidence Pack itself does not yet contain a confirmed original indication or formal DrugBank MOA record (flagged as Data Gap DG002). The TxGNN model predicts it may be effective for **Dermatofibrosarcoma Protuberans**, a PDGFB-driven soft-tissue tumor, but this direction is currently supported only by **1 mechanistic review article** and **no registered clinical trials**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not confirmed in Evidence Pack (DrugBank/TFDA record pending — see DG002); publicly documented use is chronic myeloid leukemia |
| Predicted New Indication | Dermatofibrosarcoma Protuberans |
| TxGNN Prediction Score | 99.31% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism-of-action data for this specific record is not available in the Evidence Pack (DG002, High severity). Based on publicly known pharmacology, nilotinib belongs to the class of small-molecule tyrosine kinase inhibitors that block BCR-ABL, PDGFR (α/β), and KIT signaling, and its efficacy in Philadelphia-chromosome-positive leukemias is well established.

Dermatofibrosarcoma protuberans (DFSP) is a rare, locally aggressive soft-tissue sarcoma driven in the vast majority of cases by a COL1A1-PDGFB gene fusion, which produces constitutively activated PDGFB signaling through the PDGFR receptor. This is the same receptor family targeted by nilotinib. Imatinib, a related first-generation PDGFR/BCR-ABL/KIT inhibitor, is already an approved standard-of-care therapy for unresectable or metastatic DFSP, which lends strong biological plausibility to the idea that other PDGFR-inhibiting TKIs in the same pharmacological class — including nilotinib — could show activity against the same tumor driver.

The single literature record retrieved for this prediction (Roskoski, 2018) is a pharmacology review of small-molecule PDGFR inhibitors across neoplastic disorders, which supports the mechanistic rationale but does not constitute direct clinical evidence for nilotinib in DFSP specifically. The prediction should therefore be regarded as mechanistically plausible but clinically unproven at this stage.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [29408302](https://pubmed.ncbi.nlm.nih.gov/29408302/) | 2018 | Review | Pharmacological Research | Reviews small-molecule PDGFR inhibitors across neoplastic disorders; establishes the mechanistic basis for targeting PDGFR-driven tumors (e.g., DFSP) with this drug class, which includes nilotinib. |

---

## New Zealand Market Information

Nilotinib is currently not marketed in New Zealand, and no product authorizations are on file (0 licenses recorded).

---

## Cytotoxicity

Nilotinib is an antineoplastic tyrosine kinase inhibitor (BCR-ABL/PDGFR/KIT class), so this section is included.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (BCR-ABL/PDGFR/KIT tyrosine kinase inhibitor) |
| Myelosuppression Risk | Low to Moderate — class-associated thrombocytopenia, neutropenia, and anemia are known with BCR-ABL TKIs; drug-specific hematologic toxicity data not yet available in this Evidence Pack |
| Emetogenicity Classification | Low |
| Monitoring Items | CBC with differential, liver function tests, electrolytes, and ECG/QTc (BCR-ABL TKIs of this class carry a known QT-prolongation signal) |
| Handling Protection | Oral antineoplastic agent — handle per institutional hazardous-drug precautions pending confirmation from the official package insert |

*Note: The above reflects general knowledge of the drug's pharmacological class. Formal TFDA/DrugBank-sourced toxicity data has not yet been retrieved (see DG001, DG002) and should be confirmed before clinical use.*

---

## Safety Considerations

Please refer to the package insert for safety information. Note that TFDA package insert warnings/contraindications are currently marked as a **Blocking** data gap (DG001) in this Evidence Pack — this must be resolved before the candidate can proceed to a formal S1 safety review.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is mechanistically plausible (shared PDGFR-pathway rationale with the approved analog imatinib) but is currently supported only by a general pharmacology review, with zero registered clinical trials and no New Zealand marketing status. Critically, the Blocking-severity safety data gap (DG001) means a formal S1 safety review cannot yet be completed.

**To proceed, the following is needed:**
- Resolve DG001: obtain and parse the TFDA/manufacturer package insert for warnings, contraindications, and DDI data
- Resolve DG002: confirm original indication and formal MOA via DrugBank API query
- Targeted literature/trial search specifically for "nilotinib" + "dermatofibrosarcoma protuberans" or "PDGFB fusion sarcoma" (current search may have been too narrow, given zero hits despite biological plausibility)
- Case-report or compassionate-use evidence review, given DFSP is a rare disease where RCTs are unlikely
- Confirm route of administration and dosing feasibility relative to the approved CML regimen if repurposing is pursued
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


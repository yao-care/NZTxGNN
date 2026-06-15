---
layout: default
title: Axitinib
parent: 僅模型預測 (L5)
nav_order: 40
evidence_level: L5
indication_count: 10
---

# Axitinib
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

# Axitinib: From Advanced Renal Cell Carcinoma to Unclassified Renal Cell Carcinoma

## One-Sentence Summary

Axitinib (INLYTA®) is an oral targeted therapy globally approved for advanced renal cell carcinoma, acting as a potent selective inhibitor of vascular endothelial growth factor receptors (VEGFR-1, -2, and -3).
The TxGNN model predicts it may be effective for **Unclassified Renal Cell Carcinoma** — a diagnostically challenging subtype defined by exclusion, currently lacking dedicated treatment standards — with **2 real-world observational studies** providing indirect supporting evidence.
This is a mechanistically plausible but evidence-limited research direction; no dedicated controlled trials exist for this specific subtype.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Advanced Renal Cell Carcinoma (globally approved via AXIS trial; not registered in New Zealand) |
| Predicted New Indication | Unclassified Renal Cell Carcinoma |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Research Question |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the local regulatory database. Based on published clinical evidence, axitinib is a second-generation oral tyrosine kinase inhibitor (TKI) that selectively and potently inhibits VEGFR-1, -2, and -3 — with approximately 10-fold greater receptor affinity than earlier-generation agents such as sunitinib and sorafenib. By blocking these receptors, axitinib disrupts tumor angiogenesis: the formation of new blood vessels that solid tumors depend on for growth and metastasis. Its efficacy in advanced renal cell carcinoma has been robustly established through multiple Phase 3 trials (AXIS, KEYNOTE-426, JAVELIN Renal 101), making it a cornerstone of RCC treatment globally.

Unclassified renal cell carcinoma (uRCC) is a diagnosis of exclusion — tumors that cannot be assigned to any recognized histological subtype after thorough pathological evaluation. Despite this diagnostic ambiguity, many unclassified tumors retain biological features of VEGF-pathway dependence, including neovascularization driven by VEGF signaling. This shared molecular vulnerability provides a mechanistic rationale for VEGFR inhibition, analogous to how axitinib works in clear cell RCC. The TxGNN prediction capitalizes on this biological overlap.

However, unclassified RCC frequently harbors sarcomatoid or rhabdoid differentiation, which is associated with relative resistance to VEGFR-TKI monotherapy. For this reason, combination strategies pairing axitinib with immune checkpoint inhibitors (ICIs) may offer better outcomes in this subtype — a direction consistent with the broader shift in RCC treatment toward ICI+TKI combinations.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02156895](https://clinicaltrials.gov/study/NCT02156895) | N/A | Completed | 111 | Post-marketing surveillance of INLYTA® in real clinical practice; observational design, no control arm — monitors safety and usage patterns but cannot differentiate efficacy by RCC subtype |
| [NCT04033991](https://clinicaltrials.gov/study/NCT04033991) | N/A | Completed | 684 | Real-world retrospective database study (UK specialist oncology centre) evaluating PFS of sunitinib (first-line) followed by axitinib (second-line) in metastatic/advanced RCC, stratified by MSKCC/IMDC risk — provides indirect safety data relevant to unclassified RCC patients treated within the mRCC population |

> **Note:** No dedicated clinical trial specifically enrolling patients with unclassified RCC and testing axitinib has been registered. Both trials above include unclassified RCC patients only incidentally within broader mRCC cohorts.

---

## Literature Evidence

Currently no related literature specifically addressing axitinib in unclassified renal cell carcinoma is available.

---

## New Zealand Market Information

Axitinib is not currently approved or marketed in New Zealand. No product authorizations are on record with Medsafe.

---

## Cytotoxicity

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Targeted therapy — VEGFR-selective TKI (second-generation oral small-molecule inhibitor; not conventional cytotoxic) |
| Myelosuppression Risk | Low to moderate — less myelosuppression than conventional cytotoxics; anemia is the most commonly reported haematological adverse event; severe neutropenia or thrombocytopenia are uncommon |
| Emetogenicity Classification | Low — oral targeted therapy; nausea and vomiting are reported but generally mild and manageable without routine prophylactic antiemetics |
| Monitoring Items | Blood pressure (hypertension is an on-target class effect; occurs in ~40% of patients), thyroid function (hypothyroidism), liver enzymes (ALT/AST), CBC, renal function (creatinine, eGFR), urinary protein |
| Handling Protection | Standard precautions for oral cytotoxic agents apply; personnel preparing or administering should follow institutional cytotoxic handling protocols |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Research Question**

**Rationale:**
While VEGFR inhibition by axitinib is mechanistically plausible in unclassified RCC — given shared VEGF-pathway biology with other RCC subtypes — the existing evidence consists solely of indirect observational data from general mRCC populations, and no Phase 2 or 3 trial has specifically enrolled unclassified RCC patients as the primary target population.

**To proceed, the following is needed:**

- Dedicated prospective clinical trial data for axitinib in unclassified RCC (Phase 2, ideally with biomarker stratification by molecular subtype)
- Molecular profiling of unclassified tumors to identify VEGF-pathway-dependent subsets most likely to respond (e.g., VHL-mutant, VEGF-high expression)
- Evaluation of axitinib + ICI combination strategy, given that sarcomatoid/unclassified RCC shows relative TKI monotherapy resistance but enhanced ICI sensitivity
- Prospective safety monitoring plan addressing hypertension management, hepatotoxicity, and thyroid dysfunction in this population
- Regulatory pathway analysis for New Zealand/Taiwan if clinical development reaches Phase 3 stage
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


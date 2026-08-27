---
layout: default
title: Fulvestrant
parent: 僅模型預測 (L5)
nav_order: 161
evidence_level: L5
indication_count: 10
---

# Fulvestrant
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

# Fulvestrant: From Hormone Receptor-Positive Breast Cancer to HIV Infectious Disease

## One-Sentence Summary

Fulvestrant is a selective estrogen receptor degrader (SERD) whose established use, as reflected throughout this evidence pack's trial and literature context, is hormone receptor-positive (HR+), HER2-negative advanced/metastatic breast cancer. The TxGNN model's top-ranked prediction for this drug is **HIV infectious disease**, but this direction is currently supported by **0 clinical trials** and only **1 loosely related publication** (which does not concern fulvestrant, HIV, or estrogen-receptor pathways directly). Evidence strength is minimal, and the model's own rationale flags this prediction as likely reflecting a spurious graph-level association rather than a clinically meaningful signal.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hormone receptor-positive (HR+), HER2-negative advanced/metastatic breast cancer (derived from evidence-pack context; no structured regulatory indication text was provided) |
| Predicted New Indication | HIV infectious disease |
| TxGNN Prediction Score | 99.91% |
| Evidence Level | L5 (model prediction only, no supporting studies) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this evaluation (flagged as a High-severity data gap requiring a DrugBank lookup). Based on the evidence context surrounding this drug's other predicted indications, fulvestrant is a SERD that binds and degrades the estrogen receptor, and its efficacy in HR+/HER2- breast cancer is well established through decades of clinical use and dozens of registered trials.

There is no known pathophysiological relationship between estrogen-receptor-driven breast cancer and HIV infection. The single associated publication (PMID 40343334) is a multi-cohort omics analysis of HTLV-1-associated myelopathy — a distinct retrovirus-driven neuroinflammatory disorder — and does not mention fulvestrant, estrogen receptor biology, or HIV.

The evidence pack's own mechanistic annotation for this prediction states that the high TxGNN score likely reflects an indirect knowledge-graph connection between estrogen-receptor-related genes and viral-infection nodes, rather than a biologically or clinically meaningful link. This prediction should be treated as a hypothesis-generation artifact rather than a repurposing signal until independent pharmacological or virological evidence emerges.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [40343334](https://pubmed.ncbi.nlm.nih.gov/40343334/) | 2025 | Cohort/Omics mechanistic study (preprint) | Research Square | Multi-cohort cross-omics analysis of HTLV-1-associated myelopathy (HAM), a neglected retroviral neuroinflammatory disorder; identifies disease mechanisms and candidate therapeutic targets. Does not evaluate fulvestrant, HIV, or estrogen-receptor pathways directly. |

---

## New Zealand Market Information

No marketing authorizations are currently registered for fulvestrant in New Zealand (0 licenses on file); the drug's status is "not marketed" in this jurisdiction.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted/hormonal therapy — selective estrogen receptor degrader (SERD), not conventional cytotoxic chemotherapy |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

Please refer to the package insert for safety information. (TFDA package insert warnings/contraindications and DDI data are currently blocked by a data gap — DG001 — pending retrieval and parsing of the official package insert.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted association between fulvestrant and HIV infectious disease has no supporting clinical trials and only one tangentially related, non-specific publication; the evidence pack itself characterizes the underlying knowledge-graph signal as likely non-clinical. Combined with the drug's unmarketed status in New Zealand and blocked safety data, this candidate does not meet the threshold to advance past initial screening.

**To proceed, the following is needed:**
- TFDA/package insert warnings, contraindications, and DDI data (DG001, blocking)
- DrugBank-sourced mechanism of action detail (DG002)
- Any direct pharmacological, virological, or preclinical evidence linking estrogen-receptor degradation to HIV pathogenesis or treatment
- Re-validation of the disease-label mapping used by TxGNN, given that a lower-ranked prediction for this same drug (multiple endocrine neoplasia) was found to reflect a clear disease-ontology mismatch — the same risk should be ruled out here before further investment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


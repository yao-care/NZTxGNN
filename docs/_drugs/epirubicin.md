---
layout: default
title: Epirubicin
parent: Model Prediction Only (L5)
nav_order: 181
evidence_level: L5
indication_count: 7
last_modified_at: 2026-11-19
monitor_inn: "Epirubicin"
predicted_indications: ["primary pulmonary lymphoma", "well-differentiated fetal adenocarcinoma of the lung", "small cell lung carcinoma", "pulmonary blastoma", "lung mixed small cell and squamous cell carcinoma", "myeloid leukemia", "upper aerodigestive tract neoplasm"]
description: "Epirubicin: predicted new indication Primary pulmonary lymphoma (evidence level L5). In New Zealand: 3 Pharmac Schedule items"
---

# Epirubicin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

## Epirubicin: Primary pulmonary lymphoma
{: .text-alpha }

## One-Sentence Summary

Epirubicin is an anthracycline cytotoxic drug that damages DNA by intercalation and topoisomerase II inhibition.
The TxGNN model predicts it may be relevant to **primary pulmonary lymphoma**, but no clinical trial and no study testing epirubicin in this disease was found, so the evidence level is **L5** (model prediction only).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Primary pulmonary lymphoma |
| TxGNN Prediction Score | 0.9971 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 3 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for epirubicin: "Epirubicin is an anthracycline cytotoxic agent. Although it is known that anthracyclines can interfere with a number of biochemical and biological functions within eukaryotic cells, the precise mechanisms of epirubicin's cytotoxic and/or antiproliferative properties have not been completely elucidated." ([DailyMed: Ellence (epirubicin hydrochloride) prescribing information, Pharmacia & Upjohn Company LLC](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=0a03c798-a652-4895-b29c-3b521a89ba42), label version 2026-07-24; accessed 2026-11-19.) Based on general pharmacology, epirubicin is an anthracycline (a 4' epimer of doxorubicin) that interferes with DNA synthesis and function and inhibits topoisomerase II.

Anthracyclines are a core component of many lymphoma regimens (CHOP-like regimens), so a topoisomerase II-targeting drug is mechanistically plausible against lymphoma arising in the lung. However, this reasoning is indirect. Doxorubicin is the established anthracycline in lymphoma, which weakens the case for epirubicin specifically, and no study found tests epirubicin in primary pulmonary lymphoma.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

The only item directly mentioning epirubicin is a general review. The remaining items concern lymphoma without testing epirubicin. All were retrieved as related literature and none tests epirubicin in primary pulmonary lymphoma.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7686469](https://pubmed.ncbi.nlm.nih.gov/7686469/) | 1993 | Review | Drugs | General review of epirubicin pharmacology and use in cancer chemotherapy; equivalent response and survival to doxorubicin regimens in several cancers |
| [1866500](https://pubmed.ncbi.nlm.nih.gov/1866500/) | 1991 | Case report | Rev Invest Clin | Single case of primary lung non-Hodgkin lymphoma; no epirubicin-specific finding |
| [36237246](https://pubmed.ncbi.nlm.nih.gov/36237246/) | 2022 | Case report and review | Transl Cancer Res | Pleural MALT lymphoma that was misdiagnosed; not an epirubicin study |
| [39192408](https://pubmed.ncbi.nlm.nih.gov/39192408/) | 2024 | Retrospective cohort | Zhongguo Shi Yan Xue Ye Xue Za Zhi | Primary extranodal DLBCL in the rituximab era; not epirubicin-specific |
| [10526668](https://pubmed.ncbi.nlm.nih.gov/10526668/) | 1999 | Clinical study | Cancer J Sci Am | VEBEP regimen plus radiotherapy in advanced Hodgkin disease |
| [40728626](https://pubmed.ncbi.nlm.nih.gov/40728626/) | 2025 | Retrospective analysis | Ann Hematol | Pola-R-CHP in DLBCL subgroups; not epirubicin-specific |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2204177 | Baxter | Epirubicin hydrochloride | Inj 1 mg for ECP | Yes |
| 2224143 | Epirubicin Ebewe | Epirubicin hydrochloride | Inj 2 mg per ml, 100 ml vial | Yes |
| 2224127 | Epirubicin Ebewe | Epirubicin hydrochloride | Inj 2 mg per ml, 25 ml vial | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (anthracycline class) |
| Myelosuppression Risk | High (neutropenia and thrombocytopenia are expected with anthracyclines) |
| Emetogenicity Classification | Moderate to high |
| Monitoring Items | CBC with differential, liver and renal function, cardiac function (cumulative cardiotoxicity) |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

- **Drug Interactions**: 100 interactions were found in the interaction screening. Examples include major-level interactions with cimetidine and dolasetron, and moderate-level interactions with, for example, clarithromycin, levofloxacin, bupropion and famotidine.

Please refer to the package insert for further safety information, including warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high (0.9971), but no study tests epirubicin in primary pulmonary lymphoma, so the evidence level is L5. Doxorubicin is the established anthracycline in lymphoma, and epirubicin carries cumulative cardiotoxicity and myelosuppression risks.

**To proceed, the following is needed:**
- Clinical or preclinical data testing epirubicin specifically in primary pulmonary lymphoma
- Comparison against standard anthracycline-containing lymphoma regimens
- Package insert warnings and contraindications
- Mechanism of action data
- A cardiac and haematological safety monitoring plan

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Ellence (epirubicin hydrochloride) prescribing information, Pharmacia & Upjohn Company LLC](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=0a03c798-a652-4895-b29c-3b521a89ba42) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Trastuzumab
parent: Model Prediction Only (L5)
nav_order: 462
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-30
monitor_inn: "TRASTUZUMAB"
predicted_indications: ["malignant cutaneous granular cell skin tumor", "ectomesenchymoma", "human herpesvirus 8-related tumor", "middle ear neuroendocrine tumor", "kidney pelvis sarcomatoid transitional cell carcinoma", "prostatic urethra urothelial carcinoma"]
description: "Trastuzumab: predicted new indication Malignant cutaneous granular cell skin tumor (evidence level L5). Approved use: Not recorded in the local data"
---

# Trastuzumab
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Trastuzumab: Malignant cutaneous granular cell skin tumor
{: .text-alpha }

## One-Sentence Summary

Trastuzumab is a HER2-targeted antibody, originally used to treat HER2-positive breast carcinoma.
The TxGNN model predicts it may be effective for several breast cancer subtypes. These predictions are mostly HER2-defined sub-populations of the existing indication rather than true new uses.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Malignant cutaneous granular cell skin tumor |
| TxGNN Prediction Score | 0.9948 |
| Evidence Level | L5 for progesterone-receptor positive breast cancer (L4 for the top-ranked normal breast-like subtype) |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 3 (valid: 3) |
| Recommended Decision | Proceed with Guardrails (for the progesterone-receptor positive term; Hold for the top-ranked normal breast-like subtype) |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. From established knowledge, trastuzumab binds the HER2 receptor, which blocks HER2 signalling and engages immune-mediated killing of tumour cells (antibody-dependent cellular cytotoxicity).

The predicted terms are hormone-receptor or molecular subtypes of breast cancer. A substantial share of progesterone-receptor positive tumours also overexpress HER2 (HR+/HER2+), and so do some progesterone-receptor negative tumours (HR-/HER2+). In these groups, trastuzumab's benefit comes from HER2 dependency, which hormone-receptor status does not remove. Combining it with endocrine therapy and CDK4/6 inhibitors is an established strategy. This makes these predictions biomarker-defined subsets of the approved HER2-positive indication.

The same reasoning limits the predictions. Benefit is not expected in HER2-negative disease, including triple-negative cancer and luminal A tumours. The top-ranked "normal breast-like" subtype is usually HER2-negative, so its link to trastuzumab is only indirect. The lower-ranked rare tumours (ranks 5-10) have a high model score but no trials or literature and no documented HER2 mechanism, so they are not recommended for further work at this stage.

---

## Clinical Trial Evidence

Trials for **progesterone-receptor positive breast cancer** (the best-supported term), up to 10 most relevant:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01275677](https://clinicaltrials.gov/study/NCT01275677) | Phase 3 | Completed | 3270 | Adjuvant chemotherapy with or without trastuzumab in node-positive or high-risk node-negative breast cancer |
| [NCT04629846](https://clinicaltrials.gov/study/NCT04629846) | Phase 3 | Completed | 517 | QL1209 (pertuzumab biosimilar) vs pertuzumab, each with trastuzumab and docetaxel, in HER2-positive, ER/PR-negative breast cancer |
| [NCT01750073](https://clinicaltrials.gov/study/NCT01750073) | Phase 2 | Completed | 92 | Neoadjuvant paclitaxel and cyclophosphamide with or without trastuzumab |
| [NCT00194779](https://clinicaltrials.gov/study/NCT00194779) | Phase 2 | Completed | 50 | Neoadjuvant chemotherapy regimen in HER2-positive breast cancer |
| [NCT02260531](https://clinicaltrials.gov/study/NCT02260531) | Phase 2 | Completed | 36 | Cabozantinib alone or with trastuzumab in breast cancer brain metastases |
| [NCT00134680](https://clinicaltrials.gov/study/NCT00134680) | Phase 2 | Completed | 33 | Letrozole plus trastuzumab in ErbB2-positive, ER and/or PR-positive metastatic breast cancer |
| [NCT04886531](https://clinicaltrials.gov/study/NCT04886531) | Phase 2 | Recruiting | 30 | Pre-operative neratinib, endocrine therapy and trastuzumab in ER-positive, HER2-positive cancers |
| [NCT04334330](https://clinicaltrials.gov/study/NCT04334330) | Phase 2 | Unknown | 34 | Palbociclib, trastuzumab, pyrotinib and fulvestrant for brain metastases in ER/PR-positive, HER2-positive disease |
| [NCT02152943](https://clinicaltrials.gov/study/NCT02152943) | Phase 1 | Completed | 37 | Everolimus, letrozole and trastuzumab in hormone receptor-positive, HER2-positive tumours |
| [NCT00053339](https://clinicaltrials.gov/study/NCT00053339) | Phase 3 | Withdrawn | 0 | Trastuzumab with or without tamoxifen; withdrawn, so no data |

Notes: NCT01275677 is the strongest trial and directly supports trastuzumab-based regimens. For the top-ranked normal breast-like term, the only trial is [NCT07669610](https://clinicaltrials.gov/study/NCT07669610) (Phase 3, not yet recruiting, n=140), which tests trastuzumab rezetecan, a different agent, in triple-negative disease, so it gives no direct support.

---

## Literature Evidence

Publications for **progesterone-receptor positive breast cancer**, up to 10 most relevant:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [32353342](https://pubmed.ncbi.nlm.nih.gov/32353342/) | 2020 | RCT (Phase 2) | Lancet Oncol | monarcHER: abemaciclib plus trastuzumab, with or without fulvestrant, vs trastuzumab plus chemotherapy in HR+/HER2+ advanced breast cancer |
| [28945833](https://pubmed.ncbi.nlm.nih.gov/28945833/) | 2017 | RCT (Phase 2) | Ann Oncol | WSG-ADAPT HER2+/HR-: 12 weeks of neoadjuvant trastuzumab and pertuzumab with or without paclitaxel |
| [27179402](https://pubmed.ncbi.nlm.nih.gov/27179402/) | 2016 | RCT (Phase 2) | Lancet Oncol | NeoSphere 5-year analysis of neoadjuvant pertuzumab and trastuzumab |
| [15894097](https://pubmed.ncbi.nlm.nih.gov/15894097/) | 2005 | Meta-analysis | Lancet | Effects of chemotherapy and hormonal therapy in early breast cancer on recurrence and 15-year survival |
| [31410192](https://pubmed.ncbi.nlm.nih.gov/31410192/) | 2019 | Multi-cohort analysis | Theranostics | Molecular features and trastuzumab responsiveness of ER+/PR+/HER2+ (triple-positive) breast cancer |
| [21151204](https://pubmed.ncbi.nlm.nih.gov/21151204/) | 2011 | Review | Nat Rev Clin Oncol | HER2 and hormone receptor-positive breast cancer: combining aromatase inhibitors with anti-HER2 therapy |
| [35640077](https://pubmed.ncbi.nlm.nih.gov/35640077/) | 2022 | Guideline | J Clin Oncol | ASCO guideline update on systemic therapy for advanced HER2-positive breast cancer |
| [40339592](https://pubmed.ncbi.nlm.nih.gov/40339592/) | 2025 | Phase 2a | Lancet Oncol | Zanidatamab plus palbociclib and fulvestrant in HR+/HER2+ metastatic breast cancer |
| [41780642](https://pubmed.ncbi.nlm.nih.gov/41780642/) | 2026 | RCT | Ann Oncol | DESTINY-Breast06: trastuzumab deruxtecan in HR+, HER2-low/-ultralow disease (a different agent) |
| [26253814](https://pubmed.ncbi.nlm.nih.gov/26253814/) | 2015 | Review | Breast | Clinical implications of intrinsic molecular subtypes of breast cancer |

Note: for the luminal A or B term, the key supporting paper is the pivotal adjuvant trastuzumab trial, [16236738](https://pubmed.ncbi.nlm.nih.gov/16236738/) (2005, N Engl J Med). For the top-ranked normal breast-like term, the single publication ([19466513](https://pubmed.ncbi.nlm.nih.gov/19466513/)) is a morphology study of basal-like tumours and does not address trastuzumab.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2669994 | Baxter | Inj 1 mg for ECP | Not available (valid) |
| 2670127 | Herzuma | Inj 150 mg vial | Not available (valid) |
| 2670135 | Herzuma | Inj 440 mg vial | Not available (valid) |


---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (HER2-directed monoclonal antibody), not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions (low as a single agent; additive when combined with chemotherapy) |
| Emetogenicity Classification | Low |
| Monitoring Items | Cardiac function (LVEF) before and during treatment; CBC and liver and renal function when given with chemotherapy |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Cardiotoxicity**: The literature compiled for this report highlights trastuzumab-related cardiotoxicity (PMID 17687157), and cardiac monitoring is recommended.
- **Drug Interactions**: 94 interactions are recorded. Major interactions include deferiprone, samarium (153Sm) lexidronam, adalimumab and baricitinib. Moderate interactions include paclitaxel, warfarin, dicoumarol, azathioprine, alemtuzumab, zidovudine, chloramphenicol and several vaccines.

Package insert warnings and contraindications were not available, so please refer to the package insert for the complete safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails** (progesterone-receptor positive, progesterone-receptor negative and luminal A or B terms, restricted to HER2-positive disease). **Hold** for the normal breast-like subtype and for ranks 5-10.

**Rationale:**
- Large completed Phase 3 trials and multiple RCTs support trastuzumab in HER2-positive breast cancer, including HR+ and HR- subsets. These are subsets of the existing indication rather than new uses.
- The top-ranked normal breast-like term and the rare tumours (ranks 5-10) have no direct evidence and no established HER2 mechanism.

**To proceed, the following is needed:**
- Confirm HER2 positivity (IHC/ISH) for every patient; benefit is not expected in HER2-negative disease.
- Cardiac function monitoring (LVEF) plan.
- Review of the 94 recorded drug interactions, especially the major ones.
- Package insert warnings and contraindications (not yet obtained) and mechanism of action data.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


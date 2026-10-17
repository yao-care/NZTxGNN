---
layout: default
title: Tamoxifen
parent: Moderate Evidence (L3-L4)
nav_order: 437
evidence_level: L4
indication_count: 10
last_modified_at: 2026-10-15
description: "Tamoxifen: predicted new indication Mammary Paget disease (evidence level L4). Approved use: Female breast carcinoma"
monitor_inn: "Tamoxifen"
predicted_indications: ["mammary Paget disease", "Ehrlich tumor carcinoma", "botryoid-type embryonal rhabdomyosarcoma of the vagina"]
---

# Tamoxifen
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **10** 
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

# Tamoxifen: From Female Breast Carcinoma to Mammary Paget Disease

## One-Sentence Summary

Tamoxifen is an anti-estrogen drug originally used to treat breast carcinoma (including invasive ductal carcinoma).
The TxGNN model predicts it may be effective for **Mammary Paget Disease**, but the supporting evidence is thin: **1 loosely related clinical trial** and **10 publications**, mostly case reports and case series.
Among the model's top 10 predictions, the best-supported indication is **Breast Carcinoma in Situ** (see the note at the end of the Conclusion).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Female breast carcinoma; invasive ductal breast carcinoma; breast ductal adenocarcinoma (no approved indication text is available from the New Zealand licences) |
| Predicted New Indication | Mammary Paget disease |
| TxGNN Prediction Score | 99.69% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the data compiled for this report. Based on known pharmacology, tamoxifen acts on the estrogen receptors (ERα and ERβ) and also binds GPER. It is used as an anti-estrogen in hormone-receptor-positive breast cancer.

Mammary Paget disease is frequently associated with underlying ductal carcinoma in situ or invasive ductal carcinoma, which is often hormone-receptor-positive. This makes tamoxifen's anti-estrogen activity biologically plausible. The link is indirect, however: no trial in the dataset tests tamoxifen in Paget disease. The available support is a handful of case reports, for example one tamoxifen response in an extensive Paget disease of the nipple and one in hormone-receptor-positive extramammary Paget disease (a different condition).

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00002920](https://clinicaltrials.gov/study/NCT00002920) | Phase 3 | Completed | 313 | Medroxyprogesterone vs observation to prevent endometrial pathology in postmenopausal breast cancer patients on tamoxifen. Paget's disease of the nipple is among the eligible conditions, but the trial does not test tamoxifen as a treatment for it. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25759627](https://pubmed.ncbi.nlm.nih.gov/25759627/) | 2014 | Meta-analysis (cohort) | Breast Care | Local recurrence after mastectomy vs breast-conserving surgery in Paget's disease; total recurrence is reported as high as 20-40% |
| [1648987](https://pubmed.ncbi.nlm.nih.gov/1648987/) | 1991 | Review/series | Br J Surg | 48 women with Paget's disease of the nipple; most were treated surgically and tamoxifen was used in one case |
| [14965622](https://pubmed.ncbi.nlm.nih.gov/14965622/) | 2001 | Case report | Breast | Extensive nipple Paget's disease; response achieved with tamoxifen, plus radiation therapy |
| [34463889](https://pubmed.ncbi.nlm.nih.gov/34463889/) | 2022 | Case report | Invest New Drugs | Successful tamoxifen treatment of hormone-receptor-positive metastatic extramammary Paget disease |
| [16277886](https://pubmed.ncbi.nlm.nih.gov/16277886/) | 2005 | Case series | Clin Breast Cancer | Paget's disease of the nipple as a local recurrence after breast-conservation treatment |
| [17319355](https://pubmed.ncbi.nlm.nih.gov/17319355/) | 2006 | Case series | Niger J Clin Pract | 8 of 240 breast cancer patients in Benin City had Paget's disease of the nipple-areola complex |
| [8955252](https://pubmed.ncbi.nlm.nih.gov/8955252/) | 1996 | Case report | Am Surg | Paget's disease of the male breast with a review of 32 reported cases |
| [19112575](https://pubmed.ncbi.nlm.nih.gov/19112575/) | 2009 | Case report | Arch Gynecol Obstet | Vulvar and breast Paget's disease with synchronous underlying cancer |
| [12924421](https://pubmed.ncbi.nlm.nih.gov/12924421/) | 2003 | Case report | Surg Today | Synchronous bilateral breast cancer with Paget's disease and invasive ductal carcinoma |
| [18288984](https://pubmed.ncbi.nlm.nih.gov/18288984/) | 2008 | Review | Curr Med Chem | General review of therapies for bone-loss-associated diseases; only tangentially relevant |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2556952 | Tamoxifen Sandoz | Tab 20 mg | Not available (licence cancelled) |
| 2556944 | Tamoxifen Sandoz | Tab 10 mg | Not available (licence cancelled) |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Hormonal (selective estrogen receptor modulator), not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert; the contraindication signals below suggest attention to platelet and white cell counts, calcium, and vascular events |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Contraindication signals from the knowledge graph (not label text):** thrombocytopenia, leukopenia, hypercalcaemia, nephrocalcinosis, phosphorus metabolism disease, stroke and cerebrovascular disorders, cerebral infarction, pulmonary embolism, vision disorder and blindness. These should be verified against the official package insert.
- **Drug interactions:** The interaction query completed, but it returned only pharmacological target interactions (GPER, estrogen receptor α and β, maxi-Cl⁻ channel, VRAC), not drug-drug interactions with graded severity.
- Package insert warnings and contraindications could not be retrieved, so please refer to the package insert for authoritative safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Paget disease prediction rests on case reports and a plausible hormone-receptor mechanism, with no trial testing tamoxifen in this condition. In addition, tamoxifen has no currently valid New Zealand licence.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (from Medsafe), which are a blocking gap for safety screening
- Detailed mechanism of action data
- Prospective or at least well-designed retrospective data on tamoxifen in hormone-receptor-positive mammary Paget disease
- Clarification of the regulatory pathway, given that both New Zealand licences are cancelled

**Note on other predictions:** Of the other nine top predictions, **breast carcinoma in situ** has the strongest direct evidence (L1, "Proceed with Guardrails"). That evidence includes the TAM-01 placebo-controlled Phase 3 trial of low-dose tamoxifen (10-year follow-up, [PMID 36917758](https://pubmed.ncbi.nlm.nih.gov/36917758/)), and it falls within the breast-cancer spectrum of the original indication. Suggested guardrails are restricting use to hormone-receptor-positive disease, weighing the benefit against thromboembolic and endometrial risks, and considering low-dose regimens. **Estrogen-receptor-positive breast cancer** is also rated L1, but it overlaps with the original indication and is not true repurposing.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Palbociclib
parent: Model Prediction Only (L5)
nav_order: 345
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-28
description: "Palbociclib: predicted new indication Hyperthyroidism (evidence level L5). Approved use: Not stated in the licence data"
monitor_inn: "PALBOCICLIB"
predicted_indications: ["hyperthyroidism", "rheumatoid arthritis", "thrombotic disease", "resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta"]
---

# Palbociclib
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

## Palbociclib: From Breast Cancer Treatment to Hyperthyroidism
{: .text-alpha }

## One-Sentence Summary

Palbociclib is a CDK4/6 inhibitor used in oncology. The top TxGNN prediction (hyperthyroidism) has no supporting evidence, so this report focuses on the best-supported candidate, **rheumatoid arthritis**. That candidate has **0 clinical trials** and **4 publications** (one case report, one cohort study, two preclinical studies), so the evidence is limited to early, indirect signals.

*Note: The original indication was not available in the New Zealand licence data compiled for this report. Palbociclib is a CDK4/6 inhibitor and, per the literature retrieved, is used for hormone receptor-positive, HER2-negative breast cancer.*

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the licence data; literature indicates HR+/HER2- breast cancer |
| Predicted New Indication | Rheumatoid arthritis (the top-ranked prediction, hyperthyroidism, has no evidence; see below) |
| TxGNN Prediction Score | 0.9936 (rheumatoid arthritis); 0.9944 for hyperthyroidism |
| Evidence Level | L5 (rheumatoid arthritis); L5 (hyperthyroidism) |
| New Zealand Market Status | Marketed (valid licences: 3 of 3) |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold (rheumatoid arthritis is a research question only) |

### All Predicted Indications

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Recommendation |
|------|------|------|------|------|
| 1 | Hyperthyroidism | 0.9944 | L5 | Hold |
| 2 | Rheumatoid arthritis | 0.9936 | L4 | Research question |
| 3 | Thrombotic disease | 0.9932 | L5 | Hold |
| 4 | Resistance to thyroid hormone (THR-beta mutation) | 0.9930 | L5 | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this report. Palbociclib belongs to the CDK4/6 inhibitor class, and its efficacy in breast cancer relies on blocking cell-cycle progression.

For **rheumatoid arthritis**, there is a plausible but indirect biological rationale. CDK4/6 drives proliferation of synovial fibroblasts and immune cells. Preclinical work shows CDK6-dependent synovial hyperplasia in arthritic mice, and cell-cycle inhibition combined with cytokine blockade enhanced antiarthritic effects in animal models without increased immune suppression. A single case report describes improvement of rheumatoid arthritis in a breast cancer patient treated with palbociclib. These findings are anecdotal or preclinical and do not establish efficacy in patients.

For the other predictions, the data does not support a link:
- **Hyperthyroidism** and **thyroid hormone resistance**: no trials or literature, and no mechanism linking CDK4/6 inhibition to these conditions is supported. The scores are TxGNN model (deep learning) predictions only.
- **Thrombotic disease**: the literature reports thromboembolism as a possible *adverse event* of CDK4/6 inhibitors. This is a safety signal, not a therapeutic benefit, and it contradicts the high prediction score.

## Clinical Trial Evidence

Currently no related clinical trials registered for rheumatoid arthritis or any of the other predicted indications.

## Literature Evidence

Rheumatoid arthritis (the primary candidate):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [33587021](https://pubmed.ncbi.nlm.nih.gov/33587021/) | 2021 | Case report | Modern Rheumatology Case Reports | A breast cancer patient with pre-existing rheumatoid arthritis showed improvement while on palbociclib. |
| [40504547](https://pubmed.ncbi.nlm.nih.gov/40504547/) | 2025 | Cohort | The Oncologist | Examines pre-existing and emerging immune-mediated diseases in breast cancer patients on CDK4/6 inhibitors plus endocrine therapy; specific findings could not be assessed from the available text. |
| [25165034](https://pubmed.ncbi.nlm.nih.gov/25165034/) | 2016 | Preclinical (animal) | Annals of the Rheumatic Diseases | CDK inhibition combined with cytokine blockade enhanced antiarthritic effects without increasing immune suppression in animal models. |
| [39940918](https://pubmed.ncbi.nlm.nih.gov/39940918/) | 2025 | Preclinical | International Journal of Molecular Sciences | Synovial hyperplasia in arthritic mice and TNF-α-induced synovial fibroblast proliferation are CDK6-dependent and CDK4-independent. |

Thrombotic disease (safety-related literature; most relevant items):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36794339](https://pubmed.ncbi.nlm.nih.gov/36794339/) | 2023 | Pharmacovigilance and systematic review | Expert Opinion on Drug Safety | Investigates the association between CDK4/6 inhibitors and thrombosis. |
| [39123221](https://pubmed.ncbi.nlm.nih.gov/39123221/) | 2024 | Pharmacovigilance (FAERS) | BMC Pharmacology & Toxicology | Compares adverse event profiles of palbociclib, abemaciclib and ribociclib. |
| [35300061](https://pubmed.ncbi.nlm.nih.gov/35300061/) | 2022 | Cohort | Cancer Management and Research | Real-world thromboembolic events with ribociclib in metastatic breast cancer. |

These describe thrombosis as a risk, not a therapeutic effect. No literature was available for hyperthyroidism or thyroid hormone resistance.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2709074 | Palbociclib Pfizer | Tab 125 mg | Not available (valid) |
| 2709066 | Palbociclib Pfizer | Tab 100 mg | Not available (valid) |
| 2709058 | Palbociclib Pfizer | Tab 75 mg | Not available (valid) |

All three authorizations are valid; the drug is currently marketed, according to the data compiled for this report.

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (CDK4/6 inhibitor), not a conventional cytotoxic |
| Myelosuppression Risk | Medium to high; bone marrow suppression is a recognised common adverse event of CDK4/6 inhibitors, and preclinical work notes palbociclib-induced myelosuppression |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Complete blood count; liver and renal function |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Drug Interactions**: 100 interactions were identified. Major interactions include **clarithromycin**, **cobicistat**, **deferiprone** and **samarium (153Sm) lexidronam**. Many moderate interactions were also listed, for example aprepitant, dexamethasone, cimetidine, simvastatin, saxagliptin, miconazole, clotrimazole, cisapride and cilostazol.
- **Adverse event signals from the literature**: bone marrow suppression, gastrointestinal toxicity, interstitial lung disease and thromboembolic events have been reported with CDK4/6 inhibitors.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Rheumatoid arthritis has biologically plausible but indirect support (one case report, one cohort study, and preclinical studies) and no clinical trials, so it is a research question rather than an actionable repurposing candidate. Other predictions rest on model scores alone or are contradicted by safety signals. In addition, all three New Zealand authorizations are valid and the drug is marketed, but the package insert safety data are missing.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe
- Detailed mechanism of action data
- Review of the full text of the 2025 cohort study (PMID 40504547) on immune-mediated diseases in CDK4/6 inhibitor patients
- Controlled clinical evidence, starting with an exploratory trial in rheumatoid arthritis, with myelosuppression and infection risk weighed against benefit
- Clarification of the New Zealand regulatory pathway given the three valid licences
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


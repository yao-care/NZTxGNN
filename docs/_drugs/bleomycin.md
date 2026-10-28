---
layout: default
title: Bleomycin
parent: Model Prediction Only (L5)
nav_order: 68
evidence_level: L5
indication_count: 6
monitor_inn: "bleomycin"
predicted_indications: ["cauda equina neoplasm", "adult astrocytic tumour", "reticulum cell sarcoma", "primary pulmonary lymphoma", "pulmonary blastoma", "well-differentiated fetal adenocarcinoma of the lung"]
last_modified_at: 2026-10-28
description: "Bleomycin: predicted new indication cauda equina neoplasm (evidence level L5). Approved use: Not recorded in the local data"
---

# Bleomycin
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

## Bleomycin: DNA-strand-scission cytotoxic glycopeptide antibiotic → cauda equina neoplasm
{: .text-alpha }

## One-Sentence Summary

Bleomycin is a cytotoxic anticancer antibiotic used mainly in lymphomas and other cancers.
The TxGNN model predicts it may be effective for **cauda equina neoplasm**, with a score of 0.9930.
This prediction currently has **no clinical trials** and **one publication** (a retrospective cohort on CNS germinoma that does not study this disease), so the evidence is model-based only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | cauda equina neoplasm |
| TxGNN Prediction Score | 0.9930 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Bleomycin is generally known as a DNA-damaging cytotoxic agent that causes strand breaks, so activity against tumour cells is plausible in principle.

The one retrieved paper (PMID 9440744, 1998) describes radiation therapy for relapsed CNS germinoma after a chemotherapy regimen that included bleomycin. It does not show that bleomycin works in tumours of the cauda equina. The high score is a computational prediction only. Poor penetration of bleomycin into the central nervous system is a major barrier to this use.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [9440744](https://pubmed.ncbi.nlm.nih.gov/9440744/) | 1998 | Cohort (retrospective) | Journal of Clinical Oncology | Radiation therapy as salvage for CNS germinoma that relapsed after primary chemotherapy (carboplatin, etoposide, bleomycin). It does not address cauda equina neoplasm directly. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2204215 | Baxter | Inj 1,000 iu for ECP | Not recorded in the local data |
| 2343207 | DBL Bleomycin Sulfate | Inj 15,000 iu, vial | Not recorded in the local data |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (DNA-damaging glycopeptide antibiotic) |
| Myelosuppression Risk | Low (bleomycin is known to be relatively marrow-sparing) |
| Emetogenicity Classification | Low |
| Monitoring Items | Lung function and chest imaging (pulmonary toxicity), skin, liver and renal function, CBC |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

- **Drug Interactions**: 205 interactions are recorded. The listed examples include levofloxacin (minor) and several others with unclassified severity, such as pantoprazole, palonosetron, aprepitant, morphine, omeprazole, vancomycin, prednisone, hydrocortisone and ondansetron.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No clinical trial or publication directly studies bleomycin in cauda equina neoplasm. The score is a model prediction only, and limited CNS penetration together with known pulmonary toxicity argue against advancing without further data.

**To proceed, the following is needed:**
- Studies or case series of bleomycin in spinal or cauda equina tumours, including intrathecal or local delivery
- Mechanism of action data
- Package insert warnings and contraindications from Medsafe
- A safety monitoring plan, especially for pulmonary toxicity
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


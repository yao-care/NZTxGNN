---
layout: default
title: Sirolimus
parent: Model Prediction Only (L5)
nav_order: 425
evidence_level: L5
indication_count: 6
last_modified_at: 2026-11-04
monitor_inn: "SIROLIMUS"
predicted_indications: ["liposarcoma", "ovarian myxoid liposarcoma", "uterine corpus perivascular epithelioid cell tumor", "benign PEComa", "lung PEComa", "vulva sarcoma"]
description: "Sirolimus: predicted new indication Liposarcoma (evidence level L5). Approved use: Not recorded in the local data"
---

# Sirolimus
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

## Sirolimus: Liposarcoma
{: .text-alpha }

## One-Sentence Summary

Sirolimus is an mTOR inhibitor that is marketed in New Zealand as Rapamune. The TxGNN model predicts it may be useful for **liposarcoma**. Support is thin: **1 Phase 2 single-arm trial** (a sirolimus combination, with no posted results) and no published study testing sirolimus in liposarcoma, so the evidence level is **L5**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Liposarcoma |
| TxGNN Prediction Score | 0.9989 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 3 (valid: 3) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the data compiled for this report. Sirolimus is known to inhibit mTORC1, a kinase that drives cell growth and proliferation. Activation of the PI3K/AKT/mTOR pathway has been reported in soft tissue sarcomas, including liposarcoma. This gives a plausible biological reason to test an mTOR inhibitor in this tumour type.

This is a mechanistic argument only. The only trial found tested sirolimus together with cyclophosphamide, so sirolimus's own contribution cannot be separated out. The supporting literature is mostly reviews of targeted sarcoma therapy and papers on cancer risk after transplantation, which are indirect. The high TxGNN score is a computational prediction and does not replace clinical evidence.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02821507](https://clinicaltrials.gov/study/NCT02821507) | Phase 2 | Completed | 70 | Single-arm, multicentre trial of sirolimus plus cyclophosphamide in metastatic or unresectable myxoid liposarcoma and chondrosarcoma (2014-06 to 2021-12). No results or liposarcoma-specific data are available. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39796641](https://pubmed.ncbi.nlm.nih.gov/39796641/) | 2024 | Review | Cancers | Overview of novel targeted agents in soft tissue sarcoma. Indirect context only. |
| [37222206](https://pubmed.ncbi.nlm.nih.gov/37222206/) | 2023 | Review | Current Opinion in Oncology | Rationale and clinical trial results for molecular-targeted agents in advanced sarcomas. Indirect. |
| [20497911](https://pubmed.ncbi.nlm.nih.gov/20497911/) | 2010 | Review | Bulletin du Cancer | Targeted treatment of rare connective tissue tumours and sarcomas, grouped by molecular alteration. Indirect. |
| [16434506](https://pubmed.ncbi.nlm.nih.gov/16434506/) | 2006 | RCT | Journal of the American Society of Nephrology | In adult kidney transplant recipients, sirolimus after early cyclosporine withdrawal was associated with lower cancer risk. Not a sarcoma study. |
| [26093731](https://pubmed.ncbi.nlm.nih.gov/26093731/) | 2015 | Review | Transplantation Proceedings | Cancer screening in kidney transplant patients on long-term immunosuppression. Unrelated to liposarcoma efficacy. |

None of these studies tests sirolimus in liposarcoma.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2109638 | Rapamune | Tab 1 mg | Not available (valid) |
| 2059339 | Rapamune | Oral liq 1 mg per ml | Not available (valid) |
| 2258218 | Rapamune | Tab 2 mg | Not available (valid) |

---

## Safety Considerations

Package insert warnings and contraindications are not available in the data compiled for this report. Please refer to the package insert for safety information.

**Drug Interactions:** 100 interacting drugs are recorded. Examples of the major-level interactions:
- Amphotericin B (including the lipid complex formulation)
- Mesalazine
- Balsalazide
- Clarithromycin

Moderate-level interactions include corticosteroids (hydrocortisone, dexamethasone, betamethasone, budesonide, triamcinolone), several antidiabetic agents (metformin, pioglitazone, acarbose, alogliptin, canagliflozin, dapagliflozin, chlorpropamide, albiglutide), cimetidine and aprepitant.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The only relevant trial is a single-arm Phase 2 study of a sirolimus combination that has no available results. No published study tests sirolimus in liposarcoma, so the evidence level is L5 and the prediction rests on mechanistic plausibility and the model score.

**To proceed, the following is needed:**
- Published results of NCT02821507, ideally with a liposarcoma-specific subgroup analysis.
- Evidence that separates sirolimus's own effect from that of its combination partner, such as a randomised or comparative study.
- Package insert warnings and contraindications for a safety review.
- Detailed mechanism-of-action data.
- An interaction-management plan, particularly for the major-level interacting drugs, in patients who would be on oncology co-medications.

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


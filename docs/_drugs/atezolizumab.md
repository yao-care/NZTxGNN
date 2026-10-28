---
layout: default
title: Atezolizumab
parent: Model Prediction Only (L5)
nav_order: 49
evidence_level: L5
indication_count: 6
monitor_inn: "ATEZOLIZUMAB"
predicted_indications: ["uterine ligament adenocarcinoma", "endocervical carcinoma", "adenoid cystic carcinoma of the cervix uteri", "uterine ligament serous adenocarcinoma", "signet ring cell variant cervical mucinous adenocarcinoma", "intestinal variant cervical mucinous adenocarcinoma"]
last_modified_at: 2026-10-28
description: "Atezolizumab: predicted new indication Endocervical carcinoma (evidence level L5). Approved use: Transitional cell carcinoma"
---

# Atezolizumab
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

## Atezolizumab: From Transitional Cell Carcinoma to Endocervical Carcinoma
{: .text-alpha }

## One-Sentence Summary

Atezolizumab is a drug whose recorded original indication in the knowledge graph is transitional cell carcinoma (a relation recorded in the knowledge graph, not a confirmed approval).
The TxGNN model predicts it may be effective for **endocervical carcinoma**, with **2 clinical trials** (one Phase 2, one Phase 1) and **1 publication** that is only loosely related to this prediction.
No efficacy results are available in the data compiled for this report, so the prediction is still at the research-question stage.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Transitional cell carcinoma (relation recorded in the knowledge graph; no approved indication text is available in the local licence records) |
| Predicted New Indication | Endocervical carcinoma (the highest-ranked prediction, uterine ligament adenocarcinoma, has no supporting studies, so this report focuses on the best-supported prediction, ranked second) |
| TxGNN Prediction Score | 0.9992 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Number of Approvals | 2 licences (both single-ingredient, both valid) |
| Recommended Decision | Hold |

**Note on evidence level:** The two trials study cervical cancer broadly (recurrent, persistent or metastatic cervical cancer; node-positive locally advanced cervical cancer). Neither is shown to be restricted to endocervical carcinoma, and the one publication covers small cell neuroendocrine carcinoma of the cervix, a different histology. Because no cited study directly addresses endocervical carcinoma itself, the evidence level is capped at L5, even though the underlying data pack scored it L2.

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Atezolizumab is known as a PD-L1-directed antibody, and this is general background rather than something taken from the data provided. Blocking PD-L1 may restore T-cell activity against tumours, which is the rationale behind its use in transitional cell (urothelial) carcinoma.

The link to cervical cancer is plausible but unproven here. Both are solid tumours in which immune checkpoint signalling can help cancer cells evade immune attack. The trials combine atezolizumab with bevacizumab (an anti-VEGF antibody) or with chemoradiotherapy as an immune primer. These designs are consistent with the proposed mechanism, but the data compiled for this report contain no efficacy outcomes, so the link rests on trial design only.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02921269](https://clinicaltrials.gov/study/NCT02921269) | Phase 2 | Completed | 11 | Atezolizumab plus bevacizumab in recurrent, persistent or metastatic cervical cancer. Small study; no results provided. |
| [NCT03738228](https://clinicaltrials.gov/study/NCT03738228) | Phase 1 | Completed | 40 | Atezolizumab as an immune primer and with extended-field chemoradiotherapy in node-positive locally advanced cervical cancer. Focus on immune activation and feasibility; no results provided. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37467967](https://pubmed.ncbi.nlm.nih.gov/37467967/) | 2023 | Review | Biomedical Journal | Review of small cell neuroendocrine carcinoma of the cervix, a rare and aggressive tumour. Evidence is limited to case series and case reports, and treatment has followed small cell lung cancer approaches. Not specific to endocervical carcinoma or atezolizumab. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2524953 | Tecentriq | Inj 60 mg per ml, 20 ml vial | Not stated in the licence record |
| 2655233 | Baxter | Inj 1 mg for ECP | Not stated in the licence record |

## Safety Considerations

- **Drug Interactions:** 75 interactions were identified. Examples:
  - **Major:** adalimumab, baricitinib, BCG live vaccine (Tice substrain).
  - **Moderate:** corticosteroids (hydrocortisone, prednisone, prednisolone, dexamethasone, methylprednisolone, budesonide, betamethasone, triamcinolone, deflazacort), roflumilast, azathioprine, anakinra, alemtuzumab, alefacept, and several vaccines (tetanus toxoid, anthrax, adenovirus).

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high (0.9992), and two completed trials exist in cervical cancer. However, neither shows results, both are small or early phase, and no cited study directly addresses endocervical carcinoma. The other five predictions have no trial or literature support at all.

**To proceed, the following is needed:**
- Published efficacy and safety results for NCT02921269 and NCT03738228, with the histology breakdown (endocervical subgroup)
- Mechanism of action data, which is currently missing
- Package insert warnings and contraindications from Medsafe, which is currently a blocking gap for safety screening
- Approved indication text for the New Zealand licences, to confirm the actual labelled uses
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


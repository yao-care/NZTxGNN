---
layout: default
title: Durvalumab
parent: Model Prediction Only (L5)
nav_order: 170
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Durvalumab: predicted new indication Prostatic urethra urothelial carcinoma (evidence level L5). Approved use: Transitional cell carcinoma"
---

# Durvalumab
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Durvalumab: From Transitional Cell Carcinoma to Prostatic Urethra Urothelial Carcinoma

## One-Sentence Summary

Durvalumab is a PD-L1 immune checkpoint inhibitor whose original indication in the data compiled for this report is transitional cell carcinoma (urothelial cancer).
The TxGNN model predicts it may be effective for **prostatic urethra urothelial carcinoma**, but **no clinical trials and no publications** currently support this specific indication.
Related urothelial and variant-histology indications have some early-phase trial activity (see below), so the top-ranked prediction rests on model similarity alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Transitional cell carcinoma (from the knowledge-graph indication data; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Prostatic urethra urothelial carcinoma |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, durvalumab is an anti-PD-L1 antibody, and its efficacy in urothelial carcinoma is the basis of its original indication. Mechanistically, it may be applicable to histologically related tumours.

Prostatic urethral urothelial carcinoma shares its histology with transitional cell carcinoma, and it is plausibly PD-L1 responsive. The high TxGNN score reflects this similarity. However, no trials or literature were supplied for this specific site, so the link remains theoretical.

Other predictions in the list follow the same logic. Urothelial variants (sarcomatoid bladder and kidney pelvis carcinoma, renal pelvis papillary carcinoma) are close to the original indication. Cervical and uterine ligament tumours have only a general rationale for PD-L1 blockade in solid tumours.

## Clinical Trial Evidence

There are no clinical trials for the top-ranked indication, prostatic urethra urothelial carcinoma. Currently no related clinical trials are registered for it.

For context, trials found for other predicted indications in the same family are below.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03912818](https://clinicaltrials.gov/study/NCT03912818) | Phase 2 | Terminated | 7 | Durvalumab plus neoadjuvant chemotherapy in variant histology bladder cancer; terminated early with very low enrollment, so it is underpowered |
| [NCT02812420](https://clinicaltrials.gov/study/NCT02812420) | Early Phase 1 | Active, not recruiting | 54 | Pre-surgical durvalumab plus tremelimumab in muscle-invasive, high-risk urothelial carcinoma ineligible for cisplatin; no results provided |
| [NCT03452332](https://clinicaltrials.gov/study/NCT03452332) | Phase 1 | Completed | 20 | Hypofractionated radiotherapy with durvalumab and tremelimumab in recurrent or metastatic cervical, vaginal or vulvar cancer; early safety and feasibility, not specific to endocervical carcinoma |

## Literature Evidence

There is no literature for the top-ranked indication. For context, publications found for the endocervical carcinoma prediction (rank 6) are below.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [42518016](https://pubmed.ncbi.nlm.nih.gov/42518016/) | 2026 | Nonrandomized clinical trial | Oncology and Therapy | Phase 1 study of ceralasertib plus durvalumab in Chinese patients with advanced solid tumours refractory to prior anti-PD-(L)1 therapy; assesses safety, pharmacokinetics and antitumour activity |
| [37467967](https://pubmed.ncbi.nlm.nih.gov/37467967/) | 2023 | Review | Biomedical Journal | Small cell neuroendocrine carcinoma of the cervix: molecular basis and therapeutic advances; the data are limited to case series and reports |

## New Zealand Market Information

All three licences are cancelled. No approved indication text is recorded for them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2578425 | Imfinzi | Inj 50 mg per ml, 2.4 ml vial | Cancelled |
| 2578433 | Imfinzi | Inj 50 mg per ml, 10 ml vial | Cancelled |
| 2642166 | Baxter | Inj 1 mg for ECP | Cancelled |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Immunotherapy (anti-PD-L1 monoclonal antibody), not a conventional cytotoxic agent |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Drug Interactions**: 81 interactions were identified. The main ones are:
  - **Major**: adalimumab
  - **Moderate**: corticosteroids (hydrocortisone, triamcinolone, dexamethasone, betamethasone, budesonide, prednisolone, prednisone, methylprednisolone, deflazacort), roflumilast, alefacept, alemtuzumab, anakinra, tetanus toxoid and anthrax vaccine
  - **Minor**: zinc salts (acetate, gluconate, sulfate, chloride)

Corticosteroid and other immunosuppressant interactions deserve attention, because they may blunt the immune-mediated effect of a checkpoint inhibitor. For other safety information, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction has a very high model score but only L5 evidence: no trials, no literature, and no mechanism data. The New Zealand licences are all cancelled. Related variant-histology urothelial indications (ranks 2 and 3) have some early-phase trials, but these are small, early or terminated, and they could be considered research questions.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (currently the main blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Clinical or observational evidence specific to prostatic urethral urothelial carcinoma, or a decision to prioritise the better-supported rank 2 and 3 indications
- Clarification of the New Zealand regulatory status, since all licences are cancelled
- Route and formulation compatibility assessment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


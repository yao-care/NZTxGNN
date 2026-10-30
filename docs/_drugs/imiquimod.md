---
layout: default
title: Imiquimod
parent: Model Prediction Only (L5)
nav_order: 233
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-30
monitor_inn: "IMIQUIMOD"
predicted_indications: ["pre-malignant neoplasm", "benign neoplasm of buccal mucosa", "cervical neuroblastoma", "odontogenic cyst", "benign neoplasm of tongue", "nasopharyngeal teratoma", "cystic neoplasm", "inner ear neoplasm", "neoplasm of major salivary gland", "schwannoma of jugular foramen"]
description: "Imiquimod: predicted new indication Pre-malignant neoplasm (evidence level L5). Approved use: Not recorded in the local data"
---

# Imiquimod
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

## Imiquimod: Pre-malignant neoplasm
{: .text-alpha }

## One-Sentence Summary

Imiquimod is a topical immune response modifier, originally used for skin conditions such as common warts, anogenital HPV infection, superficial basal cell carcinoma and actinic keratosis.
The TxGNN model predicts it may be effective for **pre-malignant neoplasm**, with **2 clinical trials** and **9 publications** currently supporting this direction.
Part of this evidence (actinic keratosis) overlaps with an existing indication, so it is not a pure repurposing signal.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Pre-malignant neoplasm |
| TxGNN Prediction Score | 0.9992 |
| Evidence Level | L5 (see note below) |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Recommended Decision | Proceed with Guardrails |

*Evidence level note:* The two registered trials are small (n=20 and n=9), and one was terminated early. Only one is completed, and it is an open-label study in actinic keratosis, an already established use. No criteria for L1 or L2 are met, so the evidence is graded L3, based on systematic reviews and observational reports. This is more conservative than the L1 shown in the underlying scoring.

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, imiquimod is a Toll-like receptor 7 (TLR7) agonist. It triggers local innate and adaptive immune activation (interferon-alpha, TNF-alpha, IL-12), which can clear dysplastic and pre-malignant epithelium.

Its original indications (actinic keratosis, superficial basal cell carcinoma, HPV-related warts) are themselves pre-malignant, malignant or virally driven epithelial lesions. Extending the drug to other intraepithelial neoplasias (cervical, vulvar, anal, penile Bowenoid papulosis) is mechanistically coherent, because many are HPV-driven or depend on immune-mediated clearance.

Caution: actinic keratosis is already an established use, so it should not be counted as new evidence. The truly new settings (cervical, vulvar, anal and penile lesions) are supported mainly by reviews and case reports.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00175643](https://clinicaltrials.gov/study/NCT00175643) | Phase 3 | Completed | 20 | Open-label study of imiquimod 5% cream, 3 days per week in 1 or 2 cycles, for actinic keratoses on the head; assessed duration of effect. Small and non-randomised. |
| [NCT02329171](https://clinicaltrials.gov/study/NCT02329171) | Phase 3 | Terminated | 9 | Randomised trial of topical imiquimod for high-grade cervical intraepithelial neoplasia, as a non-surgical alternative to LLETZ. Terminated with only 9 participants, so it is underpowered. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23235673](https://pubmed.ncbi.nlm.nih.gov/23235673/) | 2012 | Systematic Review (Cochrane) | Cochrane Database Syst Rev | Interventions for anal canal intraepithelial neoplasia, a pre-malignant HPV-associated condition |
| [21491403](https://pubmed.ncbi.nlm.nih.gov/21491403/) | 2011 | Systematic Review (Cochrane) | Cochrane Database Syst Rev | Medical interventions for high-grade vulval intraepithelial neoplasia |
| [26516853](https://pubmed.ncbi.nlm.nih.gov/26516853/) | 2015 | Review | Int J Mol Sci | Combined treatments with photodynamic therapy for non-melanoma skin cancer |
| [20505896](https://pubmed.ncbi.nlm.nih.gov/20505896/) | 2010 | Review | Skin Therapy Lett | Current management of actinic keratoses, including topical field therapy |
| [15584683](https://pubmed.ncbi.nlm.nih.gov/15584683/) | 2004 | Review | Semin Cutan Med Surg | Topical treatments (fluorouracil, diclofenac, imiquimod, photodynamic therapy) for non-melanoma skin cancer and precursor lesions |
| [30284955](https://pubmed.ncbi.nlm.nih.gov/30284955/) | 2019 | Case report | Int J STD AIDS | Successful treatment of high-grade vulval intra-epithelial neoplasia with imiquimod 5% in a renal transplant recipient |
| [15601490](https://pubmed.ncbi.nlm.nih.gov/15601490/) | 2004 | Case report | Int J STD AIDS | Clearance of penile Bowenoid papulosis with topical imiquimod 5% |
| [29500135](https://pubmed.ncbi.nlm.nih.gov/29500135/) | 2018 | Preclinical (animal) | Urol Oncol | Pharmacokinetics of investigational TLR7 agonists in a rat model |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2715775 | Padagis | Cream 5%, 250 mg sachet | Not recorded (valid) |
| 2543907 | Perrigo | Cream 5%, 250 mg sachet | Not recorded (valid) |


---

## Safety Considerations

- **Disease associations of concern (knowledge graph, not label text)**: photosensitivity disease, photoparoxysmal response, sunburn, autoimmune disease and graft versus host disease are flagged as contraindication-type associations.
- **Drug Interactions**: 59 potential interactions were identified, but none has a graded severity. Examples include warfarin, clopidogrel, acetylsalicylic acid, prednisone, prednisolone, budesonide, hydroxychloroquine and simvastatin.
- **Literature safety signals**: A case of malignant conversion of florid oral papillomatosis during imiquimod therapy, and a case of erythema multiforme and one of lichen planopilaris after use, have been reported. These come from other predicted indications but are relevant to overall risk.

Please refer to the package insert for full warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanism is plausible and there is a body of reviews and case reports for HPV-related intraepithelial neoplasias. However, the registered trials are very small, one was terminated, and the one completed trial concerns an established indication. Evidence is supportive, not confirmatory, and the New Zealand licences are all cancelled.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data from DrugBank
- Adequately powered randomised trials in the new settings (cervical, vulvar, anal)
- Safety monitoring plan, with attention to photosensitivity, autoimmune conditions and the malignant-conversion signal

---

*Other predicted indications (benign neoplasm of buccal mucosa, odontogenic cyst, and others) have very limited or no supporting evidence and are not recommended for further action at this stage.*
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


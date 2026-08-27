---
layout: default
title: Imiquimod
parent: 僅模型預測 (L5)
nav_order: 172
evidence_level: L5
indication_count: 10
---

# Imiquimod
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

# Imiquimod: From Actinic Keratosis/Genital Warts to Pre-Malignant Neoplasm

## One-Sentence Summary

Imiquimod is a topical Toll-like receptor 7 (TLR7) agonist, best known for treating HPV-related external genital/perianal warts, actinic keratosis, and superficial basal cell carcinoma.
The TxGNN model predicts it may be effective more broadly for **pre-malignant neoplasm** (a category spanning HPV- and UV-induced precancerous epithelial lesions),
with **19 clinical trials** (9 directly relevant) and **9 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in the NZ regulatory dataset (drug not currently marketed in NZ, 0 licenses on file). Based on general drug information, imiquimod's established indications are actinic keratosis, superficial basal cell carcinoma, and external genital/perianal warts (condyloma acuminata). |
| Predicted New Indication | Pre-malignant neoplasm |
| TxGNN Prediction Score | 99.92% (rank 1164) |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Formal DrugBank MOA text is a data gap for this drug, but the evidence pack's own repurposing rationale supplies a clear mechanistic account: imiquimod is a TLR7 agonist that induces local interferon-α and IL-12 production, driving an innate/cell-mediated immune response that clears HPV-infected cells and abnormally proliferating epidermal keratinocytes. This mechanism is already clinically established in actinic keratosis and in HPV-driven precancerous lesions such as vulvar and anal intraepithelial neoplasia (VIN/AIN).

"Pre-malignant neoplasm" as predicted here is a broad umbrella term rather than a single disease entity — it aggregates cervical intraepithelial neoplasia (CIN), lentigo maligna, actinic keratosis, VIN, and related precursor lesions. All of these share a common biological thread with imiquimod's known indications: they are either HPV-driven or UV-driven epithelial dysplasias amenable to local immune-mediated clearance, which is mechanistically continuous with imiquimod's approved use in genital warts and actinic keratosis.

Because the predicted category is heterogeneous, the strength of evidence varies considerably by specific lesion type — CIN and lentigo maligna have completed Phase 2/3 trial data, while other sub-entities within the category are supported only by case reports or preclinical work. This heterogeneity is the main caveat behind the "Proceed with Guardrails" recommendation rather than an unqualified "Go."

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02329171](https://clinicaltrials.gov/study/NCT02329171) | Phase 3 | Terminated | 9 | Topical imiquimod for high-grade CIN as a non-invasive alternative to LLETZ excision; terminated early with very small enrollment, weakening the evidence. |
| [NCT03233412](https://clinicaltrials.gov/study/NCT03233412) | Phase 2 | Completed | 90 | Randomized trial evaluating topical imiquimod for high-grade cervical intraepithelial lesions (Brazil). |
| [NCT01720407](https://clinicaltrials.gov/study/NCT01720407) | Phase 3 | Completed | 259 | Neoadjuvant imiquimod prior to surgery for lentigo maligna (facial intraepidermal melanocytic proliferation) to reduce excision size and margin-positive risk. |
| [NCT00175643](https://clinicaltrials.gov/study/NCT00175643) | Phase 3 | Completed | 20 | Imiquimod 5% cream, 3 days/week for 1–2 cycles, for actinic keratoses of the head. |
| [NCT01229319](https://clinicaltrials.gov/study/NCT01229319) | Phase 4 | Unknown | 20 | Imiquimod 3.75% cream after cryotherapy for hypertrophic actinic keratoses on hands/forearms. |
| [NCT04219358](https://clinicaltrials.gov/study/NCT04219358) | Phase 1 | Terminated | 49 | Comparison of 5% imiquimod, 0.05% imiquimod, and 0.05% nanoencapsulated imiquimod gel for actinic cheilitis (a premalignant lip lesion). |
| [NCT00941811](https://clinicaltrials.gov/study/NCT00941811) | Phase 2 | Completed | 5 | Mechanistic/efficacy study of imiquimod in HPV-associated vulvar intraepithelial neoplasia (VIN 2/3) and anogenital warts. |
| [NCT04883645](https://clinicaltrials.gov/study/NCT04883645) | Early Phase 1 | Completed | 16 | Neoadjuvant topical TLR7 agonist (imiquimod/Aldara) immunotherapy in early-stage oral squamous cell carcinoma. |
| [NCT02242929](https://clinicaltrials.gov/study/NCT02242929) | Phase 3 | Unknown | 145 | Non-inferiority RCT: surgical excision vs. curettage + imiquimod for nodular basal cell carcinoma. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23235673](https://pubmed.ncbi.nlm.nih.gov/23235673/) | 2012 | Review (Cochrane) | Cochrane Database of Systematic Reviews | Systematic review of interventions, including imiquimod, for anal canal intraepithelial neoplasia (AIN). |
| [21491403](https://pubmed.ncbi.nlm.nih.gov/21491403/) | 2011 | Review (Cochrane) | Cochrane Database of Systematic Reviews | Systematic review of medical interventions, including imiquimod, for high-grade vulval intraepithelial neoplasia. |
| [26516853](https://pubmed.ncbi.nlm.nih.gov/26516853/) | 2015 | Review | Int J Mol Sci | Reviews topical/combined treatments, including imiquimod, for non-melanoma skin cancer and its precursors. |
| [20505896](https://pubmed.ncbi.nlm.nih.gov/20505896/) | 2010 | Review | Skin Therapy Letter | Overview of actinic keratosis management including topical field therapies such as imiquimod. |
| [15584683](https://pubmed.ncbi.nlm.nih.gov/15584683/) | 2004 | Review | Semin Cutan Med Surg | Reviews topical strategies, including imiquimod, for non-melanoma skin cancer and precursor lesions. |
| [29500135](https://pubmed.ncbi.nlm.nih.gov/29500135/) | 2018 | Cohort (animal model) | Urologic Oncology | Pharmacokinetics/pharmacodynamics of TLR7 agonists (related to imiquimod) intravesically, exploring extension of TLR7 agonism to premalignant bladder lesions. |
| [30284955](https://pubmed.ncbi.nlm.nih.gov/30284955/) | 2019 | Case Report | Int J STD AIDS | Successful treatment of high-grade VIN with imiquimod 5% in an immunosuppressed renal transplant recipient. |
| [18931984](https://pubmed.ncbi.nlm.nih.gov/18931984/) | 2008 | Case Report | Der Hautarzt | Case of disseminated superficial actinic porokeratosis with coexisting premalignant skin lesions resistant to topical treatment. |
| [15601490](https://pubmed.ncbi.nlm.nih.gov/15601490/) | 2004 | Case Report | Int J STD AIDS | Bowenoid papulosis (premalignant anogenital condition) successfully cleared with topical imiquimod 5% cream. |

---

## New Zealand Market Information

Imiquimod currently has no marketing authorization on file in New Zealand (0 registered products, market status: not marketed). No product-level dosage form or approved-indication data is available to tabulate.

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and DDI data are all marked as data gaps in this evidence pack — notably DG001, a **Blocking**-severity gap that prevents a formal S1 safety pre-assessment.)

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanistic rationale (TLR7-mediated clearance of HPV/UV-driven precancerous epithelium) is strong and independently supported by completed Phase 2/3 trials in specific sub-entities (high-grade CIN, lentigo maligna), yielding an overall L2 evidence level. However, "pre-malignant neoplasm" is a heterogeneous umbrella category — several contributing trials are small or terminated — and a Blocking safety data gap (no NZ/TFDA package insert) means the candidate cannot yet clear a full S1 safety review.

**To proceed, the following is needed:**
- NZ/TFDA package insert data — warnings, contraindications, DDI (resolves Blocking gap DG001)
- Confirmed DrugBank MOA and drug category data (resolves High-severity gap DG002)
- A specific target lesion within "pre-malignant neoplasm" (e.g., CIN, actinic keratosis, or VIN) to focus the clinical development program, rather than the broad predicted category
- Route/formulation compatibility assessment, since imiquimod is a topical agent and several candidate lesions (e.g., cervical, oral) require confirmation of adequate local access
- Regulatory pathway assessment given the drug is not currently marketed in New Zealand

*Note: Predictions ranked #2–#10 in this evidence pack (e.g., benign neoplasm of buccal mucosa, cervical neuroblastoma, odontogenic cyst) all scored L4–L5 evidence with a Hold recommendation and are not detailed here.*
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


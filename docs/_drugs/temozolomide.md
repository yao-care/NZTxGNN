---
layout: default
title: Temozolomide
parent: 僅模型預測 (L5)
nav_order: 332
evidence_level: L5
indication_count: 2
---

# Temozolomide
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
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

# Temozolomide: From Glioblastoma to Adult Astrocytic Tumour

## One-Sentence Summary

Temozolomide is an oral alkylating agent whose established use, as documented in the underlying trial and literature evidence, is newly diagnosed glioblastoma — the most common and aggressive primary brain tumour in adults. The TxGNN model predicts it may also be effective across the broader **Adult Astrocytic Tumour** category, a prediction already supported by **2 clinical trials** and **20 publications**, including the landmark Stupp-regimen randomized trial that underlies current glioma standard of care.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Glioblastoma / malignant glioma (per evidence literature; no New Zealand license text available since the drug is not currently marketed there) |
| Predicted New Indication | Adult Astrocytic Tumour |
| TxGNN Prediction Score | 99.36% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism-of-action data is not available in structured form. Based on information within the supporting literature, temozolomide is a second-generation imidazotetrazine alkylating agent that is administered orally and exerts a DNA-damaging (alkylating) effect on rapidly dividing tumour cells.

The relationship between the original and predicted indications is very close rather than distant: glioblastoma is itself classified as a grade IV astrocytic tumour, and anaplastic astrocytoma (grade III) is part of the same tumour family. The "Adult Astrocytic Tumour" prediction therefore largely reflects an extension of temozolomide's already-established efficacy across the full astrocytic tumour spectrum (e.g., recurrent WHO grade III/IV astrocytoma), rather than a mechanistically novel application.

Mechanistically, the alkylating/DNA-damaging activity of temozolomide is not specific to one histologic grade — it targets proliferating glial-lineage tumour cells broadly, which is consistent with activity across astrocytic tumours of varying grade, as reflected in the randomized trial evidence below (e.g., the NOA-08 and CeTeG/NOA-09 trials in anaplastic astrocytoma/glioblastoma populations).

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00052455](https://clinicaltrials.gov/study/NCT00052455) | Phase 3 | Completed | 500 | Randomized trial comparing temozolomide alone vs. PCV (procarbazine, lomustine, vincristine) in recurrent WHO grade III/IV astrocytic tumours |
| [NCT00960492](https://clinicaltrials.gov/study/NCT00960492) | Phase 1 | Completed | 26 | Dose-finding study of XL184 combined with temozolomide and radiotherapy in first-line glioblastoma |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15758009](https://pubmed.ncbi.nlm.nih.gov/15758009/) | 2005 | RCT | N Engl J Med | Landmark EORTC-NCIC trial: radiotherapy plus concomitant/adjuvant temozolomide vs. radiotherapy alone in newly diagnosed glioblastoma |
| [19269895](https://pubmed.ncbi.nlm.nih.gov/19269895/) | 2009 | RCT (5-yr follow-up) | Lancet Oncol | 5-year survival analysis confirming durable benefit of temozolomide + radiotherapy in glioblastoma |
| [26670971](https://pubmed.ncbi.nlm.nih.gov/26670971/) | 2015 | RCT | JAMA | Tumor-Treating Fields plus temozolomide vs. temozolomide alone as maintenance therapy for glioblastoma |
| [30782343](https://pubmed.ncbi.nlm.nih.gov/30782343/) | 2019 | RCT | Lancet | CeTeG/NOA-09: lomustine-temozolomide vs. standard temozolomide in MGMT-methylated glioblastoma |
| [22578793](https://pubmed.ncbi.nlm.nih.gov/22578793/) | 2012 | RCT | Lancet Oncol | NOA-08: temozolomide alone vs. radiotherapy alone in elderly malignant astrocytoma |
| [24552317](https://pubmed.ncbi.nlm.nih.gov/24552317/) | 2014 | RCT | N Engl J Med | Randomized trial of bevacizumab added to standard temozolomide/radiotherapy in newly diagnosed glioblastoma |
| [40779733](https://pubmed.ncbi.nlm.nih.gov/40779733/) | 2025 | RCT (Phase II/III) | J Clin Oncol | NRG Oncology BN007: dual immune checkpoint blockade in MGMT-unmethylated newly diagnosed glioblastoma |
| [36809318](https://pubmed.ncbi.nlm.nih.gov/36809318/) | 2023 | Review | JAMA | Overview of glioblastoma and other primary adult brain malignancies, including temozolomide-based standard of care |
| [29075865](https://pubmed.ncbi.nlm.nih.gov/29075865/) | 2017 | Review | Curr Oncol Rep | Review of glioblastoma treatment approaches in older adults |
| [25920709](https://pubmed.ncbi.nlm.nih.gov/25920709/) | 2015 | Clinical trial report | J Neurooncol | Exploratory cohort of concurrent radiotherapy and temozolomide in anaplastic astrocytic gliomas |

---

## New Zealand Market Information

Temozolomide currently holds no product authorizations in New Zealand (market status: 未上市 / not marketed; total licenses: 0).

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (alkylating agent, imidazotetrazine class) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Cytotoxic drug handling precautions are expected given the alkylating-agent classification; specific institutional protocol requirements should follow package insert guidance |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Efficacy evidence for the astrocytic tumour indication is strong (L1, multiple completed Phase 3 RCTs), but a Blocking data gap exists — TFDA/NZ package insert warnings and contraindications are unavailable, which prevents even a preliminary (S1) safety assessment. The drug also has no current market authorization in New Zealand.

**To proceed, the following is needed:**
- Official package insert / label safety data (warnings, contraindications) from a regulatory source
- Confirmed mechanism-of-action documentation (e.g., via DrugBank API)
- Drug-drug interaction data (current DDI query returned no results)
- Assessment of route/formulation availability for potential New Zealand market entry
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


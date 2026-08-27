---
layout: default
title: Ifosfamide
parent: 僅模型預測 (L5)
nav_order: 170
evidence_level: L5
indication_count: 10
---

# Ifosfamide
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

# Ifosfamide: From Testicular Carcinoma and Soft Tissue Sarcoma to Female Breast Carcinoma

## One-Sentence Summary

Ifosfamide is an oxazaphosphorine alkylating agent historically established for testicular carcinoma and soft tissue sarcoma. The TxGNN model predicts it may also be effective for **Female Breast Carcinoma**, with **8 clinical trials** and **20 publications** currently supporting this direction — though formal New Zealand regulatory and safety label data are not yet available.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Testicular carcinoma / soft tissue sarcoma (per literature on file; no formal NZ-approved indication text available — drug is not marketed) |
| Predicted New Indication | Female Breast Carcinoma |
| TxGNN Prediction Score | 99.91% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, ifosfamide is part of the oxazaphosphorine alkylating agent class (structural analog of cyclophosphamide), its efficacy in testicular carcinoma and soft tissue sarcoma has been proven, and mechanistically may be applicable to female breast carcinoma.

Ifosfamide is a prodrug activated primarily by hepatic and intratumoral CYP3A4/CYP2B6 into 4-hydroxyifosfamide, which forms DNA cross-links and halts proliferation of rapidly dividing cells. This is a broad-spectrum cytotoxic mechanism not restricted to its original tumor types. Literature in this evidence pack directly demonstrates that CYP3A4, CYP2C9, and CYP2B6 are expressed in breast tumor tissue microsomes and actively metabolize ifosfamide intratumorally (PMID 14970873), and that ifosfamide produces measurable DNA damage in both peripheral lymphocytes and breast tumor tissue in breast cancer patients (PMID 11138456) — supporting biological plausibility beyond a purely computational signal.

Clinically, ifosfamide has already been used off-label in combination regimens (with paclitaxel, vinorelbine, etoposide, epirubicin, doxorubicin) for anthracycline-resistant and metastatic breast cancer since the 1990s, including a Phase 3 RCT directly comparing an ifosfamide-containing regimen against a standard doublet (NCT00954174). This body of empirical use, combined with the mechanistic rationale, makes the TxGNN prediction reasonable, though most trial data are older, small, or of unconfirmed final status.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00954174](https://clinicaltrials.gov/study/NCT00954174) | Phase 3 | Unknown | 637 | Randomized trial of paclitaxel+carboplatin vs. paclitaxel+ifosfamide in chemo-naïve carcinosarcoma of uterus/fallopian tube/peritoneum/ovary |
| [NCT00006032](https://clinicaltrials.gov/study/NCT00006032) | Phase 2 | Terminated | N/A | Intensive-dose topotecan + ifosfamide/mesna + etoposide (TIME) followed by autologous stem cell rescue in metastatic breast cancer |
| [NCT00026078](https://clinicaltrials.gov/study/NCT00026078) | Phase 2 | Unknown | 42 | Docetaxel + ifosfamide as first-line chemotherapy in metastatic breast cancer |
| [NCT00003086](https://clinicaltrials.gov/study/NCT00003086) | Phase 1/2 | Terminated | 12 | Sequential double autologous bone marrow transplant with samarium-153 for Stage IV breast cancer |
| [NCT00012311](https://clinicaltrials.gov/study/NCT00012311) | Phase 2 | Unknown | N/A | Multi-cycle high-dose chemotherapy vs. optimized conventional-dose chemotherapy in metastatic breast cancer |
| [NCT00002854](https://clinicaltrials.gov/study/NCT00002854) | Phase 1 | Completed | 33 | Sequential high-dose cisplatin, cyclophosphamide, etoposide, ifosfamide, carboplatin, taxol with autologous stem cell support |
| [NCT00020722](https://clinicaltrials.gov/study/NCT00020722) | Phase 2 | Terminated | 7 | Chemotherapy followed by stem cell transplant plus activated T-cell therapy in Stage IV breast cancer |
| [NCT04279509](https://clinicaltrials.gov/study/NCT04279509) | N/A | Unknown | 35 | Organoid-based high-throughput drug screen assay for refractory solid tumors (in vitro selection, not direct clinical efficacy) |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [11932893](https://pubmed.ncbi.nlm.nih.gov/11932893/) | 2002 | Phase II open-label | Cancer | Paclitaxel (24-h infusion) + ifosfamide in anthracycline-resistant metastatic breast carcinoma |
| [7695982](https://pubmed.ncbi.nlm.nih.gov/7695982/) | 1995 | Cohort/PK study | Eur J Cancer | Pharmacokinetics, metabolism and clinical effect of ifosfamide combined with anthracyclines in breast cancer patients |
| [9226029](https://pubmed.ncbi.nlm.nih.gov/9226029/) | 1997 | Cohort | Tumori | Ifosfamide and etoposide in previously treated patients with advanced breast cancer |
| [11138456](https://pubmed.ncbi.nlm.nih.gov/11138456/) | 2000 | Translational/PK-PD | Cancer Chemother Pharmacol | Ifosfamide metabolism and DNA damage in tumour and peripheral blood lymphocytes of breast cancer patients |
| [14970873](https://pubmed.ncbi.nlm.nih.gov/14970873/) | 2004 | Ex vivo mechanistic | Br J Cancer | CYP3A4, CYP2C9 and CYP2B6 expression and ifosfamide turnover in breast cancer tissue microsomes |
| [3286879](https://pubmed.ncbi.nlm.nih.gov/3286879/) | 1988 | Review | J Natl Cancer Inst | Foundational review confirming ifosfamide's proven activity in soft tissue sarcoma and testicular carcinoma |
| [39306877](https://pubmed.ncbi.nlm.nih.gov/39306877/) | 2024 | Retrospective (unclassified) | Curr Probl Cancer | Ifosfamide-based chemotherapy experience in metaplastic breast cancer, a chemo-resistant breast cancer subtype |
| [8918497](https://pubmed.ncbi.nlm.nih.gov/8918497/) | 1996 | Cohort (unclassified) | J Clin Oncol | Ifosfamide + vinorelbine as first-line chemotherapy for metastatic breast cancer |
| [2347057](https://pubmed.ncbi.nlm.nih.gov/2347057/) | 1990 | Cohort (unclassified) | Cancer Chemother Pharmacol | Ifosfamide/methotrexate/5-FU (substituting cyclophosphamide) effective in CMF-resistant breast cancer |
| [8873839](https://pubmed.ncbi.nlm.nih.gov/8873839/) | 1996 | Cohort (unclassified) | J Chemother | Ifosfamide + mesna + epirubicin (IMEpi) as second-line chemotherapy in advanced breast cancer, 50% overall response rate |

## New Zealand Market Information

Ifosfamide currently holds no product authorizations in New Zealand (0 licenses on file; market status: not marketed).

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (oxazaphosphorine alkylating agent, analog of cyclophosphamide) |
| Myelosuppression Risk | High — dose-limiting hematologic toxicity is well documented; several trials in this evidence pack (e.g., NCT00003597, NCT00187109) specifically studied G-CSF/thrombopoietin support for ifosfamide-induced cytopenias, and multiple publications describe therapy-related MDS/AML following ifosfamide-containing regimens as a cumulative bone-marrow toxicity signal |
| Emetogenicity Classification | Moderate to High (dose-dependent; higher at doses typically used in sarcoma/breast regimens) |
| Monitoring Items | CBC with differential, renal function (BUN/creatinine — nephrotoxicity risk), urinalysis for hemorrhagic cystitis (mesna uroprotection required, referenced across multiple trials/literature), neurological status (case-reported ifosfamide-induced encephalopathy, PMID 41818182), liver function |
| Handling Protection | Yes — must follow cytotoxic/hazardous drug handling regulations (preparation, administration, and waste disposal) |

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
An L1 evidence level — anchored by a Phase 3 RCT, multiple Phase 1/2 trials, and mechanistic/translational literature specific to breast tumor tissue — supports pursuing this repurposing hypothesis, but a Blocking data gap on TFDA/Medsafe label content and the drug's unmarketed NZ status prevent an unconditional "Go."

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently a Blocking data gap (DG001)
- Confirmed mechanism-of-action documentation from DrugBank — currently a High-severity gap (DG002)
- Follow-up on NCT00954174 (Phase 3, status Unknown) to determine whether results were published
- Drug-drug interaction data (current DDI query returned not_found)
- Regulatory pathway assessment for import/use given ifosfamide is not currently marketed in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


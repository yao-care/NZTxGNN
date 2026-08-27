---
layout: default
title: Olaparib
parent: 僅模型預測 (L5)
nav_order: 254
evidence_level: L5
indication_count: 1
---

# Olaparib
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Olaparib: From Ovarian Cancer to Breast Cancer

## One-Sentence Summary

Olaparib is a PARP1/2 inhibitor originally developed for maintenance treatment of platinum-sensitive relapsed, BRCA-mutated high-grade serous ovarian, fallopian tube, or peritoneal cancer. The TxGNN model predicts it may also be effective for **Female Breast Carcinoma**, a prediction already strongly corroborated by **80 clinical trials** and **20 publications**, including two landmark Phase III RCTs (OlympiAD, OlympiA) that have led to approved breast cancer indications for olaparib in other jurisdictions.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Ovarian cancer — maintenance treatment of platinum-sensitive relapsed, BRCA-mutated high-grade serous epithelial ovarian/fallopian tube/peritoneal cancer |
| Predicted New Indication | Female Breast Carcinoma |
| TxGNN Prediction Score | 99.09% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Olaparib is a PARP1/2 (poly ADP-ribose polymerase) inhibitor. It blocks base-excision repair of single-strand DNA breaks, which in tumour cells carrying BRCA1/2 germline mutations (i.e. homologous recombination deficiency, HRD) leads to synthetic lethality — the cancer cell, already unable to repair double-strand breaks via homologous recombination, cannot compensate for the loss of the base-excision pathway either, and dies. Normal cells retain functional homologous recombination repair and are largely spared.

Ovarian cancer and breast cancer share the same underlying genetic vulnerability: both are core BRCA1/2-associated malignancies, and BRCA-mutated tumours in either tissue rely on the same DNA-repair deficiency that olaparib exploits. This mechanistic overlap is why the TxGNN prediction is biologically plausible rather than a spurious network association — the drug does not need a breast-specific mechanism, only a BRCA/HRD-positive tumour, which occurs in both cancer types.

This is also not a purely theoretical extrapolation: olaparib has already progressed through Phase III confirmatory trials in breast cancer (OlympiAD for metastatic disease, OlympiA for high-risk early-stage adjuvant use) and holds approved breast cancer indications in other regulatory jurisdictions (FDA, EMA), which independently validates the repurposing rationale identified by the model.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02282020](https://clinicaltrials.gov/study/NCT02282020) | Phase 3 | Completed | 266 | OlympiAD: Olaparib vs physician's choice chemotherapy in gBRCA-mutated, HER2-negative metastatic breast cancer |
| [NCT00679783](https://clinicaltrials.gov/study/NCT00679783) | Phase 2 | Completed | 99 | Early AZD2281 (olaparib) study establishing response rate in BRCA-mutated/triple-negative breast cancer, foundational for later Phase III design |
| [NCT04330040](https://clinicaltrials.gov/study/NCT04330040) | Phase 4 | Completed | 202 | Post-marketing real-world study in Indian patients with platinum-sensitive relapsed ovarian cancer and gBRCA1/2-mutated metastatic breast cancer |
| [NCT02418624](https://clinicaltrials.gov/study/NCT02418624) | Phase 1 | Completed | 25 | Carboplatin-olaparib sequencing vs capecitabine as first-line therapy in BRCA1/2-mutated HER2-negative advanced breast cancer |
| [NCT05498155](https://clinicaltrials.gov/study/NCT05498155) | Phase 2 | Active, not recruiting | 50 | Neoadjuvant olaparib monotherapy vs olaparib + durvalumab in BRCA-mutated, early-stage HER2-negative breast cancer |
| [NCT03109080](https://clinicaltrials.gov/study/NCT03109080) | Phase 1 | Completed | 24 | Olaparib combined with radiation therapy in inflammatory/locoregionally advanced/metastatic or residual triple-negative breast cancer |
| [NCT01623349](https://clinicaltrials.gov/study/NCT01623349) | Phase 1 | Completed | 118 | Oral PI3K inhibitor (BKM120/BYL719) plus olaparib in recurrent triple-negative breast cancer or high-grade serous ovarian cancer |
| [NCT02624973](https://clinicaltrials.gov/study/NCT02624973) | Phase 2 | Active, not recruiting | 200 | PETREMAC: personalized treatment strategies in high-risk breast cancer using predictive biomarkers |
| [NCT04041128](https://clinicaltrials.gov/study/NCT04041128) | Early Phase 1 | Completed | 14 | Pre-surgical window study of PARP inhibition on cellular/molecular changes in primary ovarian and breast cancer |
| [NCT05209529](https://clinicaltrials.gov/study/NCT05209529) | Phase 2 | Withdrawn | 0 | Neoadjuvant olaparib + durvalumab for BRCA-associated triple-negative breast cancer (did not proceed) |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36228963](https://pubmed.ncbi.nlm.nih.gov/36228963/) | 2022 | RCT | Annals of Oncology | OlympiA: overall survival results of adjuvant olaparib in gBRCA1/2-mutated, high-risk early breast cancer |
| [34081848](https://pubmed.ncbi.nlm.nih.gov/34081848/) | 2021 | RCT | New England Journal of Medicine | OlympiA primary results: adjuvant olaparib reduces recurrence in BRCA1/2-mutated early breast cancer |
| [28578601](https://pubmed.ncbi.nlm.nih.gov/28578601/) | 2017 | RCT | New England Journal of Medicine | OlympiAD primary results: olaparib shows antitumor activity in metastatic breast cancer with germline BRCA mutation |
| [30689707](https://pubmed.ncbi.nlm.nih.gov/30689707/) | 2019 | RCT | Annals of Oncology | OlympiAD final overall survival and tolerability vs chemotherapy of physician's choice |
| [36893711](https://pubmed.ncbi.nlm.nih.gov/36893711/) | 2023 | RCT | European Journal of Cancer | OlympiAD extended follow-up confirming survival and safety profile |
| [33119476](https://pubmed.ncbi.nlm.nih.gov/33119476/) | 2020 | Phase 2 Trial | Journal of Clinical Oncology | TBCRC 048: olaparib activity in metastatic breast cancer with somatic BRCA or other homologous recombination gene mutations |
| [34143979](https://pubmed.ncbi.nlm.nih.gov/34143979/) | 2021 | Phase 2 Trial | Cancer Cell | I-SPY2: durvalumab + olaparib + paclitaxel increases pathologic complete response in high-risk HER2-negative breast cancer |
| [35163586](https://pubmed.ncbi.nlm.nih.gov/35163586/) | 2022 | Review | International Journal of Molecular Sciences | Mechanisms, biomarkers and emerging therapies (including PARP inhibitors) for chemotherapy-resistant triple-negative breast cancer |
| [33710534](https://pubmed.ncbi.nlm.nih.gov/33710534/) | 2021 | Review | Targeted Oncology | Overview of PARP inhibitors, including olaparib, approved for BRCA-mutated HER2-negative breast cancer |
| [39520738](https://pubmed.ncbi.nlm.nih.gov/39520738/) | 2024 | Phase 2 Trial | Breast (Edinburgh) | NOBROLA: olaparib monotherapy in HRD-positive, non-germline-BRCA-mutated advanced triple-negative breast cancer |

---

## New Zealand Market Information

Olaparib currently has no marketing authorization on record in New Zealand (0 licenses).

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (PARP inhibitor) — not a conventional cytotoxic chemotherapy agent |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions (no drug-specific toxicity data in current evidence pack; anaemia, neutropenia and thrombocytopenia are class-recognized effects of PARP inhibitors) |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | CBC with differential, renal and hepatic function |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanistic rationale (BRCA/HRD-driven synthetic lethality) is strong, and evidence is already at L1 strength — two completed Phase III RCTs (OlympiAD, OlympiA) directly support olaparib's efficacy in BRCA-mutated breast cancer, and it holds approved breast cancer indications elsewhere. However, the drug is not currently marketed in New Zealand, and safety/regulatory data (package insert warnings, contraindications, MOA, DDI) are all outstanding, so guardrails are needed before advancing.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently a blocking data gap for safety assessment
- Confirmed mechanism of action data from DrugBank
- BRCA1/2 or HRD biomarker testing pathway/protocol for patient selection
- New Zealand market entry/registration assessment, since olaparib is not currently authorized
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


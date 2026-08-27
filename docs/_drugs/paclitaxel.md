---
layout: default
title: Paclitaxel
parent: 僅模型預測 (L5)
nav_order: 261
evidence_level: L5
indication_count: 10
---

# Paclitaxel
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

# Paclitaxel: From Antineoplastic Chemotherapy to Female Breast Carcinoma

## One-Sentence Summary

Paclitaxel is a taxane-class antineoplastic agent already used worldwide as standard-of-care chemotherapy for multiple solid tumors. The TxGNN model's top prediction — **Female Breast Carcinoma** — is supported by **13 clinical trials** and **20 publications**, though this reflects an already well-established global oncology use rather than a novel biological hypothesis; the main gap is that paclitaxel is currently **not marketed in New Zealand**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from local regulatory records (drug is not marketed in New Zealand; no license data on file) |
| Predicted New Indication | Female Breast Carcinoma |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L1 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data from DrugBank/TFDA has not been retrieved (see Data Gap DG002). Based on available evidence in this pack, paclitaxel is a taxane that binds β-tubulin to stabilize microtubules and block spindle disassembly, driving G2/M cell-cycle arrest and apoptosis. This is a well-documented cytotoxic mechanism, not a new hypothesis.

Importantly, the trial and literature evidence base for "female breast carcinoma" does not represent a genuinely novel repurposing signal — paclitaxel (and albumin-bound paclitaxel) is already a globally established, guideline-recommended chemotherapy backbone for breast cancer, frequently combined with trastuzumab, lapatinib, or other targeted agents. The TxGNN model's high score most likely reflects this pre-existing, extensively documented drug-disease relationship rather than an untested mechanistic bridge.

The practical significance of this candidate is therefore primarily regulatory: paclitaxel is not currently marketed in New Zealand for any indication, so the "new indication" opportunity here is a market-access question (registration/import pathway) rather than a scientific discovery question. Any evaluation should treat this distinction explicitly when deciding next steps.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00003992](https://clinicaltrials.gov/study/NCT00003992) | Phase 2 | Completed | 200 | Paclitaxel + trastuzumab adjuvant therapy in HER2-overexpressing stage II/IIIA breast cancer |
| [NCT00281658](https://clinicaltrials.gov/study/NCT00281658) | Phase 3 | Completed | 444 | Lapatinib + paclitaxel vs. placebo + paclitaxel in ErbB2-amplified metastatic breast cancer; direct efficacy data |
| [NCT00455533](https://clinicaltrials.gov/study/NCT00455533) | Phase 2 | Completed | 384 | Randomized biomarker study comparing sequential AC→ixabepilone vs. AC→paclitaxel in early-stage breast cancer |
| [NCT02280252](https://clinicaltrials.gov/study/NCT02280252) | Phase 2 | Completed | 69 | Concurrent paclitaxel and radiation in locally advanced breast cancer, multiethnic cohort |
| [NCT00272987](https://clinicaltrials.gov/study/NCT00272987) | Phase 3 | Terminated | 63 | Paclitaxel + trastuzumab + lapatinib vs. paclitaxel + trastuzumab + placebo in ErbB2+ metastatic breast cancer |
| [NCT04753177](https://clinicaltrials.gov/study/NCT04753177) | Phase 2/3 | Unknown | 120 | Neoadjuvant combined hormone therapy in premenopausal ER+/HER2- locally advanced breast cancer |
| [NCT00054028](https://clinicaltrials.gov/study/NCT00054028) | Phase 1/2 | Completed | 31 | Suramin combined with paclitaxel in advanced (stage IIIB/IV) metastatic breast cancer |
| [NCT00544167](https://clinicaltrials.gov/study/NCT00544167) | N/A | Completed | 45 | Adjuvant doxorubicin/cyclophosphamide followed by paclitaxel + sorafenib in high-risk early-stage breast cancer |
| [NCT02225652](https://clinicaltrials.gov/study/NCT02225652) | Phase 2 | Completed | 11 | Dose-dense FEC followed by weekly paclitaxel in primary breast cancer |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31783552](https://pubmed.ncbi.nlm.nih.gov/31783552/) | 2019 | Review | Biomolecules | Overview of paclitaxel's mechanistic and clinical effects in breast cancer, including resistance mechanisms |
| [9282422](https://pubmed.ncbi.nlm.nih.gov/9282422/) | 1997 | Review | Drug and Therapeutics Bulletin | Early review of paclitaxel/docetaxel licensing extension to metastatic breast carcinoma |
| [11147586](https://pubmed.ncbi.nlm.nih.gov/11147586/) | 2000 | Cohort | Cancer | Phase II trial of doxorubicin + paclitaxel in advanced breast carcinoma; importance of prior anthracycline exposure |
| [32461977](https://pubmed.ncbi.nlm.nih.gov/32461977/) | 2020 | Cohort | BioMed Research International | Real-world efficacy of neoadjuvant epirubicin/cyclophosphamide + weekly paclitaxel/trastuzumab in HER2+ breast carcinoma |
| [39317691](https://pubmed.ncbi.nlm.nih.gov/39317691/) | 2024 | Preclinical | Chemical Biology & Drug Design | Paclitaxel combination therapeutic potential and in vivo biomarkers in breast carcinoma |
| [39009452](https://pubmed.ncbi.nlm.nih.gov/39009452/) | 2024 | Preclinical | Journal for ImmunoTherapy of Cancer | Paclitaxel's effect on tumor-associated macrophages enhancing PD-1 blockade in breast cancer |
| [20665703](https://pubmed.ncbi.nlm.nih.gov/20665703/) | 2011 | Preclinical | Journal of Cellular Physiology | ZD6474 enhances paclitaxel antiproliferative/apoptotic effects in breast carcinoma cells |
| [31515668](https://pubmed.ncbi.nlm.nih.gov/31515668/) | 2019 | Preclinical/in vitro | Cancer Chemotherapy and Pharmacology | SRSF3 downregulation sensitizes breast cancer cells to paclitaxel treatment |
| [24823476](https://pubmed.ncbi.nlm.nih.gov/24823476/) | 2014 | Preclinical/Genetic | Nature Communications | TEKT4 germline variations enriched in breast cancer resistant to paclitaxel |
| [17272681](https://pubmed.ncbi.nlm.nih.gov/17272681/) | 2007 | Preclinical | Molecular Pharmacology | Reversal of stathmin-mediated resistance to paclitaxel and vinblastine in breast carcinoma cells |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (taxane class — microtubule-stabilizing agent) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions (TFDA insert not yet retrieved — see Data Gap DG001) |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Standard cytotoxic drug handling precautions apply, consistent with taxane-class chemotherapy agents |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple completed Phase 2 and Phase 3 trials (e.g., NCT00003992, NCT00281658, NCT00455533) support paclitaxel's efficacy in breast cancer, and this use is already globally established rather than experimental. However, New Zealand-specific regulatory and safety data are absent, and the drug is currently unmarketed there, so guardrails are needed before any local deployment decision.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert with warnings, precautions, and contraindications (Blocking gap, DG001)
- DrugBank-sourced mechanism of action detail to confirm mechanistic relevance (High priority gap, DG002)
- Formal drug-drug interaction (DDI) query, currently returning "not found"
- Assessment of the New Zealand registration/import pathway given zero current authorizations
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


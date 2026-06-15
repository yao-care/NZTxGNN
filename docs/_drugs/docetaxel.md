---
layout: default
title: Docetaxel
parent: 僅模型預測 (L5)
nav_order: 123
evidence_level: L5
indication_count: 10
---

# Docetaxel
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

# Docetaxel: From Solid Tumor Chemotherapy to Female Breast Carcinoma

## One-Sentence Summary

Docetaxel (Taxotere) is a semisynthetic taxane — one of the most widely used cytotoxic chemotherapy agents globally for solid tumors, including breast cancer, non-small cell lung cancer, and prostate cancer — though it currently holds no Medsafe authorization in New Zealand.
The TxGNN model predicts it may be effective for **Female Breast Carcinoma** in the New Zealand setting,
with **50 clinical trials** and **20 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Solid tumor chemotherapy (taxane class; no New Zealand registration) |
| Predicted New Indication | Female Breast Carcinoma |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Docetaxel is a semisynthetic taxoid derived from the European yew tree (*Taxus baccata*). As a microtubule-stabilizing agent, it promotes the assembly of tubulin into stable microtubule bundles while inhibiting their depolymerization. This hyperstabilization blocks the metaphase-to-anaphase transition during mitosis, arresting the cell cycle and inducing apoptosis preferentially in rapidly proliferating cancer cells.

Female breast carcinoma is characterized by high proliferative activity, making it particularly susceptible to taxane-class agents. Docetaxel has been a cornerstone of breast cancer treatment globally for over two decades. Multiple Phase 3 randomized controlled trials have established significant overall survival (OS) and disease-free survival (DFS) benefits across all major treatment settings: neoadjuvant (AC→T, TCHP), adjuvant (TAC, TC, dose-dense regimens), and metastatic (single-agent or combination with capecitabine, trastuzumab, pertuzumab). The mechanistic match between taxane action and breast carcinoma biology is exceptionally well-characterized and broadly replicated.

Although docetaxel holds no current Medsafe authorization in New Zealand, its extensive global regulatory approval record — FDA (1996), EMA, and multiple Asian health authorities — and the weight of its clinical evidence base make it a strong candidate for a Medsafe abridged or bibliographic regulatory pathway. The TxGNN model's high prediction score (99.90%) is consistent with this established evidence, reflecting the model's ability to correctly identify drugs with robust cross-validated clinical signals.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00002707](https://clinicaltrials.gov/study/NCT00002707) | Phase 3 | Completed | 2,411 | Large RCT comparing preoperative AC vs. AC+docetaxel (pre- or post-operative) in Stage II/III breast cancer; established the neoadjuvant docetaxel backbone |
| [NCT00089479](https://clinicaltrials.gov/study/NCT00089479) | Phase 3 | Completed | 2,611 | AC→docetaxel vs. AC→docetaxel+capecitabine in high-risk breast cancer; confirmed AC→T as standard adjuvant backbone |
| [NCT01275677](https://clinicaltrials.gov/study/NCT01275677) | Phase 3 | Completed | 3,270 | TC or AC→paclitaxel ± trastuzumab in HER2-low invasive breast cancer; large adjuvant RCT confirming taxane regimen efficacy across HER2 subtypes |
| [NCT00193011](https://clinicaltrials.gov/study/NCT00193011) | Phase 3 | Completed | 150 | Weekly docetaxel vs. CMF as adjuvant therapy in breast cancer patients ≥65 years or unsuitable for anthracyclines; supports docetaxel use in older/frail patients |
| [NCT00002544](https://clinicaltrials.gov/study/NCT00002544) | Phase 3 | Completed | 300 | Mitoxantrone vs. FEC (with/without docetaxel) as first-line chemotherapy for metastatic breast cancer with poor prognosis; established first-line efficacy benchmarks |
| [NCT01354522](https://clinicaltrials.gov/study/NCT01354522) | Phase 3 | Completed | 204 | TAC vs. TCX (docetaxel/cyclophosphamide/capecitabine) as adjuvant therapy in HER2-negative breast cancer; evaluated capecitabine addition to taxane backbone |
| [NCT00431080](https://clinicaltrials.gov/study/NCT00431080) | Phase 3 | Completed | 478 | Dose-dense G-CSF-supported FEC→docetaxel vs. paclitaxel as adjuvant chemotherapy in node-positive breast cancer; supports taxane sequencing in high-risk disease |
| [NCT02003209](https://clinicaltrials.gov/study/NCT02003209) | Phase 3 | Completed | 315 | TCHP (docetaxel/carboplatin/trastuzumab/pertuzumab) ± estrogen deprivation in HR+/HER2+ locally advanced breast cancer; key pCR data with dual HER2 blockade |
| [NCT04066335](https://clinicaltrials.gov/study/NCT04066335) | Observational | Unknown | 1,498 | Large observational safety study of Nanoxel M (docetaxel nanoparticle formulation) in cancer patients including breast cancer; real-world tolerability data at scale |
| [NCT00003565](https://clinicaltrials.gov/study/NCT00003565) | Phase 2 | Completed | 109 | Population pharmacokinetics of docetaxel in Caucasian and African-American solid tumor patients; supports individualized dosing and ethnic PK considerations |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28398846](https://pubmed.ncbi.nlm.nih.gov/28398846/) | 2017 | Phase 3 RCT | J Clin Oncol | ABC Trials (USOR 06-090, NSABP B-46-I, NSABP B-49): TC×6 vs. TaxAC in early breast cancer; demonstrated TaxAC superiority in high-risk disease, defining current adjuvant standard |
| [11481357](https://pubmed.ncbi.nlm.nih.gov/11481357/) | 2001 | Phase 2 RCT | J Clin Oncol | Dose-dense doxorubicin+docetaxel ± tamoxifen as preoperative therapy in operable breast cancer; high pathological response rates with dose-dense taxane-anthracycline approach |
| [12599222](https://pubmed.ncbi.nlm.nih.gov/12599222/) | 2003 | Phase 2 | Cancer | Capecitabine + docetaxel + epirubicin (TEX) as first-line in locally advanced/metastatic breast carcinoma; promising activity and manageable toxicity in triple combination |
| [27997437](https://pubmed.ncbi.nlm.nih.gov/27997437/) | 2017 | Cohort | Anti-cancer Drugs | Retrospective study on adjuvant docetaxel-based chemotherapy and breast cancer-related lymphedema; identified fluid retention syndrome as a contributing risk factor |
| [19856651](https://pubmed.ncbi.nlm.nih.gov/19856651/) | 2009 | Phase 1/2 | Tumori | Dose-finding study of weekly docetaxel + gemcitabine in anthracycline-refractory metastatic breast cancer; established feasible combination schedule for second-line use |
| [9364543](https://pubmed.ncbi.nlm.nih.gov/9364543/) | 1997 | Phase 2 | Oncology | Docetaxel + vinorelbine combination in metastatic breast cancer and NSCLC; response rates 23–36% confirming docetaxel's broad single-agent and combination activity |
| [26874836](https://pubmed.ncbi.nlm.nih.gov/26874836/) | 2017 | Retrospective | Breast Cancer (Tokyo) | Docetaxel + cyclophosphamide + trastuzumab (HER-TC) as neoadjuvant chemotherapy in HER2+ breast cancer; evaluated pCR rates by hormone receptor subtype |
| [15585076](https://pubmed.ncbi.nlm.nih.gov/15585076/) | 2004 | Phase 2 | Clin Breast Cancer | Docetaxel + cisplatin as primary chemotherapy for locally advanced breast cancer (≥5 cm); pCR evaluation with platinum-taxane neoadjuvant combination |
| [15161988](https://pubmed.ncbi.nlm.nih.gov/15161988/) | 2004 | Review | The Oncologist | Comprehensive 10-year clinical experience review of paclitaxel and docetaxel in breast cancer across metastatic, adjuvant, and neoadjuvant settings |
| [7595719](https://pubmed.ncbi.nlm.nih.gov/7595719/) | 1995 | Review | J Clin Oncol | Foundational review of docetaxel's preclinical and clinical profile; established the scientific rationale for taxoid use in solid tumor oncology |

---

## New Zealand Market Information

Docetaxel currently holds **no Medsafe authorizations** in New Zealand. There are no registered products to list.

> **Note:** Docetaxel is approved in numerous other jurisdictions — including the United States (FDA, 1996), European Union (EMA), Japan (PMDA), and Taiwan — for breast cancer, NSCLC, prostate cancer, gastric cancer, and head and neck cancer. A Medsafe application referencing these established international approvals via an abridged or bibliographic data pathway would be the recommended regulatory route.

---

## Cytotoxicity

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic — Taxane class (microtubule-stabilizing agent; semisynthetic taxoid) |
| Myelosuppression Risk | **High** — Neutropenia is the dose-limiting toxicity; febrile neutropenia occurs in 25–40% of patients without G-CSF prophylaxis. Thrombocytopenia and anaemia also reported. Primary G-CSF prophylaxis is recommended with most standard regimens |
| Emetogenicity Classification | Low to moderate (lower emetogenic potential than anthracyclines; dexamethasone premedication is standard to reduce fluid retention and hypersensitivity reactions) |
| Monitoring Items | CBC with differential (before each cycle); liver function tests (ALT, AST, total bilirubin, alkaline phosphatase — dose adjustment required if elevated); renal function; weight and fluid status (edema/fluid retention); peripheral neuropathy assessment; cardiac monitoring when combined with anthracyclines |
| Handling Protection | Must follow cytotoxic drug handling regulations — closed-system drug transfer devices (CSTDs) recommended; appropriate PPE required (double chemotherapy-rated gloves, gown, eye protection) during preparation and administration |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Docetaxel has one of the strongest clinical evidence bases in oncology for female breast carcinoma, with multiple completed Phase 3 RCTs demonstrating OS and DFS benefits across neoadjuvant, adjuvant, and metastatic settings (L1 evidence level). The primary barrier to clinical use in New Zealand is the absence of a Medsafe authorization, not any gap in clinical efficacy data.

**To proceed, the following is needed:**
- Submission of a Medsafe authorization application referencing existing FDA/EMA approvals (abridged or bibliographic regulatory pathway)
- Full New Zealand-compliant prescribing information including contraindications, black-box warnings (neutropenia, hypersensitivity reactions, fluid retention), and dosing guidance
- Pharmacovigilance plan for post-market safety monitoring in the New Zealand setting
- G-CSF prophylaxis protocol assessment and local formulary integration with New Zealand oncology pharmacy services
- Breast cancer multidisciplinary team (MDT) endorsement and integration into local treatment pathways (neoadjuvant, adjuvant, and metastatic protocols)
- HER2 testing infrastructure confirmation to ensure appropriate patient selection for trastuzumab/pertuzumab-containing docetaxel regimens
- Assessment of any ethnicity-specific pharmacokinetic considerations relevant to the New Zealand patient population (Māori, Pasifika)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Trastuzumab Emtansine
parent: 僅模型預測 (L5)
nav_order: 350
evidence_level: L5
indication_count: 4
---

# Trastuzumab Emtansine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **4** 個
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

# Trastuzumab Emtansine: From HER2-Positive Breast Cancer to Progesterone-Receptor Positive Breast Cancer

## One-Sentence Summary

Trastuzumab emtansine (T-DM1, DrugBank DB05773) is an antibody-drug conjugate already used worldwide for HER2-positive breast cancer.
The TxGNN model predicts it may also be effective specifically for **progesterone-receptor (PR) positive breast cancer**,
with **4 clinical trials** and **15 publications** currently associated with this direction, though the drug itself is not registered on the local market.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in local (Taiwan/NZ) regulatory data — no licenses on file. Globally, trastuzumab emtansine (Kadcyla) is approved for HER2-positive breast cancer, generally in patients previously treated with trastuzumab and a taxane. |
| Predicted New Indication | Progesterone-receptor positive breast cancer |
| TxGNN Prediction Score | 99.82% (rank 2195) |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in this Evidence Pack (marked as a High-severity data gap). Based on general drug class knowledge, trastuzumab emtansine is an antibody-drug conjugate (ADC) that links trastuzumab, a monoclonal antibody targeting HER2, to DM1 (emtansine), a maytansinoid microtubule inhibitor. The antibody component delivers the cytotoxic payload specifically to HER2-expressing tumor cells, combining HER2 pathway blockade with targeted chemotherapy.

Progesterone-receptor (PR) status is a hormone-receptor biomarker that commonly co-occurs with HER2 positivity in a subset of breast cancers (HR+/HER2+ disease), rather than a mechanistically distinct disease. Since trastuzumab emtansine's activity depends on HER2 expression rather than hormone-receptor status, its efficacy would be expected to extend to HER2-positive tumors regardless of PR status — which is consistent with the TxGNN model surfacing this indication as a molecularly-defined subgroup within the drug's existing target population.

This is further supported by the clinical trial evidence below, which includes a completed Phase 3 randomized trial evaluating trastuzumab/pertuzumab-based regimens in early HER2-positive breast cancer, and by an extensive literature base (ASCO/EGTM guidelines, biomarker reviews) that explicitly discusses HR (including PR) status alongside HER2 status in guiding anti-HER2 ADC therapy.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03726879](https://clinicaltrials.gov/study/NCT03726879) | Phase 3 | Completed | 454 | IMpassion050: randomized, double-blind, placebo-controlled trial of atezolizumab vs. placebo added to neoadjuvant ddAC-Paclitaxel-Trastuzumab-Pertuzumab in early HER2-positive breast cancer (T2-4, N1-3, M0) |
| [NCT02326974](https://clinicaltrials.gov/study/NCT02326974) | Phase 2 | Active, not recruiting | 164 | Evaluates T-DM1 in combination with pertuzumab in the preoperative setting; studies impact of HER2 heterogeneity on treatment response |
| [NCT04675827](https://clinicaltrials.gov/study/NCT04675827) | Phase 2 | Terminated | 139 | DECRESCENDO: de-escalation study of adjuvant chemotherapy in HER2-positive, ER-negative, node-negative early breast cancer achieving pathological complete response after neoadjuvant dual HER2 blockade |
| [NCT06131424](https://clinicaltrials.gov/study/NCT06131424) | N/A (observational) | Completed | 1151 | Multicenter retrospective study estimating prevalence and clinical characteristics of HER2-low locally-advanced/metastatic breast cancer previously classified as HER2-negative |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35640077](https://pubmed.ncbi.nlm.nih.gov/35640077/) | 2022 | Guideline | J Clin Oncol | ASCO guideline update on systemic therapy for HER2-positive advanced breast cancer |
| [29939838](https://pubmed.ncbi.nlm.nih.gov/29939838/) | 2018 | Guideline | J Clin Oncol | ASCO clinical practice guideline update for systemic therapy in advanced HER2-positive breast cancer, incorporating hormone-receptor status |
| [24799465](https://pubmed.ncbi.nlm.nih.gov/24799465/) | 2014 | Guideline | J Clin Oncol | Earlier ASCO evidence-based guideline on systemic therapy for advanced HER2-positive breast cancer |
| [28259011](https://pubmed.ncbi.nlm.nih.gov/28259011/) | 2017 | Guideline/Review | Eur J Cancer | EGTM updated guidelines on ER/PR and HER2 biomarker testing to guide endocrine and anti-HER2 (including T-DM1) therapy selection |
| [33726508](https://pubmed.ncbi.nlm.nih.gov/33726508/) | 2021 | Review | Future Oncology | Reviews treatment trends in HR+/HER2+ breast cancer, including trastuzumab emtansine among novel anti-HER2 therapies |
| [39631485](https://pubmed.ncbi.nlm.nih.gov/39631485/) | 2024 | Review | Pharmacological Research | Reviews targeted and cytotoxic inhibitors in breast cancer, discussing management by HER2/HR/ER/PR status |
| [24892840](https://pubmed.ncbi.nlm.nih.gov/24892840/) | 2013 | Review | Clin Adv Hematol Oncol | Reviews integration of new data into metastatic breast cancer practice, stratified by ER/PR/HER2 subtype |
| [34215766](https://pubmed.ncbi.nlm.nih.gov/34215766/) | 2021 | Observational | Scientific Reports | ChangeHER real-world study on prognostic relevance of HER2-positivity gain in metastatic breast cancer treated with pertuzumab/T-DM1 |
| [35251981](https://pubmed.ncbi.nlm.nih.gov/35251981/) | 2022 | Case Report/Review | Frontiers in Oncology | Case report and literature review of HER2-positive breast cancer with leptomeningeal disease (ER-negative, PR-negative tumor characterized) |
| [40642740](https://pubmed.ncbi.nlm.nih.gov/40642740/) | 2025 | Case Report/Review | J Medical Cases | Long-term follow-up case and literature review of durable response with an anti-HER2 ADC in metastatic breast cancer |

---

## New Zealand Market Information

This drug currently has no registered authorizations on file (0 licenses) — trastuzumab emtansine is not marketed locally at this time.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy — HER2-targeted antibody-drug conjugate (ADC) delivering a cytotoxic maytansinoid payload (DM1) |
| Myelosuppression Risk | No structured toxicity data in this Evidence Pack; thrombocytopenia is a well-recognized, often dose-limiting toxicity for this drug class per general prescribing knowledge — please refer to the package insert for confirmed rates |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Platelet count/CBC with differential, liver function tests, cardiac function (LVEF), infusion-related reactions |
| Handling Protection | Cytotoxic payload warrants handling per institutional hazardous/cytotoxic drug protocols |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
A Blocking-severity data gap (missing TFDA/Medsafe label warnings and contraindications) prevents completion of the S1 safety pre-assessment, and the drug is not currently marketed locally (0 authorizations). While the mechanistic rationale is sound and one completed Phase 3 trial plus supporting guideline-level literature exist, the predicted "new indication" largely reflects a biomarker subgroup of an already-established HER2-positive breast cancer population rather than a distinct novel use.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert warnings and contraindications (resolve DG001)
- Confirmed mechanism of action data from DrugBank (resolve DG002)
- Drug-drug interaction data (currently not found)
- Local regulatory pathway assessment given zero current NZ market authorizations
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


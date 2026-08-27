---
layout: default
title: Pertuzumab
parent: 僅模型預測 (L5)
nav_order: 274
evidence_level: L5
indication_count: 10
---

# Pertuzumab
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

# Pertuzumab: From HER2-Positive Breast Cancer to Progesterone-Receptor Positive Breast Cancer

## One-Sentence Summary

Pertuzumab (Perjeta®) is an anti-HER2 monoclonal antibody already used in combination regimens for HER2-positive breast cancer. The TxGNN model's top prediction highlights **progesterone-receptor (PR) positive breast cancer**, supported by **10 clinical trials** and **20 publications** — but this signal largely reflects an existing biomarker subgroup within pertuzumab's known HER2-positive breast cancer use rather than a mechanistically novel indication. The drug is currently **not marketed in New Zealand**, and a Medsafe/TFDA package insert data gap blocks formal safety pre-assessment.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HER2-positive breast cancer (inferred from evidence pack context — all cited pivotal trials, e.g. CLEOPATRA/APHINITY-class studies, use pertuzumab + trastuzumab + docetaxel in HER2+ disease) |
| Predicted New Indication | Progesterone-receptor positive breast cancer |
| TxGNN Prediction Score | 99.93% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, detailed original mechanism-of-action data from DrugBank is not available (data gap). Based on the mechanistic rationale supplied in the evidence pack, pertuzumab is a monoclonal antibody that binds HER2 domain II, blocking HER2–HER3 heterodimerization and downstream signaling — a mechanism distinct from, and complementary to, trastuzumab's HER2 domain IV binding.

Importantly, the evidence pack's own rationale flags a key caveat: this predicted indication is **not a true mechanistic extension**. Every supporting trial (NeoSphere, APHINITY-class studies, the QL1209 biosimilar program, etc.) enrolled HER2-positive breast cancer patients and simply stratified outcomes by hormone-receptor status. PR-positive status is a biomarker subgroup *within* pertuzumab's existing HER2-positive breast cancer approval, not a mechanistically distinct new disease. Accordingly, any use must be constrained to the **HER2-positive AND PR-positive** population — extrapolation to HER2-negative/PR-positive breast cancer is not supported by this evidence.

Two related predictions in the same evidence pack (rank 3: PR-negative breast cancer, evidence level L1; rank 4: luminal A/B breast tumor, evidence level L2) reinforce this pattern — TxGNN is largely surfacing biomarker-defined sub-populations of the same underlying HER2-positive breast cancer indication, rather than genuinely new disease areas.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04629846](https://clinicaltrials.gov/study/NCT04629846) | Phase 3 | Completed | 517 | QL1209 (pertuzumab biosimilar) vs. reference pertuzumab + trastuzumab + docetaxel in HER2+/ER-PR-negative early or locally advanced breast cancer (Grade A) |
| [NCT05802225](https://clinicaltrials.gov/study/NCT05802225) | Phase 3 | Active, not recruiting | 398 | BCD-178 biosimilar vs. Perjeta as neoadjuvant therapy for HER2+, ER/PR-negative breast cancer |
| [NCT02326974](https://clinicaltrials.gov/study/NCT02326974) | Phase 2 | Active, not recruiting | 164 | T-DM1 + pertuzumab preoperative therapy studying HER2 heterogeneity impact in early-stage HER2+ breast cancer |
| [NCT00545688](https://clinicaltrials.gov/study/NCT00545688) | Phase 2 | Completed | 417 | 4-arm neoadjuvant regimen study (Herceptin/docetaxel/pertuzumab); pathological complete response as endpoint, relevant to HR stratification (Grade A) |
| [NCT06131424](https://clinicaltrials.gov/study/NCT06131424) | N/A (retrospective) | Completed | 1151 | Non-interventional study on HER2-low prevalence, characteristics and treatment patterns in metastatic breast cancer |
| [NCT03058939](https://clinicaltrials.gov/study/NCT03058939) | Phase 2 | Withdrawn | 0 | Weekly neoadjuvant paclitaxel response rate in Nigerian women with breast cancer; not pertuzumab-specific |
| [NCT02689921](https://clinicaltrials.gov/study/NCT02689921) | Phase 2 | Unknown | 7 | NEOADAPT: chemo-free neoadjuvant aromatase inhibitor + pertuzumab/trastuzumab in HR+/HER2+ localized breast cancer (Grade B, underpowered) |
| [NCT03726879](https://clinicaltrials.gov/study/NCT03726879) | Phase 3 | Completed | 454 | IMpassion050: atezolizumab vs. placebo added to neoadjuvant ddAC-paclitaxel-trastuzumab-pertuzumab in early HER2+ breast cancer (Grade A) |
| [NCT00999804](https://clinicaltrials.gov/study/NCT00999804) | Phase 2 | Active, not recruiting | 128 | TBCRC 023: lapatinib + trastuzumab ± endocrine therapy for 12 vs. 24 weeks in HER2-overexpressing breast cancer |
| [NCT04675827](https://clinicaltrials.gov/study/NCT04675827) | Phase 2 | Terminated | 139 | DECRESCENDO: chemotherapy de-escalation with SC pertuzumab + trastuzumab in HER2+/ER-negative/node-negative early breast cancer (Grade B) |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [27179402](https://pubmed.ncbi.nlm.nih.gov/27179402/) | 2016 | RCT | Lancet Oncology | NeoSphere 5-year follow-up: neoadjuvant pertuzumab + trastuzumab + docetaxel improves pathological complete response in HER2+ breast cancer |
| [28945833](https://pubmed.ncbi.nlm.nih.gov/28945833/) | 2017 | RCT | Annals of Oncology | WSG-ADAPT HER2+/HR- trial: 12-week neoadjuvant dual HER2 blockade ± weekly paclitaxel, efficacy/safety and predictive markers |
| [38906970](https://pubmed.ncbi.nlm.nih.gov/38906970/) | 2024 | RCT | British Journal of Cancer | QL1209 (pertuzumab biosimilar) equivalence to reference pertuzumab in HER2+/ER-PR-negative breast cancer |
| [37609714](https://pubmed.ncbi.nlm.nih.gov/37609714/) | 2023 | RCT | Future Oncology | DECRESCENDO: de-escalating chemotherapy in HER2+, ER-negative, node-negative early breast cancer |
| [37166817](https://pubmed.ncbi.nlm.nih.gov/37166817/) | 2023 | RCT | JAMA Oncology | WSG-TP-II: neoadjuvant endocrine therapy + trastuzumab/pertuzumab vs. de-escalated chemotherapy in HR+/HER2+ early breast cancer |
| [30106636](https://pubmed.ncbi.nlm.nih.gov/30106636/) | 2018 | RCT (Phase 2) | Journal of Clinical Oncology | PERTAIN: first-line trastuzumab + aromatase inhibitor ± pertuzumab in HER2+/HR+ metastatic or locally advanced breast cancer |
| [35640077](https://pubmed.ncbi.nlm.nih.gov/35640077/) | 2022 | Review | Journal of Clinical Oncology | ASCO Guideline Update on systemic therapy for advanced HER2-positive breast cancer |
| [28973704](https://pubmed.ncbi.nlm.nih.gov/28973704/) | 2017 | Review | Southern Medical Journal | Overview of neoadjuvant and adjuvant therapies across breast cancer molecular subtypes |
| [40282499](https://pubmed.ncbi.nlm.nih.gov/40282499/) | 2025 | Cohort | Cancers | Adjuvant treatment proposal for pT1-T2N0M0 HER2-positive **and ER/PR-positive** breast cancer including targeted/anti-hormonal therapy |
| [33902424](https://pubmed.ncbi.nlm.nih.gov/33902424/) | 2022 | Review | Endocrine, Metabolic & Immune Disorders Drug Targets | Review of immunotherapy options for breast cancer, including trastuzumab/pertuzumab context |

## New Zealand Market Information

Pertuzumab currently has no market authorization in New Zealand (market status: Not marketed; 0 licenses on record). No product/dosage-form information is available to summarize.

## Cytotoxicity

Pertuzumab is a HER2-targeted therapy used in the treatment of breast cancer, so this section is included.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (anti-HER2 monoclonal antibody; not a conventional cytotoxic agent) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

Please refer to the package insert for safety information. A blocking data gap exists: the TFDA/Medsafe package insert (warnings, contraindications, drug interactions) has not yet been obtained, which currently prevents formal S1 safety pre-assessment.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Evidence level L1 (multiple completed Phase 3 RCTs) supports pertuzumab's efficacy in HER2-positive breast cancer, but the "PR-positive" label represents a biomarker subgroup of an existing indication rather than a novel repurposing signal, and use must be restricted to the HER2-positive AND PR-positive population. A blocking safety data gap (no package insert on file) currently prevents formal safety clearance.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications, drug interactions) — currently blocking S1 safety assessment
- Detailed mechanism-of-action data from DrugBank
- Confirmation of HER2 status alongside PR status in target population definitions, to avoid inappropriate extrapolation to HER2-negative/PR-positive disease
- New Zealand regulatory/market-entry assessment, given current unmarketed status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Pertuzumab
parent: Model Prediction Only (L5)
nav_order: 359
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-28
monitor_inn: "PERTUZUMAB"
predicted_indications: ["ectomesenchymoma", "malignant cutaneous granular cell skin tumor", "human herpesvirus 8-related tumor", "middle ear neuroendocrine tumor", "prostatic urethra urothelial carcinoma", "kidney pelvis sarcomatoid transitional cell carcinoma"]
description: "Pertuzumab: predicted new indication Ectomesenchymoma (evidence level L5). Approved use: Not recorded in the local data"
---

# Pertuzumab
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Pertuzumab: Ectomesenchymoma
{: .text-alpha }

## One-Sentence Summary

Pertuzumab is a HER2-targeted monoclonal antibody, originally used to treat HER2-positive breast carcinoma.
This is best understood as a receptor-subtype refinement of the existing HER2-positive indication rather than a wholly new use.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Ectomesenchymoma |
| TxGNN Prediction Score | 0.9971 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Pertuzumab binds the extracellular dimerization domain (subdomain II) of HER2 and blocks its pairing with HER3 and EGFR. This dampens downstream PI3K/AKT and MAPK signalling. Detailed mechanism-of-action data was not available in the data compiled for this report, so this description is based on established knowledge of pertuzumab.

Roughly half of HER2-positive breast cancers also express oestrogen and/or progesterone receptors (HR+/HER2+ disease). Because pertuzumab acts on HER2 itself, it applies to these tumours as well. The "PR-positive" label therefore describes a subset of the already-approved HER2-positive population. Crosstalk between the hormone-receptor and HER2 pathways is also the rationale for combining anti-HER2 therapy with endocrine therapy in this group.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04629846](https://clinicaltrials.gov/study/NCT04629846) | Phase 3 | Completed | 517 | Pertuzumab biosimilar QL1209 vs reference pertuzumab, each with trastuzumab and docetaxel, in early or locally advanced HER2-positive, ER/PR-negative breast cancer. This is an equivalence study, so it supports pertuzumab efficacy in HER2-positive disease but not specifically in PR-positive tumours. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37166817](https://pubmed.ncbi.nlm.nih.gov/37166817/) | 2023 | RCT (WSG-TP-II) | JAMA Oncol | Neoadjuvant endocrine therapy plus trastuzumab and pertuzumab compared with de-escalated chemotherapy in HR-positive/HER2-positive early breast cancer |
| [42150145](https://pubmed.ncbi.nlm.nih.gov/42150145/) | 2026 | RCT | J Clin Oncol | Survival analysis of WSG TP-II: neoadjuvant trastuzumab and pertuzumab with endocrine therapy vs chemotherapy in HR+/HER2+ early breast cancer |
| [27179402](https://pubmed.ncbi.nlm.nih.gov/27179402/) | 2016 | RCT | Lancet Oncol | NeoSphere 5-year analysis: progression-free survival, disease-free survival and safety of neoadjuvant pertuzumab plus trastuzumab |
| [38906970](https://pubmed.ncbi.nlm.nih.gov/38906970/) | 2024 | RCT | Br J Cancer | Phase 3 equivalence trial of biosimilar QL1209 vs reference pertuzumab in HER2-positive, ER/PR-negative disease |
| [41401771](https://pubmed.ncbi.nlm.nih.gov/41401771/) | 2026 | RCT | ESMO Open | Phase 3 equivalence trial of biosimilar TQB2440 vs reference pertuzumab in HER2-positive early or locally advanced breast cancer |
| [28945833](https://pubmed.ncbi.nlm.nih.gov/28945833/) | 2017 | RCT (Phase 2) | Ann Oncol | WSG-ADAPT HER2+/HR-: 12 weeks of neoadjuvant trastuzumab and pertuzumab, with or without weekly paclitaxel |
| [35640077](https://pubmed.ncbi.nlm.nih.gov/35640077/) | 2022 | Guideline | J Clin Oncol | ASCO guideline update on systemic therapy for advanced HER2-positive breast cancer |
| [27057657](https://pubmed.ncbi.nlm.nih.gov/27057657/) | 2016 | Review | Cancer Treat Rev | Review of HR+/HER2+ breast cancer, including pathway crosstalk and treatment |
| [41387082](https://pubmed.ncbi.nlm.nih.gov/41387082/) | 2026 | Cohort | Breast | Retrospective study of adjuvant ovarian function suppression and aromatase inhibitors in premenopausal HR+/HER2+ patients |
| [40983817](https://pubmed.ncbi.nlm.nih.gov/40983817/) | 2025 | Review | Breast Cancer | Signalling pathway interactions and clinical translation in HR+/HER2+ breast cancer |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2444208 | Perjeta (valid) | Inj 30 mg per ml, 14 ml vial | Not recorded |
| 2555328 | Baxter (valid) | Inj 420 mg for ECP | Not recorded |


---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (HER2-directed monoclonal antibody), not a conventional cytotoxic |
| Myelosuppression Risk | Low as a single agent; the main haematological burden comes from combined chemotherapy such as docetaxel |
| Emetogenicity Classification | Low |
| Monitoring Items | Left ventricular ejection fraction (cardiac function) and CBC when combined with chemotherapy; liver and renal function |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Drug Interactions**: Idelalisib (Moderate interaction, DDInter). Pertuzumab's pharmacological target is HER2 (ERBB2).
- **Cardiac safety**: The pertuzumab trials in the compiled literature (TRYPHAENA, BERENICE) were designed to assess cardiac safety of pertuzumab with trastuzumab and chemotherapy, so cardiac monitoring is relevant.

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Pertuzumab has a strong evidence base in HER2-positive breast cancer, including a completed Phase 3 trial and several randomised trials in HR+/HER2+ disease (WSG-TP-II, PERTAIN). However, this prediction is a subtype refinement of the existing indication.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Confirmation of the mechanism of action from DrugBank
- PR-positive subgroup efficacy data, such as PERTAIN and WSG-TP-II results
- Clarification of the regulatory path in New Zealand, since both licences are cancelled
- A cardiac monitoring plan for combination use

**Other predictions (brief note):** The remaining predictions are weaker. "Breast tumor luminal A or B" (L2) is relevant only to the HER2-positive luminal B subset and has no linked trials. "Progesterone-receptor negative breast cancer" has supporting evidence (L1, Proceed with Guardrails), similar to the PR-positive entry. The other seven predictions, including normal breast-like subtype, ectomesenchymoma and the urothelial carcinomas, have no clinical or literature evidence and no clear HER2-dependent mechanism, so they are rated Hold.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


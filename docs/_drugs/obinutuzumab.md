---
layout: default
title: Obinutuzumab
parent: 僅模型預測 (L5)
nav_order: 250
evidence_level: L5
indication_count: 3
---

# Obinutuzumab
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

# Obinutuzumab: From Chronic Lymphocytic Leukemia to Follicular Lymphoma

## One-Sentence Summary

Obinutuzumab is a type II, glycoengineered anti-CD20 monoclonal antibody previously established in CD20-positive B-cell malignancies such as chronic lymphocytic leukemia (CLL). The TxGNN model predicts strong efficacy for **Follicular Lymphoma**, and unlike most model-only predictions this one is already backed by **50 clinical trials** (including the pivotal Phase 3 GALLIUM study) and **20 publications** identified in this evidence pack.

*Note: this evidence pack contains three TxGNN-predicted indications for obinutuzumab. Two additional predictions — CLL/SLL with IGHV somatic hypermutation, and pregerminal-center CLL/SLL — returned zero matching trials or literature (Evidence Level L5, decision stage S0, recommendation Hold) and are not discussed further below.*

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Chronic Lymphocytic Leukemia (CLL) — per clinical trial evidence (NCT02877550); no Taiwan/NZ license record available to confirm formally |
| Predicted New Indication | Follicular Lymphoma |
| TxGNN Prediction Score | 99.18% |
| Evidence Level | L1 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Obinutuzumab is a second-generation, glycoengineered, type II anti-CD20 monoclonal antibody. Compared with first-generation anti-CD20 agents (e.g., rituximab), its glycoengineered Fc region enhances antibody-dependent cellular cytotoxicity (ADCC) and direct B-cell killing, in addition to complement-dependent cytotoxicity (CDC).

CD20 is stably and highly expressed on malignant B cells in both CLL and follicular lymphoma (FL), so the drug's core mechanism transfers directly across these indications. This is not a purely speculative prediction: obinutuzumab already carries a regulatory-grade evidence base in FL — the Phase 3 GALLIUM trial (NCT01332968, n=1,401) demonstrated superior progression-free survival for obinutuzumab-based versus rituximab-based immunochemotherapy in previously untreated advanced FL, and this has been corroborated by multiple follow-on combination studies (with polatuzumab vedotin, venetoclax, lenalidomide, zanubrutinib, and others).

In short, the mechanistic rationale (shared CD20 target across CD20+ B-cell malignancies) and the clinical evidence base are mutually reinforcing, which is why this candidate reaches Evidence Level L1 rather than remaining a pure model prediction.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01332968](https://clinicaltrials.gov/study/NCT01332968) | Phase 3 | Completed | 1401 | GALLIUM: obinutuzumab + chemo vs rituximab + chemo in previously untreated advanced indolent NHL; pivotal trial supporting FL indication |
| [NCT01059630](https://clinicaltrials.gov/study/NCT01059630) | Phase 3 | Completed | 413 | GADOLIN: bendamustine vs bendamustine + obinutuzumab in rituximab-refractory indolent NHL |
| [NCT03817853](https://clinicaltrials.gov/study/NCT03817853) | Phase 4 | Completed | 114 | Post-marketing study of obinutuzumab short-duration (90-min) infusion safety in previously untreated advanced FL |
| [NCT04034056](https://clinicaltrials.gov/study/NCT04034056) | N/A (non-interventional) | Completed | 299 | Real-world effectiveness/safety of obinutuzumab in previously untreated advanced FL, relapse rate at 24 months |
| [NCT03332017](https://clinicaltrials.gov/study/NCT03332017) | Phase 2 | Completed | 217 | ROSEWOOD: zanubrutinib + obinutuzumab vs obinutuzumab monotherapy in relapsed/refractory FL |
| [NCT02611323](https://clinicaltrials.gov/study/NCT02611323) | Phase 1/2 | Completed | 133 | Obinutuzumab + polatuzumab vedotin + venetoclax in relapsed/refractory FL |
| [NCT03113422](https://clinicaltrials.gov/study/NCT03113422) | Phase 2 | Completed | 56 | Venetoclax + obinutuzumab + bendamustine as front-line therapy in high tumor burden FL |
| [NCT02600897](https://clinicaltrials.gov/study/NCT02600897) | Phase 1/2 | Completed | 114 | Obinutuzumab + polatuzumab vedotin + lenalidomide in relapsed/refractory FL |
| [NCT01582776](https://clinicaltrials.gov/study/NCT01582776) | Phase 1/2 | Completed | 317 | Obinutuzumab + lenalidomide in FL and relapsed/refractory aggressive B-cell lymphoma |
| [NCT05783596](https://clinicaltrials.gov/study/NCT05783596) | Phase 2 | Active, not recruiting | 47 | Glofitamab + obinutuzumab as first-line treatment of FL and marginal zone lymphoma |

*40 additional trials (mostly early-phase combination studies or currently recruiting) are on file but not shown here.*

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28976863](https://pubmed.ncbi.nlm.nih.gov/28976863/) | 2017 | RCT | New England Journal of Medicine | Primary GALLIUM results: obinutuzumab-based vs rituximab-based chemotherapy in previously untreated advanced FL |
| [29856692](https://pubmed.ncbi.nlm.nih.gov/29856692/) | 2018 | RCT | Journal of Clinical Oncology | GALLIUM sub-analysis: impact of chemotherapy backbone on efficacy/safety of obinutuzumab vs rituximab |
| [37506346](https://pubmed.ncbi.nlm.nih.gov/37506346/) | 2023 | RCT | Journal of Clinical Oncology | ROSEWOOD: zanubrutinib + obinutuzumab vs obinutuzumab monotherapy in relapsed/refractory FL |
| [31296423](https://pubmed.ncbi.nlm.nih.gov/31296423/) | 2019 | RCT | The Lancet Haematology | GALEN: obinutuzumab + lenalidomide in relapsed/refractory FL |
| [37767550](https://pubmed.ncbi.nlm.nih.gov/37767550/) | 2024 | RCT | Haematologica | Polatuzumab vedotin + bendamustine + rituximab or obinutuzumab in relapsed/refractory FL |
| [39830356](https://pubmed.ncbi.nlm.nih.gov/39830356/) | 2024 | Review | Frontiers in Pharmacology | Rapid review of efficacy, safety, and cost-effectiveness of obinutuzumab in FL |
| [31360086](https://pubmed.ncbi.nlm.nih.gov/31360086/) | 2017 | Review | Blood and Lymphatic Cancer: Targets and Therapy | Impact of obinutuzumab alone and in combination for FL |
| [38660754](https://pubmed.ncbi.nlm.nih.gov/38660754/) | 2024 | Review | Turkish Journal of Haematology | Comprehensive review of FL management, including obinutuzumab-based regimens |
| [28276536](https://pubmed.ncbi.nlm.nih.gov/28276536/) | 2016 | Review | Drugs of Today | Overview of obinutuzumab in FL |
| [35180337](https://pubmed.ncbi.nlm.nih.gov/35180337/) | 2022 | Review | Oncology (Williston Park) | Current and emerging therapies in FL, including anti-CD20 antibodies |

*10 additional publications are on file with pending classification.*

---

## New Zealand Market Information

No New Zealand authorization records are currently available — obinutuzumab is not marketed in New Zealand (未上市, 0 licenses on file). Market entry status will need to be confirmed directly with Medsafe.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy / Immunotherapy (glycoengineered anti-CD20 monoclonal antibody; not a conventional cytotoxic chemotherapeutic) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The follicular lymphoma prediction is supported by a pivotal completed Phase 3 RCT (GALLIUM) plus a large, consistent body of follow-on trials and literature (Evidence Level L1), giving this candidate a substantially stronger footing than a pure model prediction. However, obinutuzumab is not currently marketed in New Zealand, and drug-level safety and mechanism-of-action data remain unresolved (DG001: TFDA/Medsafe package insert — Blocking; DG002: MOA — High), so the candidate cannot yet clear a full safety pre-screen.

**To proceed, the following is needed:**
- Medsafe/TFDA package insert (warnings, contraindications, DDI) to resolve DG001 before safety pre-screening
- Structured drug mechanism-of-action data from DrugBank to resolve DG002
- Confirmation of New Zealand market entry pathway/status, since no local license currently exists
- Formal review of the two lower-confidence TxGNN predictions (CLL/SLL molecular subtypes) if those patient subpopulations become a priority — currently Hold, no supporting trials or literature identified
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


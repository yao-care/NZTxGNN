---
layout: default
title: Insulin Aspart
parent: 僅模型預測 (L5)
nav_order: 174
evidence_level: L5
indication_count: 10
---

# Insulin Aspart
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

# Insulin Aspart: From Diabetes Mellitus (Original Indication Unconfirmed) to Type 1 Diabetes Mellitus

## One-Sentence Summary

> Insulin aspart is a rapid-acting recombinant insulin analogue; the evidence pack does not contain a confirmed original-indication text (Blocking Data Gap DG001), though insulin analogues of this class are used generally for diabetes mellitus glycemic control.
> The TxGNN model's top prediction is **Type 1 Diabetes Mellitus**, supported by **10+ clinical trials** and **20 publications**.
> Importantly, the model's own rationale flags this as **not genuine repurposing** — insulin aspart is already a standard-of-care therapy for Type 1 Diabetes; the "prediction" reconfirms an existing pharmacological use rather than proposing a novel indication.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in this evidence pack (Blocking Data Gap DG001 — TFDA/NZ package insert not yet retrieved) |
| Predicted New Indication | Type 1 Diabetes Mellitus |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in this evidence pack (Data Gap DG002). Based on known pharmacology, insulin aspart is a rapid-acting human insulin analogue (class: insulin analogues) that mimics endogenous prandial insulin secretion by binding the insulin receptor to promote peripheral glucose uptake and suppress hepatic glucose output.

Type 1 diabetes mellitus is characterized by absolute insulin deficiency from autoimmune beta-cell destruction, and exogenous insulin replacement — including rapid-acting analogues like insulin aspart — is the established standard of care. Mechanistically, this makes the model's prediction directly applicable.

However, this candidate should be interpreted differently from a typical repurposing case. Per the evidence pack's own repurposing rationale, insulin aspart's applicability to Type 1 Diabetes is not a "new" therapeutic hypothesis but reflects its existing, well-established clinical use; the empty `original_indications` field in this dataset is a data-collection gap, not a clinical fact indicating the drug lacks an approved diabetes indication. This candidate is best read as a validation signal for the TxGNN model rather than a novel repurposing opportunity.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01682902](https://clinicaltrials.gov/study/NCT01682902) | Phase 1 | Completed | 43 | Continuous subcutaneous infusion comparison of NN1218 formulations vs. NovoLog® (insulin aspart) in Type 1 diabetes |
| [NCT00322257](https://clinicaltrials.gov/study/NCT00322257) | Phase 3 | Terminated | 596 | Inhaled mealtime insulin vs. subcutaneous insulin aspart (both with insulin detemir) in Type 1 diabetes; efficacy and pulmonary safety comparison |
| [NCT02546401](https://clinicaltrials.gov/study/NCT02546401) | Phase 3 | Completed | 22 | Pre-meal vs. post-meal insulin aspart bolus timing in Type 1 diabetic patients on insulin pumps |
| [NCT01464099](https://clinicaltrials.gov/study/NCT01464099) | Phase 1 | Completed | 24 | Bioequivalence of NovoLog® 100 U/mL vs. 200 U/mL formulations via CSII + mealtime bolus in Type 1 diabetes |
| [NCT00992537](https://clinicaltrials.gov/study/NCT00992537) | Phase 1 | Completed | 27 | PK/PD comparison of NN5401 (insulin degludec/aspart) vs. NN1250 (degludec) vs. insulin aspart in Type 1 diabetes |
| [NCT05413369](https://clinicaltrials.gov/study/NCT05413369) | Phase 3 | Completed | 582 | iGlarLixi vs. IDegAsp (insulin degludec/aspart) in Chinese Type 2 diabetes inadequately controlled on oral agents |
| [NCT02518945](https://clinicaltrials.gov/study/NCT02518945) | Phase 3 | Completed | 26 | Dapagliflozin added to liraglutide + insulin in Type 1 diabetes; insulin used as background therapy |
| [NCT03660553](https://clinicaltrials.gov/study/NCT03660553) | Phase 4 | Terminated | 7 | Simplified basal-only insulin regimen vs. basal-bolus in elderly patients; small sample, terminated early |
| [NCT03800875](https://clinicaltrials.gov/study/NCT03800875) | Phase 2 | Completed | 24 | Dual-hormone (insulin-pramlintide) closed-loop delivery without carbohydrate counting in Type 1 diabetes |
| [NCT00748137](https://clinicaltrials.gov/study/NCT00748137) | N/A | Unknown | 150 | Bolus insulin dose-calculator card vs. fixed carbohydrate exchange in pediatric Type 1 diabetes |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37863084](https://pubmed.ncbi.nlm.nih.gov/37863084/) | 2023 | RCT | Lancet | ONWARDS 6: once-weekly insulin icodec vs. once-daily degludec as part of basal-bolus regimen (with aspart) in Type 1 diabetes |
| [36623517](https://pubmed.ncbi.nlm.nih.gov/36623517/) | 2023 | RCT | Lancet Diabetes Endocrinol | EXPECT trial: insulin degludec vs. detemir, both combined with insulin aspart, in pregnant women with Type 1 diabetes |
| [21333580](https://pubmed.ncbi.nlm.nih.gov/21333580/) | 2011 | RCT (systematic review) | Diabetes Metab | Systematic review comparing insulin aspart with regular human insulin in Type 1/Type 2 diabetes |
| [41697686](https://pubmed.ncbi.nlm.nih.gov/41697686/) | 2026 | Review | JAMA | General review of Type 1 diabetes pathophysiology, epidemiology, and complications |
| [37290466](https://pubmed.ncbi.nlm.nih.gov/37290466/) | 2023 | Review | Lancet Diabetes Endocrinol | Management of Type 1 diabetes in pregnancy, including insulin technology updates |
| [15871555](https://pubmed.ncbi.nlm.nih.gov/15871555/) | 2003 | Review | Treatments in Endocrinology | Spotlight review on insulin aspart efficacy in Type 1 and Type 2 diabetes |
| [12215068](https://pubmed.ncbi.nlm.nih.gov/12215068/) | 2002 | Review | Drugs | Insulin aspart review: rapid absorption and glycemic control vs. regular human insulin |
| [31345519](https://pubmed.ncbi.nlm.nih.gov/31345519/) | 2019 | Review | Endocrinol Metab Clin North Am | Type 1 diabetes in pregnancy, insulin management strategies |
| [25143741](https://pubmed.ncbi.nlm.nih.gov/25143741/) | 2014 | Review | Vasc Health Risk Manag | Insulin degludec/aspart combination for Type 1 and Type 2 diabetes treatment |
| [18710361](https://pubmed.ncbi.nlm.nih.gov/18710361/) | 2008 | Review | Expert Opin Pharmacother | Biphasic insulin aspart 30 for treatment of Type 1 diabetes mellitus |

## New Zealand Market Information

No authorizations are on file for insulin aspart in this evidence pack — `taiwan_regulatory.market_status` is "Not Marketed" and `total_licenses` is 0. This is consistent with Blocking Data Gap DG001 (TFDA/NZ package insert not yet retrieved) and should be verified directly against the regulator's product register before any market-facing claims are made.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Ten clinical trials and ten publications, including multiple completed Phase 3 RCTs, support insulin aspart's role in Type 1 diabetes management, giving this candidate the strongest evidence level (L1) in the dataset. However, the evidence pack's own rationale indicates this is not a genuine repurposing signal but a reconfirmation of insulin aspart's existing, standard-of-care use in Type 1 diabetes — and a Blocking safety data gap (DG001) and zero New Zealand market authorizations mean the candidate cannot proceed past initial safety review as-is.

**To proceed, the following is needed:**
- TFDA/NZ package insert (warnings, contraindications) — resolves Blocking Data Gap DG001
- Detailed mechanism of action data from DrugBank — resolves High-severity Data Gap DG002
- Drug-drug interaction data (current query status: not found)
- Confirmation of New Zealand registration/market intent, given current status is "Not Marketed"
- Clarification of whether this candidate should be tracked as a repurposing opportunity at all, given it reflects an already-approved use rather than a novel indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


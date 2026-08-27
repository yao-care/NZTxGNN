---
layout: default
title: Insulin Glulisine
parent: 僅模型預測 (L5)
nav_order: 176
evidence_level: L5
indication_count: 10
---

# Insulin Glulisine
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

# Insulin Glulisine: From Diabetes Mellitus (Established Use) to Type 1 Diabetes Mellitus

## One-Sentence Summary

Insulin glulisine is a rapid-acting insulin analogue whose established clinical role is mealtime (bolus) glycemic control in diabetes mellitus. The TxGNN model predicts efficacy for **Type 1 Diabetes Mellitus**, and this is backed by an unusually large body of evidence — **83 clinical trials** and **19 publications** in the evidence pack — because this "prediction" largely overlaps with the drug's already-approved therapeutic use rather than representing a novel repurposing hypothesis.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not specified in the regulatory dataset (0 licenses on file). As a rapid-acting insulin analogue, its established clinical use is mealtime/bolus glycemic control in diabetes mellitus (Type 1 and Type 2). |
| Predicted New Indication | Type 1 Diabetes Mellitus |
| TxGNN Prediction Score | 99.55% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (`[Data Gap]`). Based on known pharmacology, insulin glulisine is a recombinant rapid-acting human insulin analogue that acts as an exogenous replacement for endogenous prandial insulin, lowering postprandial blood glucose by promoting peripheral glucose uptake and suppressing hepatic glucose output through the insulin receptor pathway.

Type 1 diabetes mellitus is a disease of absolute insulin deficiency caused by autoimmune β-cell destruction. Exogenous rapid-acting insulin, including glulisine, is the standard-of-care bolus component of basal-bolus therapy in this population — this is not a novel indication discovered through drug repositioning, but the drug's core, already-established pharmacological use.

This has an important implication for interpretation: the evidence pack's own rationale notes that the TxGNN score here reflects real, well-established biology rather than a new repurposing hypothesis. Practically, this means the "prediction" should be read as a validation signal for the model (it correctly recovers a known indication) rather than as a genuine new-market opportunity — the actionable gap is regulatory (the drug is not currently marketed in New Zealand), not scientific.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00607087](https://clinicaltrials.gov/study/NCT00607087) | Phase 4 | Completed | 289 | Insulin glulisine vs. aspart and lispro via CSII pump in T1D; assessed unexplained hyperglycemia, infusion set occlusion, and hypoglycemia. |
| [NCT00115570](https://clinicaltrials.gov/study/NCT00115570) | Phase 3 | Completed | 572 | Glulisine vs. lispro in children/adolescents with T1D over 26 weeks; safety and efficacy comparison. |
| [NCT00290979](https://clinicaltrials.gov/study/NCT00290979) | Phase 3 | Completed | 250 | Non-inferiority of HMR1964 (glulisine) vs. insulin lispro on HbA1c change in T1D over 28 weeks. |
| [NCT01204593](https://clinicaltrials.gov/study/NCT01204593) | Phase 4 | Completed | 206 | Basal-bolus therapy (glargine + glulisine) in T1D patients previously uncontrolled on any insulin regimen. |
| [NCT00397553](https://clinicaltrials.gov/study/NCT00397553) | Phase 3 | Completed | 104 | Efficacy and safety of subcutaneous glulisine with glargine as basal insulin in T1D. |
| [NCT00526513](https://clinicaltrials.gov/study/NCT00526513) | Phase 4 | Completed | 188 | Apidra (glulisine) combined with basal insulin in T1D and T2D; glycemic control and hypoglycemia incidence. |
| [NCT00539448](https://clinicaltrials.gov/study/NCT00539448) | Phase 4 | Completed | 98 | Effects of glargine and glulisine in T1D over 26 weeks; HbA1c change and dosing. |
| [NCT00545337](https://clinicaltrials.gov/study/NCT00545337) | Phase 3 | Completed | 60 | Efficacy and safety of glulisine (HMR1964) with glargine in T1D over 26 weeks. |
| [NCT00964574](https://clinicaltrials.gov/study/NCT00964574) | Phase 4 | Completed | 68 | Efficacy and safety of subcutaneous glulisine with glargine in T1D, including patient satisfaction. |
| [NCT02518945](https://clinicaltrials.gov/study/NCT02518945) | Phase 3 | Completed | 26 | Dapagliflozin as add-on to liraglutide and insulin in T1D (insulin as background therapy). |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [16308840](https://pubmed.ncbi.nlm.nih.gov/16308840/) | 2005 | RCT | Hormone and Metabolic Research | Multinational RCT (n=672) comparing glulisine to lispro in adults with T1D for efficacy and safety. |
| [41366610](https://pubmed.ncbi.nlm.nih.gov/41366610/) | 2026 | RCT (Phase III) | Diabetes, Obesity & Metabolism | Immunogenicity, efficacy, and safety of a biosimilar glulisine vs. originator product in T1D. |
| [21457066](https://pubmed.ncbi.nlm.nih.gov/21457066/) | 2011 | RCT | Diabetes Technology & Therapeutics | Randomized 3-way crossover comparing glulisine to aspart and lispro via CSII in T1D. |
| [28544684](https://pubmed.ncbi.nlm.nih.gov/28544684/) | 2017 | Cohort | Pediatrics International | 1-year evaluation of glulisine via CSII in 20 children with T1D; improved postprandial glucose. |
| [16123473](https://pubmed.ncbi.nlm.nih.gov/16123473/) | 2005 | Cohort | Diabetes Care | Pharmacokinetics, prandial glucose control, and safety of glulisine vs. regular human insulin in pediatric T1D. |
| [19496630](https://pubmed.ncbi.nlm.nih.gov/19496630/) | 2009 | Review | Drugs | Comprehensive review of glulisine's role in diabetes management. |
| [18076215](https://pubmed.ncbi.nlm.nih.gov/18076215/) | 2008 | Review | Clinical Pharmacokinetics | Review of clinical pharmacokinetics and pharmacodynamics of glulisine. |
| [19614947](https://pubmed.ncbi.nlm.nih.gov/19614947/) | 2009 | Comparative Study | Diabetes, Obesity & Metabolism | Efficacy and safety of glulisine vs. lispro in Japanese patients with T1D. |
| [35933650](https://pubmed.ncbi.nlm.nih.gov/35933650/) | 2022 | Observational | Acta Diabetologica | Real-world comparison of glulisine, lispro, and aspart for CSII pump treatment in T1D. |
| [29159123](https://pubmed.ncbi.nlm.nih.gov/29159123/) | 2016 | PK/PD Study | Journal of Clinical & Translational Endocrinology | Pharmacokinetics/pharmacodynamics of glargine-glulisine basal-bolus therapy in T1D across standardized meals. |

---

## New Zealand Market Information

Insulin glulisine currently holds **no product licenses in New Zealand** (0 authorizations, market status: not marketed).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The clinical trial and literature base for glulisine in T1D is extensive and mature (L1 evidence, multiple completed Phase 3/4 trials), but this reflects confirmation of an already-established core indication rather than a genuine repurposing discovery — the drug is not currently marketed in New Zealand, so the practical barrier is regulatory, not scientific.

**To proceed, the following is needed:**
- Medsafe/NZ package insert data (warnings, contraindications, DDI) — currently a Blocking data gap (DG001)
- Detailed mechanism of action documentation from DrugBank (DG002)
- A formal NZ market-entry/registration assessment, since this is not a novel indication but an unmarketed product gap
- Clarification that lower-ranked predicted indications (ranks 2–10, all L4–L5) are not being pursued: several (e.g., lipodystrophy-related diagnoses) likely reflect insulin as a *causative* factor rather than a treatment, and should be flagged as safety signals rather than repurposing candidates
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Insulin Lispro
parent: Model Prediction Only (L5)
nav_order: 239
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-15
description: "Insulin Lispro: predicted new indication Autoimmune oophoritis (evidence level L5). Approved use: Type 2 diabetes mellitus, type 1 diabetes mellitus…"
monitor_inn: "INSULIN LISPRO"
predicted_indications: ["autoimmune oophoritis", "thiamine-responsive dysfunction syndrome", "focal stiff limb syndrome", "classic stiff person syndrome", "opsismodysplasia", "drug-induced localized lipodystrophy", "pancreatic agenesis", "centrifugal lipodystrophy", "pressure-induced localized lipoatrophy"]
---

# Insulin Lispro
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Insulin Lispro: From Diabetes Mellitus to Autoimmune Oophoritis

## One-Sentence Summary

Insulin lispro is a rapid-acting insulin analogue, originally used to control blood glucose in type 1 and type 2 diabetes mellitus and to treat diabetic ketoacidosis.
The TxGNN model predicts it may be effective for **autoimmune oophoritis**, but **no clinical trials and no publications** support this prediction.
It is a computational signal only and should be treated as hypothesis-generating.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Type 2 diabetes mellitus, type 1 diabetes mellitus, diabetic ketoacidosis (the New Zealand licence records contain no approved indication text) |
| Predicted New Indication | Autoimmune oophoritis |
| TxGNN Prediction Score | 99.78% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Based on known information, insulin lispro is a rapid-acting insulin analogue whose efficacy in diabetes is well established. It replaces or supplements endogenous insulin to lower blood glucose.

The only plausible link to autoimmune oophoritis is indirect. Autoimmune polyendocrine syndromes can occur together with insulin-dependent autoimmune diabetes. This is a co-occurrence of diseases, not a treatment rationale. Insulin would not be expected to modify ovarian autoimmunity.

The very high score (0.998) therefore most likely reflects relationships in the knowledge graph rather than genuine therapeutic potential. Other top predictions show similar patterns (shared diabetic phenotype, shared anti-GAD autoimmunity, or insulin-injection adverse effects).

## Other Predicted Indications Reviewed

| Rank | Predicted Indication | Score | Evidence Level | Assessment |
|------|------|------|------|------|
| 2 | Thiamine-responsive dysfunction syndrome | 99.37% | L5 | Insulin would only manage the diabetes component, not the underlying transporter defect |
| 3 | Focal stiff limb syndrome | 99.36% | L5 | Link is shared anti-GAD autoimmunity; no therapeutic rationale |
| 4 | Classic stiff person syndrome | 99.36% | L5 | Insulin would only treat comorbid diabetes |
| 5 | Opsismodysplasia | 99.34% | L5 | Pathway-level link (INPPL1/SHIP2) only; hypothesis |
| 6 | Drug-induced localized lipodystrophy | 99.09% | L5 | Insulin is more likely a cause than a treatment (safety signal) |
| 7 | Pancreatic agenesis | 99.09% | L4 | Insulin replacement is rational but overlaps with existing diabetes use; evidence indirect |
| 8 | Centrifugal lipodystrophy | 99.04% | L5 | No rationale for benefit |
| 9 | Pressure-induced localized lipoatrophy | 99.03% | L5 | Likely adverse-effect association; insulin may contribute |

Pancreatic agenesis is the only prediction with a mechanistically sound rationale, but it is essentially standard insulin replacement rather than true repurposing.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

For the top-ranked prediction (autoimmune oophoritis), currently no related literature is available.

The only publication found across the predictions relates to pancreatic agenesis (rank 7):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12150359](https://pubmed.ncbi.nlm.nih.gov/12150359/) | 2002 | Review | Journal of the American Pharmaceutical Association | General review of insulin therapy in type 2 diabetes and how to start it; not specific to pancreatic agenesis |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 490865 | Humalog (cancelled) | Inj 100 u per ml, 3 ml | Not recorded |
| 2702983 | Humalog (cancelled) | Inj 100 u per ml, 10 ml vial | Not recorded |

## Safety Considerations

- **Drug Interactions**: The interaction screen returned 100 interacting drugs in total. The list reviewed here mostly shows moderate-level interactions, with a few minor ones. Examples include:
  - Sympathomimetics and beta-agonists: phenylephrine, pseudoephedrine, epinephrine, salbutamol, formoterol
  - Beta-blocker: acebutolol
  - Other glucose-lowering agents: metformin, alogliptin, albiglutide
  - Corticosteroids: hydrocortisone (moderate); topical hydrocortisone and alclometasone (minor)
  - Hormonal agents: estradiol, ethinylestradiol, levonorgestrel
  - Others: acetazolamide, hydrochlorothiazide, doxycycline, ethanol, alpelisib
- **Injection-site effects**: Localized lipodystrophy (lipohypertrophy and lipoatrophy) is a recognized effect of injected insulin. It relates directly to predictions ranked 6 and 9.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction, autoimmune oophoritis, rests only on a computational score, with no trials or literature and no plausible mechanism by which insulin lispro would treat it. Insulin lispro has no currently valid New Zealand licence. Several other predictions reflect adverse-effect associations or comorbid diabetes rather than genuine repurposing potential.

**To proceed, the following is needed:**
- Mechanism of action data for insulin lispro
- Package insert warnings and contraindications (not available in the data compiled for this report)
- Any clinical or preclinical evidence linking insulin to ovarian autoimmunity
- Clarification of current New Zealand licensing, since both Humalog licences are cancelled
- If pursued at all, a focused review of pancreatic agenesis, where insulin replacement is already standard care, rather than the high-scoring but unsupported predictions
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


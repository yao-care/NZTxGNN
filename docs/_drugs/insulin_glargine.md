---
layout: default
title: Insulin Glargine
parent: Model Prediction Only (L5)
nav_order: 237
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-30
monitor_inn: "INSULIN GLARGINE"
predicted_indications: ["autoimmune oophoritis", "thiamine-responsive dysfunction syndrome", "focal stiff limb syndrome", "classic stiff person syndrome", "opsismodysplasia", "pancreatic agenesis", "drug-induced localized lipodystrophy", "centrifugal lipodystrophy", "pressure-induced localized lipoatrophy", "idiopathic localized lipodystrophy"]
description: "Insulin Glargine: predicted new indication Autoimmune oophoritis (evidence level L5). Approved use: Not recorded in the local data"
---

# Insulin Glargine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

## Insulin Glargine: Autoimmune oophoritis
{: .text-alpha }

## One-Sentence Summary

Insulin glargine is a long-acting insulin analogue, originally used to treat type 1 and type 2 diabetes mellitus and diabetic ketoacidosis.
The TxGNN model predicts it may be effective for **autoimmune oophoritis**, but **no clinical trials and no publications** currently support this prediction.
It is a model-only signal and most likely reflects the association between autoimmune ovarian disease and type 1 diabetes, not a real therapeutic effect.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Autoimmune oophoritis |
| TxGNN Prediction Score | 0.9988 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, insulin glargine is a long-acting basal insulin analogue, its efficacy in diabetes mellitus is well established, and its effect is glycaemic control through insulin replacement.

Autoimmune oophoritis often occurs together with autoimmune polyendocrine syndromes, which can include type 1 diabetes. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. There is no demonstrated mechanism by which insulin would treat ovarian autoimmunity itself. At best, insulin would manage a co-existing diabetes.

**Other top-ranked predictions** show the same pattern. None has a clinical trial, and none has direct evidence for insulin glargine:

| Rank | Predicted Indication | Score | Evidence Level | Assessment |
|------|------|------|------|------|
| 2 | Thiamine-responsive dysfunction syndrome | 0.9961 | L5 | Insulin treats the diabetes component only |
| 3 | Focal stiff limb syndrome | 0.9960 | L5 | Linked through anti-GAD autoimmunity and type 1 diabetes; no mechanism for the neurological disorder |
| 4 | Classic stiff person syndrome | 0.9960 | L5 | Same as above |
| 5 | Opsismodysplasia | 0.9959 | L5 | Insulin/PI3K pathway proximity only |
| 6 | Pancreatic agenesis | 0.9943 | L5 | Insulin replacement is coherent, but this overlaps with the existing indication and is not a new mechanism. The four retrieved papers are indirect. |
| 7 | Drug-induced localized lipodystrophy | 0.9942 | L5 | Insulin injection is itself a known cause, so this is likely a confounded association and could worsen the condition |
| 8 | Centrifugal lipodystrophy | 0.9939 | L5 | TxGNN model score only |
| 9 | Pressure-induced localized lipoatrophy | 0.9938 | L5 | Mechanical cause, not insulin deficiency |
| 10 | Idiopathic localized lipodystrophy | 0.9934 | L5 | Unsupported; injection-site lipodystrophy is a known adverse effect |

## Clinical Trial Evidence

Currently no related clinical trials registered for autoimmune oophoritis.

## Literature Evidence

Currently no related literature available for autoimmune oophoritis.

For reference, the only indication with retrieved publications is the rank 6 prediction, pancreatic agenesis. These papers are indirect and none evaluates insulin glargine in that condition:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12150359](https://pubmed.ncbi.nlm.nih.gov/12150359/) | 2002 | Review | J Am Pharm Assoc | Practical aspects of starting insulin therapy in type 2 diabetes |
| [19322513](https://pubmed.ncbi.nlm.nih.gov/19322513/) | 2009 | Review | Acta Diabetol | Secondary diabetes in endocrine disorders such as acromegaly and Cushing syndrome |
| [32871938](https://pubmed.ncbi.nlm.nih.gov/32871938/) | 2020 | Case report | Medicine | MODY5 treated with a GLP-1 receptor agonist |
| [25818213](https://pubmed.ncbi.nlm.nih.gov/25818213/) | 2015 | Veterinary cohort (non-human) | J Vet Intern Med | Pancreatic enzymes and ultrasound findings in diabetic cats |

## New Zealand Market Information

All three licences are valid, so the product is currently marketed in New Zealand.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2146746 | Lantus | Inj 100 u per ml, 10 ml | Not available (valid) |
| 2146754 | Lantus | Inj 100 u per ml, 3 ml | Not available (valid) |
| 2275406 | Lantus SoloStar | Inj 100 u per ml, 3 ml disposable pen | Not available (valid) |

## Safety Considerations

- **Drug Interactions**: 100 interactions were recorded. The list reviewed here includes mostly moderate-level and a few minor-level interactions. Main ones:
  - Other glucose-lowering drugs (metformin, pioglitazone, alogliptin, albiglutide), with additive glucose-lowering effects
  - Sympathomimetics and beta-agonists (epinephrine, phenylephrine, pseudoephedrine, salbutamol, formoterol), which can reduce glucose control
  - Corticosteroids (hydrocortisone) and oestrogens (estradiol, ethinylestradiol), which can raise glucose
  - Beta-blockers (acebutolol), which may mask hypoglycaemia symptoms
  - Others: acetazolamide, doxycycline, hydrochlorothiazide, ethanol, alpelisib
  - Minor: topical corticosteroids (hydrocortisone, alclometasone)
- For the lipodystrophy predictions in particular, injection-site lipodystrophy is a recognised adverse effect of insulin, so these predictions carry a risk of harm.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction is supported only by a model score, with no trials or publications (Evidence Level L5). The score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. The drug is marketed in New Zealand, with 3 valid licences of 3.

**To proceed, the following is needed:**
- Direct clinical or mechanistic evidence for insulin glargine in autoimmune oophoritis, or confirmation that the signal is only a diabetes-comorbidity artefact
- Mechanism of action data (currently unavailable)
- Package insert warnings and contraindications from Medsafe
- Confirmation of current New Zealand regulatory status (3 valid licences of 3)
- If a repurposing direction is pursued, consider pancreatic agenesis first, but only as an extension of existing insulin replacement and not as a new mechanism
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


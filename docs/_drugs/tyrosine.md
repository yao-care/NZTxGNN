---
layout: default
title: Tyrosine
parent: Model Prediction Only (L5)
nav_order: 470
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Tyrosine: predicted new indication Cauda equina syndrome (evidence level L5). Approved use: Not recorded"
monitor_inn: "TYROSINE"
predicted_indications: ["cauda equina syndrome", "obsolete neurogenic bladder (disease)", "angle-closure glaucoma", "hyperthyroidism", "postural orthostatic tachycardia syndrome", "hyperthyroxinemia", "resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta", "aqueous misdirection", "traumatic glaucoma", "neovascular glaucoma"]
---

# Tyrosine
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

# Tyrosine: From an Unspecified Original Indication to Cauda Equina Syndrome

## One-Sentence Summary

Tyrosine is an amino acid and precursor of catecholamines and thyroid hormones. Its only New Zealand authorization (a powder sachet product) has been cancelled, and no approved indication is recorded.
The TxGNN model predicts it may be effective for **cauda equina syndrome**, but this is a graph-based prediction only, with **0 clinical trials** and **0 publications** supporting this specific indication.
Evidence is very weak, so the recommendation is to hold.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded (the Medsafe license has no approved indication text) |
| Predicted New Indication | Cauda equina syndrome |
| TxGNN Prediction Score | 99.77% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, tyrosine is a catecholamine precursor (dopamine, norepinephrine) and a building block of thyroid hormones. No original indication is recorded in the New Zealand license data, so the link between an original use and the new indication cannot be assessed.

For cauda equina syndrome, the data compiled for this report found no mechanistic link. The condition is a compressive injury to the lumbosacral nerve roots and is treated surgically. The high score (0.998) reflects a graph-based association only, and no study connects tyrosine to nerve-root injury.

Several other predicted indications were also reviewed (see the table below). None has convincing support. Two noteworthy points:
- **Hyperthyroidism**: tyrosine is a substrate for thyroid hormone synthesis, so supplying it would not be expected to treat hormone excess and could theoretically worsen it.
- **Postural orthostatic tachycardia syndrome (POTS)**: the only paper is a single case report using alpha-methyl-para-tyrosine, a tyrosine hydroxylase *inhibitor*. This is opposite pharmacology to tyrosine supplementation.

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Trials | Publications |
|------|------|------|------|------|------|
| 1 | Cauda equina syndrome | 99.77% | L5 | 0 | 0 |
| 2 | Obsolete neurogenic bladder (disease) | 99.55% | L5 | 0 | 0 |
| 3 | Angle-closure glaucoma | 99.54% | L5 | 0 | 4 |
| 4 | Hyperthyroidism | 99.51% | L5 | 1 | 20 |
| 5 | Postural orthostatic tachycardia syndrome | 99.46% | L4 | 0 | 1 |
| 6 | Hyperthyroxinemia | 99.46% | L5 | 0 | 3 |
| 7 | Resistance to thyroid hormone (TRβ mutation) | 99.40% | L5 | 0 | 4 |
| 8 | Aqueous misdirection | 99.35% | L5 | 0 | 0 |
| 9 | Traumatic glaucoma | 99.35% | L5 | 0 | 1 |
| 10 | Neovascular glaucoma | 99.32% | L5 | 0 | 4 |

## Clinical Trial Evidence

Currently no related clinical trials are registered for cauda equina syndrome.

For the rank 4 prediction (hyperthyroidism), one trial exists, but it does not test tyrosine alone:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06264544](https://clinicaltrials.gov/study/NCT06264544) | Not applicable | Not yet recruiting | 150 | Placebo-controlled trial of a zinc, selenium and L-tyrosine supplement to prevent thyrotoxicosis in adults with elevated TPOAb and the SEPP1 r25191G/A polymorphism. It is a combination supplement study and provides no efficacy evidence for tyrosine. |

## Literature Evidence

Currently no related literature is available for cauda equina syndrome.

Selected publications retrieved for other predicted indications (all indirect; none tests tyrosine as a treatment):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36848916](https://pubmed.ncbi.nlm.nih.gov/36848916/) | 2023 | Review | Lancet Diabetes Endocrinol | General review of hyperthyroidism causes, diagnosis and management |
| [34913664](https://pubmed.ncbi.nlm.nih.gov/34913664/) | 2021 | Review | Am Fam Physician | Evaluation and treatment of thyroiditis |
| [42812246](https://pubmed.ncbi.nlm.nih.gov/42812246/) | 2026 | Cohort | Front Med | Plasma amino acid alterations in early primary angle-closure glaucoma; an association, not evidence of benefit |
| [27758063](https://pubmed.ncbi.nlm.nih.gov/27758063/) | 2017 | Cohort | Clin Exp Ophthalmol | Serum autoantibody course after acute angle-closure glaucoma attack |
| [39063020](https://pubmed.ncbi.nlm.nih.gov/39063020/) | 2024 | Case report | Int J Mol Sci | Chronic fatigue syndrome case responding to alpha-methyl-p-tyrosine (a tyrosine hydroxylase inhibitor); not about POTS or tyrosine supplementation |
| [10579356](https://pubmed.ncbi.nlm.nih.gov/10579356/) | 1999 | Basic research | Endocrinology | Novel thyroid hormone receptor beta mutations impairing ligand binding |
| [35087972](https://pubmed.ncbi.nlm.nih.gov/35087972/) | 2021 | Review | Rom J Ophthalmol | Review of neovascular glaucoma causes and treatment |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2668742 | Tyrosine1000 | Powder, 4 g sachets | Not recorded (license status: cancelled) |

## Safety Considerations

Please refer to the package insert for safety information. No warnings or contraindications were available in the data compiled for this report, and no drug interaction records were found. Since the product license is cancelled, a current package insert may not be available from Medsafe.

One theoretical concern is worth noting: because tyrosine is a thyroid hormone precursor, use in hyperthyroid or thyroid-related conditions would need careful consideration.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
All predicted indications are supported only by the model score (L4 at best, mostly L5). No trial or study shows tyrosine benefits cauda equina syndrome, and the plausible mechanisms are either absent or point the wrong way (hyperthyroidism, POTS). The only New Zealand authorization is cancelled.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data for tyrosine
- A credible mechanistic rationale for the top predicted indication, plus any preclinical or clinical studies testing tyrosine directly
- Clarification of the original approved indication and the reason for license cancellation
- Review of whether the "obsolete neurogenic bladder" term reflects an ontology mapping problem rather than a real target
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


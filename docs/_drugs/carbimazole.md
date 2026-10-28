---
layout: default
title: Carbimazole
parent: Model Prediction Only (L5)
nav_order: 85
evidence_level: L5
indication_count: 2
monitor_inn: "CARBIMAZOLE"
predicted_indications: ["resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta", "hyperthyroxinemia"]
last_modified_at: 2026-10-27
description: "Carbimazole: predicted new indication resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta (evidence level L5). Approved use: Not recorded in the local data"
---

# Carbimazole
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

## Carbimazole: thionamide antithyroid prodrug (thyroid peroxidase inhibitor) → resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta
{: .text-alpha }

## One-Sentence Summary

Carbimazole is an antithyroid drug that is converted in the body to methimazole and reduces thyroid hormone production.
The TxGNN model predicts it for **resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta**, but the data compiled for this report contain **no clinical trials** and only **1 publication**, a case report that does not show a benefit of carbimazole.
The mechanistic rationale is weak and potentially harmful, so the recommendation is **Hold**.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta |
| TxGNN Prediction Score | 0.9971 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the compiled data. Carbimazole is known to be a prodrug of methimazole, which inhibits thyroid peroxidase and so lowers the synthesis of thyroid hormones.

In resistance to thyroid hormone caused by a thyroid hormone receptor beta mutation, the defect lies at the receptor. Patients typically show elevated free T4 with a thyrotropin (TSH) level that is not suppressed. In theory, lowering hormone synthesis could reduce circulating T4 and T3. However, it would also remove negative feedback and raise TSH further. That risks goitre and stimulation of the thyrotrophs in the pituitary, and pituitary resistance would make this worse.

The high TxGNN score of 0.9971 is a computational output only. Nothing in the compiled data shows that carbimazole helps people with this condition, and the mechanistic reasoning points toward possible harm rather than benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [24165508](https://pubmed.ncbi.nlm.nih.gov/24165508/) | 2013 | Case report | BMJ Case Reports | A young man had persistently elevated free T4 (25–35.7 pmol/L) with non-suppressed TSH and a firm goitre. He was treated intermittently with carbimazole for about 10 years. The report is about the differential diagnosis of this laboratory pattern and does not show a clinical benefit of carbimazole. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form |
|---------|------|------|
| 594423 | Neo-Mercazole | Tab 5 mg |

## Safety Considerations

Please refer to the package insert for safety information.

Beyond the lack of label data, a pharmacological concern applies to this prediction. Suppressing thyroid hormone output in a receptor-level resistance state could further raise TSH and promote goitre.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone. The single retrieved article is a case report on diagnosis and shows no benefit of carbimazole, so the evidence level is L5. The mechanism suggests a risk of worsening TSH stimulation rather than a therapeutic gain.

**To proceed, the following is needed:**
- Clinical or mechanistic studies that directly test an antithyroid thionamide in this receptor-resistance condition
- Mechanism-of-action data for carbimazole and its active metabolite
- The package insert warnings and contraindications for the New Zealand product
- A specialist endocrinology assessment of whether TSH elevation and goitre risk make this use unacceptable
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


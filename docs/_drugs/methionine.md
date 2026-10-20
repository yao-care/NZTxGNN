---
layout: default
title: Methionine
parent: Model Prediction Only (L5)
nav_order: 290
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Methionine: predicted new indication Acne (disease) (evidence level L5). Approved use: Not recorded"
monitor_inn: "methionine"
predicted_indications: ["acne (disease)", "severe nonproliferative diabetic retinopathy", "craniostenosis cataract", "mature cataract", "nuclear senile cataract", "cortical cataract", "tetanic cataract", "immature cataract", "diabetic cataract"]
---

# Methionine
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

# Methionine: From No Registered Indication to Acne

## One-Sentence Summary

Methionine is an essential sulfur-containing amino acid. In New Zealand it was only found as a component of a metabolic-formula powder (MMA/PA Anamix Infant) whose licence is now cancelled, and no approved indication is recorded.
The TxGNN model predicts it may be effective for **acne**, but there are **0 clinical trials** and only **2 indirect publications** behind this prediction.
This is a computational prediction with very weak supporting evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded (no approved indication text in the licence) |
| Predicted New Indication | Acne (disease) |
| TxGNN Prediction Score | 99.9996% |
| Evidence Level | L4 (indirect mechanism-related literature only; no clinical trials) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, methionine is an essential amino acid and a key part of the methionine cycle, in which homocysteine is a metabolite. The only product found in New Zealand is a specialised amino-acid formula, and no efficacy in any original indication is documented in the data compiled for this report.

The link to acne is weak. One cohort study reported elevated plasma homocysteine in patients taking isotretinoin for cystic acne. Homocysteine is a methionine-cycle metabolite, but this finding concerns a drug side effect, not methionine as a treatment. The other paper is a case report of a neonatal MTHFR mutation with encephalopathy, hair loss and neonatal acne, which is not about treating acne. The very high TxGNN score reflects network proximity in a knowledge graph and is not clinical evidence.

Other predicted indications are mostly eye-related, such as cataract subtypes and diabetic retinopathy. Among them, **diabetic cataract** has the most plausible pathway-level rationale, through methionine sulfoxide reductase B1 (MsrB1), which protects lens cells from oxidative stress. It is classed as a research question, but it does not show that methionine supplementation helps.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [11277950](https://pubmed.ncbi.nlm.nih.gov/11277950/) | 2001 | Cohort | Int J Dermatol | Plasma homocysteine was elevated in patients on isotretinoin for cystic acne; this concerns a drug effect, not methionine therapy |
| [39357918](https://pubmed.ncbi.nlm.nih.gov/39357918/) | 2024 | Case report | BMJ Case Reports | Neonate with a novel MTHFR mutation, encephalopathy, hair loss, marfanoid features and neonatal acne; not about acne treatment |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2386313 | MMA/PA Anamix Infant | Powder, can | Not recorded (licence cancelled) |

---

## Safety Considerations

- **Disease associations from the knowledge graph (not label text):** methionine is linked to contraindication-type relationships with many conditions. The main groups are:
  - Cardiovascular: hypertension, heart disease, hyperlipidaemia, ventricular fibrillation, digitalis poisoning
  - Kidney: chronic kidney disease, end-stage renal failure, nephrolithiasis, nephrocalcinosis, acute urate nephropathy
  - Metabolic: diabetes mellitus, diabetic ketoacidosis, hypercalcaemia, phosphorus metabolism disorders, sarcoidosis
  - Amino-acid disorders: phenylketonuria and other phenylalanine metabolism disorders
- **Drug Interactions (5 recorded):**
  - Moderate: activated charcoal
  - Minor: ephedrine, pseudoephedrine, mexiletine, flecainide

Package insert warnings and contraindications were not available, so please refer to the package insert for label-based safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone. There are no clinical trials, the literature is indirect, and the only New Zealand licence is cancelled with no recorded indication. The knowledge-graph safety signals (cardiovascular, kidney, metabolic) also call for caution.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data (for example from DrugBank)
- Direct preclinical or clinical evidence that methionine has a benefit in acne, or in diabetic cataract as the most plausible alternative
- Confirmation of a suitable route and formulation, since the only product found was a cancelled formula powder
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


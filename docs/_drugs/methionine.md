---
layout: default
title: Methionine
parent: Model Prediction Only (L5)
nav_order: 290
evidence_level: L5
indication_count: 10
monitor_inn: "methionine"
predicted_indications: ["acne (disease)", "severe nonproliferative diabetic retinopathy", "craniostenosis cataract", "mature cataract", "diabetes mellitus type 2 associated cataract", "nuclear senile cataract", "cortical cataract", "tetanic cataract", "immature cataract", "diabetic cataract"]
last_modified_at: 2026-10-30
description: "Methionine: predicted new indication Acne (disease) (evidence level L5). Approved use: Not stated in the licence record"
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

## Methionine: From an Unspecified Original Use to Acne (Disease)
{: .text-alpha }

## One-Sentence Summary

Methionine is an essential sulfur-containing amino acid. In New Zealand it is licensed as a single-ingredient component of the product MMA/PA Anamix Infant, but the licence record contains no approved indication text.
The TxGNN model predicts it may be effective for **acne (disease)**, with **no clinical trials** and **2 publications** that are only indirectly related.
This is a computational prediction without supporting clinical data.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the licence record |
| Predicted New Indication | Acne (disease) |
| TxGNN Prediction Score | 1.0000 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 (valid: 1) |
| Recommended Decision | Hold |

The evidence level is L5. The two retrieved papers do not directly study methionine as a treatment for acne (one links homocysteine to isotretinoin therapy, the other is an unrelated MTHFR mutation case report), so the L4 label in the source data is not supported.

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Methionine is an essential amino acid involved in the methionine cycle, which produces homocysteine, and in methylation and transsulfuration pathways. The licence record does not state an approved indication, so the relationship between an original use and acne cannot be assessed.

The retrieved literature does not establish a mechanistic link. One cohort study found elevated plasma homocysteine (a methionine-cycle metabolite) in patients on isotretinoin for cystic acne. That is an association with a drug effect, not with the disease itself. The other paper is a case report of an MTHFR mutation with neonatal encephalopathy, which happened to mention neonatal acne among its skin findings.

The very high TxGNN score is a model output only. It should be treated as a hypothesis to examine, not as evidence of benefit.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [11277950](https://pubmed.ncbi.nlm.nih.gov/11277950/) | 2001 | Cohort | Int J Dermatol | Plasma homocysteine was elevated in patients taking isotretinoin for cystic acne; this is a drug-effect association, not evidence for methionine as a treatment |
| [39357918](https://pubmed.ncbi.nlm.nih.gov/39357918/) | 2024 | Case report | BMJ Case Reports | Neonate with a novel MTHFR mutation, encephalopathy, hair loss and marfanoid features; neonatal acne was one of several skin findings |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2386313 | MMA/PA Anamix Infant | Powder, can | Not stated in the licence record |

The data compiled for this report show 1 valid single-ingredient licence. Any score on this page comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence; it is not an approved indication, and none is claimed here.

---

## Safety Considerations

- **Knowledge-graph contraindication relations** (recorded in the TxGNN knowledge graph, not taken from label text): hypertension and other hypertensive disorders, kidney disease (including chronic kidney disease and end-stage renal failure), diabetes mellitus and diabetic ketoacidosis, hypercalcemia-related conditions, sarcoidosis, nephrocalcinosis, nephrolithiasis, acute urate nephropathy, heart and cardiovascular disease, hyperlipidemia (including familial), digitalis poisoning, ventricular fibrillation, phenylketonuria and other disorders of phenylalanine metabolism. These should be verified against the product label.
- **Drug Interactions** (5 recorded):
  - Moderate: activated charcoal.
  - Minor: ephedrine, pseudoephedrine, mexiletine, flecainide.

Please refer to the package insert for label warnings and contraindications, which were not available in the data compiled for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The acne prediction rests on a model score alone. No trials exist, and the two papers retrieved are indirect and do not test methionine. The knowledge-graph contraindication relations (cardiovascular, renal, metabolic) and the missing label data add further caution.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data for methionine
- Literature or trial data that directly examine methionine, or the methionine-homocysteine pathway, in acne
- Clarification of the approved indication for the licensed product
- A review of the other predicted indications. Diabetic cataract has the most relevant indirect mechanistic literature (methionine sulfoxide reductase studies in lens cells) but is still preclinical and does not test methionine administration.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


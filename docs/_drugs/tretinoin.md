---
layout: default
title: Tretinoin
parent: Model Prediction Only (L5)
nav_order: 466
evidence_level: L5
indication_count: 9
last_modified_at: 2026-11-19
monitor_inn: "TRETINOIN"
predicted_indications: ["rheumatoid nodulosis", "rheumatoid factor-positive polyarticular juvenile idiopathic arthritis", "juvenile idiopathic arthritis", "juvenile chronic polyarthritis", "juvenile arthritis due to defect in LACC1", "osteoarthritis", "pseudoachondroplasia", "acromesomelic dysplasia, Hunter-Thompson type", "Quinquaud's folliculitis decalvans"]
description: "Tretinoin: predicted new indication Rheumatoid nodulosis (evidence level L5). Approved use: Not recorded in the local data"
---

# Tretinoin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

## Tretinoin: Rheumatoid nodulosis
{: .text-alpha }

## One-Sentence Summary

Tretinoin is a retinoid originally used for acne, acute promyelocytic leukaemia, acquired keratosis and palmoplantar keratoderma.
The TxGNN model predicts it may be effective for **rheumatoid nodulosis**,
but there are **0 clinical trials** and **0 relevant publications** supporting this direction, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Rheumatoid nodulosis |
| TxGNN Prediction Score | 0.9984 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for tretinoin: "Tretinoin induces cytodifferentiation and decreased proliferation of APL cells in culture and in vivo. In APL patients, tretinoin treatment produces an initial maturation of the primitive promyelocytes derived from the leukemic clone, followed by a repopulation of the bone marrow and peripheral blood by normal, polyclonal hematopoietic cells in patients achieving complete remission (CR)." ([DailyMed: tretinoin prescribing information, Par Health USA, LLC](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=9c4ae9d9-c2a0-4995-a27a-e3f0f0284db9), label version 2026-08-20; accessed 2026-11-19.) Based on known information, tretinoin is a retinoid (a vitamin A derivative) whose efficacy in acne, keratinisation disorders and acute promyelocytic leukaemia is established. Mechanistically, retinoids may modulate immune cell differentiation and inflammatory signalling, which could plausibly relate to rheumatoid-type inflammation.

No data compiled for this report directly supports that link. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. The other top predictions (juvenile idiopathic arthritis variants, osteoarthritis, rare skeletal dysplasias, and folliculitis decalvans) all have the same evidence level (L5, model prediction only). Of these, folliculitis decalvans is the most biologically plausible, given tretinoin's established use in follicular and keratinisation disorders. It is still an unvalidated hypothesis.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for rheumatoid nodulosis.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 235229 | Vesanoid | Tretinoin | Cap 10 mg | Yes |
| 2358751 | ReTrieve | Tretinoin | Crm 0.5 mg per g | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Differentiation-inducing retinoid (targeted therapy) used in acute promyelocytic leukaemia; not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions (typically blood counts, liver function and lipids for systemic use) |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Disease associations flagged in the knowledge graph (not label text):** sunburn, anxiety and other neurotic or mental disorders (including psychotic, schizophreniform and schizoaffective disorders), dysthymic disorder, kidney disease, liver disease, gallbladder disease, pancreatitis, congestive heart failure, adrenocortical insufficiency and Addison disease, Crohn disease and inflammatory bowel disease, atopic eczema, osteoporosis (including postmenopausal), cutaneous T-cell lymphomas (mycosis fungoides and variants), diabetes mellitus and diabetic ketoacidosis, and pyoureter.
- **Drug interactions:** 100 interactions were found in total. Major-level interactions include **doxycycline, tetracycline, minocycline** and **vitamin A**. Moderate-level interactions include **aprepitant, clarithromycin** and **naltrexone**. One minor interaction is levofloxacin. Others, such as proton pump inhibitors and corticosteroids, have unclassified severity.
- **Paediatric and skeletal concerns:** systemic retinoids carry teratogenicity, bone and growth risks. These are relevant to the juvenile arthritis and skeletal dysplasia predictions.

For labelled warnings and contraindications, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for rheumatoid nodulosis has a very high model score but no supporting trials or literature (L5).

**To proceed, the following is needed:**
- Mechanism of action data and a specific mechanistic link to rheumatoid nodulosis
- Preclinical or clinical evidence in rheumatoid nodulosis or related autoimmune arthritis
- Medsafe package insert warnings and contraindications
- Evaluation of whether any topical or other route is feasible for the target condition
- If a lead is wanted, a separate evaluation of folliculitis decalvans, the most plausible predicted dermatological indication

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Currently, detailed mechanism of action data is not available" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: tretinoin prescribing information, Par Health USA, LLC](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=9c4ae9d9-c2a0-4995-a27a-e3f0f0284db9) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


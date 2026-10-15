---
layout: default
title: Tretinoin
parent: Model Prediction Only (L5)
nav_order: 466
evidence_level: L5
indication_count: 10
---

# Tretinoin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Tretinoin: From Acne and Acute Promyelocytic Leukaemia to Rheumatoid Nodulosis

## One-Sentence Summary

Tretinoin is a retinoid originally used for acne, acute promyelocytic leukaemia, acquired keratosis and palmoplantar keratoderma.
The TxGNN model predicts it may be effective for **rheumatoid nodulosis**,
but there are **0 clinical trials** and **0 relevant publications** supporting this direction, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Acne, acute promyelocytic leukaemia, acquired keratosis, palmoplantar keratoderma (the approved indication text on the New Zealand licences is blank, so these come from the knowledge graph) |
| Predicted New Indication | Rheumatoid nodulosis |
| TxGNN Prediction Score | 99.84% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, tretinoin is a retinoid (a vitamin A derivative) whose efficacy in acne, keratinisation disorders and acute promyelocytic leukaemia is established. Mechanistically, retinoids may modulate immune cell differentiation and inflammatory signalling, which could plausibly relate to rheumatoid-type inflammation.

No data compiled for this report directly supports that link. The high score most likely reflects the drug's proximity to related autoimmune and arthritis nodes in the knowledge graph rather than demonstrated benefit. The other top predictions (juvenile idiopathic arthritis variants, spondyloarthropathy susceptibility, osteoarthritis, rare skeletal dysplasias, and folliculitis decalvans) all have the same evidence level (L5, model prediction only). Of these, folliculitis decalvans is the most biologically plausible, given tretinoin's established use in follicular and keratinisation disorders. It is still an unvalidated hypothesis.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for rheumatoid nodulosis.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 235229 | Vesanoid (cancelled) | Capsule 10 mg | Not stated |
| 2358751 | ReTrieve (cancelled) | Cream 0.5 mg per g | Not stated |

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
The prediction for rheumatoid nodulosis has a very high model score but no supporting trials or literature (L5). Tretinoin has no currently valid New Zealand licence, and its safety profile (teratogenicity, many knowledge-graph contraindications, and major drug interactions) argues against advancing without supporting evidence.

**To proceed, the following is needed:**
- Mechanism of action data and a specific mechanistic link to rheumatoid nodulosis
- Preclinical or clinical evidence in rheumatoid nodulosis or related autoimmune arthritis
- Medsafe package insert warnings and contraindications
- Evaluation of whether any topical or other route is feasible for the target condition
- If a lead is wanted, a separate evaluation of folliculitis decalvans, the most plausible predicted dermatological indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


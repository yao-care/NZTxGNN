---
layout: default
title: Taliglucerase Alfa
parent: Model Prediction Only (L5)
nav_order: 436
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-30
description: "Taliglucerase Alfa: predicted new indication Hurler syndrome (evidence level L5). Approved use: Gaucher disease"
monitor_inn: "TALIGLUCERASE ALFA"
predicted_indications: ["Hurler syndrome", "Scheie syndrome", "benign neoplasm of adrenal gland", "autosomal ichthyosis syndrome with fatal disease course", "cholesteryl ester storage disease"]
---

# Taliglucerase Alfa
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

## Taliglucerase Alfa: From Gaucher Disease to Hurler Syndrome
{: .text-alpha }

## One-Sentence Summary

Taliglucerase alfa is a recombinant glucocerebrosidase enzyme replacement therapy, originally used to treat Gaucher disease.
The TxGNN model predicts it may be effective for **Hurler syndrome**, but there are **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Gaucher disease |
| Predicted New Indication | Hurler syndrome |
| TxGNN Prediction Score | 0.9952 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the source database. Based on known information, taliglucerase alfa is a recombinant glucocerebrosidase (beta-glucosidase) that hydrolyses glucosylceramide, and its efficacy in Gaucher disease is established.

Hurler syndrome (MPS I) is caused by a deficiency of alpha-L-iduronidase, which leads to accumulation of glycosaminoglycans (dermatan sulfate and heparan sulfate). Both Gaucher disease and Hurler syndrome are lysosomal storage disorders, so the high score most likely reflects similarities the TxGNN model (deep learning) learned between related diseases, not biological evidence.

However, glucocerebrosidase does not break down the substrates that accumulate in MPS I, so there is no direct enzymatic rationale. Secondary glycolipid accumulation does occur in MPS, but this does not justify treatment with this enzyme. The link is a network-based prediction only.

Other predictions from the same model show a similar pattern:

| Predicted Indication | Score | Assessment |
|------|------|------|
| Scheie syndrome | 0.9929 | Attenuated MPS I; same substrate mismatch as Hurler syndrome |
| Benign neoplasm of adrenal gland | 0.9928 | No plausible mechanistic link; likely a TxGNN model (deep learning) artefact |
| Autosomal ichthyosis syndrome with fatal disease course | 0.9924 | Tenuous link through glucosylceramide metabolism in the skin barrier; IV enzyme does not reach the epidermis or cross the blood-brain barrier effectively |
| Cholesteryl ester storage disease | 0.9912 | Different enzyme deficiency (lysosomal acid lipase); a specific enzyme replacement is already the approved approach |

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2541106 (cancelled) | Elelyso | Inj 200 unit vial | Not recorded |

## Safety Considerations

Please refer to the package insert for safety information. No drug interaction records were found for this drug, and the data compiled for this report did not include warnings or contraindications from the New Zealand package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no supporting clinical or literature evidence (L5), and the proposed mechanism does not hold up: glucocerebrosidase does not act on the glycosaminoglycans that accumulate in Hurler syndrome. The only New Zealand licence is cancelled, and safety data are missing.

**To proceed, the following is needed:**
- Mechanism of action data from DrugBank
- Package insert warnings and contraindications from Medsafe (currently a blocking gap for safety screening)
- Preclinical evidence showing that glucocerebrosidase has any effect on MPS I substrates or outcomes
- Consideration of established alternatives, since approved enzyme replacement therapies already exist for MPS I and for cholesteryl ester storage disease
- Clarification of the New Zealand supply status given the cancelled licence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


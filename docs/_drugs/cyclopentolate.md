---
layout: default
title: Cyclopentolate
parent: Model Prediction Only (L5)
nav_order: 123
evidence_level: L5
indication_count: 3
monitor_inn: "Cyclopentolate"
predicted_indications: ["cauda equina syndrome", "obsolete neurogenic bladder (disease)", "irritable bowel syndrome"]
last_modified_at: 2026-11-12
description: "Cyclopentolate: predicted new indication Cauda Equina Syndrome (evidence level L5). In New Zealand: 2 Pharmac Schedule items"
---

# Cyclopentolate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

## Cyclopentolate: Antimuscarinic (anticholinergic) cycloplegic → Cauda Equina Syndrome
{: .text-alpha }

## One-Sentence Summary

Cyclopentolate is a topical antimuscarinic eye-drop medicine used to relax the focusing muscle of the eye.
The TxGNN model predicts it may be relevant to **Cauda Equina Syndrome**, but **no clinical trials and no publications** were found to support this direction, so it remains a computational prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Cauda Equina Syndrome |
| TxGNN Prediction Score | 0.9954 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Clinical Pharmacology section of the US FDA-approved prescribing information for cyclopentolate: "This anticholinergic preparation blocks the responses of the sphincter muscle of the iris and the accommodative muscle of the ciliary body to cholinergic stimulation, producing pupillary dilation (mydriasis) and paralysis of accommodation (cycloplegia). It acts rapidly, but has a shorter duration than atropine." ([DailyMed: Cyclogyl (cyclopentolate hydrochloride) prescribing information, Alcon Laboratories, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=1d008c76-7210-4ace-90fa-7cd22762e12e), label version 2024-06-25; accessed 2026-11-20.) Based on general pharmacology, cyclopentolate is an antimuscarinic (anticholinergic) agent, meaning it blocks muscarinic receptors.

Cauda equina syndrome can cause bladder problems, such as an overactive or poorly emptying bladder, and antimuscarinic drugs are sometimes used for certain bladder symptoms. This gives a loose class-level link between the drug and the predicted disease.

The link is weak, however. The main treatment for cauda equina syndrome is urgent surgical decompression of the compressed nerves, and an eye-drop cycloplegic would not address that underlying problem. Cyclopentolate is also formulated only for the eye, not for systemic or bladder use. The high score is a model output and is not backed by any study.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 203351 | Cyclogyl | Cyclopentolate hydrochloride | Eye drops 1% | Yes |
| 2603160 | Minims Cyclopentolate | Cyclopentolate hydrochloride | Eye drops 1%, single dose | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for safety information.

As a class, antimuscarinic drugs can cause systemic effects such as dry mouth, rapid heart rate and central nervous system effects, which is relevant if use beyond the eye were ever considered.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on the model score and a general class-level argument alone. There are no trials or publications on this disease, and the drug's eye-only formulation does not match the needs of the predicted condition.

**To proceed, the following is needed:**
- Detailed mechanism of action data
- Package insert warnings and contraindications from Medsafe
- Any published or registered studies of antimuscarinics in cauda equina syndrome
- Assessment of whether a suitable non-ophthalmic formulation and route would be feasible

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-20 | "Detailed mechanism of action data is not currently available" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Cyclogyl (cyclopentolate hydrochloride) prescribing information, Alcon Laboratories, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=1d008c76-7210-4ace-90fa-7cd22762e12e) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


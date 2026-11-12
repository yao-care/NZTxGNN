---
layout: default
title: Threonine
parent: Model Prediction Only (L5)
nav_order: 450
evidence_level: L5
indication_count: 1
last_modified_at: 2026-11-12
description: "Threonine: predicted new indication gastroparesis (evidence level L5). Approved use: No approved indication text available"
monitor_inn: "threonine"
predicted_indications: ["gastroparesis (disease)"]
---

# Threonine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Threonine: From Metabolic Nutritional Support (MMA/PA Formulas) to gastroparesis
{: .text-alpha }

## One-Sentence Summary

Threonine is an essential amino acid. In New Zealand it has been used in MMA/PA dietary-management powders (for methylmalonic and propionic acidaemia), although no approved indication text is recorded for these products.
The TxGNN model predicts it may be effective for **gastroparesis**, but there are currently **0 clinical trials** and **0 publications** supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | No approved indication text available |
| Predicted New Indication | Gastroparesis |
| TxGNN Prediction Score | 0.9932 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (4 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 4 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, threonine is an essential amino acid used in specialised amino-acid formulas, and no mechanistic link to gastric motility, gastric emptying, or enteric neuromuscular function can be established from the data compiled for this report.

The high prediction score (0.9932) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. One speculative possibility is that threonine is abundant in intestinal mucins, so it might play a role in mucosal integrity or general nutritional support. This has not been verified and would need independent confirmation.

The original indication is also not documented. A direct comparison between the original and new indication is therefore not possible at this stage.

<!-- review:begin nz-lic-20261106-threonine-1 -->

> **Review correction (2026-11-06)**: Originally stated: "The original indication is also not documented, because the authorization records contain no indication text." Removed: it conflicted with the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 4 listings for this drug, 4 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-threonine-1 -->

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2386313 | MMA/PA Anamix Infant | Aminoacid formula without isoleucine, methionine, threonine… | Powder, can | Yes |
| 2498812 | MMA/PA Anamix Junior | Aminoacid formula without methionine, threonine and valine | Powder (neutral), 18 g sachets | Yes |
| 2668564 | MMA/PA Explore 5 | Aminoacid formula without methionine, threonine and valine | Powder, 12.5 g sachets | Yes |
| 2668572 | MMA/PA Express 15 | Aminoacid formula without methionine, threonine and valine | Powder, 25 g sachets | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Knowledge-graph contraindication signals** (from drug-disease contraindication links in the TxGNN knowledge graph, not from label text): phenylketonuria and disorders of phenylalanine metabolism.

No drug interaction records were found. For all other safety information, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score, with no clinical trials, no literature, and no documented mechanism linking threonine to gastroparesis. In addition, package insert safety information has not been reviewed.

<!-- review:begin nz-lic-20261106-threonine-2 -->

> **Review correction (2026-11-06)**: Originally stated: "In addition, the drug is marketed in New Zealand, with 4 valid licences out of 4, and package insert safety information has not been reviewed." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 4 listings for this drug, 4 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-threonine-2 -->

**To proceed, the following is needed:**
- Mechanism of action data (for example, from DrugBank) and a plausible biological link to gastric motility
- Package insert warnings and contraindications from Medsafe, needed before any safety screening
- A literature and trial search specific to threonine and gastroparesis to look for any supporting evidence
- Clarification of the original approved indications for the MMA/PA products
- Assessment of route and formulation compatibility, as well as the significance of the phenylalanine-metabolism contraindication signals

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "because the authorization records contain no indication text" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "the drug is marketed in New Zealand, with 4 valid licences out of 4, and" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


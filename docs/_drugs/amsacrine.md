---
layout: default
title: Amsacrine
parent: Model Prediction Only (L5)
nav_order: 38
evidence_level: L5
indication_count: 5
monitor_inn: "AMSACRINE"
drugbank_id: DB00276
predicted_indications: ["hyperthyroidism", "plasma cell myeloma", "indolent plasma cell myeloma", "resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta", "bronchitis"]
last_modified_at: 2026-11-20
description: "Amsacrine: predicted new indication hyperthyroidism (evidence level L5). In New Zealand: 2 Pharmac Schedule items"
---

# Amsacrine
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

## Amsacrine: topoisomerase II inhibitor and DNA intercalator → hyperthyroidism
{: .text-alpha }

## One-Sentence Summary

Amsacrine is a cytotoxic antineoplastic agent that works by poisoning topoisomerase II and intercalating into DNA.
The TxGNN model predicts it may be useful for **hyperthyroidism**, but **no clinical trials and no publications** were found for this prediction, so it rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | hyperthyroidism |
| TxGNN Prediction Score | 0.9937 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

ChEMBL (EMBL-EBI) records the mechanism of action of amsacrine as: DNA topoisomerase II inhibitor ([ChEMBL CHEMBL43 (amsacrine): drug mechanism](https://www.ebi.ac.uk/chembl/explore/compound/CHEMBL43), CC BY-SA 3.0; accessed 2026-11-20.) Amsacrine is known as a topoisomerase II inhibitor and DNA intercalator, a cytotoxic mechanism aimed at rapidly dividing cancer cells.

No plausible mechanistic link between this action and excess thyroid hormone is evident. Hyperthyroidism is a benign, manageable endocrine condition with established, far less toxic treatments. The high model score is therefore best read as a statistical signal that needs independent confirmation, not as a biologically supported finding. The cytotoxic profile, including myelosuppression and cardiotoxicity concerns, makes this use unfavourable on risk-benefit grounds.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2577437 | Amsidine | Amsacrine | Inj 50 mg per ml, 1.5 ml ampoule | Yes |
| 2680521 | AmsaLyo | Amsacrine | Inj 75 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (topoisomerase II inhibitor / DNA intercalator) |
| Myelosuppression Risk | High (expected for this class; please refer to the package insert) |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | CBC with differential, cardiac function, liver and renal function, electrolytes |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no supporting trials or literature, no plausible mechanism, and a toxic cytotoxic drug would be set against a benign, well-treated condition. The evidence level is L5 (model prediction only).

**To proceed, the following is needed:**
- Mechanism of action data and a credible biological hypothesis linking amsacrine to thyroid hormone excess
- Package insert warnings and contraindications from Medsafe for a safety screen
- Preclinical or clinical evidence that directly studies hyperthyroidism
- A risk-benefit comparison against existing hyperthyroidism treatments

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-20 | "Currently, detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [ChEMBL CHEMBL43 (amsacrine): drug mechanism](https://www.ebi.ac.uk/chembl/explore/compound/CHEMBL43) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


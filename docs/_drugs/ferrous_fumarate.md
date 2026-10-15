---
layout: default
title: Ferrous Fumarate
parent: Model Prediction Only (L5)
nav_order: 197
evidence_level: L5
indication_count: 1
---

# Ferrous Fumarate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

# Ferrous Fumarate: From Iron Deficiency Anaemia to Non-Syndromic Esophageal Malformation

## One-Sentence Summary

Ferrous fumarate is an oral iron supplement, originally used to treat iron deficiency anaemia and related anaemias.
The TxGNN model predicts it may be effective for **non-syndromic esophageal malformation**,
but there are currently **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Iron deficiency anaemia and related anaemias (folic acid deficiency anaemia, anaemia of prematurity, hypochromic anaemia, microcytic anaemia); acne also appears in the knowledge graph. No approved indication text is available from the New Zealand licence record. |
| Predicted New Indication | Non-syndromic esophageal malformation |
| TxGNN Prediction Score | 99.49% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 (0 currently valid, 1 cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, ferrous fumarate is an oral iron salt whose efficacy in correcting iron deficiency and related anaemias is well established.

The predicted condition is a different kind of problem. Esophageal malformations (such as esophageal atresia or tracheoesophageal fistula) are structural defects that arise during embryonic development. Restoring iron levels would not be expected to correct such a defect, so no plausible mechanistic link can be drawn from the data compiled for this report.

The high score (0.995) is a knowledge-graph prediction only, and no clinical or published evidence backs it up. It may reflect graph artefacts, such as shared gene or pathway neighbours, rather than a real therapeutic relationship. Oral iron is also known to irritate the oesophageal and gastrointestinal lining, which argues against benefit in this setting.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2162156 (cancelled) | Ferro-tab | Tab 200 mg (65.7 mg elemental) | Not available |

## Safety Considerations

- **Drug Interactions**: 80 potential interactions were identified in total. Examples from the compiled list include:
  - **Moderate**: proton pump inhibitors (rabeprazole, omeprazole, esomeprazole, lansoprazole, dexlansoprazole, pantoprazole), tetracycline-class antibiotics (doxycycline, tetracycline, minocycline), levofloxacin, antacids and mineral products (magnesium oxide, calcium carbonate, aluminium hydroxide, magaldrate), pancrelipase, sodium sulfate, triethylenetetramine.
  - **Minor**: cimetidine, zinc sulfate, zinc acetate.
- **Disease associations flagged as contraindications in the knowledge graph** (not drawn from product label text): hypertensive disorders, anxiety and neurotic disorders, porphyrias, kidney disease, liver disease, gallbladder disease, peptic ulcer disease, diabetes mellitus and diabetic ketoacidosis, hyperglycaemia, hyperparathyroidism, hypercalcaemia, nephrocalcinosis, sarcoidosis, sickle cell anaemia, primary hyperoxaluria, hereditary haemochromatosis, haemosiderosis, iron metabolism diseases, and Wilson disease.

For product-label warnings and contraindications, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score. There are no clinical trials or publications, no plausible mechanistic link, and the only New Zealand authorisation has been cancelled. Oral iron's known mucosal irritation also argues against use in an oesophageal condition.

**To proceed, the following is needed:**
- Mechanism of action data for ferrous fumarate
- Any independent biological or clinical evidence linking iron supplementation to oesophageal development or malformation
- Package insert warnings and contraindications, to allow a proper safety screen
- Confirmation of route and formulation compatibility with the proposed use
- Review of whether the knowledge-graph link is an artefact before investing further
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


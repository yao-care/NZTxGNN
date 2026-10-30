---
layout: default
title: Levothyroxine
parent: Model Prediction Only (L5)
nav_order: 272
evidence_level: L5
indication_count: 4
monitor_inn: "LEVOTHYROXINE"
predicted_indications: ["renal hypodysplasia/aplasia", "bilateral renal agenesis", "Potter sequence", "Carney complex"]
last_modified_at: 2026-10-30
description: "Levothyroxine: predicted new indication Renal hypodysplasia/aplasia (evidence level L5). Approved use: Not recorded in the local data"
---

# Levothyroxine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

## Levothyroxine: Renal hypodysplasia/aplasia
{: .text-alpha }

## One-Sentence Summary

Levothyroxine is a synthetic thyroid hormone, recorded in the knowledge graph for hypothyroidism and other thyroid conditions.
The TxGNN model predicts it may be effective for **renal hypodysplasia/aplasia**,
but there are **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Renal hypodysplasia/aplasia |
| TxGNN Prediction Score | 0.9967 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (9 of 9 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 9 (valid: 9) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Levothyroxine is a synthetic thyroid hormone replacement. In the knowledge graph its recorded uses are mainly thyroid-related (hypothyroidism, goitre, thyroid cancer, Hashimoto thyroiditis), plus hyperlipidaemia. These are recorded uses, not approved indications confirmed by local label text.

The link to renal hypodysplasia/aplasia is weak. Thyroid hormone contributes to general fetal development, including the kidney, but this is a speculative connection. Renal hypodysplasia/aplasia is a structural congenital malformation, usually driven by developmental gene defects (for example PAX2, HNF1B, RET). Postnatal levothyroxine is not expected to reverse it. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence, rather than a real therapeutic rationale.

Other top-ranked predictions show the same pattern: bilateral renal agenesis (0.9960), Potter sequence (0.9933) and Carney complex (0.9918). None has supporting trials or literature. Carney complex has only a tenuous indirect link through thyroid involvement.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2534800 | Synthroid | Tab 100 mcg | Not stated in the records |
| 250287 | Eltroxin | Tab 50 mcg | Not stated in the records |
| 2534797 | Synthroid | Tab 50 mcg | Not stated in the records |
| 2534789 | Synthroid | Tab 25 mcg | Not stated in the records |
| 250295 | Eltroxin | Tab 100 mcg | Not stated in the records |

Showing 5 of 9 valid single-ingredient authorizations. Others include Eltroxin and Mercury Pharma products in 50 mcg and 100 mcg strengths.

## Safety Considerations

- **Contraindication relations in the knowledge graph** (not label text): hypertension, pituitary hormone deficiencies and hypopituitarism, hypogonadotropic hypogonadism (including Kallmann syndrome), osteoporosis (including postmenopausal), diabetes mellitus and diabetic ketoacidosis, coronary artery disease, myocardial infarction, and thymus gland disease.
- **Drug Interactions** (100 recorded; examples of moderate-level interactions): antidiabetic drugs (metformin, acarbose, pioglitazone, alogliptin, canagliflozin, dapagliflozin, empagliflozin, dulaglutide, albiglutide, chlorpropamide), proton pump inhibitors (omeprazole, rabeprazole, dexlansoprazole), calcium and magnesium salts (calcium acetate, calcium phosphate, magnesium oxide), and sympathomimetics (epinephrine, isometheptene, diethylpropion). Cimetidine is a minor interaction.

Please refer to the package insert for warnings and label contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no supporting trials or publications, and no study directly addresses renal hypodysplasia/aplasia, so the evidence level is L5. The condition is a structural congenital malformation with no plausible pharmacological rationale for levothyroxine.

**To proceed, the following is needed:**
- Mechanism of action data and a credible biological rationale linking thyroid hormone to renal development defects
- Any preclinical or clinical studies directly examining levothyroxine in this disease
- Package insert warnings and contraindications from Medsafe for a safety review
- Review of the TxGNN model (deep learning) output, whose scores may reflect similarities the model learned between related diseases rather than biological evidence, and consideration of higher-plausibility predictions
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


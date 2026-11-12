---
layout: default
title: Norfloxacin
parent: Model Prediction Only (L5)
nav_order: 328
evidence_level: L5
indication_count: 10
last_modified_at: 2026-11-12
description: "Norfloxacin: predicted new indication Hyperamylasemia (evidence level L5). Approved use: Bacterial infections, including urinary tract infection…"
monitor_inn: "NORFLOXACIN"
predicted_indications: ["hyperamylasemia", "polyclonal hyperviscosity syndrome", "congenital analbuminemia", "blood group incompatibility", "punctate epithelial keratoconjunctivitis", "premalignant hematological system disease", "diffuse scleroderma", "monoclonal gammopathy", "hematological disease associated with an acquired peripheral neuropathy", "septicemic plague"]
---

# Norfloxacin
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

## Norfloxacin: From Bacterial Infections (Urinary Tract Infection and Others) to Hyperamylasemia
{: .text-alpha }

## One-Sentence Summary

Norfloxacin is a fluoroquinolone antibacterial originally used for infections such as urinary tract infection, typhoid fever, dysentery and bacterial pneumonia.
The TxGNN model predicts it may be effective for **hyperamylasemia**, but there are **0 clinical trials** and **0 publications** supporting this prediction.
It is a prediction of the TxGNN model (deep learning) only. It may reflect similarities the model learned between related diseases, not biological evidence, and it has no supporting evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Bacterial infections, including urinary tract infection, typhoid fever, dysentery and bacterial pneumonia (the New Zealand licence has no approved indication text recorded) |
| Predicted New Indication | Hyperamylasemia |
| TxGNN Prediction Score | 0.9970 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Norfloxacin is a fluoroquinolone antibacterial that is generally understood to inhibit bacterial DNA gyrase and topoisomerase IV. Its efficacy is established in bacterial infections, not in laboratory findings.

The assessment found **no plausible mechanistic link** to hyperamylasemia. Hyperamylasemia is a laboratory finding with diverse causes, such as pancreatitis and renal impairment. It is not a primary infectious indication. The high TxGNN score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. There is no supporting trial or literature evidence.

Other predictions were also reviewed. Most have no mechanistic rationale (polyclonal hyperviscosity syndrome, congenital analbuminemia, blood group incompatibility, diffuse scleroderma and others). The only biologically plausible one is **septicemic plague** (score 0.9937). Plague is caused by *Yersinia pestis*, a susceptible Gram-negative bacterium. However, the evidence is preclinical and indirect. Norfloxacin's low serum levels may not suit systemic infection, and other agents are better supported. This is a research question, not a recommendation.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available for hyperamylasemia.

---

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2204398 | Arrow-Norfloxacin | Norfloxacin | Tab 400 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

---

## Safety Considerations

- **Contraindications / Disease Associations (knowledge graph, not label text)**: Knowledge graph records link norfloxacin to the following conditions of concern:
  - Cardiac: long QT syndrome (including familial), torsades de pointes
  - Hepatic and renal: liver failure, liver disease, cirrhosis, gallbladder disease, kidney disease
  - Neuropsychiatric: epilepsy, anxiety disorder, psychotic disorders, schizophreniform and schizoaffective disorders, peripheral neuropathy
  - Musculoskeletal: tendinitis, rhabdomyolysis
  - Metabolic: diabetes mellitus, diabetic ketoacidosis, potassium deficiency
  - Haematological: thrombocytopenia, anaemia
- **Drug Interactions**: 100 interactions were found, of which the data lists 20 and the highlights are below.
  - **Major**: hydrocortisone, dexamethasone, betamethasone, triamcinolone (corticosteroids), bupropion, chlorpropamide
  - **Moderate**: antidiabetic agents (acarbose, metformin, pioglitazone, alogliptin, albiglutide, canagliflozin), famotidine, loperamide, acetylsalicylic acid, balsalazide, bisacodyl, calcium salts, potassium citrate
- Package insert warnings and contraindications were not available. Please refer to the package insert for authoritative safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for hyperamylasemia rests on a model score alone, with no clinical trials, no literature and no plausible mechanism (Evidence Level L5). The safety profile raises concerns, including QT prolongation, tendon and neurological effects, and major interactions with corticosteroids.

<!-- review:begin nz-lic-20261031-norfloxacin-1 -->

> **Review correction (2026-10-31)**: Originally stated: "The prediction for hyperamylasemia rests on a model score alone, with no clinical trials, no literature and no plausible mechanism (Evidence Level L5). The product also has only a cancelled licence in New Zealand." Removed: it said the Schedule listings were cancelled, discontinued, or not current, which did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-10-31). The Pharmaceutical Schedule lists 1 listing for this drug, 1 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261031-norfloxacin-1 -->

**To proceed, the following is needed:**
- Mechanism of action data and a rationale linking the drug to the proposed indication
- Package insert warnings and contraindications from Medsafe
- Any clinical or preclinical evidence supporting the new indication
- If an anti-infective direction is wanted, a separate review of septicemic plague against better-supported fluoroquinolones and standard agents

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-10-31 | Removed: "The product also has only a cancelled licence in New Zealand" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Rifaximin
parent: Model Prediction Only (L5)
nav_order: 404
evidence_level: L5
indication_count: 6
last_modified_at: 2026-11-12
description: "Rifaximin: predicted new indication Oral candidiasis (evidence level L5). Approved use: Hepatic encephalopathy, hepatic coma"
monitor_inn: "RIFAXIMIN"
predicted_indications: ["oral candidiasis", "commissural lip fistula", "osteoradionecrosis of the mandible", "oral leukoedema", "burning mouth syndrome", "candidiasis"]
---

# Rifaximin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Rifaximin: From Hepatic Encephalopathy to Oral Candidiasis
{: .text-alpha }

## One-Sentence Summary

Rifaximin is a non-absorbed, gut-targeted antibiotic, originally used for hepatic encephalopathy and hepatic coma.
The TxGNN model predicts it may be effective for **oral candidiasis**, but the data compiled for this report include **no clinical trials** and only **1 publication**, and that publication points toward an increased risk of Candida infection rather than a benefit.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hepatic encephalopathy, hepatic coma (the New Zealand licence record has no approved indication text) |
| Predicted New Indication | Oral candidiasis |
| TxGNN Prediction Score | 0.9975 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data are not available. Based on known information, rifaximin is an oral, minimally absorbed antibiotic that acts within the gut. Its efficacy in hepatic encephalopathy is established, and it works by reducing gut bacteria.

The prediction does not hold up well mechanistically. Rifaximin has no known direct antifungal activity. The only publication found (PMID 34180023, 2021) is a cohort study in allogeneic stem cell transplant recipients, and it reports that rifaximin use **favoured** micafungin-resistant Candida infections. This is most likely due to disruption of the gut microbiota. It is a safety signal, not a therapeutic benefit. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. It is not supported by biological plausibility or clinical data.

Other top-ranked predictions (commissural lip fistula, osteoradionecrosis of the mandible, oral leukoedema, burning mouth syndrome) have no trials or literature and no plausible mechanistic link. A related prediction, candidiasis, shows the same pattern: the two available observational studies point toward risk rather than benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34180023](https://pubmed.ncbi.nlm.nih.gov/34180023/) | 2021 | Cohort | Annals of Hematology | In allogeneic stem cell transplant recipients, rifaximin use was associated with more micafungin-resistant Candida infections. This is a risk signal, not evidence of benefit. |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2449935 | Xifaxan | Rifaximin | Tab 550 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Knowledge-graph contraindications** (from disease-level graph data, not label text): Clostridium difficile colitis; perinatal necrotizing enterocolitis.
- **Fungal infection risk**: Available observational data link rifaximin use to Candida infections, including micafungin-resistant strains, in transplant recipients.
- **Drug Interactions**: 69 interactions were identified in total. Most listed ones are Moderate, including alpelisib, apalutamide, cyclosporine, warfarin, tolvaptan, daclatasvir, ledipasvir and cabozantinib. One **Major** interaction was flagged: the live oral cholera vaccine (Vibrio cholerae CVD 103-HgR strain).

Package insert warnings and contraindications could not be obtained. Please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests almost entirely on a model score. There are no clinical trials, the single publication suggests harm rather than benefit, and no plausible antifungal mechanism exists.

<!-- review:begin nz-lic-20261106-rifaximin-1 -->

> **Review correction (2026-11-06)**: Originally stated: "There are no clinical trials, the single publication suggests harm rather than benefit, and no plausible antifungal mechanism exists. The product is also no longer licensed in New Zealand." Removed: it conflicted with the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 1 listing for this drug, 1 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-rifaximin-1 -->

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, which are required before any safety screening
- Mechanism of action data from DrugBank
- Any prospective evidence of antifungal or oral-cavity benefit, which is currently absent
- Further evaluation of the Candida risk signal, accounting for confounding by indication in patients with cirrhosis and transplant recipients
- Clarification of the original approved indication in New Zealand

<!-- review:begin nz-lic-20261106-rifaximin-2 -->

> **Review correction (2026-11-06)**: Originally stated: "Clarification of the cancelled licence status and the original approved indication in New Zealand" Removed: it said the Schedule listings were cancelled, discontinued, or not current, which did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 1 listing for this drug, 1 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-rifaximin-2 -->

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "The product is also no longer licensed in New Zealand" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "the cancelled licence status and" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


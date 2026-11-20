---
layout: default
title: Bedaquiline
parent: Model Prediction Only (L5)
nav_order: 58
evidence_level: L5
indication_count: 4
monitor_inn: "BEDAQUILINE"
predicted_indications: ["vulvovaginal candidiasis", "fascioliasis", "urea cycle disorder", "esophageal candidiasis"]
last_modified_at: 2026-11-20
description: "Bedaquiline: predicted new indication Vulvovaginal candidiasis (evidence level L5). In New Zealand: 1 Pharmac Schedule item"
---

# Bedaquiline
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

## Bedaquiline: mycobacterial ATP synthase inhibitor → vulvovaginal candidiasis
{: .text-alpha }

## One-Sentence Summary

Bedaquiline is an antimycobacterial drug that inhibits the ATP synthase of mycobacteria. The TxGNN model predicts it may be effective for **vulvovaginal candidiasis**, but **no clinical trials and no publications** currently support this direction, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Vulvovaginal candidiasis |
| TxGNN Prediction Score | 0.9988 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Bedaquiline targets subunit c of the mycobacterial ATP synthase. This is a bacterial-specific target and is not a validated antifungal target. According to the Mechanism of Action and Microbiology sections of the US FDA-approved prescribing information for bedaquiline: "Bedaquiline is a diarylquinoline antimycobacterial drug. […] SIRTURO is a diarylquinoline antimycobacterial drug that inhibits mycobacterial ATP (adenosine 5'- triphosphate) synthase, by binding to subunit c of the enzyme that is essential for the generation of energy in M. tuberculosis." ([DailyMed: Sirturo (bedaquiline fumarate) prescribing information, Janssen Products, LP](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=1534c9ae-4948-4cf4-9f66-222a99db6d0e), label version 2025-10-17; accessed 2026-11-20.)

On current information there is no clear mechanistic link between inhibiting mycobacterial energy metabolism and treating *Candida* infection of the vulvovagina. The high model score is most likely an artefact of the network-based prediction rather than a pharmacological signal. The prediction should be treated as a hypothesis only, and no basis exists to expect clinical benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2659956 | Sirturo | Bedaquiline | Tab 100mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Drug Interactions**: 100 interactions were found in the interaction database. The entries listed here are a sample of the main ones.
  - **Major**: dexamethasone, clarithromycin, picosulfuric acid, polyethylene glycol (3350 with electrolytes), dolasetron, palonosetron, sodium sulfate
  - **Moderate**: acarbose, famotidine, doxycycline, pioglitazone, loperamide, bupropion, bisacodyl, chenodeoxycholic acid, minocycline, oxandrolone, oxymetholone, tetracycline, naltrexone

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has a high model score but no supporting trials or publications, and no plausible mechanistic link to an antifungal effect. Evidence is at the model-prediction-only level (L5).

**To proceed, the following is needed:**
- In vitro antifungal activity data against *Candida* species
- A defined mechanistic hypothesis for activity in this disease
- Package insert warnings and contraindications, and detailed mechanism of action data
- Route-of-administration compatibility assessment for a vulvovaginal indication

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-20 | "Detailed mechanism of action data is not otherwise available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Sirturo (bedaquiline fumarate) prescribing information, Janssen Products, LP](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=1534c9ae-4948-4cf4-9f66-222a99db6d0e) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


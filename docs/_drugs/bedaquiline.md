---
layout: default
title: Bedaquiline
parent: Model Prediction Only (L5)
nav_order: 58
evidence_level: L5
indication_count: 4
monitor_inn: "BEDAQUILINE"
predicted_indications: ["vulvovaginal candidiasis", "fascioliasis", "urea cycle disorder", "esophageal candidiasis"]
last_modified_at: 2026-10-28
description: "Bedaquiline: predicted new indication Vulvovaginal candidiasis (evidence level L5). Approved use: Not recorded in the local data"
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
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Bedaquiline targets subunit c of the mycobacterial ATP synthase. This is a bacterial-specific target and is not a validated antifungal target. Detailed mechanism of action data is not otherwise available in the data compiled for this report.

On current information there is no clear mechanistic link between inhibiting mycobacterial energy metabolism and treating *Candida* infection of the vulvovagina. The high model score is most likely an artefact of the network-based prediction rather than a pharmacological signal. The prediction should be treated as a hypothesis only, and no basis exists to expect clinical benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2659956 | Sirturo | Tab 100mg | Not recorded in the local data |

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
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


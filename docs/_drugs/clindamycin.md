---
layout: default
title: Clindamycin
parent: Model Prediction Only (L5)
nav_order: 105
evidence_level: L5
indication_count: 6
monitor_inn: "CLINDAMYCIN"
predicted_indications: ["punctate epithelial keratoconjunctivitis", "exposure keratitis", "non-human animal disease", "neurotrophic keratopathy", "epidemic keratoconjunctivitis", "postmenopausal atrophic vaginitis"]
last_modified_at: 2026-11-19
description: "Clindamycin: predicted new indication punctate epithelial keratoconjunctivitis (evidence level L5). Approved use: Not recorded in the local data"
---

# Clindamycin
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

## Clindamycin: lincosamide antibacterial (50S ribosomal subunit inhibitor) → punctate epithelial keratoconjunctivitis
{: .text-alpha }

## One-Sentence Summary

Clindamycin is a lincosamide antibacterial that blocks protein synthesis in bacteria.
The TxGNN model predicts it may be useful for **punctate epithelial keratoconjunctivitis**, with a score of **0.9997**.
No clinical trials or publications were found for this disease, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | punctate epithelial keratoconjunctivitis |
| TxGNN Prediction Score | 0.9997 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for clindamycin: "Clindamycin inhibits bacterial protein synthesis by binding to the 23S RNA of the 50S subunit of the ribosome. Clindamycin is bacteriostatic." ([DailyMed: Cleocin Phosphate (clindamycin phosphate) prescribing information, Pharmacia & Upjohn Company LLC](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=05a75685-0af5-407c-ac57-e380c882b49d), label version 2026-07-29; accessed 2026-11-19.) Clindamycin is a lincosamide antibacterial. It is known to inhibit the bacterial 50S ribosomal subunit and so stops bacterial protein synthesis.

The link to the predicted disease is weak. Punctate epithelial keratoconjunctivitis is often viral or inflammatory in origin, and an antibacterial would not be expected to treat it. Any benefit would probably be limited to a bacterial superinfection. The high score is a model output and does not by itself show a therapeutic mechanism.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

<!-- review:begin nz-lic-20261106-clindamycin-1 -->

> **Review correction (2026-11-06)**: Originally stated: "The local data list two licences, both of single-ingredient products. The data show a mixed picture on licence validity, so no firm statement on how many are currently valid is made here." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 2 listings for this drug, 2 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-clindamycin-1 -->

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2576392 | Dalacin C | Clindamycin | Cap hydrochloride 150 mg | Yes |
| 2387131 | Dalacin C | Clindamycin | Inj 150 mg per ml, 4 ml ampoule | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Drug Interactions**: The interaction screen returned 100 records. Those graded Moderate include balsalazide, clarithromycin, picosulfuric acid, attapulgite and kaolin. Other listed interactions, such as amphotericin B, amoxicillin, doxycycline, vancomycin, metformin and omeprazole, have no severity grade.

Clindamycin is widely recognised as a risk factor for *Clostridioides difficile* colitis. For full warnings and contraindications, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has the highest model score but no clinical trials or publications for this disease, and an antibacterial has no clear mechanistic link to a largely viral or inflammatory condition.

**To proceed, the following is needed:**
- Studies directly examining clindamycin in punctate epithelial keratoconjunctivitis, for example whether bacterial superinfection is a meaningful target
- Mechanism of action data and a check of whether an ocular route of administration is feasible
- Package insert warnings and contraindications from Medsafe for a safety review

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "The local data list two licences, both of single-ingredient products. The data show a mixed picture on licence validity, so no firm statemen" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-19 | "Currently, detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Cleocin Phosphate (clindamycin phosphate) prescribing information, Pharmacia & Upjohn Company LLC](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=05a75685-0af5-407c-ac57-e380c882b49d) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


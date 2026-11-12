---
layout: default
title: Primaquine
parent: Model Prediction Only (L5)
nav_order: 382
evidence_level: L5
indication_count: 7
last_modified_at: 2026-11-12
monitor_inn: "PRIMAQUINE"
predicted_indications: ["myiasis", "furuncular myiasis", "wound myiasis", "creeping myiasis", "toxoplasmosis", "nocardiosis", "pneumocystosis"]
description: "Primaquine: predicted new indication Myiasis (evidence level L5). Approved use: Not recorded in the local data"
---

# Primaquine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

## Primaquine: Myiasis
{: .text-alpha }

## One-Sentence Summary

Primaquine is an 8-aminoquinoline antimalarial, originally used for the radical cure of *Plasmodium vivax* malaria.
The TxGNN model predicts it may be effective for **myiasis** (infestation by fly larvae), but there are **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the model alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Myiasis |
| TxGNN Prediction Score | 0.9976 |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, primaquine is an 8-aminoquinoline whose efficacy against *P. vivax* malaria is established. It acts mainly on liver-stage parasites and on the gametocytes of the malaria parasite.

The biological link to myiasis is weak. Myiasis is an infestation of tissue by dipteran (fly) larvae, which are multicellular insects. Malaria is caused by a single-celled protozoan. Primaquine has no known activity against fly larvae.

The very high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. Three related subtypes (furuncular, wound and creeping myiasis) have almost identical scores of about 0.9973. This suggests the model's signal spread across closely related diseases. The score should not be read as evidence of real pharmacological activity.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Any use would require an unapproved-medicine pathway or a new sponsor.

<!-- review:begin nz-lic-20261106-primaquine-1 -->

> **Review correction (2026-11-06)**: Originally stated: "Both authorizations are cancelled, so no primaquine product is currently authorized in New Zealand. Any use would require an unapproved-medicine pathway or a new sponsor." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 2 listings for this drug, 2 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-primaquine-1 -->

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2587637 | Sanofi Primaquine | Primaquine | Tab 15 mg | Yes |
| 2664062 | Bayshore | Primaquine | Tab 15 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

---

## Safety Considerations

- **Disease-level contraindications (from knowledge-graph data, not label text)**: neutropenia, agranulocytosis, G6PD deficiency, hemolytic anemia, methemoglobinemia due to methemoglobin reductase deficiency, glycogen storage disease, cystinuria, galactosemia, and pyruvate dehydrogenase E3 deficiency. The G6PD and hemolysis-related entries are the most important in practice, because primaquine can cause serious hemolysis in G6PD-deficient patients.
- **Drug Interactions**: 100 interactions were recorded in total.
  - **Major**: dolasetron.
  - **Moderate**: several, including ondansetron, granisetron, palonosetron, clarithromycin, levofloxacin, famotidine, loperamide, alosetron, and a number of laxatives and bowel-preparation agents (bisacodyl, picosulfuric acid, polyethylene glycol, lactulose, lactitol, magnesium citrate, sodium sulfate, glycerin, castor oil).
  - **Minor**: metronidazole.
  - Several of these are QT-related or gastrointestinal-motility interactions.

Please refer to the package insert for label-level warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no clinical trials, no literature and no plausible mechanism, since primaquine has no known activity against fly larvae. The high score is probably an artefact of the disease hierarchy. Together these do not justify further investment for this indication.

**To proceed, the following is needed:**
- A mechanistic rationale, or in vitro or in vivo evidence of larvicidal activity, to show a real pharmacological link.
- A systematic literature search beyond the model's retrieval, to confirm that no relevant reports exist.
- Package insert warnings and contraindications, and the mechanism of action data, to support any safety assessment.
- A benefit-risk comparison with the standard treatments for myiasis (such as wound care, larval removal and ivermectin), which are likely safer.

<!-- review:begin nz-lic-20261106-primaquine-2 -->

> **Review correction (2026-11-06)**: Originally stated: "A benefit-risk comparison with the standard treatments for myiasis (such as wound care, larval removal and ivermectin), which are likely safer. / A plan for G6PD screening and a regulatory pathway, given that the drug is marketed with 2 valid New Zealand authorizations out of 2." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 2 listings for this drug, 2 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-primaquine-2 -->

Other predicted indications for primaquine, such as pneumocystosis (as a clindamycin combination) and malaria itself, have much stronger supporting evidence. They would be better candidates for evaluation.

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "Both authorizations are cancelled, so no primaquine product is currently authorized in New Zealand" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "A plan for G6PD screening and a regulatory pathway, given that the drug is marketed with 2 valid New Zealand authorizations out of 2" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


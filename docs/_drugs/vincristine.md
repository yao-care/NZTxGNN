---
layout: default
title: Vincristine
parent: Model Prediction Only (L5)
nav_order: 482
evidence_level: L5
indication_count: 2
monitor_inn: "vincristine"
predicted_indications: ["vertebral anomalies and variable endocrine and T-cell dysfunction", "retroperitoneal neoplasm"]
last_modified_at: 2026-11-24
description: "Vincristine: predicted new indication Vertebral anomalies and variable endocrine and T-cell dysfunction (evidence level L5). In New Zealand: 3 Pharmac Schedule items"
---

# Vincristine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

## Vincristine: Vertebral anomalies and variable endocrine and T-cell dysfunction
{: .text-alpha }

## One-Sentence Summary

Vincristine is a cytotoxic chemotherapy injection.
The TxGNN model gives its top prediction, **vertebral anomalies and variable endocrine and T-cell dysfunction**, a very high score of **0.9924**.
This prediction has **0 clinical trials** and **0 publications** behind it, so it is a model output only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Vertebral anomalies and variable endocrine and T-cell dysfunction |
| TxGNN Prediction Score | 0.9924 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 3 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Clinical Pharmacology section of the US FDA-approved prescribing information for vincristine: "The mechanisms of action of vincristine sulfate remain under investigation. The mechanism of action of vincristine sulfate has been related to the inhibition of microtubule formation in mitotic spindle, resulting in an arrest of dividing cells at the metaphase stage." ([DailyMed: vincristine sulfate prescribing information, Hospira, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=b22e4c8e-dbe1-4d4b-a026-e1812974071c), label version 2026-02-17; accessed 2026-11-19.) Vincristine is a vinca-alkaloid-type cytotoxic agent. It is generally understood to disrupt microtubules and halt dividing cells in metaphase.

That mechanism makes sense for fast-growing cancers such as leukaemia and lymphoma. It is hard to connect to a rare congenital syndrome affecting the vertebrae, endocrine system and T cells. The condition is not a proliferative cancer, so there is no obvious target for an antimitotic drug. A drug that suppresses dividing cells could also worsen existing T-cell dysfunction.

The very high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. The prediction should be treated as a hypothesis only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

<!-- review:begin nz-lic-20261106-vincristine-1 -->

> **Review correction (2026-11-06)**: Originally stated: "The licence records compiled for this report contain no approved indication text. All three licences are single-ingredient products and are currently valid." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Quick Overview and the table under New Zealand Market Information show the current Pharmac Schedule figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-vincristine-1 -->

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2204053 | Baxter | Vincristine sulphate | Inj 1 mg for ECP | Yes |
| 604585 | DBL Vincristine Sulfate | Vincristine sulphate | Inj 1 mg per ml, 1 ml vial | Yes |
| 604593 | DBL Vincristine Sulfate | Vincristine sulphate | Inj 1 mg per ml, 2 ml vial | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (vinca alkaloid, antimitotic) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Full blood count, liver function and neurological assessment (suggested; confirm against the package insert) |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

- **Contraindication-type relations in the knowledge graph (not label text):** liver failure, cholestasis and hyperbilirubinaemia (including Gilbert syndrome), leukopenia, peripheral neuropathy and related neuromuscular disorders (including Charcot-Marie-Tooth disease), acute urate nephropathy, and fungal and other infectious diseases. These need confirmation against the official package insert.
- **Drug interactions:** The interaction screen returned 505 entries. The one example listed as Major is **clarithromycin**. Moderate examples include aprepitant, dexamethasone, metronidazole, miconazole, rolapitant, simvastatin, rosuvastatin and tinidazole. A full interaction review is needed before any use.

Please refer to the package insert for label warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone. No trials or publications address this syndrome, and there is no plausible mechanistic link. The cytotoxic and immunosuppressive properties of vincristine could be harmful in a condition that already involves T-cell dysfunction.

A secondary prediction, **retroperitoneal neoplasm** (score 0.9923), has some supporting literature. This is mainly case reports and small series of specific tumours such as paraganglioma, rhabdomyosarcoma and kaposiform hemangioendothelioma, usually treated with vincristine-containing combinations. No trials are registered, and the studies cover varied tumour types rather than the broad category itself. This evidence is indirect, so it also stays at L5 and is best treated as a research question rather than a basis for a decision.

**To proceed, the following is needed:**
- Mechanism of action data to test whether any biological link to the predicted syndrome exists
- Medsafe package insert warnings and contraindications, which are currently missing and block safety screening
- Any published case or preclinical study that directly addresses the predicted syndrome
- For the retroperitoneal neoplasm lead, a histology-specific review of vincristine-containing regimens

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "The licence records compiled for this report contain no approved indication text. All three licences are single-ingredient products and are " | Corrected (revised 2026-11-24; earlier version kept in the record) | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-19 | "Currently, detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: vincristine sulfate prescribing information, Hospira, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=b22e4c8e-dbe1-4d4b-a026-e1812974071c) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Aprepitant
parent: Model Prediction Only (L5)
nav_order: 42
evidence_level: L5
indication_count: 10
last_modified_at: 2026-11-24
description: "Aprepitant: predicted new indication Nephrogenic syndrome of inappropriate antidiuresis (evidence level L5). Approved use: Not available in the data compiled for this report"
monitor_inn: "APREPITANT"
predicted_indications: ["nephrogenic syndrome of inappropriate antidiuresis", "hypertrichosis (disease)", "pulmonary hypertension", "leprosy", "Ambras type hypertrichosis universalis congenita", "malformation syndrome with odontal and/or periodontal component", "kyphoscoliotic heart disease", "syndrome with a Dandy-Walker malformation as major feature", "subarachnoid hemorrhage (disease)", "isolated genetic hair shaft abnormality"]
---

# Aprepitant
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

## Aprepitant: From Its Original Use (Not Recorded in the Authorization Data) to Nephrogenic Syndrome of Inappropriate Antidiuresis
{: .text-alpha }

## One-Sentence Summary

Aprepitant is a neurokinin-1 (NK1) receptor antagonist.
The TxGNN model predicts it may be effective for **nephrogenic syndrome of inappropriate antidiuresis (NSIAD)**, but this is a computational prediction only, with **0 clinical trials** and **0 relevant publications** supporting it.

<!-- review:begin nz-lic-20261106-aprepitant-1 -->

> **Review correction (2026-11-06)**: Originally stated: "Aprepitant is a neurokinin-1 (NK1) receptor antagonist. The only New Zealand authorization on record is the Emend Tri-Pack, and no approved indication text is available for it." Removed: it conflicted with the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Quick Overview and the table under New Zealand Market Information show the current Pharmac Schedule figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-aprepitant-1 -->

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in the data compiled for this report |
| Predicted New Indication | Nephrogenic syndrome of inappropriate antidiuresis |
| TxGNN Prediction Score | 0.9997 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for aprepitant: "Aprepitant is a selective high-affinity antagonist of human substance P/neurokinin 1 (NK 1 ) receptors. Aprepitant has little or no affinity for serotonin (5-HT 3 ), dopamine, and corticosteroid receptors. Aprepitant has been shown in animal models to inhibit emesis induced by cytotoxic chemotherapeutic agents, such as cisplatin, via central actions." ([DailyMed: Cinvanti (aprepitant) prescribing information, Heron Therapeutics, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=4c218d3a-a508-4abc-9ed8-a6e8e191d1b4), label version 2026-04-01; accessed 2026-11-19.) Aprepitant is generally known as an NK1 receptor antagonist, which blocks the action of substance P.

NSIAD is caused by a gain-of-function change in the vasopressin V2 receptor, and no established link exists between NK1 antagonism and this mechanism. Substance P/NK1 signalling has been loosely discussed in relation to water balance, but this is speculative. The high TxGNN score therefore reflects a computational pattern only and is not supported by clinical or literature evidence.

Other top-ranked predictions (for example hypertrichosis, leprosy, Ambras-type hypertrichosis, Dandy-Walker malformation syndromes, and genetic hair shaft abnormalities) also lack plausible mechanistic rationale or any supporting studies. Pulmonary hypertension and subarachnoid haemorrhage have only weak biological plausibility through substance P signalling, and there is no clinical evidence for either.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the top-ranked prediction. For reference, the only publication linked to any of the top 10 predictions (PMID [28261651](https://pubmed.ncbi.nlm.nih.gov/28261651/), 2016, *Oncology and Therapy*) is a Phase I pazopanib plus cisplatin dose-escalation trial in advanced solid tumours, linked to pulmonary hypertension. It does not involve aprepitant and is not relevant evidence.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2244098 | Emend Tri-Pack | Aprepitant | Cap 2 x 80 mg and 1 x 125 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Cytotoxicity

Aprepitant is not an antineoplastic drug, so this section does not apply.

## Safety Considerations

- **Knowledge-graph contraindication signal**: The TxGNN knowledge graph lists liver failure as a drug-disease contraindication. This comes from the graph, not from package insert text.
- **Drug Interactions**: 100 interactions were identified in total. Examples from the first 20 returned include:
  - **Major**: fentanyl, alfentanil, acalabrutinib
  - **Moderate**: paclitaxel, abemaciclib, nifedipine, ibuprofen, lidocaine, dihydrocodeine, rabeprazole, levonorgestrel, hydrocortisone, albendazole, fluvoxamine, fosamprenavir, fosphenytoin, fostamatinib, gefitinib, abametapir (topical)
  - **Minor**: dolutegravir

Please refer to the package insert for warnings and contraindications, which could not be retrieved.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is model-only (L5). There are no clinical trials or relevant publications, the mechanistic link to NSIAD is speculative.

<!-- review:begin nz-lic-20261106-aprepitant-2 -->

> **Review correction (2026-11-06)**: Originally stated: "There are no clinical trials or relevant publications, the mechanistic link to NSIAD is speculative, and the only New Zealand authorization has been cancelled." Removed: it said the Schedule listings were cancelled, discontinued, or not current, which did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Quick Overview and the table under New Zealand Market Information show the current Pharmac Schedule figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-aprepitant-2 -->

**To proceed, the following is needed:**
- Retrieve the Medsafe package insert to complete warnings, contraindications, and original indication information (a blocking gap for safety screening)
- Obtain detailed mechanism of action data from DrugBank
- Conduct a mechanistic and preclinical assessment of NK1 antagonism in V2 receptor-mediated antidiuresis
- Search for relevant case reports or studies in NSIAD or other predicted indications
- Review the drug interaction profile (especially major interactions) and the liver failure signal before any further consideration

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "The only New Zealand authorization on record is the Emend Tri-Pack, and no approved indication text is available for it" | Corrected (revised 2026-11-24; earlier version kept in the record) | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "and the only New Zealand authorization has been cancelled" | Corrected (revised 2026-11-24; earlier version kept in the record) | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-19 | "Detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Cinvanti (aprepitant) prescribing information, Heron Therapeutics, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=4c218d3a-a508-4abc-9ed8-a6e8e191d1b4) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


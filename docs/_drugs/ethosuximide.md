---
layout: default
title: Ethosuximide
parent: Model Prediction Only (L5)
nav_order: 187
evidence_level: L5
indication_count: 1
last_modified_at: 2026-11-12
monitor_inn: "ETHOSUXIMIDE"
predicted_indications: ["nephrogenic syndrome of inappropriate antidiuresis"]
description: "Ethosuximide: predicted new indication Nephrogenic syndrome of inappropriate antidiuresis (evidence level L5). Approved use: Not recorded in the local data"
---

# Ethosuximide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Ethosuximide: Nephrogenic syndrome of inappropriate antidiuresis
{: .text-alpha }

## One-Sentence Summary

Ethosuximide is an anti-seizure medicine originally used to treat absence epilepsy.
The TxGNN model predicts it may be effective for **nephrogenic syndrome of inappropriate antidiuresis (NSIAD)**,
but there are currently **0 clinical trials** and **0 publications** supporting this prediction, so it rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Nephrogenic syndrome of inappropriate antidiuresis |
| TxGNN Prediction Score | 0.9991 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Ethosuximide is generally known to block T-type calcium channels in thalamic neurons, which is the basis of its efficacy in absence epilepsy.

NSIAD is a different kind of condition. It is caused by gain-of-function mutations in the vasopressin V2 receptor gene (*AVPR2*), or in some cases *GNAS*. These mutations cause the receptor to signal constantly, so the kidney retains water even when vasopressin is low. No link between T-type calcium channel blockade and this renal mechanism has been established in the available data.

A possible effect of calcium-channel or downstream signalling changes on water handling in the renal collecting duct is purely speculative. The very high TxGNN score (0.9991) is a model output only, with no clinical trial or literature support. It should be treated as a hypothesis to investigate, not as evidence of efficacy.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2705117 | Zarontin | Ethosuximide | Cap 250 mg | Yes |
| 2106019 | Zarontin | Ethosuximide | Oral liq 250 mg per 5 ml | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Disease associations flagged in the knowledge graph (not label text)**: the TxGNN knowledge graph links ethosuximide to contraindication or caution signals in the following conditions:
  - Psychiatric: anxiety disorder, neurotic disorder, dysthymic disorder
  - Renal, hepatic and biliary: kidney disease, pyoureter, liver disease, gallbladder disease
  - Haematological: hematologic disease, neutropenia, agranulocytosis
  - Autoimmune and metabolic: systemic lupus erythematosus (including the paediatric form), acute intermittent porphyria

  Kidney disease is directly relevant to a renal indication such as NSIAD.
- **Drug Interactions**: 92 interactions were identified, and the 20 shown in the data are all rated Moderate. Main examples:
  - CNS depressants and opioids: morphine (including liposomal), opium, ethanol, dronabinol, nabilone
  - Antiemetics and antihistamines: aprepitant, metoclopramide, promethazine, chlorpheniramine
  - Antimalarials: chloroquine, mefloquine, hydroxychloroquine
  - Folate-related agents: folic acid, leucovorin, levoleucovorin
  - Others: sibutramine, cobicistat, deferasirox, fostamatinib

Please refer to the package insert for warnings and contraindications. The label information was not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the TxGNN model score. The safety data also raise renal and haematological concerns.

**To proceed, the following is needed:**
- Mechanism of action data (MOA) and a plausible biological link between T-type calcium channel blockade and *AVPR2*/*GNAS*-driven water retention
- A systematic literature search, and preclinical studies (for example cell or animal models of constitutive V2 receptor signalling)
- Medsafe package insert warnings and contraindications, which are currently missing and block safety screening
- A review of the regulatory pathway
- An assessment of the renal and haematological safety signals in the NSIAD patient population

<!-- review:begin nz-lic-20261106-ethosuximide-1 -->

> **Review correction (2026-11-06)**: Originally stated: "A review of the regulatory pathway, since both New Zealand licences are cancelled and the route compatibility for the new indication has not been assessed" Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 2 listings for this drug, 2 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-ethosuximide-1 -->

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "since both New Zealand licences are cancelled and the route compatibility for the new indication has not been assessed" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


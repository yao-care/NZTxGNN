---
noindex: true
sitemap: false
layout: default
title: Cyclizine Lactate
parent: Model Prediction Only (L5)
nav_order: 122
evidence_level: L5
indication_count: 0
last_modified_at: 2026-11-12
predicted_indications: []
---

# Cyclizine Lactate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **0** 
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

## Cyclizine Lactate: Repurposing Evaluation — No Predictions Available
{: .text-alpha }

## One-Sentence Summary

Cyclizine Lactate is an antihistamine/antiemetic agent; original indication data was not retrieved from the current Evidence Pack.
The TxGNN model returned **no predicted new indications** for this drug at the time of query (2026-04-20).
Due to the absence of predictions, regulatory records, and safety data, a full repurposing evaluation cannot be completed at this stage.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not available (no regulatory records found) |
| Predicted New Indication | None — TxGNN returned no predictions |
| TxGNN Prediction Score | N/A |
| Evidence Level | L5 (insufficient — no predictions, no supporting studies retrieved) |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 3 |
| Recommended Decision | **Hold** |

---

## Why is This Prediction Reasonable?

No TxGNN predictions were returned for Cyclizine Lactate in this evidence pack, so a mechanistic bridge between an original and new indication cannot be constructed at this time.

Detailed mechanism of action data is also not available in the current pack (DrugBank ID was not resolved, and MOA is marked as a data gap). Based on published pharmacological literature, Cyclizine Lactate belongs to the piperazine antihistamine class and is primarily used for nausea, vomiting, and motion sickness via H₁-receptor antagonism and central anticholinergic activity — however, these observations are drawn from external knowledge, not from verified Evidence Pack fields, and should be confirmed before proceeding.

Until TxGNN scores and MOA data are confirmed through a complete data pipeline run, no repurposing hypothesis can be evaluated.

---

## Clinical Trial Evidence

Currently no related clinical trials registered (no predicted indication available to scope a trial search).

---

## Literature Evidence

Currently no related literature available (no predicted indication available to scope a literature search).

---

## New Zealand Market Information

<!-- review:begin nz-lic-20261106-cyclizine_lactate-1 -->

> **Review correction (2026-11-06)**: Originally stated: "Cyclizine Lactate has **no registered authorizations** in New Zealand. No license records, dosage forms, or approved indication texts were returned by the TFDA/Medsafe query." Removed: leftover template text about Taiwan's TFDA, which does not apply to this New Zealand page. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-cyclizine_lactate-1 -->

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2231220 | Nausicalm | Cyclizine hydrochloride | Tab 50 mg | Yes |
| 2595877 | Hameln | Cyclizine lactate | Inj 50 mg per ml, 1 ml ampoule | Yes |
| 2696290 | Cyclizine lactate Medsurge | Cyclizine lactate | Inj 50 mg per ml, 1 ml ampoule | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

---

## Safety Considerations

Please refer to the package insert for safety information.

> All safety fields (key warnings, contraindications, drug–drug interactions) returned no data or were flagged as data gaps. The DDI query returned `not_found`.

<!-- review:begin nz-lic-20261106-cyclizine_lactate-2 -->

> **Review correction (2026-11-06)**: Originally stated: "The DDI query returned `not_found`. The TFDA package insert query reported 1 result but content was not parsed into the evidence pack — remediation is required before any safety assessment can proceed." Removed: leftover template text about Taiwan's TFDA, which does not apply to this New Zealand page. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-cyclizine_lactate-2 -->

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The evidence pack is critically incomplete — there are no TxGNN-predicted indications, no safety data, and no New Zealand market records — making it impossible to evaluate any repurposing hypothesis or assess benefit–risk at this stage.

**To proceed, the following is needed:**

- **[High — DG002]** Resolve the DrugBank ID for Cyclizine Lactate and retrieve the confirmed mechanism of action (MOA); required for mechanistic plausibility analysis.
- **[Pipeline]** Re-run the TxGNN prediction pipeline for this drug to obtain disease scores; the current `predicted_indications` array is empty, which may indicate a data ingestion or mapping failure rather than a true absence of signal.
- **[Verification]** Confirm whether the 1 result returned by the `tfda_package_insert` query (query log ID 4) contains usable content, and ingest it into the evidence pack fields before re-evaluation.

<!-- review:begin nz-lic-20261106-cyclizine_lactate-3 -->

> **Review correction (2026-11-06)**: Originally stated: "**[Blocking — DG001]** Retrieve and parse the TFDA/Medsafe package insert PDF to extract warnings, contraindications, and approved indications; this is required before any safety screening can occur. / **[High — DG002]** Resolve the DrugBank ID for Cyclizine Lactate and retrieve the confirmed mechanism of action (MOA); required for mechanistic plausibility analysis." Removed: leftover template text about Taiwan's TFDA, which does not apply to this New Zealand page. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-cyclizine_lactate-3 -->

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "Cyclizine Lactate has **no registered authorizations** in New Zealand. No license records, dosage forms, or approved indication texts were r" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "The TFDA package insert query reported 1 result but content was not parsed into the evidence pack — remediation is required before any safet" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "**[Blocking — DG001]** Retrieve and parse the TFDA/Medsafe package insert PDF to extract warnings, contraindications, and approved indicatio" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Digoxin
parent: Model Prediction Only (L5)
nav_order: 150
evidence_level: L5
indication_count: 5
monitor_inn: "DIGOXIN"
predicted_indications: ["Prinzmetal angina", "duodenal obstruction", "duodenal ulcer (disease)", "duodenogastric reflux", "hypoalphalipoproteinemia"]
last_modified_at: 2026-11-19
description: "Digoxin: predicted new indication Prinzmetal angina (evidence level L5). Approved use: Not recorded in the local data"
---

# Digoxin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

## Digoxin: Cardiac glycoside (Na⁺/K⁺-ATPase inhibitor) → Prinzmetal angina
{: .text-alpha }

## One-Sentence Summary

Digoxin is a cardiac glycoside that inhibits the Na⁺/K⁺-ATPase pump and is used as a heart-rate and heart-function medicine.
The TxGNN model predicts it may be relevant for **Prinzmetal angina**, but this is a model prediction only, with **0 clinical trials** and **1 loosely related publication** that does not study digoxin in this condition.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Prinzmetal angina |
| TxGNN Prediction Score | 0.9981 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (4 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 4 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for digoxin: "All of digoxin’s actions are mediated through its effects on Na-K ATPase. This enzyme, the “sodium pump,” is responsible for maintaining the intracellular milieu throughout the body by moving sodium ions out of and potassium ions into cells." ([DailyMed: Lanoxin (digoxin) prescribing information, Covis Pharma US, Inc](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=9ec84cac-6ecc-43dc-8006-6ccc029e2707), label version 2025-10-17; accessed 2026-11-19.) Digoxin is generally understood as a Na⁺/K⁺-ATPase inhibitor with positive inotropic and vagomimetic (slowing of conduction) effects.

On the available information, no credible mechanistic link to Prinzmetal angina can be established. Prinzmetal angina is driven by coronary vasospasm, and digoxin has no known role in preventing it. Cardiac glycosides may even raise vascular tone, which could theoretically aggravate vasospasm. The high score of 0.9981 reflects a model-based prediction pattern only and should not be read as clinical support.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10736610](https://pubmed.ncbi.nlm.nih.gov/10736610/) | 1999 | Review | Acta Physiol Pharmacol Bulg | General review of chronopharmacology in antihypertensive treatment; it does not address digoxin or vasospastic angina |

---

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2527197 | Lanoxin S29 | Digoxin | Oral liq 50 mcg per ml | Yes |
| 426490 | Lanoxin | Digoxin | Oral liq 50 mcg per ml | Yes |
| 2378078 | Lanoxin | Digoxin | Tab 250 mcg | Yes |
| 2377012 | Lanoxin PG | Digoxin | Tab 62.5 mcg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

---

## Safety Considerations

- **Drug Interactions**: 100 interactions were found in the interaction screening. Examples at moderate level include acarbose, rabeprazole, doxycycline, epinephrine, hydrocortisone, metformin, amphotericin B, bupropion, acetylsalicylic acid, dexamethasone and tetracycline. Minor-level examples include albiglutide, hyoscyamine, atropine and glycopyrronium.

Please refer to the package insert for warnings and contraindications, which were not available in the data compiled for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone. No clinical trials exist, the only linked publication does not study digoxin in this condition, and there is no plausible mechanism. Digoxin may theoretically worsen vasospasm.

**To proceed, the following is needed:**
- Mechanism of action data and a credible pharmacological rationale for coronary vasospasm
- Direct clinical or preclinical studies of digoxin in Prinzmetal angina
- Package insert warnings and contraindications from Medsafe for safety screening

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Currently, detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Lanoxin (digoxin) prescribing information, Covis Pharma US, Inc](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=9ec84cac-6ecc-43dc-8006-6ccc029e2707) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Caffeine
parent: Model Prediction Only (L5)
nav_order: 78
evidence_level: L5
indication_count: 8
monitor_inn: "Caffeine"
predicted_indications: ["nasal cavity disease", "thrombotic disease", "papillary conjunctivitis", "neuralgia", "glossodynia", "coccygodynia", "trigeminal autonomic cephalalgia", "vein disease"]
last_modified_at: 2026-11-24
description: "Caffeine: predicted new indication nasal cavity disease (evidence level L5). In New Zealand: 1 Pharmac Schedule item"
---

# Caffeine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
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

## Caffeine: adenosine receptor antagonist and phosphodiesterase inhibitor → nasal cavity disease
{: .text-alpha }

## One-Sentence Summary

Caffeine is a methylxanthine stimulant that blocks adenosine receptors.
The TxGNN model predicts it may be relevant to **nasal cavity disease**, with a score of 0.9991.
No clinical trials are registered for this prediction, and the 3 publications retrieved are a review, a preclinical formulation study and an off-topic animal study; none shows benefit in nasal disease.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | nasal cavity disease |
| TxGNN Prediction Score | 0.9991 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Caffeine is an adenosine receptor antagonist (A1, A2A, A2B and A3) and also acts on ryanodine receptors (RyR1–3) and the IP3 receptor type 1. This description relies on the receptor-binding data. According to the Mechanism of Action section of the US FDA-approved prescribing information for caffeine: "Caffeine is structurally related to other methylxanthines, theophylline, and theobromine. It is a bronchial smooth muscle relaxant, a CNS stimulant, a cardiac muscle stimulant, and a diuretic. Although the mechanism of action of caffeine in apnea of prematurity is not known, several mechanisms have been hypothesized." ([DailyMed: caffeine citrate prescribing information, Sagent Pharmaceuticals](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=5f38c395-0093-4afd-89ec-f96e5dc0934a), label version 2026-07-29; accessed 2026-11-20.)

The link to nasal cavity disease is indirect. Bitter taste receptors are present in the airway and nasal epithelium and may respond to caffeine, and a nasal caffeine gel has been studied as a delivery route for cognition after sleep deprivation. Neither finding shows benefit in any nasal disease. "Nasal cavity disease" is also a broad category, so related uses do not amount to evidence of efficacy for it.

Because no cited study directly examines caffeine in nasal cavity disease, the evidence level is L5 (model prediction only).

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [26272040](https://pubmed.ncbi.nlm.nih.gov/26272040/) | 2015 | Review | Pharmacology & Therapeutics | Bitter taste receptors are found in airway and nasal tissues; their pharmacology and role in human airways are reviewed. Not specific to caffeine treatment of nasal disease. |
| [35579146](https://pubmed.ncbi.nlm.nih.gov/35579146/) | 2022 | Preclinical formulation study | Current Drug Delivery | A thermo-sensitive nasal caffeine in situ gel was developed to enhance cognition after sleep deprivation. It is a delivery-route study, not a nasal disease study. |
| [9751618](https://pubmed.ncbi.nlm.nih.gov/9751618/) | 1998 | Animal study | Cancer Research | Black tea and caffeine inhibited lung tumours in rats given a tobacco carcinogen. Off-topic for nasal disease. |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 461253 | Biomed | Caffeine citrate | Oral liq 20 mg per ml (10 mg base per ml) | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

Caffeine is marketed in New Zealand. Its approved indication text is not recorded in the local data.

<!-- review:begin nz-lic-20261106-caffeine-1 -->

> **Review correction (2026-11-06)**: Originally stated: "Caffeine is marketed in New Zealand. The only licence on record is an oral liquid (20 mg per ml) product, Biomed (licence 461253)." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Quick Overview and the table under New Zealand Market Information show the current Pharmac Schedule figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-caffeine-1 -->

## Safety Considerations

- **Drug targets (pharmacology data):** Caffeine binds adenosine A1, A2A, A2B and A3 receptors, IP3 receptor 1, and ryanodine receptors 1–3. No specific drug–drug interaction entries were returned.

Please refer to the package insert for warnings and contraindications, as none are recorded in the data compiled for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is very high (0.9991), but no trial or publication studies caffeine in nasal cavity disease, so the evidence level is L5. The related literature is indirect: airway taste-receptor biology and a nasal delivery gel for cognition.

**To proceed, the following is needed:**
- Define a specific nasal condition (e.g. rhinitis subtype) instead of the broad category
- Preclinical or early clinical data on caffeine's effect in that condition
- Mechanism-of-action data and package insert warnings and contraindications
- A safety review for cardiovascular, sleep and anxiety effects relevant to the target population

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "The only licence on record is an oral liquid (20 mg per ml) product, Biomed (licence 461253)" | Corrected (revised 2026-11-24; earlier version kept in the record) | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-20 | "The mechanism-of-action field itself is not recorded in the data compiled for this report, so this description relies on the receptor-binding data" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt (revised 2026-11-20; earlier version kept in the record) | [DailyMed: caffeine citrate prescribing information, Sagent Pharmaceuticals](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=5f38c395-0093-4afd-89ec-f96e5dc0934a) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


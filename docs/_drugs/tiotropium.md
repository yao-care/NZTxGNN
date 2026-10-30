---
layout: default
title: Tiotropium
parent: High Evidence (L1-L2)
nav_order: 452
evidence_level: L1
indication_count: 5
last_modified_at: 2026-10-30
monitor_inn: "Tiotropium"
predicted_indications: ["obstructive lung disease", "respiratory malformation", "Rienhoff syndrome", "hyperlucent lung", "tracheal stenosis"]
description: "Tiotropium: predicted new indication Obstructive lung disease (evidence level L1). Approved use: Not recorded in the local data"
---

# Tiotropium
{: .fs-9 }

Evidence Level: **L1** | Predicted Indications: **5** 
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

## Tiotropium: Obstructive lung disease
{: .text-alpha }

## One-Sentence Summary

Tiotropium is an inhaled long-acting bronchodilator originally used for bronchitis, asthma, and pulmonary emphysema.
The TxGNN model predicts it may be effective for **obstructive lung disease**, but this is effectively an on-label use, because the predicted disease is the parent category of the drug's existing indications.
The prediction is supported by **38 clinical trials** (many Phase 3) and **20 publications** compiled for this report, including RCTs and meta-analyses.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Obstructive lung disease |
| TxGNN Prediction Score | 0.9999 |
| Evidence Level | L1 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Tiotropium is a long-acting muscarinic antagonist (LAMA). It blocks M3 receptors on airway smooth muscle and dissociates from them very slowly, producing sustained bronchodilation that allows once-daily dosing. Pharmacology data compiled for this report show it binds all five muscarinic receptor subtypes (M1–M5). Detailed mechanism of action data is otherwise not available in the data compiled for this report.

"Obstructive lung disease" is the parent category of tiotropium's established indications: COPD, emphysema, chronic bronchitis, and asthma. This is therefore not true repurposing. The prediction mostly confirms existing use, and Phase 3 trials directly support efficacy in COPD and asthma.

The model also ranked nine further diseases. Respiratory malformation (L4) is supported only by cystic fibrosis trials, which are indirect. The remaining seven (for example Rienhoff syndrome, tracheal stenosis) have no clinical evidence and no plausible mechanism. Their scores come from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. All of these were rated Hold.

---

## Clinical Trial Evidence

The most relevant of the 38 trials for obstructive lung disease (the compiled list is mostly COPD, with some asthma trials):

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00240435](https://clinicaltrials.gov/study/NCT00240435) | Phase 3 | Completed | 491 | Tiotropium Respimat (5 and 10 mcg) vs placebo and ipratropium in COPD; 12-week bronchodilator efficacy and safety |
| [NCT01431274](https://clinicaltrials.gov/study/NCT01431274) | Phase 3 | Completed | 2624 | 52-week tiotropium + olodaterol vs individual components in COPD (TONADO 1) |
| [NCT00877383](https://clinicaltrials.gov/study/NCT00877383) | Phase 3 | Completed | 1142 | Indacaterol added to tiotropium vs tiotropium alone, 12 weeks, moderate-to-severe COPD |
| [NCT00900731](https://clinicaltrials.gov/study/NCT00900731) | Phase 3 | Completed | 1598 | Indacaterol vs tiotropium 18 µg, 12 weeks, moderate-to-severe COPD |
| [NCT01854645](https://clinicaltrials.gov/study/NCT01854645) | Phase 3 | Completed | 2103 | 24-week study of PT003, PT005 and PT001 vs placebo, with open-label tiotropium as active control |
| [NCT00157235](https://clinicaltrials.gov/study/NCT00157235) | Phase 3 | Completed | 234 | Tiotropium vs placebo on exercise tolerance during pulmonary rehabilitation |
| [NCT00772538](https://clinicaltrials.gov/study/NCT00772538) | Phase 3 | Completed | 459 | Tiotropium 5 mcg vs placebo over 48 weeks as add-on therapy in severe persistent asthma |
| [NCT01316380](https://clinicaltrials.gov/study/NCT01316380) | Phase 3 | Completed | 465 | Tiotropium (2.5 and 5 µg) vs placebo over 12 weeks in mild persistent asthma |
| [NCT04780984](https://clinicaltrials.gov/study/NCT04780984) | Phase 2 | Completed | 116 | Dose-ranging pharmacodynamics, bioavailability and safety of tiotropium inhalation solution in COPD |
| [NCT02172443](https://clinicaltrials.gov/study/NCT02172443) | Phase 3 | Completed | 50 | Tiotropium 18 mcg capsules vs Atrovent MDI in COPD; small active-comparator study |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28877027](https://pubmed.ncbi.nlm.nih.gov/28877027/) | 2017 | RCT | N Engl J Med | Tests whether long-term tiotropium improves lung function and slows its decline in mild-to-moderate COPD |
| [29779416](https://pubmed.ncbi.nlm.nih.gov/29779416/) | 2018 | RCT | Am J Respir Crit Care Med | SUNSET: de-escalating long-term triple therapy to indacaterol/glycopyrronium in COPD |
| [16844726](https://pubmed.ncbi.nlm.nih.gov/16844726/) | 2006 | Meta-analysis | Thorax | Tiotropium efficacy on clinical events, symptoms, lung function and adverse events in stable COPD |
| [25046211](https://pubmed.ncbi.nlm.nih.gov/25046211/) | 2014 | Systematic review | Cochrane Database Syst Rev | Tiotropium vs placebo in COPD, including soft mist inhaler trials |
| [32727455](https://pubmed.ncbi.nlm.nih.gov/32727455/) | 2020 | Review | Respir Res | Clinical development of tiotropium in COPD; LAMA monotherapy is recommended as initial treatment for GOLD groups B, C and D |
| [33095662](https://pubmed.ncbi.nlm.nih.gov/33095662/) | 2021 | Review | Curr Med Res Opin | Tiotropium + olodaterol for reducing and managing COPD exacerbations |
| [23728670](https://pubmed.ncbi.nlm.nih.gov/23728670/) | 2013 | Systematic review | Cochrane Database Syst Rev | Inhaled steroid plus LABA combination vs tiotropium in COPD |
| [21672999](https://pubmed.ncbi.nlm.nih.gov/21672999/) | 2011 | Meta-analysis | BMJ | Mortality associated with the tiotropium mist inhaler in COPD (a safety signal to review) |
| [12010082](https://pubmed.ncbi.nlm.nih.gov/12010082/) | 2002 | Review | Drugs | Tiotropium antagonises M1, M2 and M3 receptors; slow dissociation from M3 supports once-daily dosing |
| [10069510](https://pubmed.ncbi.nlm.nih.gov/10069510/) | 1999 | Review | Life Sci | Mechanistic considerations and clinical profile of tiotropium in obstructive lung disease |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2115468 | Spiriva (valid) | Powder for inhalation, 18 mcg per dose | Not recorded |
| 2494019 | Spiriva Respimat (valid) | Solution for inhalation, 2.5 mcg per dose | Not recorded |


---

## Safety Considerations

- **Conditions flagged in the knowledge graph (not label text)**: benign prostatic hyperplasia, impaired renal function, angle-closure (closed-angle) glaucoma, and bladder neck obstruction. These are consistent with the known anticholinergic effects of the drug class. A small ocular study (NCT06525051, n=108) examined tiotropium's effect on anterior chamber parameters and intraocular pressure.
- **Drug interactions**: no drug-drug interactions were identified. The interaction data compiled for this report only list receptor targets (muscarinic M1–M5).
- **Mortality signal**: a meta-analysis of the tiotropium mist inhaler in COPD (PMID 21672999) examined mortality risk and should be reviewed before use of that device.

Please refer to the package insert for full warnings and contraindications, as label-based safety text was not available.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Obstructive lung disease is the parent category of tiotropium's existing indications, and multiple completed Phase 3 RCTs plus meta-analyses support efficacy (Level L1).

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (label-based safety screening is currently blocked by this gap)
- Detailed mechanism of action data from DrugBank
- Clarification of New Zealand licensing: whether to re-register or import, and for which indications
- A safety plan for patients with glaucoma, urinary outflow obstruction (benign prostatic hyperplasia, bladder neck obstruction), or renal impairment
- Review of the mortality signal for the Respimat inhaler device
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


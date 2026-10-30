---
layout: default
title: Aminophylline
parent: Model Prediction Only (L5)
nav_order: 33
evidence_level: L5
indication_count: 9
monitor_inn: "AMINOPHYLLINE"
predicted_indications: ["migraine disorder", "migraine with brainstem aura", "atrophoderma vermiculata", "ulerythema ophryogenesis", "Prinzmetal angina", "pulmonary hypertension", "sciatic neuropathy", "hyperthyroidism", "kyphoscoliotic heart disease"]
last_modified_at: 2026-10-30
description: "Aminophylline: predicted new indication migraine disorder (evidence level L5). Approved use: Not recorded in the local data"
---

# Aminophylline
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

## Aminophylline: adenosine antagonist and phosphodiesterase inhibitor → migraine disorder
{: .text-alpha }

## One-Sentence Summary

Aminophylline is a methylxanthine drug that blocks adenosine receptors and inhibits phosphodiesterase.
The TxGNN model predicts it may be effective for **migraine disorder**.
Support is limited: **no clinical trials** and **4 publications** (one review, two case reports and one historical review), and none is a controlled study.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | migraine disorder |
| TxGNN Prediction Score | 0.9988 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, aminophylline is a nonselective adenosine receptor antagonist and phosphodiesterase inhibitor.

Migraine may be driven in part by impaired brain energy metabolism and high adenosine levels, and adenosine signalling has been linked to trigeminovascular activation and cerebral vasodilation. A 2023 review (PMID 38059379) discusses aminophylline in pain and migraine, noting its use in post-dural headache. A case report (PMID 34308528) describes a hemiplegic migraine episode after regadenoson, an adenosine agonist. This supports the plausibility of adenosine antagonism as a countermeasure.

However, none of the cited studies tests aminophylline as a treatment in patients with migraine in a controlled way. The link therefore remains a hypothesis, and the evidence level is capped at L5.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38059379](https://pubmed.ncbi.nlm.nih.gov/38059379/) | 2023 | Review | Pain Management | Proposes that migraine involves impaired brain energy metabolism and high adenosine; discusses aminophylline as an adenosine antagonist for pain, especially post-dural headache |
| [34308528](https://pubmed.ncbi.nlm.nih.gov/34308528/) | 2022 | Case report | J Nucl Cardiol | Hemiplegic migraine episode triggered by the adenosine agonist regadenoson; methylxanthines such as aminophylline can reverse its adverse effects |
| [7728647](https://pubmed.ncbi.nlm.nih.gov/7728647/) | 1995 | Case report | Can J Cardiol | Patient with chest pain attributed to excessive adenosine effect ("myocardial migraine") |
| [14168418](https://pubmed.ncbi.nlm.nih.gov/14168418/) | 1964 | Review | Aggiornamenti Clinicoterapeutici | Historical review of headaches; no abstract available |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 333956 | DBL Aminophylline | Inj 25 mg per ml, 10 ml ampoule | Not recorded |

## Safety Considerations

- **Drug Interactions**: The data compiled for this report list 100 interactions. Among those shown, the major-severity one is **bupropion**. Moderate-severity interactions include:
  - H2 blockers: famotidine, ranitidine, cimetidine, nizatidine
  - Corticosteroids: hydrocortisone, dexamethasone, betamethasone, triamcinolone, budesonide
  - Antibiotics: doxycycline, tetracycline, minocycline, clarithromycin
  - Bowel preparations: sodium sulfate, polyethylene glycol, picosulfuric acid
  - Others: obeticholic acid, metreleptin

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The model score is very high, but no clinical trial or controlled study of aminophylline in migraine exists. The only support is a review, case reports and a mechanistic argument. The drug also has a large interaction burden.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe
- Detailed mechanism of action data
- Controlled or at least observational clinical data on aminophylline in migraine, such as post-dural headache cohorts
- An interaction management plan given the 100 recorded interactions, including bupropion
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


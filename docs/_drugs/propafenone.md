---
layout: default
title: Propafenone
parent: Model Prediction Only (L5)
nav_order: 389
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-28
monitor_inn: "Propafenone"
predicted_indications: ["manic bipolar affective disorder", "periodic paralysis with transient compartment-like syndrome", "Prinzmetal angina", "arrhythmogenic right ventricular cardiomyopathy", "nephrogenic syndrome of inappropriate antidiuresis", "trichotillomania"]
description: "Propafenone: predicted new indication Manic bipolar affective disorder (evidence level L5). Approved use: Not recorded in the local data"
---

# Propafenone
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

## Propafenone: Manic bipolar affective disorder
{: .text-alpha }

## One-Sentence Summary

Propafenone is a class IC antiarrhythmic originally used for atrial fibrillation and ventricular tachycardia.
The report below evaluates the highest-ranked prediction (manic bipolar affective disorder) and the best-supported candidate, **catecholaminergic polymorphic ventricular tachycardia (CPVT)**. The top-ranked prediction has no supporting evidence of benefit, while CPVT has **0 registered clinical trials** and **10 publications**, mostly case reports, cohort studies and preclinical mechanistic work.

---

## Quick Overview

The top-ranked TxGNN prediction (manic bipolar affective disorder) is shown first. The literature points to propafenone as a possible cause of mania, not a treatment, so the best-supported candidate (CPVT) is shown alongside it.

| Item | Rank 1: Manic bipolar affective disorder | Rank 2: CPVT (best-supported) |
|------|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Manic bipolar affective disorder |
| TxGNN Prediction Score | 0.9980 |
| Evidence Level | L5 | L3 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) | Cancelled only |
| Number of Authorizations | 1 (valid: 1) | 1 |
| Recommended Decision | Hold | Proceed with Guardrails (research question only) |

The New Zealand product record contains no approved indication text.

---

## Why is This Prediction Reasonable?

**Rank 1 (manic bipolar affective disorder).** No plausible therapeutic mechanism is supported. The available literature describes propafenone as a cause of mania (a 1985 case report) and as part of interactions that produced psychiatric adverse effects (a 2001 venlafaxine-propafenone psychosis case and a 2020 review of antipsychotic-cardiac drug interactions). This is a safety signal, not evidence of benefit. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

**Rank 2 (CPVT).** Detailed mechanism of action data is not available in the data compiled for this report. Pharmacology records show that propafenone acts on β1- and β2-adrenoceptors and on the Kv1.5 potassium channel, and it is a class IC sodium channel blocker.

CPVT is usually caused by gain-of-function mutations in the ryanodine receptor 2 (RyR2), which cause diastolic calcium leak and delayed afterdepolarizations. Preclinical studies (PMIDs 21270101, 26121139, 21798265) show that class I antiarrhythmics, including propafenone (its R-enantiomer) and flecainide, inhibit RyR2 channels and suppress calcium waves, and that this inhibition helps determine their efficacy. Flecainide is the better-established agent in CPVT and is the relevant comparator. A case report describes 35 years of effective propafenone treatment in CPVT (PMID 30820400).

Other predictions are weaker. Incessant infant ventricular tachycardia (rank 5) is a direct extension of the ventricular tachycardia indication to children, supported by small, old, uncontrolled series. Arrhythmogenic right ventricular cardiomyopathy (rank 6) is mostly associated with harm. Periodic paralysis, Prinzmetal angina, nephrogenic syndrome of inappropriate antidiuresis and trichotillomania (ranks 3, 4, 7 and 8) are predictions only, with no literature or trials.

---

## Clinical Trial Evidence

Currently no related clinical trials registered, for either the top-ranked prediction or CPVT.

---

## Literature Evidence

Literature for CPVT (the best-supported candidate):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [30820400](https://pubmed.ncbi.nlm.nih.gov/30820400/) | 2019 | Case report | HeartRhythm Case Reports | 35-year effective treatment of CPVT with propafenone |
| [21270101](https://pubmed.ncbi.nlm.nih.gov/21270101/) | 2011 | Mechanistic | Circ Arrhythm Electrophysiol | RyR2 inhibition determines the efficacy of class I antiarrhythmics in CPVT |
| [26121139](https://pubmed.ncbi.nlm.nih.gov/26121139/) | 2015 | Mechanistic | PLoS One | RyR2 channel activity determines the potency of flecainide and R-propafenone against arrhythmogenic calcium waves |
| [21798265](https://pubmed.ncbi.nlm.nih.gov/21798265/) | 2011 | Mechanistic | J Mol Cell Cardiol | Screen of class I antiarrhythmics for suppression of Ca2+ waves in casq2-deficient myocytes |
| [39564160](https://pubmed.ncbi.nlm.nih.gov/39564160/) | 2024 | Cohort | Ann Pediatr Cardiol | More than 10-year follow-up of CPVT patients (not propafenone-specific) |
| [35892906](https://pubmed.ncbi.nlm.nih.gov/35892906/) | 2022 | Systematic review | Life (Basel) | Clinical, genetic and arrhythmic outcomes of CPVT in China |
| [23965883](https://pubmed.ncbi.nlm.nih.gov/23965883/) | 2013 | Cohort | Zhongguo Dang Dai Er Ke Za Zhi | Long-term outcomes and predictors of ventricular arrhythmia in CPVT |

Literature for the top-ranked prediction (manic bipolar affective disorder):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [2579063](https://pubmed.ncbi.nlm.nih.gov/2579063/) | 1985 | Case report | J Clin Psychiatry | Mania secondary to propafenone |
| [11949740](https://pubmed.ncbi.nlm.nih.gov/11949740/) | 2001 | Case report | Int J Psychiatry Med | Organic psychosis from a venlafaxine-propafenone interaction in a bipolar patient |
| [32124390](https://pubmed.ncbi.nlm.nih.gov/32124390/) | 2020 | Review | Pharmacol Rep | Harmful interactions between antipsychotics and cardiovascular drugs |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2534339 | Rytmonorm (valid) | Tab 150 mg | Not recorded |

---

## Safety Considerations

- **Contraindications and risk conditions from the knowledge graph (not label text):** long QT syndrome (including familial forms), torsades de pointes, kidney disease, liver disease, gallbladder disease, potassium deficiency, hypotension, myocardial infarction, coronary artery disease and atherosclerosis, myasthenia gravis and other neuromuscular junction disease, hemoglobinopathy, and agranulocytosis and neutropenia (including congenital forms).
- **Pharmacological targets:** β1-adrenoceptor, β2-adrenoceptor and Kv1.5 (KCNA5). No specific drug-drug interaction entries with severity levels were returned.
- **Prediction-specific signals:** propafenone caused QRS widening in a child with ARVC (PMID 33194879) and incessant ventricular tachycardia in ARVC patients with normal left ventricular function (PMID 8316922). Class IC agents are generally cautioned in ischemic heart disease and structural heart disease, and proarrhythmia and negative inotropy need to be addressed for any new use, particularly in children.

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold for manic bipolar affective disorder and the other predictions without evidence; Proceed with Guardrails for CPVT as a research question only.**

**Rationale:**
- The top-ranked prediction is unsupported and the literature suggests harm. CPVT has a credible mechanism (RyR2 inhibition) and a long-term case report, but there are no registered trials, the clinical evidence is limited to case reports and non-specific cohorts, and flecainide is the established comparator.
- The only New Zealand authorization is cancelled, so no product is currently marketed.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Detailed mechanism of action data from DrugBank
- A head-to-head comparison with flecainide in CPVT and controlled clinical data for propafenone
- An assessment of proarrhythmia and structural-heart-disease risk in CPVT patients
- Dedicated pediatric dosing and safety evaluation if the infant ventricular tachycardia indication is pursued
- Confirmation of an available product, since the market status is cancelled only
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


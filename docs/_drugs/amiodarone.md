---
layout: default
title: Amiodarone
parent: Model Prediction Only (L5)
nav_order: 34
evidence_level: L5
indication_count: 9
last_modified_at: 2026-11-06
monitor_inn: "Amiodarone"
predicted_indications: ["catecholaminergic polymorphic ventricular tachycardia", "incessant infant ventricular tachycardia", "obsolete bundle branch block", "rheumatoid arthritis", "brachydactyly-syndactyly syndrome", "heparin cofactor 2 deficiency", "trichotillomania", "colobomatous microphthalmia-rhizomelic dysplasia syndrome", "gout"]
description: "Amiodarone: predicted new indication Catecholaminergic polymorphic ventricular tachycardia (evidence level L5). Approved use: Not recorded in the local data"
---

# Amiodarone
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

## Amiodarone: Catecholaminergic polymorphic ventricular tachycardia
{: .text-alpha }

## One-Sentence Summary

Amiodarone is an antiarrhythmic medicine used for serious heart rhythm disorders. The TxGNN model predicts it may be useful for **catecholaminergic polymorphic ventricular tachycardia (CPVT)**, with a score of 0.9978. There are **no registered clinical trials** and the retrieved publications (mainly cohort studies and case reports) do not test amiodarone for this condition, so the evidence is weak.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Catecholaminergic polymorphic ventricular tachycardia |
| TxGNN Prediction Score | 0.9978 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 3 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available for amiodarone. Pharmacology data compiled for this report show it acts on several ion channels, including the cardiac sodium channel Nav1.5 and the potassium channel Kv1.7, and it is described as a drug for angina and arrhythmias.

CPVT is a rare inherited rhythm disorder. Mutations in RYR2 or CASQ2 cause calcium leak in heart muscle cells, which triggers dangerous ventricular arrhythmias during exercise or stress. Amiodarone's calcium-channel and beta-blocking actions give a plausible but weak link to this mechanism.

The link is not supported in practice. Guideline-preferred therapy for CPVT is beta-blockers, with flecainide as an add-on, and amiodarone is generally considered ineffective for this condition. The model's high score therefore should be read as a research question and not as evidence of benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

None of the retrieved publications evaluates amiodarone as a treatment for CPVT.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [26513538](https://pubmed.ncbi.nlm.nih.gov/26513538/) | 2015 | Review | Expert Opin Pharmacother | Overview of drug treatment for ventricular arrhythmias; antiarrhythmic drugs remain important for acute conversion and chronic prevention |
| [35892906](https://pubmed.ncbi.nlm.nih.gov/35892906/) | 2022 | Systematic review | Life (Basel) | Clinical features, genetics and arrhythmic outcomes of CPVT patients from China |
| [39076628](https://pubmed.ncbi.nlm.nih.gov/39076628/) | 2022 | Cohort | Rev Cardiovasc Med | Clinical features, genetics, healthcare use and costs in CPVT patients from a Chinese city |
| [22553997](https://pubmed.ncbi.nlm.nih.gov/22553997/) | 2012 | Case report | Pacing Clin Electrophysiol | Flecainide suppressed defibrillator-induced storming in CPVT (flecainide, not amiodarone) |
| [30116135](https://pubmed.ncbi.nlm.nih.gov/30116135/) | 2018 | Case report | Turk Pediatri Arsivi | CPVT as a rare cause of sudden cardiac arrest in a 2-year-old |
| [29668588](https://pubmed.ncbi.nlm.nih.gov/29668588/) | 2018 | Case report | Medicine | Delayed diagnosis of CPVT with an RYR2 mutation in a child |
| [39735866](https://pubmed.ncbi.nlm.nih.gov/39735866/) | 2024 | Case report | Front Cardiovasc Med | Right cardiac sympathetic denervation resolved CPVT in a teenager after left-sided denervation |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2565129 | Max Health | Amiodarone hydrochloride | Inj 50 mg per ml, 3 ml ampoule | Yes |
| 770620 | Aratac | Amiodarone hydrochloride | Tab 100 mg | Yes |
| 770639 | Aratac | Amiodarone hydrochloride | Tab 200 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Conditions listed as contraindications or cautions in the data compiled for this report** (not taken from product label text): long QT syndrome (including familial), torsades de pointes, sick sinus syndrome, atrioventricular block, thyroid disease (hypothyroidism, hyperthyroidism, Graves disease, thyrotoxicosis), liver disease and drug-induced liver injury, lung conditions (interstitial lung disease, eosinophilic pneumonia, hypersensitivity pneumonitis), optic neuritis and optic nerve disease, peripheral neuropathy, potassium deficiency and corneal deposits.
- **Pharmacology**: amiodarone acts on several ion channels and receptors (Nav1.5, Kv1.7, M5 receptor), consistent with a broad effect on heart rhythm.

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is very high, but no clinical trials exist and no publication tests amiodarone in CPVT. Standard therapy (beta-blockers, with flecainide) is established, and amiodarone is generally considered ineffective for this condition. Amiodarone also carries significant thyroid, liver, lung and proarrhythmic risks. The evidence level is L5.

**To proceed, the following is needed:**
- Direct preclinical or clinical evidence that amiodarone suppresses CPVT arrhythmias
- Comparison against guideline therapy (beta-blockers, flecainide)
- Mechanism of action data
- Package insert warnings and contraindications from Medsafe

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


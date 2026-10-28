---
layout: default
title: Fluoxetine
parent: Model Prediction Only (L5)
nav_order: 208
evidence_level: L5
indication_count: 9
monitor_inn: "Fluoxetine"
predicted_indications: ["schizoid personality disorder", "histrionic personality disorder (disease)", "schizotypal personality disorder", "paranoid personality disorder", "benign paroxysmal torticollis of infancy", "agoraphobia", "manic bipolar affective disorder", "phobic disorder", "Ohdo syndrome and variants"]
last_modified_at: 2026-10-28
description: "Fluoxetine: predicted new indication Schizoid personality disorder (evidence level L5). Approved use: Relations recorded in the knowledge graph: major depressive disorder…"
---

# Fluoxetine
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

## Fluoxetine: From Depression and Anxiety Disorders to Schizoid Personality Disorder
{: .text-alpha }

## One-Sentence Summary

Fluoxetine is a selective serotonin reuptake inhibitor (SSRI) associated with depressive, anxiety, obsessive-compulsive and eating disorders.
The TxGNN model predicts it may be useful for **schizoid personality disorder**, but there are **0 clinical trials** and only **2 loosely related publications**, so the prediction is essentially unsupported by direct evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Relations recorded in the knowledge graph: major depressive disorder, unipolar and endogenous depression, dysthymic disorder, bipolar disorder, anxiety disorder, neurotic disorder, bulimia nervosa, obsessive-compulsive disorder. The New Zealand licence records contain no indication text, so none of these is confirmed here as an approved indication. |
| Predicted New Indication | Schizoid personality disorder |
| TxGNN Prediction Score | 0.9992 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Number of Approvals | 2 (both single-ingredient products; list is complete) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the data compiled for this report. Fluoxetine is an SSRI whose main target is the serotonin transporter (SERT), and it also shows activity at several serotonin receptors. Its use in depression and anxiety-spectrum conditions is well established.

Serotonergic modulation could plausibly help with mood, anxiety or obsessional symptoms that sometimes accompany cluster A personality disorders, which include schizoid, paranoid and schizotypal types. However, there is no disorder-specific mechanistic evidence for schizoid personality disorder itself. The high score appears to be shared across several personality disorder nodes, since three other personality disorders carry an identical score. It should be read as a model-level association, not as evidence of benefit.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [29955451](https://pubmed.ncbi.nlm.nih.gov/29955451/) | 2016 | Review | The Mental Health Clinician | General review of drug treatment in cluster A personality disorders (paranoid, schizoid, schizotypal). It describes how these patients tend to be isolated and have reduced quality of life. The abstract provided does not report fluoxetine-specific findings for schizoid personality disorder. |
| [16390895](https://pubmed.ncbi.nlm.nih.gov/16390895/) | 2006 | Cohort | American Journal of Psychiatry | Six-month outcomes and predictors of course in treated depression. It is not about schizoid personality disorder. |

Neither publication directly studies fluoxetine in schizoid personality disorder, which is why the evidence level is L5 and not higher.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2618575 | Fluox | Dispersible tablet 20 mg, scored | No indication text in the data compiled |
| 2450860 | Arrow-Fluoxetine | Capsule 20 mg | No indication text in the data compiled |

Both licences are valid single-ingredient products.

---

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for authoritative safety information.

**Knowledge-graph contraindication relations** (these come from the knowledge graph, not from label text, and need confirmation against the label):
- **Mood and psychiatric:** bipolar disorder, major affective disorder
- **Neurological and cardiovascular:** epilepsy, stroke and cerebrovascular disorders, cerebral infarction, myocardial infarction, coronary thrombosis, orthostatic hypotension, hypotensive disorder, postural orthostatic tachycardia syndrome, multiple system atrophy
- **Liver and gallbladder:** liver failure, liver disease, cirrhosis, gallbladder disease
- **Metabolic:** diabetes mellitus, hyperglycemia, diabetic ketoacidosis, hyperlipidemia, familial hyperlipidemia, hypertriglyceridemia, obesity, monogenic obesity
- **Blood:** neutropenia, agranulocytosis, blood coagulation disease
- **Other:** benign prostatic hyperplasia, female breast carcinoma

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone. There are no registered trials and no published study of fluoxetine in schizoid personality disorder. The linked literature is a general cluster A review and an unrelated depression outcome study.

**To proceed, the following is needed:**
- Disorder-specific clinical evidence, such as controlled studies of fluoxetine in schizoid personality disorder
- A review of the Medsafe package insert for warnings and contraindications
- Mechanism-of-action data to support a disorder-specific rationale
- Confirmation of the approved indications on the two New Zealand licences
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


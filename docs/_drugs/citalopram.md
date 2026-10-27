---
layout: default
title: Citalopram
parent: Model Prediction Only (L5)
nav_order: 100
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Citalopram: predicted new indication Obsessive-compulsive disorder (evidence level L5). Approved use: Anxiety disorder, neurotic disorder, dysthymic disorder…"
monitor_inn: "Citalopram"
predicted_indications: ["obsessive-compulsive disorder", "histrionic personality disorder (disease)", "schizoid personality disorder", "schizotypal personality disorder", "paranoid personality disorder"]
---

# Citalopram
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

## Citalopram: From Depression and Anxiety Disorders to Obsessive-Compulsive Disorder
{: .text-alpha }

## One-Sentence Summary

Citalopram is a selective serotonin reuptake inhibitor (SSRI), originally used for depressive and anxiety disorders, including major depressive disorder.
The TxGNN model predicts it may be effective for **obsessive-compulsive disorder (OCD)**,
with **4 related clinical trials** (none testing citalopram directly) and **16 publications** (about 6 citalopram-specific, mostly open-label or preliminary studies) currently supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Anxiety disorder, neurotic disorder, dysthymic disorder, unipolar and endogenous depression, major depressive disorder (from the knowledge graph; no approved indication text in the New Zealand licence record) |
| Predicted New Indication | Obsessive-compulsive disorder |
| TxGNN Prediction Score | 99.74% |
| Evidence Level | L2 (conservative; see rationale below) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, citalopram is an SSRI. Pharmacology records show it acts on the serotonin transporter (SERT) and also lists the norepinephrine transporter (NET) as a target. Its efficacy in depression and anxiety disorders is established.

Serotonergic dysfunction is central to OCD, and SSRIs are first-line drug treatment for the disorder. Several publications describe citalopram in OCD, including studies in treatment-resistant adults, children and adolescents, and a concentration-response analysis. This is consistent with the very high TxGNN score.

A caveat applies. None of the listed trials confirms citalopram as the tested intervention, so the L2 level rests on SSRI class evidence and smaller citalopram-specific studies, not on a confirmed citalopram Phase 3 RCT.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02022709](https://clinicaltrials.gov/study/NCT02022709) | Phase 4 | Completed | 78 | Compares exposure and response prevention (ERP), SSRIs and their combination in OCD, and looks for predictors of response in a Chinese population. Specific SSRI not confirmed as citalopram. |
| [NCT00074815](https://clinicaltrials.gov/study/NCT00074815) | Phase 3 | Completed | 124 | Tests whether cognitive behavioural therapy improves serotonin reuptake inhibitor treatment in children with OCD who partially responded. Specific drug not confirmed. |
| [NCT00680602](https://clinicaltrials.gov/study/NCT00680602) | Phase 4 | Completed | 158 | Randomized open comparison of group CBT versus fluoxetine in OCD. Supports SSRI class efficacy, but the drug studied is fluoxetine. |
| [NCT02285699](https://clinicaltrials.gov/study/NCT02285699) | Not applicable | Completed | 43 | Pilot study of gut microbiota in OCD versus healthy controls, with 12 weeks of open-label SSRI treatment. Background context only. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10572334](https://pubmed.ncbi.nlm.nih.gov/10572334/) | 1999 | Open-label randomized trial | Eur Psychiatry | Citalopram alone versus with clomipramine in 16 treatment-resistant OCD patients over 90 days |
| [9241005](https://pubmed.ncbi.nlm.nih.gov/9241005/) | 1997 | Single-blind study | J Clin Psychopharmacol | Compared the antiobsessional efficacy of fluvoxamine, paroxetine and citalopram in 30 OCD patients |
| [9395151](https://pubmed.ncbi.nlm.nih.gov/9395151/) | 1997 | Open pilot study | Acta Psychiatr Scand | 24-week open pilot of citalopram in OCD |
| [12839522](https://pubmed.ncbi.nlm.nih.gov/12839522/) | 2003 | Open-label study (paediatric) | Psychiatry Clin Neurosci | 8-week citalopram (mean 24 mg/day) in 15 children and adolescents with OCD |
| [9466233](https://pubmed.ncbi.nlm.nih.gov/9466233/) | 1997 | Open trial (paediatric) | J Child Adolesc Psychopharmacol | 23 children and adolescents on citalopram for 10 weeks; Y-BOCS scores improved significantly (30 to 21, p<0.001) |
| [15089116](https://pubmed.ncbi.nlm.nih.gov/15089116/) | 2004 | Cohort (preliminary) | CNS Drugs | Preliminary analysis of the relationship between citalopram concentration and clinical response in OCD |
| [10471169](https://pubmed.ncbi.nlm.nih.gov/10471169/) | 1999 | Review/commentary | Int Clin Psychopharmacol | Reviews the use of citalopram in OCD and the link between serotonin reuptake inhibitors and OCD |
| [20712565](https://pubmed.ncbi.nlm.nih.gov/20712565/) | 2010 | Review | Curr Med Chem | Past, present and future OCD drugs; SSRIs are central, but other neurotransmitter systems are likely involved |
| [22305974](https://pubmed.ncbi.nlm.nih.gov/22305974/) | 2012 | Review | BMJ Clin Evid | General OCD overview: prevalence and course |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2638738 (cancelled) | Celapram | Tab 20 mg | Not stated in the licence record |

## Safety Considerations

- **Knowledge-graph contraindication associations** (from the TxGNN knowledge graph, not from label text): long QT syndrome (including familial), torsades de pointes, epilepsy, bipolar disorder and other major affective disorders, liver disease, gallbladder disease, potassium deficiency, blood coagulation disease, heart and cardiovascular disease, neuroleptic malignant syndrome, inappropriate ADH syndrome (including nephrogenic), and serotonin syndrome.
- **Pharmacological targets**: citalopram binds the serotonin transporter (SERT) and norepinephrine transporter (NET). No drug-drug interaction data with specific co-medications were compiled.

Please refer to the package insert for the full, label-based warnings and contraindications.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
OCD responds well to SSRIs, and several open-label and preliminary studies of citalopram in adults and children support the prediction. However, no confirmed citalopram RCT is listed, and the New Zealand licence is cancelled, so any use should be cautious and well monitored.

**To proceed, the following is needed:**
- Locate or run randomized controlled trials with citalopram as the confirmed intervention in OCD
- Obtain the Medsafe package insert to confirm warnings and contraindications, particularly the QT-prolongation and cardiac concerns suggested by the knowledge graph
- Obtain mechanism of action data from DrugBank
- Clarify the regulatory pathway, since there is no currently valid New Zealand licence
- Develop a monitoring plan (e.g. ECG and electrolytes) for patients with cardiac, hepatic or bipolar risk factors
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


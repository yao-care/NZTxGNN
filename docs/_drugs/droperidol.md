---
layout: default
title: Droperidol
parent: Model Prediction Only (L5)
nav_order: 169
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Droperidol: predicted new indication Tourette syndrome (evidence level L5). Approved use: Not available in the licence data"
monitor_inn: "DROPERIDOL"
predicted_indications: ["Tourette syndrome", "trichotillomania", "manic bipolar affective disorder", "attention deficit hyperactivity disorder, inattentive type", "nephrogenic syndrome of inappropriate antidiuresis", "attention deficit-hyperactivity disorder", "headache disorder", "common cold", "hypertrichosis (disease)", "specific developmental disorder"]
---

# Droperidol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Droperidol: From an Unspecified Original Indication to Tourette Syndrome

## One-Sentence Summary

Droperidol is a butyrophenone neuroleptic (dopamine D2 antagonist) with an injectable formulation. The original approved indication is not recorded in the New Zealand licence data compiled for this report.
The TxGNN model's top prediction is **Tourette syndrome**, which has **no clinical trials and no publications** behind it, so it rests on model prediction alone.
The best-supported direction among the top 10 predictions is **headache disorder (migraine)**, with **1 registered clinical trial** and **about 20 publications**.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in the licence data |
| Predicted New Indication | Tourette syndrome |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L5 (for Tourette syndrome) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

For context, the other top-10 predictions with the strongest evidence are:

| Predicted Indication | TxGNN Score | Evidence Level | Recommendation |
|------|------|------|------|
| Headache disorder (migraine) | 99.47% | L3 | Research Question |
| Manic bipolar affective disorder | 99.82% | L3 | Research Question |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, droperidol is a butyrophenone neuroleptic with potent dopamine D2 receptor antagonism, the same pharmacological class as haloperidol and pimozide.

Haloperidol and pimozide are established treatments for tics, so a D2 antagonist predicted for Tourette syndrome is mechanistically plausible. However, no trials or literature support this prediction for droperidol itself. Its QT-prolongation risk and injection-only availability make chronic use for a tic disorder impractical.

Some other predictions are better grounded:
- **Migraine/headache:** central dopamine antagonism and antiemetic action underlie the use of neuroleptics as rescue therapy for acute migraine in emergency settings.
- **Mania/agitation:** D2 antagonism provides rapid tranquillisation. The evidence relates mostly to acute agitation and psychosis rather than bipolar mania specifically.
- **ADHD (both types):** dopamine blockade runs opposite to standard ADHD therapy, so these predictions look like model artefacts.
- **Nephrogenic syndrome of inappropriate antidiuresis, common cold, hypertrichosis:** no credible mechanistic link is apparent.

## Clinical Trial Evidence

No clinical trials are registered for Tourette syndrome (the top prediction). For the headache disorder prediction, one trial was found:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01406860](https://clinicaltrials.gov/study/NCT01406860) | Not applicable | Terminated | 19 | Droperidol vs metoclopramide + diphenhydramine for primary headache in the emergency department (randomised, double-blind). Terminated early with only 19 participants, so it cannot support efficacy conclusions. |

## Literature Evidence

No literature was found for Tourette syndrome. The most relevant publications for the headache disorder (migraine) prediction are:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12552051](https://pubmed.ncbi.nlm.nih.gov/12552051/) | 2003 | RCT | Neurology | Randomised, double-blind, placebo-controlled trial of droperidol for acute migraine |
| [11781912](https://pubmed.ncbi.nlm.nih.gov/11781912/) | 2002 | RCT | Am J Emerg Med | Randomised trial of intramuscular droperidol for acute migraine in the emergency department |
| [25600718](https://pubmed.ncbi.nlm.nih.gov/25600718/) | 2015 | Guideline/Evidence assessment | Headache | American Headache Society evidence assessment of acute migraine pharmacotherapies |
| [24875925](https://pubmed.ncbi.nlm.nih.gov/24875925/) | 2015 | Systematic review | Cephalalgia | Canadian Headache Society recommendations on migraine pain treatment in emergency settings |
| [21435315](https://pubmed.ncbi.nlm.nih.gov/21435315/) | 2011 | Systematic review | CJEM | Systematic review of butyrophenones for primary headache in the emergency department |
| [25416184](https://pubmed.ncbi.nlm.nih.gov/25416184/) | 2015 | Review | Ann Pharmacother | Safety and efficacy of droperidol for acute migraine |
| [32839811](https://pubmed.ncbi.nlm.nih.gov/32839811/) | 2020 | Review | Am J Health Syst Pharm | Safety, indications, efficacy and dosing of droperidol in the emergency department |
| [22309235](https://pubmed.ncbi.nlm.nih.gov/22309235/) | 2012 | Review | Headache | Rescue therapy for acute migraine: neuroleptics, antihistamines and others |
| [9237411](https://pubmed.ncbi.nlm.nih.gov/9237411/) | 1997 | Pilot case series | Headache | IV droperidol in 35 patients with status migrainosus or refractory migraine |
| [10452443](https://pubmed.ncbi.nlm.nih.gov/10452443/) | 1999 | Case series | Am J Emerg Med | Retrospective review of IM droperidol for acute migraine |

For the manic bipolar affective disorder prediction, the literature includes Cochrane reviews of droperidol for acute psychosis and agitation ([27976370](https://pubmed.ncbi.nlm.nih.gov/27976370/), [15495037](https://pubmed.ncbi.nlm.nih.gov/15495037/), [11406047](https://pubmed.ncbi.nlm.nih.gov/11406047/)), a case-report series on severe mania ([9789713](https://pubmed.ncbi.nlm.nih.gov/9789713/)), and a double-blind placebo-controlled study in acutely agitated patients ([321727](https://pubmed.ncbi.nlm.nih.gov/321727/)). A report of respiratory distress after droperidol for agitation ([10460140](https://pubmed.ncbi.nlm.nih.gov/10460140/)) is a relevant safety signal.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2690543 (cancelled) | Droperidol Medsurge | Injection 2.5 mg per ml, 1 ml ampoule | Not recorded |

## Safety Considerations

- **Knowledge-graph contraindication signals** (from drug-disease contraindication links, not label text), grouped by theme:
  - Cardiovascular and QT-related: long QT syndrome (including familial), torsades de pointes, hypertension, orthostatic hypotension, postural orthostatic tachycardia syndrome, multiple system atrophy
  - Neurological and psychiatric: basal ganglia disease, focal hand dystonia, blepharospasm, anxiety and related disorders, substance abuse/dependence
  - Respiratory: COPD, respiratory failure
  - Organ and metabolic: kidney disease, liver disease, gallbladder disease, inflammatory bowel disease, potassium deficiency, benign prostatic hyperplasia
  - Tumours: phaeochromocytoma and several cancers (duodenum, small intestine, heart)
- **Drug interactions:** 100 interactions were identified. Major-level examples include hydrocortisone, amphotericin B (all formulations), morphine, naloxone, clarithromycin, dolasetron, picosulfuric acid and polyethylene glycol 3350 with electrolytes. Moderate-level examples include famotidine, epinephrine, loperamide, bisacodyl and dronabinol.
- **QT prolongation:** the literature notes QT-prolongation concerns and a boxed warning, so ECG monitoring would be needed for any use.

Label warnings and contraindications from the Medsafe package insert were not available. Please refer to the package insert for authoritative safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction, Tourette syndrome, has no trials or literature and is only mechanistically plausible by class analogy. Droperidol has no currently valid New Zealand licence, is injection-only, and carries QT-prolongation risk, which makes chronic use for a tic disorder impractical. Migraine/headache has the most supporting literature (including two randomised trials) and could be pursued as a separate research question with ECG guardrails.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- The original approved indication text for the cancelled licence
- For migraine: a full review of the randomised trials and systematic reviews, plus a QT/ECG monitoring plan
- Clarification of the regulatory pathway, since there is currently no valid New Zealand licence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


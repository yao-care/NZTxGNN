---
layout: default
title: Quetiapine
parent: Model Prediction Only (L5)
nav_order: 395
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Quetiapine: predicted new indication Retinal dystrophy with or without extraocular anomalies . Trichotillo… (evidence level L5). Approved use: Bipolar disorder, major affective disorder…"
monitor_inn: "QUETIAPINE"
predicted_indications: ["retinal dystrophy with or without extraocular anomalies", "congenital disorder of glycosylation with defective fucosylation", "hydranencephaly (disease)", "distal 17p13.3 microdeletion syndrome", "polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis", "myopia X-linked", "Charcot-Marie-Tooth disease, demyelinating, type 1G", "trichotillomania", "syndromic myopia", "myopia 26, X-linked, female-limited"]
---

# Quetiapine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

## Quetiapine: From Schizophrenia and Bipolar Disorder to Trichotillomania
{: .text-alpha }

## One-Sentence Summary

Quetiapine is an atypical antipsychotic originally used for **schizophrenia and bipolar disorder**.
The TxGNN model's top-ranked prediction is retinal dystrophy, which has no supporting evidence. The only prediction among the top 10 with any supporting publications is **trichotillomania**, backed by **0 clinical trials** and **6 publications** (case reports and reviews, not controlled studies).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Bipolar disorder, major affective disorder, manic bipolar affective disorder, schizophrenia (from the knowledge graph; no approved indication text in the New Zealand licence records) |
| Predicted New Indication | Retinal dystrophy with or without extraocular anomalies (rank 1, no evidence). Trichotillomania (rank 8) is the only candidate with literature and is discussed below. |
| TxGNN Prediction Score | 99.57% (retinal dystrophy); 99.38% (trichotillomania) |
| Evidence Level | L5 for retinal dystrophy; L4 for trichotillomania |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 6 (all cancelled) |
| Recommended Decision | Hold (retinal dystrophy and the other no-evidence predictions); trichotillomania is a research question |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Quetiapine is widely known to be an atypical antipsychotic that blocks dopamine and serotonin receptors, but this is not confirmed by the data compiled for this report.

**Top-ranked prediction (retinal dystrophy):** No mechanistic link could be identified. Retinal dystrophies are typically monogenic degenerative conditions, and quetiapine's receptor antagonism has no established relevance to them. The high score is a computational output with no trials or literature behind it. The other top-10 predictions without evidence (congenital disorder of glycosylation, hydranencephaly, 17p13.3 microdeletion syndrome, polymicrogyria syndrome, X-linked and syndromic myopia, Charcot-Marie-Tooth type 1G) likewise show no plausible link.

**Trichotillomania (rank 8):** The link is plausible but indirect. Trichotillomania belongs to the obsessive-compulsive spectrum, and antipsychotics acting on serotonin 5-HT2A and dopamine D2 receptors have been used as add-on treatment in related conditions. However, the evidence is limited to case reports and reviews. One publication (PMID 11212595) concerns quetiapine **exacerbating** obsessive-compulsive symptoms, so the direction of effect is uncertain. These assessments are based on titles and partial abstracts.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

For trichotillomania (rank 8):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12405081](https://pubmed.ncbi.nlm.nih.gov/12405081/) | 2002 | Review + case report | Psychiatry | Overview of trichotillomania; a 33-year-old woman responded favourably to quetiapine |
| [19142421](https://pubmed.ncbi.nlm.nih.gov/19142421/) | 2008 | Case report/series | Rev Bras Psiquiatr | Quetiapine for trichotillomania (no abstract available) |
| [11212595](https://pubmed.ncbi.nlm.nih.gov/11212595/) | 2001 | Case report/Review | J Psychiatry Neurosci | Quetiapine worsened obsessive-compulsive symptoms in a patient with OCD and trichotillomania; review of antipsychotic-induced OCS |
| [38797877](https://pubmed.ncbi.nlm.nih.gov/38797877/) | 2025 | Review | Int J Dermatol | Review of pharmacological treatment of trichotillomania; no guidelines exist |
| [20833945](https://pubmed.ncbi.nlm.nih.gov/20833945/) | 2010 | Case report/Review | Psychosomatics | Treatment of recurrent Rapunzel syndrome and trichotillomania |
| [27840761](https://pubmed.ncbi.nlm.nih.gov/27840761/) | 2016 | Case report | Case Rep Psychiatry | Trichotillomania as a manifestation of dementia |

No literature was available for the other nine top-ranked predictions.

---

## New Zealand Market Information

All six licences are cancelled, and no approved indication text is recorded for them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2252880 | Quetapel | Tab 100 mg | Cancelled |
| 2252902 | Quetapel | Tab 200 mg | Cancelled |
| 2476274 | Quetapel | Tab 300 mg | Cancelled |
| 2476266 | Quetapel | Tab 25 mg | Cancelled |
| 2701162 | Quetiapine Viatris | Tab 25 mg | Cancelled |

---

## Safety Considerations

Package insert warnings and contraindications were not available. Please refer to the package insert for safety information.

- **Contraindication or caution signals from the knowledge graph (not label text):** cardiovascular (hypertension, orthostatic hypotension, long QT syndrome, torsades de pointes, myocardial infarction), neurological (epilepsy, basal ganglia disease), metabolic (diabetes mellitus, diabetic ketoacidosis, hyperglycaemia, hyperlipidaemia, hypertriglyceridaemia, obesity, hypothyroidism, potassium deficiency), haematological (neutropenia, agranulocytosis), and liver or gallbladder disease.
- **Drug interactions:** 100 interactions were found; the first 20 retrieved are listed here, and they are not ranked by severity, so the complete list should be reviewed. Major: **bupropion** and **morphine**. Moderate interactions include antidiabetic agents (metformin, pioglitazone, acarbose, alogliptin, albiglutide, canagliflozin, chlorpropamide), anticholinergics (atropine, hyoscyamine, glycopyrronium, clidinium), and others such as epinephrine, famotidine, dexamethasone, aprepitant, loperamide, lorcaserin and bisacodyl.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction, retinal dystrophy, and the other no-evidence predictions have high model scores but no trials, no literature, and no plausible mechanism. Trichotillomania has some case-level literature, but it is weak, mixed in direction, and contains no controlled data. It is best treated as a research question rather than a candidate for action. In New Zealand, all quetiapine licences are cancelled, so any use would require re-establishing a supply pathway.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (blocking gap for safety screening)
- Mechanism of action data
- A controlled study or systematic review of quetiapine in trichotillomania, with attention to reports of worsened obsessive-compulsive symptoms
- Confirmation of current New Zealand availability and regulatory status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


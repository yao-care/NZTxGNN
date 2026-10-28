---
layout: default
title: Oxybutynin
parent: Model Prediction Only (L5)
nav_order: 343
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-15
monitor_inn: "OXYBUTYNIN"
predicted_indications: ["restless legs syndrome", "gastroduodenitis", "peptic ulcer disease"]
description: "Oxybutynin: predicted new indication Restless legs syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Oxybutynin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

## Oxybutynin: Restless legs syndrome
{: .text-alpha }

## One-Sentence Summary

Oxybutynin is an antimuscarinic, antispasmodic drug originally used to treat overactive bladder and neurogenic bladder.
The TxGNN model predicts it may be effective for **restless legs syndrome**, but this is a computational prediction only, with **0 clinical trials** and **0 publications** currently supporting it.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Restless legs syndrome |
| TxGNN Prediction Score | 0.9974 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, oxybutynin is a muscarinic antagonist with musculotropic antispasmodic activity. Its efficacy in bladder overactivity is established, but the link to restless legs syndrome (RLS) is unclear.

RLS is mainly associated with dopaminergic dysfunction and brain iron deficiency, which are not obviously related to muscarinic blockade. Anticholinergic drugs may also worsen sleep quality and cognition. The very high TxGNN score therefore cannot be mechanistically verified from the data compiled for this report.

Two other predictions were also reviewed:
- **Gastroduodenitis** (score 0.9962): no trials or literature. Reduced gastric motility and secretion offers a theoretical rationale, but anticholinergics can worsen reflux.
- **Peptic ulcer disease** (score 0.9931): evidence level L4, with only weak, dated literature (see below).

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature is available for restless legs syndrome. The only literature is for the third-ranked prediction, peptic ulcer disease:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [14173506](https://pubmed.ncbi.nlm.nih.gov/14173506/) | 1964 | Clinical evaluation (older, likely uncontrolled) | Current Therapeutic Research | Clinical evaluation of oxybutynin chloride in gastrointestinal disease; no abstract available |
| [4380481](https://pubmed.ncbi.nlm.nih.gov/4380481/) | 1965 | Pharmacology/Review | Archives Internationales de Pharmacodynamie et de Therapie | Describes oxybutynin as a musculotropic antispasmodic with moderate anticholinergic action |
| [2360335](https://pubmed.ncbi.nlm.nih.gov/2360335/) | 1990 | Case report (adverse event) | DICP: The Annals of Pharmacotherapy | Case of oxybutynin-induced reflux esophagitis, likely from reduced lower esophageal sphincter tone; a safety signal against upper GI use |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2627167 (cancelled) | Alchemy Oxybutynin | Tab 5 mg | Not recorded |

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (not label text), main groups:
  - Obstructive and gastrointestinal: paralytic ileus, intestinal obstruction, toxic megacolon, bladder neck obstruction, benign prostatic hyperplasia, ulcerative colitis and inflammatory bowel disease, gastroesophageal reflux disease, esophagitis, hiatus hernia
  - Eye and neuromuscular: angle-closure glaucoma, myasthenia gravis
  - Cardiovascular and endocrine: hypertension, coronary artery disease, heart conduction disease, hyperthyroidism
  - Organ function: kidney disease, liver disease
- **Drug Interactions** (100 recorded; examples shown):
  - Major: potassium chloride, potassium citrate
  - Moderate: other anticholinergics (atropine, hyoscyamine, glycopyrronium, dicyclomine, trospium, propantheline), opioids (morphine, opium, loperamide), metoclopramide, aprepitant, eluxadoline, pramlintide
- **Literature signal**: oxybutynin-induced reflux esophagitis (1990 case report), relevant to the gastroduodenitis and peptic ulcer predictions.

Please refer to the package insert for official warnings, as label-based safety data were not available.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predictions rest on model scores alone (L5 for restless legs syndrome and gastroduodenitis, L4 for peptic ulcer disease), with no trials and no supporting modern literature. The mechanistic link is weak or unverifiable, there is a documented reflux safety signal for upper GI use, and the only New Zealand licence is cancelled.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- A literature search for anticholinergic or antimuscarinic effects in restless legs syndrome, including sleep-related adverse effects
- Confirmation of current regulatory and supply status in New Zealand, since the only licence is cancelled
- Route and formulation compatibility assessment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


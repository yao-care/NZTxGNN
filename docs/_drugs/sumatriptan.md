---
layout: default
title: Sumatriptan
parent: Moderate Evidence (L3-L4)
nav_order: 433
evidence_level: L4
indication_count: 1
---

# Sumatriptan
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **1** 
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

# Sumatriptan: From Migraine to Migraine with Brainstem Aura

## One-Sentence Summary

Sumatriptan is a triptan originally used to treat migraine and cluster headache.
The TxGNN model predicts it may be effective for **migraine with brainstem aura**, a subtype of migraine with aura,
but there are currently **no registered clinical trials** and only **indirect literature (18 publications, mostly reviews on migraine in general)** supporting this specific subtype.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Migraine disorder (including migraine with or without aura, cluster headache, paroxysmal hemicrania, hemicrania continua; from the knowledge graph, as no approved indication text is available in the New Zealand licence records) |
| Predicted New Indication | Migraine with brainstem aura |
| TxGNN Prediction Score | 99.74% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on established pharmacology, sumatriptan is a 5-HT1B/1D receptor agonist. It is thought to relieve migraine through cranial vasoconstriction and inhibition of the release of vasoactive neuropeptides (such as CGRP) from trigeminal nerve terminals.

Migraine with brainstem aura is a subtype of migraine with aura and shares the trigeminovascular pathophysiology of the parent disorder. Sumatriptan's efficacy in migraine is well established, so a mechanistic link is plausible.

There are two important caveats. First, the high TxGNN score most likely reflects network proximity to the approved migraine indications, and it is not independent clinical evidence. Second, triptans have historically carried cautions or contraindications in aura subtypes with brainstem symptoms (for example basilar-type or hemiplegic migraine), because of theoretical ischemic risk from vasoconstriction. One publication also reports reduced sumatriptan efficacy in migraine with aura compared with migraine without aura (PMID 25841032). This prediction therefore needs careful clinical scrutiny.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Listed below are the most relevant publications. None studies the brainstem aura subtype directly.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25600718](https://pubmed.ncbi.nlm.nih.gov/25600718/) | 2015 | Review (guideline-style) | Headache | American Headache Society evidence assessment of pharmacotherapies for acute migraine in adults |
| [23657930](https://pubmed.ncbi.nlm.nih.gov/23657930/) | 2014 | RCT | Phytotherapy Research | Ginger vs sumatriptan in 100 patients with acute migraine without aura (not the brainstem aura subtype) |
| [1313746](https://pubmed.ncbi.nlm.nih.gov/1313746/) | 1992 | Randomized trial | Cephalalgia | Double-blind, placebo-controlled trial of oral sumatriptan 200 mg in acute migraine with aura |
| [25841032](https://pubmed.ncbi.nlm.nih.gov/25841032/) | 2015 | Clinical study | Neurology | Reduced efficacy of sumatriptan in migraine with aura vs without aura |
| [8559405](https://pubmed.ncbi.nlm.nih.gov/8559405/) | 1996 | Commentary | Neurology | Subcutaneous sumatriptan and the migraine aura |
| [12917936](https://pubmed.ncbi.nlm.nih.gov/12917936/) | 2003 | Systematic review | Cochrane Database Syst Rev | Oral sumatriptan for acute migraine (a later version of this review was withdrawn, PMID 22336784) |
| [8536293](https://pubmed.ncbi.nlm.nih.gov/8536293/) | 1995 | Review | Cephalalgia | Critical review of clinical experience with sumatriptan in migraine and cluster headache |
| [26628293](https://pubmed.ncbi.nlm.nih.gov/26628293/) | 2016 | Review | Drugs | Sumatriptan/naproxen sodium: phase 3 trials show greater 2-hour pain freedom than placebo in acute migraine with or without aura |
| [27681528](https://pubmed.ncbi.nlm.nih.gov/27681528/) | 2016 | Review | Drugs | Sumatriptan nasal powder: greater pain relief than placebo in acute migraine |
| [38307660](https://pubmed.ncbi.nlm.nih.gov/38307660/) | 2024 | Review | Handbook of Clinical Neurology | Status migrainosus as a complication of migraine with or without aura |

## New Zealand Market Information

All three licences are cancelled, and no approved indication text is recorded for them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2605856 | Sumagran | Tab 100 mg | Not recorded (cancelled) |
| 2605848 | Sumagran | Tab 50 mg | Not recorded (cancelled) |
| 2647958 | Clustran | Inj 12 mg per ml, 0.5 ml prefilled pen | Not recorded (cancelled) |

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: hypertension, coronary artery disease, myocardial infarction, heart disease and cardiovascular disease, peripheral vascular disease, stroke and cerebrovascular disorders, cerebral infarction, liver disease and liver failure, kidney disease, epilepsy, diabetes mellitus and diabetic ketoacidosis, congestive heart failure, blood coagulation disease, hemoglobinopathy and anaemia, obesity, gallbladder disease, and substance abuse/dependence. The cardiovascular and cerebrovascular entries are particularly relevant to the vasoconstriction concern in brainstem aura.
- **Drug Interactions** (100 interactions recorded; main ones shown):
  - **Major**: serotonergic agents (dexfenfluramine, fenfluramine, lorcaserin, sibutramine) and 5-HT3 antagonists (ondansetron, granisetron, dolasetron, palonosetron).
  - **Moderate**: morphine, including liposomal morphine.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is mechanistically plausible but is supported only by model proximity to approved migraine indications and by general migraine literature. There are no clinical trials for the brainstem aura subtype. The historical caution about triptans in brainstem aura subtypes and the lack of any currently valid New Zealand licence further weigh against proceeding at this stage.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (needed for safety screening) and confirmation of whether brainstem aura or related subtypes are excluded
- Mechanism of action data from DrugBank
- Clinical evidence specific to brainstem aura (case series, observational data or trials), including cardiovascular and cerebrovascular safety
- Clarification of the regulatory pathway, as all three New Zealand licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Lacosamide
parent: 僅模型預測 (L5)
nav_order: 190
evidence_level: L5
indication_count: 10
---

# Lacosamide
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Lacosamide: From Epilepsy (Focal/Partial-Onset Seizures) to Manic Bipolar Affective Disorder

## One-Sentence Summary

Lacosamide is an antiseizure medication established for focal (partial-onset) seizures/epilepsy, referenced throughout the supporting trial and literature records in this evidence pack. The TxGNN model predicts it may be effective for **Manic Bipolar Affective Disorder**, with **1 clinical trial** and **14 publications** currently identified, though most of the clinical evidence to date addresses the *depressive* rather than the *manic* phase of bipolar disorder.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not established in the New Zealand label (drug not marketed); international trial/literature context indicates use in focal/partial-onset seizure epilepsy |
| Predicted New Indication | Manic Bipolar Affective Disorder |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Research Question |

---

## Why is This Prediction Reasonable?

Structured mechanism-of-action data for lacosamide was not returned from DrugBank in this evidence pack. Based on the supporting literature that was retrieved, lacosamide's known pharmacological action is selective enhancement of the **slow inactivation of voltage-gated sodium (Nav) channels**, together with an interaction with **CRMP2 (collapsin response mediator protein 2)** that affects trafficking of voltage- and ligand-gated ion channels (PMID 32693579). This is the same general mechanistic family as other membrane-stabilising antiepileptic drugs (e.g., lamotrigine, carbamazepine, valproate) that are already established mood stabilisers in bipolar disorder.

Epilepsy and bipolar disorder share a plausible mechanistic link through neuronal membrane hyperexcitability, and psychiatric mood comorbidity is well documented in epilepsy populations. Retrospective and open-label data (PMID 30251375, 33666402) and case reports of mood stabilisation in comorbid epilepsy/mood-disorder patients (PMID 28845834) support a signal for lacosamide affecting mood symptoms.

An important caveat: the strongest available clinical evidence (the retrospective cohort and the 12-week open-label pilot) and the currently recruiting Phase 3 trial (NCT07412132) are focused on the **depressive** episodes of bipolar disorder, not the manic phase named in this predicted indication. The mechanistic rationale for a specific antimanic effect is therefore indirect and should be treated cautiously.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT07412132](https://clinicaltrials.gov/study/NCT07412132) | Phase 3 | Recruiting | 40 | Randomized, controlled, double-blind trial evaluating lacosamide as augmentation therapy for major depressive episodes in Bipolar Disorder Types I and II; based on prior observational and open-label signals of effect on depressive/manic symptoms in epilepsy and bipolar disorder. No results yet available. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [30251375](https://pubmed.ncbi.nlm.nih.gov/30251375/) | 2018 | Retrospective Cohort | Psychiatry and Clinical Neurosciences | 30-day comparison of lacosamide vs. other antiepileptics in bipolar disorder patients without epilepsy — first dedicated assessment of lacosamide in BD |
| [33666402](https://pubmed.ncbi.nlm.nih.gov/33666402/) | 2021 | Open-Label Pilot Trial | Journal of Clinical Psychopharmacology | 12-week open-label pilot showing efficacy/safety signal for lacosamide in bipolar depression |
| [29253680](https://pubmed.ncbi.nlm.nih.gov/29253680/) | 2018 | Prospective multicenter study | Epilepsy & Behavior | Lacosamide associated with improved depression/anxiety symptoms in focal epilepsy patients |
| [28845834](https://pubmed.ncbi.nlm.nih.gov/28845834/) | 2017 | Case report | Acta Bio-Medica | Clinical stabilisation of mood disorder comorbid with PTSD and fronto-temporal epilepsy using lacosamide |
| [32693579](https://pubmed.ncbi.nlm.nih.gov/32693579/) | 2020 | Mechanistic review | ACS Chemical Neuroscience | Reviews CRMP2 as a druggable target relevant to lacosamide's channel-trafficking mechanism |
| [30275630](https://pubmed.ncbi.nlm.nih.gov/30275630/) | 2018 | Case report (AE) | Indian Journal of Psychological Medicine | Lacosamide-precipitated neutropenia in a patient with bipolar disorder and comorbid epilepsy — safety signal |
| [38304661](https://pubmed.ncbi.nlm.nih.gov/38304661/) | 2024 | Case report | Cureus | Case of bipolar I disorder with multiple comorbidities including epilepsy/PNES, relevant population overlap |
| [37782796](https://pubmed.ncbi.nlm.nih.gov/37782796/) | 2023 | Structural biology | PNAS | Cryo-EM structural study of Nav1.7 channel binding by antiepileptic drugs, supports Nav-channel MOA class |
| [26220372](https://pubmed.ncbi.nlm.nih.gov/26220372/) | 2015 | Preclinical | Epilepsy Research | Lacosamide modulates interictal spiking/high-frequency oscillations in a mesial temporal lobe epilepsy model |
| [22210279](https://pubmed.ncbi.nlm.nih.gov/22210279/) | 2012 | Review | Advanced Drug Delivery Reviews | Overview of chemical properties of AEDs including lacosamide, background pharmacology |

---

## New Zealand Market Information

Lacosamide is currently **not marketed** in New Zealand (0 authorizations on record); no license or approved-indication data is available for this evidence pack.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Research Question**

**Rationale:**
The mechanistic rationale (Nav-channel/CRMP2 modulation, shared with established mood-stabilising AEDs) is plausible, and a Phase 3 trial is now recruiting, but existing clinical evidence predominantly addresses bipolar *depression* rather than the *manic* phase named in this prediction, and it consists mainly of retrospective/open-label data (Evidence Level L3) rather than confirmatory RCTs.

**To proceed, the following is needed:**
- Formal TFDA/DrugBank mechanism-of-action data (currently flagged as a Blocking/High-severity data gap)
- Package insert warnings, contraindications, and drug-interaction data (all currently unavailable)
- Results from the ongoing Phase 3 trial (NCT07412132) once available
- Clarification of whether the mood-stabilising signal extends specifically to manic (not just depressive) episodes before advancing beyond a research question
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


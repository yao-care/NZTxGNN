---
layout: default
title: Risperidone
parent: Model Prediction Only (L5)
nav_order: 407
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-30
monitor_inn: "RISPERIDONE"
predicted_indications: ["gaze palsy, familial horizontal, with progressive scoliosis", "amelocerebrohypohidrotic syndrome", "Phelan-McDermid syndrome", "trichotillomania", "major affective disorder"]
description: "Risperidone: predicted new indication Gaze palsy, familial horizontal, with progressive scoliosis (evidence level L5). Approved use: Not recorded in the local data"
---

# Risperidone
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

## Risperidone: Gaze palsy, familial horizontal, with progressive scoliosis
{: .text-alpha }

## One-Sentence Summary

Risperidone is an atypical antipsychotic originally used for schizophrenia, manic episodes in bipolar disorder and autism-related irritability.
The TxGNN model's top-ranked prediction is **familial horizontal gaze palsy with progressive scoliosis**, an ultra-rare genetic disorder.
This prediction has **0 clinical trials** and **0 publications** behind it, so it is a computational signal only.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Gaze palsy, familial horizontal, with progressive scoliosis |
| TxGNN Prediction Score | 0.9976 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (11 of 11 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 11 (valid: 11) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Risperidone is generally understood to act as a dopamine D2 and serotonin 5-HT2A receptor antagonist. Its efficacy in schizophrenia and bipolar mania is well established.

**For the top-ranked prediction, no plausible mechanistic link was found.** Familial horizontal gaze palsy with progressive scoliosis is an ultra-rare disorder linked to *ROBO3*, with neurodevelopmental and structural causes. Nothing in dopamine/serotonin receptor antagonism explains a benefit in this condition. The very high score (rank 2,631 among all predictions) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological or clinical evidence. It should not be read as a treatment signal.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## Other Predicted Indications Worth Noting

The top-ranked prediction has no support, but other predictions for this drug have more evidence:

| Rank | Predicted Indication | Score | Evidence Level | Evidence Found | Recommendation |
|------|------|------|------|------|------|
| 3 | Amelocerebrohypohidrotic syndrome | 0.9969 | L5 | None. Antipsychotics can lower the seizure threshold, which is a concern in a population with epilepsy. | Hold |
| 4 | Phelan-McDermid syndrome | 0.9959 | L4 | 2 publications: a clinical management review and a zebrafish *shank3* study. No human efficacy data for risperidone. | Research Question |
| 5 | Trichotillomania | 0.9951 | L4 | 9 publications, mostly case reports and small case series of risperidone added to SSRIs. No RCT or registered trial. | Research Question |
| 6 | Major affective disorder | 0.9911 | L1 | 11 trials and 19 publications. Two completed Phase 3 RCTs of risperidone augmentation in treatment-resistant depression: [NCT00095134](https://clinicaltrials.gov/study/NCT00095134) (n=630) and [NCT00044681](https://clinicaltrials.gov/study/NCT00044681) (n=258). Supported by meta-analyses ([PMID 34986373](https://pubmed.ncbi.nlm.nih.gov/34986373/), [PMID 35861202](https://pubmed.ncbi.nlm.nih.gov/35861202/), [PMID 34238049](https://pubmed.ncbi.nlm.nih.gov/34238049/)). | Proceed with Guardrails |

The disease node for "major affective disorder" mixes unipolar depression and bipolar disorder. The bipolar trials largely reflect existing on-label use, so the repurposing-relevant signal is the depression augmentation data. It is also unclear whether every Phase 3 trial met its primary endpoint.

---

## New Zealand Market Information

All 11 licences are valid. Five representative entries:

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2347652 | Risperdal Consta | Inj 25 mg vial | Not stated (valid) |
| 2347660 | Risperdal Consta | Inj 50 mg vial | Not stated (valid) |
| 245402 | Risperdal | Tab 4 mg | Not stated (valid) |
| 2466910 | Risperidone (Teva) | Tab 2 mg | Not stated (valid) |
| 2662396 | Risperon | Oral liq 1 mg per ml | Not stated (valid) |

---

## Safety Considerations

- **Conditions flagged in the drug-disease knowledge graph** (from knowledge-graph contraindication links, not package insert text), grouped as follows:
  - Cardiovascular and cerebrovascular: stroke, cerebrovascular disorder, cerebral infarction, myocardial infarction, sudden cardiac arrest, heart conduction disease, cardiovascular disease, heart disease
  - Blood pressure: orthostatic hypotension, hypotensive disorder, multiple system atrophy, postural orthostatic tachycardia syndrome
  - Metabolic: diabetes mellitus, hyperglycemia, diabetic ketoacidosis, hyperlipidemia, familial hyperlipidemia, obesity
  - Haematological: neutropenia, agranulocytosis, hemoglobinopathy
  - Hepatic and biliary: liver failure, liver disease, gallbladder disease
  - Other: epilepsy, female breast carcinoma
- **Drug Interactions**: 100 interactions were identified. The main ones from the list reviewed are:
  - **Major**: bupropion, morphine
  - **Moderate**: metformin, acarbose, alogliptin, albiglutide, canagliflozin (glucose-lowering agents); atropine, hyoscyamine, glycopyrronium, clidinium (anticholinergics); epinephrine, hydrocortisone, amphotericin B, famotidine, loperamide, lorcaserin, bisacodyl
  - **Minor**: ranitidine
- For wording of warnings and contraindications, please refer to the package insert.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction rests only on a TxGNN model (deep learning) score. There are no trials or publications and no plausible mechanism for an ultra-rare genetic gaze disorder.

**To proceed, the following is needed:**
- The Medsafe package insert, to complete safety screening (a blocking gap)
- Detailed mechanism of action data
- If the aim is productive repurposing, redirect review to the better-supported candidate, major affective disorder (treatment-resistant depression augmentation). That review should include confirmation of Phase 3 primary endpoint outcomes, a metabolic, prolactin, extrapyramidal and somnolence monitoring plan, and verification of the cancelled market status before any regulatory pathway is considered.
- Trichotillomania and Phelan-McDermid syndrome could be handled as research questions only, pending controlled studies.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


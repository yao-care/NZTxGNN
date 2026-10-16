---
layout: default
title: Levodopa
parent: Model Prediction Only (L5)
nav_order: 270
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Levodopa: predicted new indication Rasmussen subacute encephalitis (evidence level L5). Approved use: Parkinson disease and related parkinsonian syndromes"
monitor_inn: "levodopa"
predicted_indications: ["Rasmussen subacute encephalitis"]
---

# Levodopa
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

# Levodopa: From Parkinson Disease to Rasmussen Subacute Encephalitis

## One-Sentence Summary

Levodopa is a dopamine precursor, originally used to treat Parkinson disease and related parkinsonian syndromes.
The TxGNN model predicts it may be effective for **Rasmussen subacute encephalitis**,
but currently there are **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the computational score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Parkinson disease and related parkinsonian syndromes (from the knowledge graph; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Rasmussen subacute encephalitis |
| TxGNN Prediction Score | 99.06% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only (no currently valid licences) |
| Number of Authorizations | 12 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Based on known information, levodopa is a dopamine precursor used in combination with a decarboxylase inhibitor (benserazide or carbidopa) and, in some products, entacapone. Its efficacy in Parkinson disease and other parkinsonian syndromes is established. Whether it applies mechanistically to Rasmussen encephalitis is unproven.

Rasmussen encephalitis is a rare, chronic, unilateral inflammatory brain disease. It causes refractory focal seizures, progressive hemiparesis and cognitive decline, and is thought to be driven by T-cell-mediated, autoimmune-type neuroinflammation. Levodopa treats dopamine deficiency and has no known link to this immune-mediated pathology or to seizure control.

A possible connection is basal ganglia or dopaminergic involvement, since movement disorders can occur in Rasmussen encephalitis, but this is speculative. The high score may reflect network proximity to parkinsonism-related disease nodes in the training graph rather than a true therapeutic relationship. A high TxGNN score is a computational prediction and not evidence of efficacy.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

All 12 licences are cancelled and none is currently valid. The approved indication text is not available for any of them. The five main authorizations are listed below.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 635960 | Madopar Rapid | Dispersible tablet, levodopa 50 mg with benserazide 12.5 mg | Not available |
| 2429985 | Sinemet | Tablet, levodopa 100 mg with carbidopa 25 mg | Not available |
| 2429993 | Sinemet | Tablet, levodopa 250 mg with carbidopa 25 mg | Not available |
| 2430002 | Sinemet CR | Long-acting tablet, levodopa 200 mg with carbidopa 50 mg | Not available |
| 201413 | Madopar 62.5 | Capsule, levodopa 50 mg with benserazide 12.5 mg | Not available |

## Safety Considerations

- **Contraindications (knowledge-graph-derived, not label text):** The knowledge graph links levodopa with contraindications in the following groups. These should be checked against the official package insert.
  - **Neurological:** epilepsy, multiple system atrophy, drug-induced dyskinesia
  - **Cardiovascular:** orthostatic hypotension, postural orthostatic tachycardia syndrome, myocardial infarction, coronary thrombosis, sudden cardiac arrest, heart conduction disease
  - **Psychiatric:** psychotic disorder, schizophreniform disorder, schizoaffective disorder, mental disorder, anxiety disorder, neurotic disorder, dysthymic disorder
  - **Eye:** open-angle glaucoma, angle-closure glaucoma
  - **Gastrointestinal, hepatic and renal:** gastric ulcer, peptic ulcer disease, diarrheal disease, liver disease, gallbladder disease, kidney disease, pyoureter
  - **Metabolic, haematological and oncological:** diabetes mellitus, diabetic ketoacidosis, hemoglobinopathy, melanoma
- **Relevance to the predicted indication:** The epilepsy contraindication is especially relevant because Rasmussen encephalitis is dominated by refractory seizures. Dopaminergic agents may lower the seizure threshold in some settings.
- **Drug Interactions:** 104 interactions were identified in total. Moderate-level examples include:
  - **Anticholinergics:** atropine, hyoscyamine, methscopolamine, glycopyrronium, clidinium, dicyclomine, mepenzolate
  - **Sympathomimetics:** epinephrine, ephedrine
  - **Other:** bupropion, metronidazole, pyridoxine, iron, metoclopramide

  Minor-level examples include antacids and mineral salts: aluminium hydroxide, calcium carbonate, magaldrate, and magnesium carbonate, hydroxide and oxide.

Package insert warnings and contraindications could not be retrieved for this report. Please refer to the package insert for complete safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no supporting clinical trials or literature (evidence level L5), and no plausible mechanism links levodopa to the immune-mediated pathology of Rasmussen encephalitis. The drug is also not currently marketed in New Zealand, and the epilepsy contraindication is a concern in a seizure-dominant disease.

**To proceed, the following is needed:**
- Mechanism of action data for levodopa (e.g. from DrugBank)
- Medsafe package insert warnings and contraindications, which are required before safety screening
- A systematic search for clinical or preclinical evidence of levodopa in Rasmussen encephalitis
- A mechanistic rationale connecting dopaminergic therapy to neuroinflammation or seizure control in this disease
- Assessment of seizure risk in patients with refractory epilepsy
- Clarification of route and formulation requirements, and of whether any valid New Zealand licence exists, given that all 12 are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


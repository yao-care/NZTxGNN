---
layout: default
title: Rasagiline
parent: Model Prediction Only (L5)
nav_order: 399
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-28
description: "Rasagiline: predicted new indication PLA2G6-associated neurodegeneration (evidence level L5). Approved use: Parkinson disease (and related parkinsonian syndromes)"
monitor_inn: "RASAGILINE"
predicted_indications: ["PLA2G6-associated neurodegeneration", "Rasmussen subacute encephalitis", "myelitis", "paralysis agitans, juvenile, of Hunt", "transaldolase deficiency", "polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis"]
---

# Rasagiline
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Rasagiline: From Parkinson Disease to PLA2G6-Associated Neurodegeneration
{: .text-alpha }

## One-Sentence Summary

Rasagiline is a MAO-B inhibitor originally used to treat Parkinson disease and related parkinsonian syndromes.
The TxGNN model predicts it may be effective for **PLA2G6-associated neurodegeneration**,
but there are currently **0 clinical trials** and **0 publications** supporting this direction, so the prediction is based on the computational model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Parkinson disease (and related parkinsonian syndromes) |
| Predicted New Indication | PLA2G6-associated neurodegeneration |
| TxGNN Prediction Score | 0.9971 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available in the data compiled for this report. Rasagiline is generally known as a MAO-B inhibitor, which preserves striatal dopamine, and its efficacy in Parkinson disease is established. It has also been proposed to have neuroprotective effects (anti-apoptotic, mitochondrial stabilization).

PLA2G6-associated neurodegeneration includes infantile neuroaxonal dystrophy and PLA2G6-related dystonia-parkinsonism. The parkinsonian phenotype overlaps with rasagiline's approved use, so symptomatic dopaminergic benefit is theoretically possible. However, there is no evidence that rasagiline acts on the underlying PLA2G6 lipid-metabolism and mitochondrial defect. The very high score is a computational prediction only and should be interpreted cautiously.

**Other predicted indications (all L4–L5, all Hold, with no trials or literature):**

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Assessment |
|------|------|------|------|------|
| 2 | Rasmussen subacute encephalitis | 0.9956 | L5 | No clear mechanistic link; likely reflects network proximity |
| 3 | Myelitis | 0.9932 | L5 | Speculative; standard treatment is immunomodulatory |
| 4 | Paralysis agitans, juvenile, of Hunt | 0.9925 | L5 | Historical term for juvenile parkinsonism; likely a re-labeling of an existing indication rather than true repurposing; safety and efficacy in young patients not established |
| 5 | Transaldolase deficiency | 0.9919 | L5 | No recognized relationship to MAO-B inhibition; the score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence |
| 6 | Polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis | 0.9901 | L5 | A symptomatic MAO-B inhibitor is not expected to alter structural malformations |

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2611449 (cancelled) | Azilect | Tab 1 mg | Not recorded |

## Safety Considerations

- **Disease associations flagged as contraindications in the knowledge graph** (not taken from label text): hypertension and hypertensive disorder, multiple system atrophy, postural orthostatic tachycardia syndrome, orthostatic hypotension, drug-induced dyskinesia, epilepsy, bipolar disorder and major affective disorder, liver disease, gallbladder disease, melanoma, pheochromocytoma and sympathetic paraganglioma, and several cancers (duodenum, small intestine, heart, adrenal gland).
- **Drug Interactions**: 100 interactions were identified; the 20 retrieved include:
  - **Major**: bupropion, morphine.
  - **Moderate**: epinephrine, cimetidine, chlorpropamide, glimepiride, diethylpropion, phentermine, difenoxin, diphenoxylate, dronabinol, obeticholic acid, and multiple insulin products (aspart, degludec, detemir, glargine, glulisine, human isophane, human regular, inhaled human).

Please refer to the package insert for label warnings and contraindications, which were not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests entirely on a computational score, with no clinical trials or publications. The mechanistic link to PLA2G6-associated neurodegeneration is only symptomatic and theoretical, and the New Zealand authorization has been cancelled.

**To proceed, the following is needed:**
- Mechanism of action data for rasagiline
- Package insert warnings and contraindications from Medsafe
- Preclinical or clinical evidence in PLA2G6-associated neurodegeneration (e.g., case series or disease-model studies)
- A clear regulatory pathway given the cancelled New Zealand authorization
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Entacapone
parent: Model Prediction Only (L5)
nav_order: 178
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-30
monitor_inn: "ENTACAPONE"
predicted_indications: ["PLA2G6-associated neurodegeneration", "Rasmussen subacute encephalitis", "myelitis", "paralysis agitans, juvenile, of Hunt", "transaldolase deficiency", "lethal infantile mitochondrial myopathy", "Lewy body dementia", "fructose-1,6-bisphosphatase deficiency", "polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis", "progressive supranuclear palsy-corticobasal syndrome"]
description: "Entacapone: predicted new indication PLA2G6-associated neurodegeneration (evidence level L5). Approved use: Not recorded in the local data"
---

# Entacapone
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

## Entacapone: PLA2G6-associated neurodegeneration
{: .text-alpha }

## One-Sentence Summary

Entacapone is a COMT inhibitor originally used for Parkinson disease and related parkinsonian syndromes.
The TxGNN model predicts it may be effective for **PLA2G6-associated neurodegeneration**,
but **no clinical trials and no publications** currently support this specific prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | PLA2G6-associated neurodegeneration |
| TxGNN Prediction Score | 0.9976 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, entacapone is a catechol-O-methyltransferase (COMT) inhibitor. It prolongs the action of levodopa, and its efficacy in Parkinson disease is established.

PLA2G6-associated neurodegeneration includes infantile neuroaxonal dystrophy and PLA2G6-related dystonia-parkinsonism. A rationale therefore exists for the parkinsonian features only, where extending levodopa exposure could help symptomatically. Entacapone would not address the underlying defect in lipid metabolism and membrane remodelling. This is essentially a symptomatic, not disease-modifying, hypothesis, and it is untested.

Other top-ranked predictions vary widely in plausibility:
- **Juvenile parkinsonism (paralysis agitans, juvenile, of Hunt, score 0.9960)** is closely related to the original indications, so the dopaminergic rationale is plausible, but the prediction is largely label-adjacent.
- **Lewy body dementia (0.9925)** shares alpha-synuclein pathology and dopaminergic deficits with Parkinson disease, but the supporting evidence is indirect and preclinical (L4).
- **Progressive supranuclear palsy-corticobasal syndrome (0.9904)** responds poorly to dopaminergic therapy, so the rationale is weak.
- **Rasmussen subacute encephalitis (0.9973), myelitis (0.9963), transaldolase deficiency (0.9943), lethal infantile mitochondrial myopathy (0.9928), fructose-1,6-bisphosphatase deficiency (0.9922) and polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis (0.9906)** show no plausible mechanistic link to COMT inhibition. These scores come from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered for PLA2G6-associated neurodegeneration.

For other top-ranked predictions, the only linked trials are not therapeutic studies of entacapone:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04246437](https://clinicaltrials.gov/study/NCT04246437) | Phase 1 | Recruiting | 40 | [18F]F-DOPA PET imaging in autonomic failure (alpha-synucleinopathies). A diagnostic imaging study, linked to Lewy body dementia only through shared disease context |
| [NCT02994719](https://clinicaltrials.gov/study/NCT02994719) | N/A | Active, not recruiting | 120 | Observational gait pattern analysis in neurological disease, including parkinsonian disorders. Does not test entacapone (linked to PSP-corticobasal syndrome) |

## Literature Evidence

Currently no related literature available for PLA2G6-associated neurodegeneration.

For Lewy body dementia, the only related publications are preclinical:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39259788](https://pubmed.ncbi.nlm.nih.gov/39259788/) | 2024 | Preclinical (iPSC organoid) | Science Advances | Cortical organoids with SNCA triplication model alpha-synuclein pathology in Lewy body disease and were used to identify candidate therapeutic drugs |
| [23913715](https://pubmed.ncbi.nlm.nih.gov/23913715/) | 2013 | In vitro | J Neurosci Res | Examined the effects of antiparkinsonian agents on beta-amyloid and alpha-synuclein oligomer formation in vitro |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2685299 | Entacapone Viatris (valid) | Tab 200 mg | Not available |

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for safety information.

- **Contraindications suggested by the knowledge graph** (not label text, so they need verification against the approved product information): multiple system atrophy, orthostatic hypotension, postural orthostatic tachycardia syndrome, drug-induced dyskinesia, epilepsy, liver disease, kidney disease, gallbladder disease, peptic or gastric ulcer, open-angle and angle-closure glaucoma, psychotic and other mental disorders (including schizoaffective and schizophreniform disorders, anxiety, neurotic and dysthymic disorders), diabetes mellitus and diabetic ketoacidosis, melanoma, myocardial infarction, coronary thrombosis, sudden cardiac arrest, heart conduction disease, hemoglobinopathy and diarrheal disease.
- **Drug interactions**: 82 interactions were identified. Those graded Moderate include epinephrine (systemic, ophthalmic and topical), morphine (including liposomal), dronabinol, opium, nabilone, metoclopramide and iron. Interactions with unknown severity include pantoprazole, omeprazole, lansoprazole, doxycycline, metformin, atropine, lactulose, prednisone, simvastatin and potassium chloride.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for PLA2G6-associated neurodegeneration is supported by model output only (L5). There are no trials or literature, and the mechanism could at most justify symptomatic parkinsonian benefit. In addition, the only New Zealand authorization is cancelled, and package insert safety data are missing.

**To proceed, the following is needed:**
- Mechanism of action data from DrugBank
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Case reports or preclinical data for entacapone in PLA2G6-related dystonia-parkinsonism
- Clarification of the cancelled licence status and whether a supply route exists in New Zealand
- If a nearby indication is prioritised, a focused review of juvenile parkinsonism or Lewy body dementia, which have more plausible rationales
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


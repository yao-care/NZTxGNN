---
layout: default
title: Procyclidine
parent: Model Prediction Only (L5)
nav_order: 387
evidence_level: L5
indication_count: 10
---

# Procyclidine
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

# Procyclidine: From Parkinson Disease to Rasmussen Subacute Encephalitis

## One-Sentence Summary

Procyclidine is a centrally acting anticholinergic (muscarinic antagonist) originally used for Parkinson disease and related parkinsonian and dystonic movement disorders.
The TxGNN model predicts it may be effective for **Rasmussen subacute encephalitis**,
but there are currently **0 clinical trials** and **0 publications** supporting this prediction, so it rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Parkinson disease and related parkinsonian and dystonic disorders (the New Zealand licence record has no indication text, so this comes from the knowledge graph) |
| Predicted New Indication | Rasmussen subacute encephalitis |
| TxGNN Prediction Score | 99.74% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only |
| Number of Authorizations | 1 (0 currently valid, 1 cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, procyclidine is a centrally acting muscarinic antagonist. Its efficacy in parkinsonism and dystonia is established, but it has no documented mechanistic link to Rasmussen encephalitis.

Rasmussen encephalitis is an immune-mediated, T-cell-driven neuroinflammatory disease. It causes unilateral cortical atrophy and refractory focal seizures. Anticholinergic action is not expected to modify this immune pathology. The very high TxGNN score (99.74%) most likely reflects knowledge-graph proximity to neurological and basal ganglia disease nodes rather than a validated biological rationale.

Other top-ranked predictions were also reviewed. Most (for example myelitis, ADHD and several metabolic or developmental syndromes) have no plausible mechanistic link.
- **PLA2G6-associated neurodegeneration** is the most biologically plausible for symptomatic use, because dystonia and parkinsonism overlap with procyclidine's original uses.
- **Progressive supranuclear palsy-corticobasal syndrome** shares basal ganglia features, but anticholinergics have uncertain value there and carry cognitive risks in older patients.
- **Juvenile parkinsonism (paralysis agitans, juvenile, of Hunt)** largely overlaps with the existing indication rather than being true repurposing.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 264962 (cancelled) | Kemadrin | Tab 5 mg | Not recorded |

## Safety Considerations

- **Contraindications flagged in the knowledge graph** (drug-disease links, not label text). They fall into these groups:
  - Eye: angle-closure (closed-angle) glaucoma
  - Gastrointestinal: gastric ulcer, ulcerative colitis, inflammatory bowel disease, achalasia and megaesophagus, pyloric stenosis, hiatus hernia, paralytic ileus, intestinal obstruction, toxic megacolon, familial visceral myopathy
  - Urinary: benign prostatic hyperplasia, bladder neck obstruction, pyoureter
  - Neuromuscular: myasthenia gravis and neuromuscular junction disease
  - Cardiovascular and endocrine: hypertension, coronary artery disease, hyperthyroidism (Graves disease, thyrotoxicosis)
  - Organ disease: kidney, liver and gallbladder disease
- **Drug Interactions**: 70 interactions were identified in total. Main ones:
  - **Major**: potassium chloride and potassium citrate
  - **Moderate**: other anticholinergics (atropine, hyoscyamine, glycopyrronium, dicyclomine, trospium, clidinium, mepenzolate, methscopolamine), opioids and related agents (morphine, opium, loperamide, eluxadoline), cannabinoids (dronabinol, nabilone), metoclopramide and pramlintide

Please refer to the package insert for label warnings, as these were not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score, with no clinical trials, no literature and no plausible mechanistic link to the immune-driven pathology of Rasmussen encephalitis. The New Zealand licence is also cancelled, so there is no currently marketed product.

**To proceed, the following is needed:**
- Mechanism of action data and a mechanistic rationale linking muscarinic blockade to the predicted indication
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Any preclinical or clinical evidence for Rasmussen encephalitis
- Consideration of PLA2G6-associated neurodegeneration as a more plausible symptomatic research question
- Clarification of the availability of an anticholinergic product in New Zealand, given the cancelled licence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


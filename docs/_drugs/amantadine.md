---
layout: default
title: Amantadine
parent: Model Prediction Only (L5)
nav_order: 30
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-15
description: "Amantadine: predicted new indication Rasmussen subacute encephalitis (evidence level L5). Approved use: Parkinson disease and related parkinsonian and dystonic disorders"
monitor_inn: "Amantadine"
predicted_indications: ["Rasmussen subacute encephalitis", "transaldolase deficiency", "myelitis", "PLA2G6-associated neurodegeneration"]
---

# Amantadine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

# Amantadine: From Parkinson Disease to Rasmussen Subacute Encephalitis

## One-Sentence Summary

Amantadine was originally used for Parkinson disease and related movement disorders.
The TxGNN model predicts it may be effective for **Rasmussen subacute encephalitis**,
but **no clinical trials or publications** currently support this specific prediction. The prediction rests on model output alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Parkinson disease and related parkinsonian and dystonic disorders (the approved indication text on the New Zealand licences is not available) |
| Predicted New Indication | Rasmussen subacute encephalitis |
| TxGNN Prediction Score | 99.48% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorisation) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, amantadine acts on NMDA-type glutamate receptors (the data lists GluN2A, GluN2B, GluN2C and GluN2D as targets) and modulates dopaminergic signalling. Its efficacy in Parkinson disease and drug-induced movement disorders is established.

Rasmussen encephalitis is a rare, progressive inflammatory brain disease in which glutamate-mediated excitotoxicity and neuroinflammation are thought to play a role. NMDA receptor antagonism could therefore be relevant, and this is the only plausible mechanistic link. It rests on general pharmacology rather than on supplied data, and it remains unverified.

The very high model score should be interpreted with caution: there is no supporting study of any kind for this disease.

Other predictions in the data set are worth noting:
- **Myelitis** (score 99.24%, evidence L3): the literature concerns post-polio syndrome fatigue, a late sequela of poliomyelitis. It is indirect evidence for myelitis as a disease category.
- **PLA2G6-associated neurodegeneration** (score 99.23%, evidence L4): the only publication concerns a different disease (PKAN), so this is extrapolation across subtypes.
- **Transaldolase deficiency** (score 99.27%, evidence L5): no clear mechanistic connection is evident, and the signal is likely a knowledge-graph artefact.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available for Rasmussen subacute encephalitis.

For reference, the highest-ranked alternative prediction (myelitis) has the following indirect literature, mostly on post-polio syndrome:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7611638](https://pubmed.ncbi.nlm.nih.gov/7611638/) | 1995 | RCT | Ann N Y Acad Sci | Double-blind, placebo-controlled trial of amantadine for fatigue in post-polio syndrome; 23 patients completed 6 weeks of therapy |
| [25984923](https://pubmed.ncbi.nlm.nih.gov/25984923/) | 2015 | Systematic review (Cochrane) | Cochrane Database Syst Rev | Treatment for post-polio syndrome; effectiveness of pharmacological treatment not yet established |
| [21328301](https://pubmed.ncbi.nlm.nih.gov/21328301/) | 2011 | Systematic review (Cochrane) | Cochrane Database Syst Rev | Earlier version of the same review; same conclusion on effectiveness |
| [16879288](https://pubmed.ncbi.nlm.nih.gov/16879288/) | 2006 | Guideline | Eur J Neurol | EFNS guideline on diagnosis and management of post-polio syndrome |
| [2039376](https://pubmed.ncbi.nlm.nih.gov/2039376/) | 1991 | Case series | Arch Neurol | Open-label report of post-polio fatigue treated with amantadine |
| [17874343](https://pubmed.ncbi.nlm.nih.gov/17874343/) | 2007 | Review | Neurol Neurochir Pol | Fatigue syndrome in chronic neurological disorders |
| [15561551](https://pubmed.ncbi.nlm.nih.gov/15561551/) | 2005 | Review | Phys Med Rehabil Clin N Am | Ageing with polio and post-polio syndrome |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 204749 | Symmetrel | Cap 100 mg | Not available (cancelled) |
| 788589 | Symmetrel | Cap 100 mg | Not available (cancelled) |

---

## Safety Considerations

- **Contraindications (from knowledge-graph associations, not label text)**: multiple system atrophy, postural orthostatic tachycardia syndrome, orthostatic hypotension, epilepsy, kidney disease, pyoureter, congestive heart failure, psychotic disorders (including schizophreniform and schizoaffective disorder), mental disorder, melanoma, angle-closure glaucoma, substance-related disorders, atopic eczema and dermatitis.
- **Pharmacological targets**: amantadine acts on the NMDA receptor subunits GluN2A, GluN2B, GluN2C and GluN2D. No drug-drug interaction severity levels are available.

Please refer to the package insert for full warnings and precautions.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for Rasmussen subacute encephalitis rests on a very high model score only, with no clinical trials or publications (L5). In addition, amantadine currently has no valid New Zealand authorisation, so any use would require a new regulatory pathway.

**To proceed, the following is needed:**
- Detailed mechanism of action data (MOA) from DrugBank
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- A targeted literature search for amantadine or NMDA antagonists in Rasmussen encephalitis
- Review of the post-polio syndrome systematic reviews and guideline to confirm their actual conclusions if myelitis is pursued instead
- Assessment of route and formulation availability, given that both licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


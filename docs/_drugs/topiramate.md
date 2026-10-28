---
layout: default
title: Topiramate
parent: Model Prediction Only (L5)
nav_order: 457
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-28
monitor_inn: "TOPIRAMATE"
predicted_indications: ["trigeminal nerve neoplasm"]
description: "Topiramate: predicted new indication Trigeminal nerve neoplasm (evidence level L5). Approved use: Not recorded in the local data"
---

# Topiramate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Topiramate: Trigeminal nerve neoplasm
{: .text-alpha }

## One-Sentence Summary

Topiramate is a broad-spectrum antiseizure medicine, originally used for epilepsy (generalized tonic-clonic seizures, partial epilepsy and Lennox-Gastaut syndrome).
The TxGNN model's top-ranked prediction is **trigeminal nerve neoplasm**, but there are **0 clinical trials** and **0 publications** supporting it.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Trigeminal nerve neoplasm |
| TxGNN Prediction Score | 0.9970 |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Marketed (valid licences: 14 of 14) |
| Number of Authorizations | 14 (valid: 14) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, topiramate acts through several mechanisms: blockade of voltage-gated sodium channels, enhancement of GABA-A activity, antagonism of AMPA/kainate glutamate receptors, and weak carbonic anhydrase inhibition. Its efficacy in epilepsy is well established.

For the top-ranked prediction, **trigeminal nerve neoplasm**, no plausible mechanism links these actions to anti-tumour activity. The very high score (0.9970) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, such as trigeminal neuralgia, not biological evidence. Symptomatic relief of neuropathic pain is conceivable, but that would be a different indication.


---

## Clinical Trial Evidence

Currently no related clinical trials registered for trigeminal nerve neoplasm, or for any of the other predicted indications reviewed.

---

## Literature Evidence

For trigeminal nerve neoplasm, currently no related literature is available. The table below shows the most relevant publications retrieved for the next-ranked prediction, visual epilepsy. These are indirect evidence, since none addresses visually triggered seizures specifically.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37378757](https://pubmed.ncbi.nlm.nih.gov/37378757/) | 2023 | Systematic review / network meta-analysis | J Neurol | Compares efficacy and safety of antiseizure medications for idiopathic generalized epilepsies |
| [34817852](https://pubmed.ncbi.nlm.nih.gov/34817852/) | 2021 | Systematic review (Cochrane) | Cochrane Database Syst Rev | Efficacy and tolerability of topiramate in juvenile myoclonic epilepsy |
| [35421622](https://pubmed.ncbi.nlm.nih.gov/35421622/) | 2022 | Review | Seizure | Molecular mechanisms of topiramate and its clinical value in epilepsy |
| [29898971](https://pubmed.ncbi.nlm.nih.gov/29898971/) | 2018 | Guideline | Neurology | AAN/AES practice guideline update on new antiepileptic drugs for new-onset epilepsy |
| [39899099](https://pubmed.ncbi.nlm.nih.gov/39899099/) | 2025 | Review | Continuum | Update on antiseizure medications, including pharmacokinetics, indications and use |
| [38576261](https://pubmed.ncbi.nlm.nih.gov/38576261/) | 2024 | Consensus statement | Eur J Neurol | Seizure management in primary mitochondrial diseases |
| [37684052](https://pubmed.ncbi.nlm.nih.gov/37684052/) | 2023 | Review | BMJ | Management of epilepsy during pregnancy and lactation |

For audiogenic seizures, preclinical studies include [10666508](https://pubmed.ncbi.nlm.nih.gov/10666508/) (2000, Eur J Pharmacol), where topiramate antagonized sound-induced seizures in DBA/2 mice and potentiated other anticonvulsants, and [12581224](https://pubmed.ncbi.nlm.nih.gov/12581224/) (2003, Epilepsia), which tested topiramate in two genetic generalized epilepsy rat models.

---

## New Zealand Market Information

All 14 licences are currently valid. The approved indication text is not available for any of them. Five of the main authorizations are listed below.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 437212 | Topamax | Sprinkle cap 15 mg | Valid |
| 471011 | Topamax | Tab 200 mg | Valid |
| 2350424 | Arrow-Topiramate | Tab 100 mg | Valid |
| 2465841 | Topiramate Actavis | Tab 100 mg | Valid |
| 470996 | Topamax | Tab 50 mg | Valid |

---

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for safety information.

- **Contraindication signals from the knowledge graph** (not label text): anxiety disorder, neurotic disorder, dysthymic disorder, kidney disease, pyoureter, liver disease, gallbladder disease, diarrheal disease, ocular hypertension, nephrolithiasis (including SLC26A1-related susceptibility), acute urate nephropathy, acidosis disorder, metabolic acidosis, amnestic disorder and retrograde amnesia.
- **Drug interactions:** the interaction query found 100 interactions in total; the first 20 returned are summarised here.
  - **Major:** metformin, hyoscyamine, atropine, scopolamine, glycopyrronium, clidinium, dicyclomine, mepenzolate, methscopolamine, propantheline, trospium
  - **Moderate:** pioglitazone, morphine, dronabinol, phentermine, metoclopramide, phenylbutyric acid, nabilone, glyburide, glycerol phenylbutyrate

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction (trigeminal nerve neoplasm) has a very high model score but no trials, no literature and no plausible anti-tumour mechanism, so it is not supported. The drug is marketed in New Zealand, with 14 of 14 licences valid.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data
- Disease-specific studies, such as case series or trials, for the reflex epilepsy indications
- Clarification of the current availability of the product in New Zealand, where 14 of 14 licences are valid
- Review of the knowledge-graph contraindications, especially renal, metabolic acidosis and cognitive effects, against the target population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Topiramate
parent: Model Prediction Only (L5)
nav_order: 457
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-15
description: "Topiramate: predicted new indication Trigeminal nerve neoplasm (evidence level L5). Approved use: Epilepsy (generalized tonic-clonic seizures, partial epilepsy…"
---

# Topiramate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

# Topiramate: From Epilepsy to Trigeminal Nerve Neoplasm

## One-Sentence Summary

Topiramate is a broad-spectrum antiseizure medicine, originally used for epilepsy (generalized tonic-clonic seizures, partial epilepsy and Lennox-Gastaut syndrome).
The TxGNN model's top-ranked prediction is **trigeminal nerve neoplasm**, but there are **0 clinical trials** and **0 publications** supporting it.
The more credible predicted directions are epilepsy-related reflex seizure conditions, such as visual epilepsy and audiogenic seizures, where the evidence is indirect.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Epilepsy (generalized tonic-clonic seizures, partial epilepsy, Lennox-Gastaut syndrome), based on knowledge-graph indication data; no approved indication text is available from the New Zealand licences |
| Predicted New Indication | Trigeminal nerve neoplasm (rank 1) |
| TxGNN Prediction Score | 99.70% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 14 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, topiramate acts through several mechanisms: blockade of voltage-gated sodium channels, enhancement of GABA-A activity, antagonism of AMPA/kainate glutamate receptors, and weak carbonic anhydrase inhibition. Its efficacy in epilepsy is well established.

For the top-ranked prediction, **trigeminal nerve neoplasm**, no plausible mechanism links these actions to anti-tumour activity. The very high score (0.997) probably reflects proximity in the knowledge graph to trigeminal-related conditions such as trigeminal neuralgia, not a therapeutic effect on the tumour. Symptomatic relief of neuropathic pain is conceivable, but that would be a different indication.

The other predicted indications are mostly rare reflex epilepsies (visual epilepsy, micturition-induced seizures, startle epilepsy, eating seizures, orgasm-induced seizures, thinking seizures, audiogenic seizures and reading seizures). Topiramate's general antiseizure mechanisms make these plausible. However, the supporting literature addresses the parent indication (epilepsy) and not these specific conditions, so the evidence is indirect. Audiogenic seizures have preclinical support: topiramate was anticonvulsant in DBA/2 mice and in genetic generalized epilepsy rat models, but there are no human data.

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

For audiogenic seizures (rank 8), preclinical studies include [10666508](https://pubmed.ncbi.nlm.nih.gov/10666508/) (2000, Eur J Pharmacol), where topiramate antagonized sound-induced seizures in DBA/2 mice and potentiated other anticonvulsants, and [12581224](https://pubmed.ncbi.nlm.nih.gov/12581224/) (2003, Epilepsia), which tested topiramate in two genetic generalized epilepsy rat models.

---

## New Zealand Market Information

All 14 licences are cancelled and none is currently valid. The approved indication text is not available for any of them. Five of the main authorizations are listed below.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 437212 | Topamax | Sprinkle cap 15 mg | Cancelled |
| 471011 | Topamax | Tab 200 mg | Cancelled |
| 2350424 | Arrow-Topiramate | Tab 100 mg | Cancelled |
| 2465841 | Topiramate Actavis | Tab 100 mg | Cancelled |
| 470996 | Topamax | Tab 50 mg | Cancelled |

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
The top prediction (trigeminal nerve neoplasm) has a very high model score but no trials, no literature and no plausible anti-tumour mechanism, so it is not supported. All New Zealand licences are cancelled. The epilepsy-related predictions are mechanistically plausible but rest on indirect evidence only. Visual epilepsy, thinking seizures and audiogenic seizures could be treated as research questions rather than development candidates.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data
- Disease-specific studies, such as case series or trials, for the reflex epilepsy indications
- Clarification of why all New Zealand licences were cancelled and whether a product is still available
- Review of the knowledge-graph contraindications, especially renal, metabolic acidosis and cognitive effects, against the target population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


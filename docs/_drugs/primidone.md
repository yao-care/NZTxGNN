---
layout: default
title: Primidone
parent: Model Prediction Only (L5)
nav_order: 383
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Primidone: predicted new indication Trigeminal nerve neoplasm (evidence level L5). Approved use: Epilepsy (including generalized tonic-clonic seizures…"
monitor_inn: "PRIMIDONE"
predicted_indications: ["trigeminal nerve neoplasm", "trigeminal neuralgia", "beta-ketothiolase deficiency"]
---

# Primidone
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

## Primidone: From Epilepsy to Trigeminal Nerve Neoplasm
{: .text-alpha }

## One-Sentence Summary

Primidone is an anticonvulsant originally used to treat epilepsy, including generalized tonic-clonic seizures, partial epilepsy and Lennox-Gastaut syndrome.
The TxGNN model predicts it may be effective for **trigeminal nerve neoplasm**, but this is a computational prediction only, with **0 clinical trials** and **0 publications** supporting it.
Among the other predicted indications, the reflex seizure types and trigeminal neuralgia have some indirect literature, but none of it is primidone-specific.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Epilepsy (including generalized tonic-clonic seizures, partial epilepsy, Lennox-Gastaut syndrome) |
| Predicted New Indication | Trigeminal nerve neoplasm |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorization) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, primidone is an established anticonvulsant. It is metabolized to phenobarbital and PEMA, which act on voltage-gated sodium channels and GABAergic signalling.

For the top-ranked prediction, trigeminal nerve neoplasm, there is no established link between this antiseizure activity and tumour biology. The very high score most likely reflects network proximity to trigeminal nerve-related conditions in the knowledge graph rather than a real antineoplastic effect. This prediction should be treated with caution.

The lower-ranked predictions are more biologically coherent. Several are rare reflex seizure types (orgasm-induced, audiogenic, startle, micturition-induced, eating, thinking and reading seizures). These fall within the drug's antiseizure scope. Trigeminal neuralgia is also treated with sodium channel-blocking anticonvulsants, with carbamazepine as first-line. The evidence for all of these is indirect, and none of it demonstrates a primidone-specific effect.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature was found for the top-ranked prediction, trigeminal nerve neoplasm. The most relevant records for the other predicted indications are summarized below. None is specific to the top prediction, and no randomized controlled trials were identified.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15246950](https://pubmed.ncbi.nlm.nih.gov/15246950/) | 2004 | Review | Epileptic Disord | Antiepileptic drugs for non-epilepsy indications; evidence mostly from case series and small trials (trigeminal neuralgia) |
| [8891399](https://pubmed.ncbi.nlm.nih.gov/8891399/) | 1995 | Review | Clin Neurosci | Primidone is among the most useful drugs for cortical myoclonus, often combined with others (reading seizures) |
| [116632](https://pubmed.ncbi.nlm.nih.gov/116632/) | 1979 | Case report | Arch Neurol | Drawing-induced myoclonic seizures in a 17-year-old (reflex epilepsy) |
| [8548670](https://pubmed.ncbi.nlm.nih.gov/8548670/) | 1995 | Case report | Zhonghua Yi Xue Za Zhi | Startle epilepsy presenting as drop attacks in a 3.5-year-old; primidone's role not stated |
| [184518](https://pubmed.ncbi.nlm.nih.gov/184518/) | 1976 | Preclinical (animal) | Neurologie et Psychiatrie | Effects of phenobarbital, phenytoin and primidone on brain enzymes in mice with audiogenic epilepsy |
| [14249886](https://pubmed.ncbi.nlm.nih.gov/14249886/) | 1964 | Preclinical (animal) | Biull Eksp Biol Med | Audiogenic convulsions after prior administration of primidone in animals |
| [9068876](https://pubmed.ncbi.nlm.nih.gov/9068876/) | 1996 | Review | Baillieres Clin Neurol | Overview of established antiepileptic drugs including primidone (micturition-induced seizures) |
| [19054416](https://pubmed.ncbi.nlm.nih.gov/19054416/) | 2009 | Cohort | Epilepsia | Antiepileptic drug choice in new-onset geriatric epilepsy (thinking seizures); not specific to the indication |
| [4525036](https://pubmed.ncbi.nlm.nih.gov/4525036/) | 1974 | Veterinary case report | J Am Vet Med Assoc | Long-term primidone therapy in a dog (eating seizures); indirect, non-human |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2621746 | Primidone Clinect | Tab 250 mg | Not available (authorization cancelled) |

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (these are not label text): anxiety and neurotic disorders, dysthymic disorder, porphyria and other porphyrin metabolism diseases, kidney disease, liver disease, gallbladder disease, substance abuse/dependence, sleep apnea syndromes, and blood disorders (thrombocytopenia, neutropenia, agranulocytosis, leukopenia, pancytopenia, anemia and various congenital neutropenias).
- **Drug Interactions**: 100 interactions were identified. Most of those listed are Moderate (for example doxycycline, corticosteroids such as hydrocortisone and dexamethasone, vitamin D analogues, pioglitazone, bupropion, aprepitant and metronidazole). Major interactions include **morphine** and **eliglustat**. Cimetidine is Minor.
- Primidone's metabolite phenobarbital induces liver enzymes, which underlies many of these interactions and would need a dedicated safety review.

For label warnings and precautions, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top predicted indication, trigeminal nerve neoplasm, has no clinical trials or literature, and no plausible mechanism connects primidone to tumour biology, so it rests on a computational score alone (L5). The seizure-related predictions are mechanistically more coherent but are supported only by indirect, mostly older evidence (L4, research questions at best). In New Zealand, the only authorization is cancelled.

**To proceed, the following is needed:**
- Detailed mechanism of action data
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Primidone-specific clinical evidence for the reflex seizure types or trigeminal neuralgia, and a comparison against established options such as carbamazepine
- A safety review covering the extensive drug interactions and the knowledge-graph-flagged contraindications
- Confirmation of the regulatory pathway, given that no valid New Zealand authorization currently exists
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


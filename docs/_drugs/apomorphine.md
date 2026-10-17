---
layout: default
title: Apomorphine
parent: Model Prediction Only (L5)
nav_order: 41
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Apomorphine: predicted new indication Polymicrogyria, perisylvian… (evidence level L5). Approved use: Parkinson disease"
monitor_inn: "Apomorphine"
predicted_indications: ["polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis", "congenital disorder of glycosylation with defective fucosylation", "retinal dystrophy with or without extraocular anomalies", "Charcot-Marie-Tooth disease, demyelinating, type 1G", "schizophrenia", "myopia X-linked", "atypical glycine encephalopathy", "myopia 26, X-linked, female-limited", "syndromic myopia", "hydranencephaly (disease)"]
---

# Apomorphine
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

# Apomorphine: From Parkinson Disease to Polymicrogyria, Perisylvian, with Cerebellar Hypoplasia and Arthrogryposis

## One-Sentence Summary

Apomorphine is a dopamine receptor agonist originally used to treat Parkinson disease, particularly "off" episodes in advanced stages.
The TxGNN model predicts it may be effective for **polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis**,
but **no clinical trials and no publications** currently support this prediction, so it rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Parkinson disease (knowledge graph indication; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis |
| TxGNN Prediction Score | 99.75% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the compiled record. From pharmacology data, apomorphine is a non-selective dopamine agonist acting on D1–D5 receptors, and it also binds several serotonin receptors (5-HT1A, 2A, 2B, 2C), α2-adrenoceptors (2A, 2B, 2C) and TRPA1. Its efficacy in Parkinson disease is established.

For this specific prediction, no mechanistic link could be identified. The predicted condition is an ultra-rare congenital neurodevelopmental malformation involving abnormal cortical folding, cerebellar underdevelopment and joint contractures. Dopaminergic agonism has no recognized relevance to cortical malformation or arthrogryposis. The very high score (99.75%) is a graph-based prediction only and should not be read as evidence of benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for this predicted indication.

For context, the model's fifth-ranked prediction, schizophrenia, has some literature, but it is mostly mechanistic or challenge-paradigm studies rather than therapeutic trials. Small clinical studies found no significant therapeutic effect of low-dose apomorphine in schizophrenia. A direct dopamine agonist is also theoretically counter-therapeutic there, since antipsychotics work by blocking D2 receptors.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2498723 | Movapo | Inj 10 mg per ml, 2 ml ampoule | Not available (cancelled) |
| 2581159 | Movapo | Inj 10 mg per ml, 5 ml ampoule | Not available (cancelled) |

Both licences are single-ingredient injectable products and both are cancelled, so apomorphine currently has no valid New Zealand authorization in this data.

## Safety Considerations

- **Contraindication signals (from the knowledge graph, not label text):** multiple system atrophy, postural orthostatic tachycardia syndrome, orthostatic hypotension, drug-induced dyskinesia, long QT syndrome (including familial), torsades de pointes, liver disease, gallbladder disease, psychotic disorders (including schizophreniform and schizoaffective disorder), mental disorder, potassium deficiency, substance abuse/dependence, coronary artery disease, impaired renal function, priapism and cerebrovascular disorder.
- **Pharmacological interactions:** apomorphine acts on multiple receptor families (dopamine D1–D5, serotonin 5-HT1A/2A/2B/2C, α2-adrenoceptors, TRPA1). Concurrent use of drugs acting on these systems warrants caution. No drug–drug interaction severity ratings were available.

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no supporting trials or literature (L5), no plausible mechanistic link to the target disease, and apomorphine currently has no valid New Zealand authorization. The high model score alone is insufficient to justify further investment.

**To proceed, the following is needed:**
- A credible mechanistic hypothesis linking dopamine receptor signalling to the pathology of this malformation syndrome
- Preclinical or other supporting evidence in relevant models
- Detailed mechanism of action data
- Package insert warnings and contraindications from Medsafe
- Clarification of the regulatory pathway, given that both New Zealand licences are cancelled
- Review of the other top-ranked predictions: most also have no evidence (L5), and schizophrenia (L4) has indirect evidence that does not support therapeutic benefit
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


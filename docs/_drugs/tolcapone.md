---
layout: default
title: Tolcapone
parent: Model Prediction Only (L5)
nav_order: 455
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Tolcapone: predicted new indication Rasmussen subacute encephalitis (evidence level L5). Approved use: Parkinson disease (and related parkinsonian syndromes)"
monitor_inn: "TOLCAPONE"
predicted_indications: ["Rasmussen subacute encephalitis", "myelitis", "PLA2G6-associated neurodegeneration", "transaldolase deficiency", "fructose-1,6-bisphosphatase deficiency", "Lewy body dementia", "X-linked intellectual disability-ataxia-apraxia syndrome", "polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis", "X-linked intellectual disability-cerebellar hypoplasia syndrome", "paralysis agitans, juvenile, of Hunt"]
---

# Tolcapone
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

# Tolcapone: From Parkinson Disease to Rasmussen Subacute Encephalitis

## One-Sentence Summary

Tolcapone is a COMT inhibitor originally used for Parkinson disease and related parkinsonian syndromes.
The TxGNN model predicts it may be effective for **Rasmussen subacute encephalitis**,
but there are currently **0 clinical trials** and **0 publications** supporting this prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Parkinson disease (and related parkinsonian syndromes) |
| Predicted New Indication | Rasmussen subacute encephalitis |
| TxGNN Prediction Score | 99.93% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorization) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Tolcapone is a catechol-O-methyltransferase (COMT) inhibitor. Detailed mechanism of action data are not available in the data compiled for this report, so this is based on its known drug class only. Its established role is in Parkinson disease, where blocking COMT prolongs the effect of levodopa-based dopaminergic therapy.

Rasmussen encephalitis is an immune-mediated, inflammatory epilepsy that affects one brain hemisphere. It has no obvious connection to COMT inhibition or dopaminergic pathways. The high score (0.9993) is a computational prediction, and it probably reflects network proximity to neurological indications in the knowledge graph rather than a demonstrated biological link. No mechanistic rationale is supported by the available data.

Other top-ranked predictions are also unsupported by trials or literature. Two are worth noting:
- **Lewy body dementia** (score 99.64%) is an alpha-synucleinopathy closely related to Parkinson disease, so a dopaminergic rationale is plausible. The only supplied publication is a preclinical iPSC organoid study that did not test tolcapone.
- **Paralysis agitans, juvenile, of Hunt** (score 99.52%) is a historical term for juvenile-onset parkinsonism. It is likely an overlap with the existing label rather than a true new indication.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for Rasmussen subacute encephalitis.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 487236 | Tasmar (cancelled) | Tab 100 mg | Not available |

## Safety Considerations

- **Knowledge graph contraindication signals** (from drug-disease relationships, not label text): liver disease, gallbladder disease, melanoma, hypotensive disorder, rhabdomyolysis, nodular fasciitis, tardive dyskinesia, lingual-facial-buccal dyskinesia.
- **Drug Interactions**: 65 interactions were identified. Among those listed, the one major interaction is with **methylene blue**. Moderate interactions include epinephrine, morphine, opium, codeine, dronabinol, nabilone, naltrexone, metoclopramide, promethazine, chlorpheniramine, cetirizine, dextromethorphan and ethanol. Warfarin and dicoumarol are minor. Omeprazole is of unknown severity.

Package insert warnings and contraindications were not available. Please refer to the package insert for full safety information. Tolcapone is also known to carry a hepatotoxicity warning, which is relevant to any new use.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score only, with no clinical trials, no literature and no plausible mechanistic link between COMT inhibition and Rasmussen encephalitis. Tolcapone is also no longer actively authorized in New Zealand, and its hepatotoxicity profile raises the bar for any new use.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (Medsafe) to enable safety screening
- Detailed mechanism of action data (e.g., from DrugBank)
- A mechanistic rationale and preclinical evidence linking COMT inhibition to Rasmussen encephalitis
- Consideration of Lewy body dementia as a more biologically plausible candidate for further investigation as a research question
- Clarification of the New Zealand regulatory pathway, since the only licence is cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


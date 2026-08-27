---
layout: default
title: Primidone
parent: 僅模型預測 (L5)
nav_order: 289
evidence_level: L5
indication_count: 10
---

# Primidone
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Primidone: From Epilepsy/Essential Tremor to Trigeminal Nerve Neoplasm

## One-Sentence Summary

Primidone is a barbiturate-class prodrug (metabolized to phenobarbital + PEMA) used as an antiepileptic/antitremor agent, acting via GABA-A receptor potentiation and high-frequency sodium-channel blockade. The TxGNN model's top-ranked prediction is **Trigeminal Nerve Neoplasm**, but this candidate currently has **0 clinical trials** and **0 publications** supporting it, and the evidence pack's own mechanistic rationale flags the association as a likely knowledge-graph artefact ("trigeminal" term-adjacency confusion) rather than a genuine pharmacological link.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in evidence pack (no market authorization on file); drug class per rationale text: barbiturate-derivative antiepileptic/antitremor agent |
| Predicted New Indication | Trigeminal Nerve Neoplasm |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data (`original_moa`) is formally marked as a data gap in this evidence pack. However, the model's own rationale text supplies working pharmacology: primidone is a barbiturate prodrug, metabolized to phenobarbital and PEMA, and acts through GABA-A receptor potentiation combined with high-frequency sodium-channel blockade — the classical mechanism underlying anticonvulsant and antitremor effects.

For the top-ranked candidate, **Trigeminal Nerve Neoplasm**, no plausible mechanistic bridge exists between this GABA-A/sodium-channel pathway and tumour growth pathways in trigeminal nerve sheath tumours. The evidence pack explicitly notes this: the high TxGNN score "may reflect knowledge-graph term-adjacency confusion (the word 'trigeminal') rather than a true pharmacological relationship." This is a case where a high similarity score should **not** be read as strong biological plausibility.

By contrast, several lower-ranked candidates in this pack have a far more coherent mechanistic story: reflex/stimulus-triggered epilepsies (audiogenic seizures, startle epilepsy, micturition-induced seizures, reading seizures, thinking/eating seizures) sit squarely within primidone's established anticonvulsant pharmacology, and trigeminal neuralgia has a historical precedent of barbiturate use predating carbamazepine. These are discussed further below.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

Primidone currently has no market authorization on file (0 licenses); no product/dosage-form records are available.

## Other Predicted Indications (Ranked Candidates)

The evidence pack scores 10 candidate indications for primidone. Ranked by TxGNN score, only the reflex-epilepsy cluster and trigeminal neuralgia reach L4 (preclinical/mechanistic literature, no controlled trials); the remainder are L5 (model prediction only) and held.

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Decision Stage | Recommendation | Note |
|------|----------------------|-------------|-----------------|-----------------|------------------|------|
| 1 | Trigeminal nerve neoplasm | 99.99% | L5 | S0 | Hold | Likely KG term-confusion, no evidence |
| 2 | Orgasm-induced seizures | 99.99% | L5 | S0 | Hold | No literature or trials |
| 3 | Audiogenic seizures | 99.99% | L4 | S1 | Research Question | 12 publications; classic AED animal-screening model, primidone/phenobarbital textbook evidence, but human data limited to case reports |
| 4 | Startle epilepsy | 99.99% | L4 | S1 | Research Question | 1 case report only |
| 5 | Micturition-induced seizures | 99.99% | L4 | S1 | Research Question | 15 publications, incl. NEJM comparative trial (PMID 3925335, carbamazepine/phenobarbital/phenytoin/primidone), but not disease-specific |
| 6 | Eating seizures | 99.99% | L5 | S0 | Hold | Single veterinary (canine) case report — not human data |
| 7 | Thinking seizures | 99.99% | L5 | S0 | Hold | 1 indirect geriatric-AED cohort study |
| 8 | Reading seizures | 99.99% | L4 | S1 | Research Question | 9 publications on reflex-epilepsy family, no disease-specific trial |
| 9 | Trigeminal neuralgia | 99.98% | L4 | S1 | Research Question | Historical barbiturate precedent (1957) pre-dating carbamazepine; most literature is on carbamazepine, not primidone directly |
| 10 | Beta-ketothiolase deficiency | 99.96% | L5 | S0 | Hold | No mechanistic overlap (mitochondrial acetyl-CoA disorder); no evidence |

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked candidate (Trigeminal Nerve Neoplasm) has zero supporting trials or literature, and the evidence pack itself attributes the high score to likely knowledge-graph noise rather than a real pharmacological signal. Across all 10 candidates in this pack, none exceed L4 (case reports, animal models, and general AED reviews); no candidate has a controlled human trial specific to the predicted indication.

**To proceed, the following is needed:**
- TFDA/regulatory package insert warnings and contraindications (currently blocking — DG001)
- Confirmed DrugBank mechanism-of-action and formal original-indication text (currently a high-severity gap — DG002)
- If pursuing further inquiry, re-scope toward the reflex-epilepsy cluster (audiogenic seizures, startle epilepsy, reading seizures) or trigeminal neuralgia, which have comparatively stronger mechanistic and literature support than the top-scored candidate
- Drug-drug interaction data (currently not found)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


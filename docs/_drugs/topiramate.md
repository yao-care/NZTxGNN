---
layout: default
title: Topiramate
parent: 僅模型預測 (L5)
nav_order: 345
evidence_level: L5
indication_count: 9
---

# Topiramate
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
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

# Topiramate: From Epilepsy to Trigeminal Nerve Neoplasm

## One-Sentence Summary

Topiramate is a well-established second-generation anticonvulsant, originally developed for the treatment of partial-onset and generalized epilepsy (and later migraine prophylaxis). The TxGNN model's top-ranked prediction proposes it may be effective for **Trigeminal Nerve Neoplasm**, but currently **0 clinical trials** and **0 publications** support this specific link — this is a model-score-only signal with no corroborating evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Epilepsy (partial-onset and generalized seizures); migraine prophylaxis (per published literature — formal Taiwan/NZ licensed indication text not available) |
| Predicted New Indication | Trigeminal Nerve Neoplasm |
| TxGNN Prediction Score | 99.70% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data for topiramate is not available in this evidence pack (drug-level MOA field is a documented data gap). Based on the literature captured elsewhere in this pack, topiramate is known to act on multiple targets — voltage-gated sodium and calcium channels, GABA-A receptors, AMPA/kainate glutamate receptors, and carbonic anhydrase isoenzymes — which together underlie its broad-spectrum antiseizure and migraine-preventive effects.

Trigeminal nerve neoplasm (typically a schwannoma or other nerve-sheath tumor) is a structurally and pathologically distinct condition from epilepsy or migraine. None of topiramate's known ion-channel or enzyme-inhibitory mechanisms have an established link to tumor suppression, and no preclinical or clinical literature in this evidence pack connects topiramate to neoplastic disease of the trigeminal nerve.

By contrast, several lower-ranked TxGNN predictions for topiramate (e.g., visual epilepsy, thinking seizures, reading seizures, audiogenic seizures — ranks 2–9) are directly consistent with topiramate's known antiepileptic pharmacology and are backed by substantial clinical trial and literature evidence. This suggests the model's top-ranked "novel" indication (trigeminal nerve neoplasm) lacks the biological plausibility and evidentiary support seen in its other, more conventional seizure-related predictions.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

Topiramate is not currently marketed in New Zealand, and no product authorization records are available in this evidence pack.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (trigeminal nerve neoplasm) has zero supporting clinical trials or literature, and no plausible mechanistic link connects topiramate's known ion-channel/enzyme-modulating pharmacology to nerve tumor treatment — this is currently a model-score-only signal (L5) with no corroborating evidence.

**To proceed, the following is needed:**
- TFDA/package insert warnings and contraindications (currently a Blocking data gap — DG001)
- Confirmed mechanism of action data (DG001/DG002)
- Any preclinical or case-level evidence linking topiramate to neural tumor pathways before further evaluation
- Reassessment of whether lower-ranked, evidence-rich predictions (e.g., seizure-related indications) represent more actionable repurposing candidates than the top TxGNN score alone
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


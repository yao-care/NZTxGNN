---
layout: default
title: Amorolfine
parent: 僅模型預測 (L5)
nav_order: 27
evidence_level: L5
indication_count: 10
---

# Amorolfine
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

# Amorolfine: From Onychomycosis to Drug-induced Osteoporosis

## One-Sentence Summary

Amorolfine is a morpholine-class topical antifungal drug, primarily used to treat onychomycosis (nail fungal infections) via nail lacquer formulation.
The TxGNN model predicts it may be effective for **Drug-induced Osteoporosis**,
however with **0 clinical trials** and **0 publications** currently supporting this direction, the evidence base is entirely absent.

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Onychomycosis (nail fungal infection) — drug not registered in Taiwan; based on international known use |
| Predicted New Indication | Drug-induced Osteoporosis |
| TxGNN Prediction Score | 99.9978% |
| Evidence Level | L5 |
| Taiwan Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this Evidence Pack. Based on known pharmacological information, amorolfine is a morpholine-class antifungal agent applied topically as a nail lacquer. It inhibits ergosterol biosynthesis in fungal cells — specifically targeting Δ14-reductase and Δ7-Δ8 isomerase enzymes — resulting in accumulation of toxic sterol intermediates that disrupt fungal cell membrane integrity. Its efficacy against onychomycosis caused by dermatophytes, yeasts, and moulds has been well established in international markets.

The mechanistic link to drug-induced osteoporosis is extremely tenuous. The ergosterol biosynthesis pathway shares distant evolutionary homology with cholesterol metabolism, and certain azole-class antifungals (e.g., itraconazole) have occasionally been explored in bone metabolism research due to their Hedgehog pathway inhibition. However, amorolfine belongs to the morpholine class — not the azole class — and acts on different downstream enzymatic targets. More critically, amorolfine is exclusively a topical preparation with negligible systemic bioavailability, meaning it cannot achieve plasma concentrations sufficient for any systemic pharmacological effect.

It is most plausible that this TxGNN prediction arises from knowledge graph topology — specifically, shared network neighbourhood between nodes representing "ergosterol/sterol metabolism" and "bone metabolism/osteoporosis" — rather than any direct pharmacological rationale. The absence of MOA data in this Evidence Pack further limits the ability to evaluate this prediction rigorously.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This prediction is supported solely by TxGNN model output (L5 evidence) with zero clinical trials, zero literature, and no mechanistic basis linking a topical morpholine antifungal to systemic bone metabolism. The route of administration (topical nail lacquer) is fundamentally incompatible with treating a systemic condition such as drug-induced osteoporosis.

**To proceed, the following is needed:**
- Detailed mechanism of action data (MOA) from DrugBank to assess any theoretical sterol-bone metabolism linkage
- Preclinical in vitro or in vivo data demonstrating any effect on osteoclast/osteoblast activity
- Identification of a plausible systemic formulation, as current topical-only availability is a hard barrier for any non-dermatological indication
- Review of whether TxGNN's prediction reflects a genuine biological signal or a graph topology artefact (e.g., shared disease node clustering)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


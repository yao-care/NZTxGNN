---
layout: default
title: Tropicamide
parent: 僅模型預測 (L5)
nav_order: 354
evidence_level: L5
indication_count: 3
---

# Tropicamide
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

# Tropicamide: From Ophthalmic Mydriasis to Cauda Equina Syndrome

## One-Sentence Summary

Tropicamide is a non-selective muscarinic acetylcholine receptor (M-receptor) antagonist historically used only as a topical ophthalmic agent for pupil dilation and cycloplegia. The TxGNN model predicts it may be effective for **Cauda Equina Syndrome**, but currently **no clinical trials** and **no publications** support this direction — the prediction rests entirely on graph-topology similarity to other anticholinergic drugs.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Topical ophthalmic mydriasis / cycloplegia (no formal indication text on file; systemic use not established) |
| Predicted New Indication | Cauda Equina Syndrome |
| TxGNN Prediction Score | 99.53% |
| Evidence Level | L5 (model prediction only, no supporting studies) |
| New Zealand Market Status | 未上市 (Not marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data from DrugBank is flagged as a blocking data gap in this evidence pack (DG002). Based on information embedded in the model's own rationale, tropicamide is a non-selective M-receptor antagonist with pharmacology documented only for topical ophthalmic use (mydriasis/cycloplegia); no systemic pharmacokinetic data exists.

Cauda equina syndrome is a neurosurgical emergency driven by mechanical compression of lumbosacral nerve roots, producing lower motor neuron damage and loss of bladder, bowel, and perineal function — its definitive treatment is surgical decompression, not pharmacotherapy. An anticholinergic mechanism has no direct correspondence to this pathology, and could plausibly worsen neurogenic urinary retention rather than help it. The TxGNN score of 0.995 most likely reflects the graph's topological proximity between tropicamide and other anticholinergic drugs linked to urinary/neurologic symptom nodes, not any causal or clinical evidence — this is explicitly flagged in the model's own rationale as a mechanism-direction concern.

Two lower-ranked predictions for this drug (neurogenic bladder, irritable bowel syndrome) follow a more conventional class-effect logic — other anticholinergics (oxybutynin, dicyclomine) are established in those indications — but tropicamide itself has never been studied systemically for any of the three predicted indications.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

Tropicamide is not currently marketed in New Zealand under this evidence pack's regulatory data — 0 product authorizations are on file.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
All three TxGNN-predicted indications for tropicamide sit at Evidence Level L5 with zero clinical trials or literature, and the top-ranked prediction (cauda equina syndrome) has a mechanistic-direction concern (potential worsening of urinary retention) rather than a clear supporting rationale. A blocking data gap (DG001: TFDA/regulatory package insert warnings and contraindications) also prevents this candidate from entering the S1 safety review stage.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data (warnings, contraindications) to resolve blocking gap DG001
- Confirmed mechanism-of-action data from DrugBank to resolve gap DG002
- Preclinical or mechanistic studies specifically evaluating systemic anticholinergic effects relevant to cauda equina syndrome, neurogenic bladder, or IBS
- Systemic pharmacokinetic/dosing data, since tropicamide has no established non-ophthalmic route of administration
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Entacapone
parent: 僅模型預測 (L5)
nav_order: 135
evidence_level: L5
indication_count: 10
---

# Entacapone
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

# Entacapone: From Parkinson's Disease to PLA2G6-Associated Neurodegeneration

## One-Sentence Summary

Entacapone is a selective COMT (catechol-O-methyltransferase) inhibitor used as adjunct therapy to levodopa/carbidopa in Parkinson's disease to extend dopaminergic activity and reduce motor fluctuations.
The TxGNN model predicts it may be effective for **PLA2G6-Associated Neurodegeneration**, with **0 clinical trials** and **0 publications** currently supporting this direction.
This prediction is based on model inference alone, with no direct clinical or experimental evidence available.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Parkinson's disease (adjunct to levodopa/carbidopa) |
| Predicted New Indication | PLA2G6-associated neurodegeneration |
| TxGNN Prediction Score | 99.76% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the evidence pack. Based on known pharmacological information, Entacapone is a peripheral COMT inhibitor — it blocks the enzyme responsible for degrading levodopa in peripheral tissues before it crosses the blood-brain barrier, thereby increasing brain dopamine availability. It is used exclusively as an add-on to levodopa/carbidopa in Parkinson's disease to smooth out "wearing-off" motor fluctuations.

PLA2G6-Associated Neurodegeneration (PLAN), classified as Neurodegeneration with Brain Iron Accumulation type 2 (NBIA2), is caused by mutations in the *PLA2G6* gene encoding a calcium-independent phospholipase A2. The resulting disruption in phospholipid metabolism leads to mitochondrial membrane dysfunction and progressive iron deposition predominantly in the basal ganglia. Crucially, certain PLAN subtypes — particularly the atypical adult-onset form — present with prominent Parkinsonian features including bradykinesia, rigidity, and tremor, providing a theoretical basis for dopaminergic augmentation.

However, the mechanistic link is highly indirect. The dominant pathology in PLAN is lipid dysregulation and iron accumulation, not primary dopaminergic neurodegeneration. COMT inhibition could at best offer symptomatic palliation for Parkinson-like features in a subset of patients, but does not address the underlying cause. The TxGNN model most likely identified this association through network proximity of shared neurodegeneration nodes rather than a direct pharmacological mechanism, and the prediction should be interpreted with caution.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Entacapone is currently **not marketed** in New Zealand. No drug authorizations are on record (total licenses: 0).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This prediction is supported by model inference only (Evidence Level L5), with no clinical trials, published literature, or preclinical studies linking Entacapone to PLA2G6-associated neurodegeneration; the mechanistic connection is highly indirect and does not address the primary disease pathology of phospholipid dysregulation and iron deposition.

**To proceed, the following is needed:**
- Preclinical studies in PLA2G6-mutant cell or animal models to test whether dopaminergic agents provide measurable benefit
- Evidence that levodopa monotherapy shows any symptomatic response in PLAN Parkinsonism (a prerequisite for evaluating a COMT inhibitor add-on)
- Detailed safety profile and full MOA data from DrugBank and the package insert (currently flagged as Data Gap)
- Regulatory pathway assessment for New Zealand, given that Entacapone is not currently authorised in the market
- Review of the broader TxGNN top-10 prediction list — **Lewy body dementia** (rank 7, L4 evidence) and **juvenile paralysis agitans of Hunt** (rank 4, biologically plausible) represent stronger near-term research candidates than the rank-1 prediction
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Tolcapone
parent: 僅模型預測 (L5)
nav_order: 343
evidence_level: L5
indication_count: 10
---

# Tolcapone
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

# Tolcapone: From Parkinson's Disease to Rasmussen Subacute Encephalitis

## One-Sentence Summary

Tolcapone is a COMT (catechol-O-methyltransferase) inhibitor known to be used as adjunct therapy to levodopa/carbidopa in Parkinson's disease. The TxGNN model's top-ranked prediction for this drug is **Rasmussen Subacute Encephalitis**, but this direction is currently supported by **0 clinical trials** and **0 publications** — it is a pure model-generated hypothesis with no corroborating evidence.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Parkinson's disease, adjunct to levodopa/carbidopa (not captured in this evidence pack's structured fields; based on tolcapone's known pharmacological classification) |
| Predicted New Indication | Rasmussen Subacute Encephalitis |
| TxGNN Prediction Score | 99.93% |
| Evidence Level | L5 |
| Taiwan Market Status | 未上市 (Not marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (marked as a High-severity data gap in this evidence pack — DG002, pending DrugBank API query). Based on known information, tolcapone is a COMT inhibitor that reduces peripheral and central catecholamine breakdown, prolonging the effect of levodopa in Parkinson's disease. Its proven efficacy is limited to this dopaminergic pathway.

Rasmussen subacute encephalitis, by contrast, is a rare autoimmune/neuroinflammatory disorder of childhood, driven by T-cell-mediated cortical inflammation rather than catecholamine metabolism. The evidence pack's own mechanistic assessment is explicit on this point: there is **no known mechanistic link** between COMT inhibition and Rasmussen encephalitis's autoimmune pathology, and no trial or literature evidence exists to bridge the two.

In short, this candidate reflects a graph-based statistical association from the TxGNN model rather than a biologically grounded hypothesis. It should be treated as an open research question at most, not a near-term repurposing candidate.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## Taiwan Market Information

Tolcapone is not currently marketed in Taiwan (0 authorizations on record), so no product license table is available.

## Safety Considerations

Please refer to the package insert for safety information. Note that TFDA package insert data for tolcapone is marked as a **Blocking** data gap (DG001) in this evidence pack, which by itself prevents this candidate from entering the S1 safety initial evaluation stage regardless of the efficacy hypothesis. Separately, other candidates evaluated for this drug reference a known boxed hepatotoxicity warning associated with tolcapone — this should be independently confirmed against the official TFDA/manufacturer package insert once obtained.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (Rasmussen subacute encephalitis) carries only L5 evidence (model prediction alone, no trials or literature), and the stated mechanistic rationale explicitly finds no known pathological connection to tolcapone's COMT-inhibitory action. Combined with the blocking absence of TFDA safety data, this candidate cannot currently proceed past S0.

**To proceed, the following is needed:**
- TFDA package insert / label data (DG001, Blocking) — required before any safety review
- DrugBank-sourced MOA detail (DG002)
- Preclinical or mechanistic evidence linking catecholamine metabolism to Rasmussen encephalitis pathology, if this direction is to be pursued further
- Note: two other predictions in this same evidence pack carry stronger mechanistic plausibility and L4 evidence — **Lewy body dementia** (rank 6, dopamine/α-synuclein pathway literature) and **paralysis agitans, juvenile, of Hunt** (rank 10, likely a historical synonym for juvenile Parkinsonism, directly aligned with tolcapone's approved MOA) — these may warrant separate evaluation ahead of the top-ranked candidate.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


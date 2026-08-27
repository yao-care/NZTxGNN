---
layout: default
title: Vildagliptin
parent: 僅模型預測 (L5)
nav_order: 362
evidence_level: L5
indication_count: 10
---

# Vildagliptin
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

# Vildagliptin: From Type 2 Diabetes Mellitus to Classic Stiff Person Syndrome

## One-Sentence Summary

Vildagliptin is a dipeptidyl peptidase-4 (DPP-4) inhibitor originally developed for the treatment of type 2 diabetes mellitus. The TxGNN model's top-ranked prediction is **Classic Stiff Person Syndrome**, but this direction currently has **zero clinical trials** and **zero publications** supporting it, and the model's own rationale indicates the score is likely driven by a shared knowledge-graph node (GAD65) rather than a genuine pharmacological connection.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Type 2 Diabetes Mellitus (based on known drug class; not derived from a New Zealand license, as none exists) |
| Predicted New Indication | Classic Stiff Person Syndrome |
| TxGNN Prediction Score | 99.88% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (flagged as a High-severity data gap). Based on known information, vildagliptin is an orally active DPP-4 inhibitor — it blocks the enzymatic degradation of the incretin hormones GLP-1 and GIP, thereby enhancing glucose-dependent insulin secretion and suppressing glucagon release. Its efficacy in type 2 diabetes is well established, both as monotherapy and in combination regimens.

Classic Stiff Person Syndrome, however, is a rare autoimmune neurological disorder driven primarily by autoantibodies against glutamic acid decarboxylase 65 (GAD65), which impairs GABAergic inhibitory neurotransmission. The evidence pack's own mechanistic assessment concludes that GAD65 is also a core autoimmune antigen in type 1 diabetes, and it is this shared "GAD65/diabetes" node in the knowledge graph — not any known pharmacological pathway — that most plausibly explains the high TxGNN score. Vildagliptin has no documented immunomodulatory activity and no known effect on central inhibitory synaptic transmission, so there is no established mechanistic bridge between DPP-4 inhibition and correction of GAD65-mediated autoimmune neuronal dysfunction.

In short, this prediction should be read as a knowledge-graph embedding artifact rather than a mechanistically grounded repurposing hypothesis. It is not supported by any clinical, preclinical, or literature-based evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: TFDA package insert warnings/contraindications are flagged internally as a Blocking data gap — this item must be resolved before any S1 safety pre-assessment can proceed.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication (Classic Stiff Person Syndrome) has no clinical trial or literature support, and the underlying mechanistic rationale is assessed as a likely artifact of shared graph nodes (GAD65) rather than genuine pharmacology. Combined with the absence of TFDA/package-insert safety data (a Blocking gap) and the drug's non-marketed status in New Zealand, there is no basis to advance this candidate beyond hypothesis stage.

**To proceed, the following is needed:**
- Verified mechanism of action documentation (DrugBank/primary literature)
- TFDA package insert (warnings, contraindications, DDI) to clear the Blocking safety gap
- Any preclinical evidence of DPP-4/incretin pathway involvement in GAD65-mediated autoimmune neurological disease, if this hypothesis is to be pursued further
- Independent expert (neuro-immunology) review of the mechanistic plausibility before any further investment

**Additional observation:** Among the 10 TxGNN-predicted indications reviewed for this candidate, **Type 1 Diabetes Mellitus** (rank 10, score 99.37%) is notably better supported — with 50 clinical trials and 20 publications, including one completed RCT (PMID 33124663, rapamycin + vildagliptin in long-standing T1DM) — and reaches Evidence Level L2 / Decision Stage S2 ("Research Question"). While still an adjunct/exploratory role rather than disease-modifying therapy (DPP-4 inhibition cannot halt the autoimmune β-cell destruction underlying T1DM), it represents a substantially more actionable avenue than the top-ranked Stiff Person Syndrome prediction and may warrant separate evaluation.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


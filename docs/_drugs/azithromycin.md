---
layout: default
title: Azithromycin
parent: 僅模型預測 (L5)
nav_order: 42
evidence_level: L5
indication_count: 10
---

# Azithromycin
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

# Azithromycin: From Bacterial Infections to Polyclonal Hyperviscosity Syndrome

## One-Sentence Summary

Azithromycin is a broad-spectrum macrolide antibiotic widely used for bacterial infections including respiratory tract infections, community-acquired pneumonia, and sexually transmitted infections.
The TxGNN model predicts it may be effective for **Polyclonal Hyperviscosity Syndrome**,
with **0 clinical trials** and **0 publications** currently supporting this direction — evidence is limited to model prediction only.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Bacterial infections (respiratory tract infections, community-acquired pneumonia, STIs) |
| Predicted New Indication | Polyclonal Hyperviscosity Syndrome |
| TxGNN Prediction Score | 99.81% |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the Evidence Pack. Based on known pharmacology, azithromycin is a macrolide antibiotic that inhibits bacterial protein synthesis by binding to the 50S ribosomal subunit. Beyond its antibacterial effects, azithromycin exhibits well-documented immunomodulatory properties — including inhibition of pro-inflammatory cytokines (IL-6, IL-8), modulation of NF-κB signalling, and blockade of autophagy flux in certain immune and tumour cell lines. These secondary properties have generated significant interest in repurposing azithromycin beyond infectious indications.

Polyclonal hyperviscosity syndrome is characterised by overproduction of polyclonal immunoglobulins (typically IgM, IgG, or IgA) from dysregulated B-cell or plasma cell populations, leading to pathologically elevated serum viscosity. While azithromycin's anti-inflammatory properties could theoretically dampen chronic immune activation, no established mechanistic pathway links its known actions to suppression of polyclonal immunoglobulin synthesis or clearance of circulating paraproteins.

The TxGNN prediction most likely reflects graph node proximity to plasmacyte disease nodes (e.g., monoclonal gammopathy) in the knowledge graph — a probable graph-structure false positive rather than a true mechanistic signal. At this stage, the biological rationale for this specific indication is insufficient to justify further investigation without foundational preclinical data.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Azithromycin is not currently registered in New Zealand. No authorizations on record (total licenses: 0).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Despite a high TxGNN prediction score (99.81%), this indication is unsupported by any clinical or preclinical evidence and is most likely a graph-structure artifact arising from knowledge graph proximity to plasmacyte disease nodes — biological plausibility for azithromycin in polyclonal hyperviscosity syndrome is not currently established.

**To proceed, the following is needed:**
- Mechanistic studies establishing a direct link between azithromycin and suppression of polyclonal immunoglobulin overproduction
- Preclinical data in relevant hyperviscosity or B-cell dysregulation models
- MOA data obtained from DrugBank (DG002 remediation) to clarify the pharmacological basis for any immune-modulatory claims
- Formal biological plausibility assessment to distinguish this prediction from graph noise before committing further research resources
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


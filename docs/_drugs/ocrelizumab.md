---
layout: default
title: Ocrelizumab
parent: 僅模型預測 (L5)
nav_order: 251
evidence_level: L5
indication_count: 5
---

# Ocrelizumab
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

Using the evidence pack as provided (no skill applies here — this is a direct content-generation task from a fully-specified template).

# Ocrelizumab: From Multiple Sclerosis to HER2 Positive Breast Carcinoma

## One-Sentence Summary

Ocrelizumab is an anti-CD20 monoclonal antibody; based on the mechanistic notes accompanying this evidence pack, its established use is in multiple sclerosis (B-cell depletion), though this is not confirmed in the formal indication/MOA fields (flagged as a data gap). The TxGNN model predicts possible efficacy in **HER2 Positive Breast Carcinoma**, but this prediction is currently supported by **0 clinical trials** and **0 publications** — it is a pure graph-relation signal with no corroborating mechanistic or clinical evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Multiple Sclerosis (referenced only in the rationale narrative; not present in the drug's formal `original_indications`/`original_moa` fields — see data gap DG002) |
| Predicted New Indication | HER2 Positive Breast Carcinoma |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the drug-level field (flagged as data gap DG002, High severity). Based on the rationale notes embedded in this evidence pack, ocrelizumab is an anti-CD20 monoclonal antibody whose known effect is depletion of CD20-positive B cells, with its approved use referenced as multiple sclerosis.

HER2-positive breast carcinoma is driven by overexpression/amplification of the HER2/neu receptor tyrosine kinase — a pathway with no established interaction with CD20+ B-cell depletion. While tumor-infiltrating B cells are present in some breast cancer tissue, there is no evidence that depleting them reverses HER2-driven tumor growth.

The high TxGNN score (99.89%) most likely reflects a graph-topology association within the knowledge graph rather than a biologically interpretable mechanism. The same pattern repeats across all five predicted indications in this pack (HER2+, normal breast-like, PR+, luminal A/B, PR− breast cancer) — all are breast cancer molecular subtypes with no known CD20/B-cell-depletion linkage, and all are rated L5 with a Hold recommendation. Rank 4 does carry 19 attached literature records, but on inspection these are keyword mismatches (B-cell developmental biology, hepatitis B vaccines, HLA-B allele typing) unrelated to either ocrelizumab or breast cancer, and should be treated as noise rather than supporting evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Ocrelizumab is not currently marketed in New Zealand (0 authorizations on file); no product license records are available for this evidence pack.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Key warnings, contraindications, and DDI data are all unavailable in this pack — TFDA/Medsafe package insert data is flagged as a Blocking data gap, DG001.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by TxGNN's graph score (L5) — zero clinical trials, zero relevant literature, and no plausible mechanistic link between CD20+ B-cell depletion and HER2-driven breast cancer. The drug is also not marketed in New Zealand, and core safety data is blocked by a missing package insert.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently Blocking (DG001)
- Confirmed mechanism of action and original approved indication — currently High severity gap (DG002)
- Preclinical/mechanistic studies establishing a biological pathway between B-cell depletion and HER2 signaling
- Any real-world clinical or case-level evidence in breast cancer populations before advancing past S0
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


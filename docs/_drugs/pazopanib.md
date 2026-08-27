---
layout: default
title: Pazopanib
parent: 僅模型預測 (L5)
nav_order: 268
evidence_level: L5
indication_count: 10
---

# Pazopanib
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

# Pazopanib: From Undocumented Original Indication to Renal Cell Carcinoma Associated with Xp11.2 Translocations/TFE3 Gene Fusions

## One-Sentence Summary

Pazopanib's original approved indication is not recorded in the current evidence pack (no Taiwan/NZ license history on file). The TxGNN model predicts it may be effective for **renal cell carcinoma associated with Xp11.2 translocations/TFE3 gene fusions**, but this is currently supported by **0 clinical trials** and **0 publications** — the prediction stands entirely on the model score.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented (no licenses on file; `original_indications` field empty) |
| Predicted New Indication | Renal cell carcinoma associated with Xp11.2 translocations/TFE3 gene fusions |
| TxGNN Prediction Score | 99.63% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (flagged as a High-severity data gap — DG002). However, the repurposing rationale attached to this prediction notes that pazopanib's known targets, VEGFR/PDGFR, are biologically relevant to this tumour type: TFE3 fusion-driven RCC commonly shows high VEGF expression, giving a plausible pathway-level rationale for anti-angiogenic activity.

That said, this link is described in the source rationale itself as theoretical only. There is no clinical trial or literature evidence — for this specific, rare RCC subtype — to substantiate the mechanistic hypothesis. The original indication being undocumented in this pack also means no direct disease-similarity comparison (e.g., to other RCC subtypes where pazopanib has established use) can be made from the available data.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## New Zealand Market Information

Not marketed — 0 authorizations on file for pazopanib in the current dataset.

## Cytotoxicity

Pazopanib is an antineoplastic agent (all predicted and referenced indications in this evidence pack are oncologic). Literature entries elsewhere in this pack consistently describe it as a multi-target tyrosine kinase inhibitor.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (multi-target tyrosine kinase inhibitor; VEGFR/PDGFR pathway, per rationale and literature evidence in this pack) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high (99.63%), but this specific indication — an ultra-rare, molecularly defined RCC subtype — has zero clinical trials or literature support (Evidence Level L5: model prediction only). Blocking data gap DG001 (TFDA/NZ warnings and contraindications) also prevents even a preliminary safety assessment.

**To proceed, the following is needed:**
- TFDA package insert warnings/contraindications (DG001, blocking)
- Confirmed mechanism of action via DrugBank API (DG002)
- Disease-specific clinical evidence (trials, case reports, or series) for Xp11.2 translocation/TFE3 fusion-associated RCC
- Consider prioritizing other candidates in this same evidence pack with materially stronger support before advancing this one — notably **liposarcoma** (9 trials incl. multiple Phase 2, 20 publications) and **dermatofibrosarcoma protuberans** (4 trials incl. a dedicated Phase 2, 14 publications)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


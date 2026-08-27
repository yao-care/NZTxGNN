---
layout: default
title: Pirfenidone
parent: 僅模型預測 (L5)
nav_order: 279
evidence_level: L5
indication_count: 10
---

# Pirfenidone
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

# Pirfenidone: From Idiopathic Pulmonary Fibrosis to Extracutaneous Mastocytoma

## One-Sentence Summary

Pirfenidone is an oral, non-cytotoxic antifibrotic agent historically used for idiopathic pulmonary fibrosis (IPF), based on incidental mentions in the underlying literature evidence. TxGNN's top-ranked prediction is **Extracutaneous Mastocytoma**, with a **99.71%** prediction score, but this candidate currently has **0 clinical trials** and **0 publications** — it is a pure network-model output with no independent supporting evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Idiopathic Pulmonary Fibrosis (IPF) — inferred from literature abstract (PMID 29702057); no formal Taiwan/NZ license record exists |
| Predicted New Indication | Extracutaneous Mastocytoma |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L5 |
| New Zealand Market Status | 未上市 (Not Marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data from DrugBank is not available for this candidate (data gap). However, associated literature in the evidence pack describes pirfenidone as "a broad-spectrum, noncytotoxic, oral antifibrotic agent" that inhibits transforming growth factor-β1 (TGF-β1), platelet-derived growth factor (PDGF), epidermal growth factor (EGF), and fibroblast growth factor (FGF) signaling, thereby reducing fibroblast proliferation and collagen synthesis. This antifibrotic activity underlies its established use in IPF.

Extracutaneous mastocytoma is a rare mast cell neoplasm with a pathophysiology centered on mast cell proliferation and KIT signaling rather than fibrotic tissue remodeling. No direct mechanistic literature links pirfenidone's antifibrotic pathway to mast cell tumor biology — the repurposing rationale recorded for this candidate explicitly states there is no mechanistic publication support, only the TxGNN network prediction score (0.997).

Because the connection between the original indication (a fibrotic lung disease) and the predicted indication (a mast cell neoplasm) is not mechanistically substantiated in the current evidence base, this prediction should be treated as an early-stage hypothesis generated purely from knowledge-graph relationships, not as a pharmacologically grounded candidate.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Pirfenidone is currently **not marketed** in New Zealand, with **0** Medsafe authorizations on record. No product/dosage form information is available.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This candidate sits at Evidence Level L5 / decision stage S0 — a model prediction with no clinical trial or literature support and no mechanistic rationale beyond the TxGNN score. There is insufficient basis to advance this indication.

**To proceed, the following is needed:**
- Confirmed DrugBank/TFDA-equivalent mechanism of action (MOA) data
- Preclinical or mechanistic studies linking antifibrotic activity to mast cell tumor biology
- At least early-phase clinical or case-level evidence for extracutaneous mastocytoma
- TFDA/Medsafe package insert data (warnings, contraindications, DDI) — currently a Blocking data gap per the evidence pack
- New Zealand regulatory/market entry pathway assessment, given zero current authorizations

---

**Note on other TxGNN predictions for pirfenidone:** Among the 10 candidates evaluated, only **fibroblastic neoplasm** (rank 9, score 99.23%) progressed to decision stage S1 with actual literature support (6 publications, Evidence Level L4). That literature shows pirfenidone inhibiting TGF-β1-driven fibroblast/myofibroblast activity in Dupuytren's disease models, but also includes two adverse-event case reports (undifferentiated pleomorphic sarcoma; eruptive dermatofibromas) observed during pirfenidone use — findings that run counter to the antifibrotic hypothesis and warrant caution. If a repurposing candidate is to be prioritized from this drug's prediction set, fibroblastic neoplasm — not extracutaneous mastocytoma — is the one with an actual evidence trail, and it also carries a Hold recommendation pending resolution of the conflicting safety signal.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


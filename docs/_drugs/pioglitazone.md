---
layout: default
title: Pioglitazone
parent: 僅模型預測 (L5)
nav_order: 278
evidence_level: L5
indication_count: 9
---

# Pioglitazone
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

# Pioglitazone: From Type 2 Diabetes to Opsismodysplasia

## One-Sentence Summary

Pioglitazone is a thiazolidinedione (TZD)-class PPAR-γ agonist and insulin sensitizer, established for treating type 2 diabetes mellitus. The TxGNN model's top-ranked prediction is **Opsismodysplasia**, a rare skeletal dysplasia, with a **99.59% prediction score** but **0 clinical trials** and **0 publications** supporting this direction — the accompanying mechanistic review explicitly found no known biological link between pioglitazone's pathway and this disease.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Type 2 diabetes mellitus (established pharmacological use; not captured in evidence pack fields — Data Gap DG002) |
| Predicted New Indication | Opsismodysplasia |
| TxGNN Prediction Score | 99.59% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for pioglitazone is not available in this evidence pack (Data Gap DG002). Based on established pharmacological knowledge, pioglitazone is a thiazolidinedione-class PPAR-γ agonist that improves insulin sensitivity, and its efficacy in type 2 diabetes is well documented.

Opsismodysplasia, however, is a rare skeletal dysplasia caused by mutations in *INPPL1* (SHIP2), affecting skeletal development. The evidence pack's own mechanistic review states explicitly that there is **no known biological relationship** between pioglitazone's PPAR-γ/insulin-sensitizing pathway and the *INPPL1*-driven pathophysiology of this disease.

The high TxGNN score (99.59%) reflects similarity within the model's graph embedding space rather than a validated mechanistic or clinical association. With zero supporting trials or literature, this candidate should be treated as a hypothesis-generating signal only, not a mechanistically grounded repurposing opportunity.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Pioglitazone currently has no marketing authorization on record in New Zealand (0 licenses; market status: not marketed).

---

## Safety Considerations

Please refer to the package insert for safety information. Note: the TFDA/regulatory package insert (warnings and contraindications) is flagged as a **Blocking** data gap (DG001) in this evidence pack, meaning a formal safety initial evaluation (S1) cannot currently proceed.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked predicted indication (Opsismodysplasia) has no supporting clinical trials or literature and an explicitly refuted mechanistic link, placing it at evidence level L5 (model prediction only). Separately, a blocking data gap (DG001 — missing TFDA warnings/contraindications) prevents any safety pre-assessment regardless of efficacy signal strength.

**To proceed, the following is needed:**
- TFDA/regulatory package insert data (warnings, contraindications) — resolves DG001 (Blocking)
- Confirmed mechanism-of-action documentation — resolves DG002
- Disease-specific preclinical or mechanistic evidence directly linking PPAR-γ agonism to opsismodysplasia pathophysiology
- If pursuing alternative candidates from this batch (e.g., lipodystrophy-spectrum diseases, where PPAR-γ's role in adipocyte differentiation offers a more plausible rationale), targeted literature search and expert mechanistic review before advancing past S0
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


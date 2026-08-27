---
layout: default
title: Trametinib
parent: 僅模型預測 (L5)
nav_order: 346
evidence_level: L5
indication_count: 10
---

# Trametinib
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

# Trametinib: From BRAF-Mutant Melanoma to Choroideremia

## One-Sentence Summary

Trametinib is a MEK inhibitor whose established oncology use (reflected throughout this evidence pack's trial and literature data) is in BRAF V600-mutant melanoma, typically combined with a BRAF inhibitor such as dabrafenib. The TxGNN model's top-ranked prediction for this drug is **Choroideremia**, but this direction is currently supported by **0 clinical trials** and **0 publications** — it is a pure model association with no mechanistic or empirical backing found in this pack.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in this evidence pack's regulatory fields (`original_indications` empty, `original_moa` flagged as a data gap); trial/literature evidence throughout the pack consistently associates trametinib with BRAF V600-mutant melanoma, usually as part of a dabrafenib + trametinib regimen |
| Predicted New Indication | Choroideremia |
| TxGNN Prediction Score | 99.31% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is marked as a data gap in this evidence pack. Based on the mechanistic notes attached to other predicted indications in this batch, trametinib is an allosteric MEK1/2 inhibitor acting on the MAPK/ERK signalling pathway, which underlies its established efficacy in BRAF V600-mutant melanoma.

Choroideremia, however, is an inherited retinal degeneration caused by loss-of-function mutations in the *CHM* gene (REP1 protein deficiency) — a pathway with no known connection to MAPK/ERK signalling. The evidence pack's own repurposing rationale for this indication states there is no known pathological link between the two, and explicitly flags this candidate as a likely false positive arising purely from graph-embedding similarity in the TxGNN model.

In short: the mechanistic story that supports trametinib in melanoma does not extend to choroideremia. This prediction should be treated as a model artifact rather than a biologically grounded hypothesis unless independent mechanistic or experimental data emerges.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

No authorizations on record — Trametinib is not marketed in New Zealand (0 licenses in the tracked database).

---

## Cytotoxicity

*Trametinib is an antineoplastic agent (MEK inhibitor used in oncology), so this section applies.*

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (MEK inhibitor; non-cytotoxic mechanism) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

Please refer to the package insert for safety information. (Note: TFDA package insert warnings/contraindications are recorded as a **Blocking** data gap — DG001 — which prevents a complete initial safety assessment for this drug.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (choroideremia) has no clinical trial or literature support and lacks a plausible mechanistic link to trametinib's known MAPK/ERK-based pharmacology — the evidence pack itself flags it as a probable false positive.

**To proceed, the following is needed:**
- TFDA/package insert safety data (warnings, contraindications) to close data gap DG001, which is currently blocking any safety evaluation for this drug
- Independent mechanistic or preclinical evidence linking MEK inhibition to *CHM*/REP1-related retinal degeneration before this specific indication is reconsidered
- Confirmed DrugBank MOA and original-indication data to close data gap DG002
- Consider redirecting evaluation effort toward the more evidence-backed candidates in this same prediction batch — e.g., BRAF V600-mutant non-cutaneous/acral melanoma subtypes (L2–L3 evidence, "Research Question" stage), which have real supporting trials and a coherent mechanistic rationale, unlike choroideremia
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


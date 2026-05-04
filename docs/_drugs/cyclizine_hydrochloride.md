---
layout: default
title: Cyclizine Hydrochloride
parent: 僅模型預測 (L5)
nav_order: 18
evidence_level: L5
indication_count: 0
---

# Cyclizine Hydrochloride
{: .fs-9 }

證據等級: **L5** | 預測適應症: **0** 個
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

# Cyclizine Hydrochloride: Pending Evaluation — No TxGNN Predictions Available

## One-Sentence Summary

Cyclizine hydrochloride is a first-generation piperazine antihistamine (H1 receptor antagonist) primarily used in clinical practice as an antiemetic and for motion sickness.
The current evidence pack contains **no TxGNN repurposing predictions** for this compound, and the drug is **not registered in Taiwan**.
A repurposing evaluation cannot be completed until model predictions and core safety data are obtained.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Motion sickness, nausea and vomiting (pharmacological class basis; not found in Taiwan registry) |
| Predicted New Indication | None (no TxGNN predictions available) |
| TxGNN Prediction Score | N/A |
| Evidence Level | L5 — Model prediction only (no predictions generated) |
| Taiwan Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why Evaluation Cannot Proceed

Currently, no TxGNN predictions are available for cyclizine hydrochloride, so no repurposing direction can be assessed or mechanistically justified at this time.

For context, cyclizine hydrochloride belongs to the piperazine class of first-generation antihistamines. It exerts its antiemetic effect primarily through H1 receptor antagonism in the central nervous system, particularly at the vomiting center (medulla oblongata) and vestibular pathways. It also has mild anticholinergic properties, which contribute to its efficacy in motion sickness.

Detailed mechanism of action data was not available from the evidence pack. Before a repurposing hypothesis can be constructed — for example, exploring whether its CNS H1/muscarinic antagonism could be relevant to vertigo, hyperemesis gravidarum, or other histamine-mediated conditions — TxGNN model predictions and confirmed MOA data must first be retrieved.

---

## Safety Considerations

Please refer to the package insert for safety information.

> No Taiwan FDA warnings, contraindications, or drug interaction data were available in this evidence pack. Overseas regulatory sources (FDA, EMA, or UK BNF) should be consulted as interim references before any clinical or research use.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The evidence pack contains no TxGNN predictions, no Taiwan regulatory data, and no safety information, making any repurposing recommendation premature and unsupported.

**To proceed, the following is needed:**

- **Run TxGNN model predictions** for cyclizine hydrochloride to identify candidate repurposing indications
- **Retrieve MOA data** from DrugBank API (query log shows a successful hit — results should be extracted and incorporated)
- **Obtain Taiwan FDA package insert** warnings and contraindications (query log shows a successful hit — PDF should be parsed and data populated)
- **Cross-reference overseas regulatory filings** (FDA Orange Book, EMA EPAR, or UK SmPC) to supplement the zero-license Taiwan baseline
- **Re-run evidence pack pipeline** (Phase 2 → Phase 5) after the above data gaps are resolved to enable a full L1–L5 evidence assessment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


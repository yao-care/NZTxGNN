---
layout: default
title: Insulin Glargine
parent: 僅模型預測 (L5)
nav_order: 175
evidence_level: L5
indication_count: 10
---

# Insulin Glargine
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

# Insulin Glargine: From Diabetes Mellitus to Autoimmune Oophoritis

## One-Sentence Summary

Insulin glargine is a long-acting basal insulin analogue originally developed for glycemic control in Type 1 and Type 2 diabetes mellitus. The TxGNN model's top-ranked prediction for this drug is **Autoimmune Oophoritis** (score 99.88%), but this candidate is currently supported by **0 clinical trials** and **0 publications** — the evidence pack's own rationale flags this as a likely network-embedding artifact rather than a genuine pharmacological signal.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Diabetes mellitus (Type 1 and Type 2) — general pharmacological knowledge; not present in the New Zealand regulatory dataset (drug is unmarketed there) |
| Predicted New Indication | Autoimmune Oophoritis |
| TxGNN Prediction Score | 99.88% |
| Evidence Level | L5 |
| New Zealand Market Status | 未上市 (Not marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the evidence pack (data gap DG002, severity High). Based on general pharmacological knowledge, insulin glargine acts as an insulin receptor agonist, promoting peripheral glucose uptake and suppressing hepatic gluconeogenesis; its efficacy in diabetes mellitus is well established.

The mechanistic rationale supplied with this candidate is explicit about its weakness: autoimmune oophoritis is a recognized component of autoimmune polyglandular syndrome (APS), which frequently co-occurs with Type 1 diabetes. The TxGNN score most likely reflects this shared comorbidity network in the knowledge graph — a "diabetes–autoimmune polyglandular syndrome–oophoritis" path — rather than any direct effect of insulin on ovarian autoimmune destruction.

There is no known mechanism by which exogenous insulin replacement would modulate autoimmune attack on ovarian tissue. Insulin's role in APS-associated diabetes is purely metabolic/supportive for the diabetes component, not immunomodulatory for the oophoritis component. This mechanistic gap, combined with the complete absence of clinical or literature evidence, means the prediction should be treated as a hypothesis-generating signal only, not a repurposing lead.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Insulin glargine currently holds **0 authorizations** in the New Zealand regulatory dataset (market status: 未上市 / Not marketed). No license records are available to summarize.

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and drug interaction data are all marked as data gaps in this evidence pack; TFDA package insert retrieval — DG001 — is flagged as **Blocking** for a full safety assessment.)

---

## Other Predicted Indications Screened

This evidence pack scored 10 candidate indications for insulin glargine. For transparency, the top TxGNN-ranked candidate (above) is not the most clinically credible one in the set — a screening summary is provided below:

| Rank | Disease | Score | Evidence Level | Decision | Note |
|------|---------|-------|------|------|------|
| 1 | Autoimmune oophoritis | 99.88% | L5 | Hold | Likely comorbidity-network artifact (APS/T1D shared graph path) |
| 2 | Thiamine-responsive dysfunction syndrome (TRMA) | 99.61% | L4 | Research Question | Insulin treats the diabetes component of TRMA; supportive, not disease-modifying; no direct evidence |
| 3 | Focal stiff limb syndrome | 99.60% | L5 | Hold | Anti-GAD65/T1D comorbidity artifact, not a treatment mechanism |
| 4 | Classic stiff person syndrome | 99.60% | L5 | Hold | Same anti-GAD65 comorbidity artifact; standard SPS therapy is GABAergic/immunomodulatory, unrelated to insulin |
| 5 | Opsismodysplasia | 99.59% | L5 | Hold | INPPL1/SHIP2 pathway graph-embedding link only; no treatment mechanism |
| **6** | **Pancreatic agenesis** | 99.43% | **L3** | **Proceed with Guardrails** | **Most credible candidate**: insulin replacement is already standard supportive care for β-cell loss in this condition (6 supporting reviews/case reports); however this is an established clinical extension, not a novel repurposing hypothesis |
| 7 | Drug-induced localized lipodystrophy | 99.42% | L4 | Hold | **Reversed causality**: insulin injection is a known *cause* of this condition, not a treatment |
| 8 | Centrifugal lipodystrophy | 99.39% | L5 | Hold | Same lipodystrophy cluster; likely reversed-causality/graph-clustering artifact |
| 9 | Pressure-induced localized lipoatrophy | 99.38% | L4 | Hold | Same reversed-causality concern as #7 |
| 10 | Idiopathic localized lipodystrophy | 99.34% | L5 | Hold | Score clustering (99.3–99.4%) across #7–10 indicates batch graph-embedding similarity, not independent pharmacological evidence |

**Key takeaway:** Ranks 7–10 (the lipodystrophy cluster) and ranks 1, 3, 4 (the autoimmune/APS cluster) should be treated with particular caution — several rationales explicitly flag possible **direction-reversed** signals (insulin as *cause* rather than *treatment*) or comorbidity-network artifacts rather than true drug–disease relationships. Rank 6 (pancreatic agenesis) is the only candidate with real literature support and a direct, non-reversed mechanism, though it represents an already-established clinical practice rather than a new discovery.

---

## Conclusion and Next Steps

**Decision: Hold** (for the top-ranked candidate, Autoimmune Oophoritis)

**Rationale:**
This candidate has no clinical trial or literature support (L5), and its own mechanistic rationale indicates the TxGNN score likely arises from a comorbidity-network artifact (shared APS/T1D graph neighbors) rather than a genuine drug–disease relationship. There is no established biological pathway for insulin to treat autoimmune ovarian destruction.

**To proceed, the following is needed:**
- TFDA package insert data (DG001, Blocking) — required before any safety evaluation can proceed for insulin glargine on any indication
- Confirmed mechanism of action data (DG002)
- Preclinical/mechanistic evidence specifically linking insulin signaling to ovarian autoimmune pathology, independent of the shared T1D/APS comorbidity confound
- If pursuing a more credible near-term lead instead, consider advancing **pancreatic agenesis** (rank 6, L3, Proceed with Guardrails) rather than autoimmune oophoritis, while recognizing it reflects existing clinical practice rather than novel repurposing
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Ziprasidone
parent: 僅模型預測 (L5)
nav_order: 365
evidence_level: L5
indication_count: 10
---

# Ziprasidone
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

# Ziprasidone: From Schizophrenia/Bipolar Disorder to Trichotillomania

## One-Sentence Summary

Ziprasidone is an atypical (second-generation) antipsychotic internationally approved for schizophrenia and bipolar I disorder. The TxGNN model's **top-ranked** prediction for this drug is **Trichotillomania (hair-pulling disorder)**, with a prediction score of **99.83%**, but currently **zero clinical trials and zero publications** support this specific link — the evidence base is model-prediction-only (L5), and the recommended decision is **Hold**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Schizophrenia / Bipolar I Disorder (acute manic or mixed episodes) — internationally approved indications *(not found in this Evidence Pack; New Zealand licensing data is empty because the product is not marketed here)* |
| Predicted New Indication | Trichotillomania |
| TxGNN Prediction Score | 99.83% |
| Evidence Level | L5 (model prediction only, no supporting studies) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for ziprasidone is not available in this Evidence Pack (flagged as a High-severity data gap, DG002). Based on general pharmacological knowledge, ziprasidone is an atypical antipsychotic combining D2 dopamine receptor antagonism with 5-HT2A antagonism and 5-HT1A partial agonism — a serotonin–dopamine modulation profile that underlies its established efficacy in schizophrenia and bipolar disorder.

The TxGNN model links this serotonin–dopamine mechanism to trichotillomania on the theoretical basis that impulse-control disorders may involve similar dopaminergic/serotonergic dysregulation. However, the model's own rationale explicitly flags this as a **weak and unvalidated mechanistic link**: no clinical trial or published literature currently connects ziprasidone specifically to trichotillomania. This places the prediction at the lowest confidence tier (L5) — a graph-based inference rather than an evidence-backed hypothesis.

**Important context:** Among ziprasidone's other TxGNN-predicted indications in this Evidence Pack, "major affective disorder" (rank 3, score 99.66%) is far better supported — 29 clinical trials (including multiple completed Phase 2/3/4 RCTs) and 20 publications, rated **L1 / Proceed with Guardrails**. Several other ranked predictions (e.g., hydranencephaly, congenital disorder of glycosylation, X-linked myopia variants) appear to be low-confidence knowledge-graph noise with no plausible mechanistic connection to an antipsychotic's pharmacology. Decision-makers should weigh the full prediction list rather than relying on TxGNN rank alone.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for ziprasidone in trichotillomania.

---

## Literature Evidence

Currently no related literature available for ziprasidone in trichotillomania.

---

## New Zealand Market Information

Ziprasidone is currently **not marketed** in New Zealand — there are no Medsafe product authorizations on record (total_licenses = 0), so no product table can be produced.

---

## Safety Considerations

Please refer to the package insert for safety information. *(Key warnings, contraindications, and drug–drug interaction data for ziprasidone were not available in this Evidence Pack; Medsafe/TFDA package-insert retrieval is flagged as a Blocking data gap, DG001.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top TxGNN-ranked prediction (trichotillomania) has no supporting clinical trials or literature and rests solely on a theoretical, unvalidated mechanistic hypothesis — insufficient to justify advancing this specific indication at this time.

**To proceed, the following is needed:**
- Preclinical or case-level evidence specifically linking ziprasidone to trichotillomania or related impulse-control disorders
- Resolution of the Blocking data gap: Medsafe/TFDA approved package insert (warnings and contraindications)
- Resolution of the High-severity data gap: confirmed mechanism-of-action documentation from DrugBank
- Given the disparity in evidence strength, consider prioritizing evaluation of **major affective disorder** (L1, 29 trials, 20 publications) as a more actionable repurposing candidate for this drug ahead of trichotillomania
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


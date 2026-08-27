---
layout: default
title: Finasteride
parent: 僅模型預測 (L5)
nav_order: 152
evidence_level: L5
indication_count: 6
---

# Finasteride
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
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

Using the report template supplied in the system prompt to synthesize this Evidence Pack. Before drafting, a few notes on how I resolved ambiguities in the data (flagging rather than guessing, per the no-fabrication rule):

- `drug.original_indications` is empty and `original_moa` is `[Data Gap]` (see DG002). The evidence pack's own `repurposing_rationale.mechanistic_link` fields for ranks 2, 5, and 6 independently state that Finasteride works via 5α-reductase/DHT inhibition and is used clinically for hair loss — so I've grounded the "original indication/MOA" context in those in-pack statements rather than outside knowledge, while flagging that no formally sourced TFDA/DrugBank record exists in this pack.
- Per the template, all trial/literature tables use `predicted_indications[0]` (the top-ranked candidate, Ambras type hypertrichosis), which has zero trials and zero literature — this drives most of the report's caution.
- `taiwan_regulatory.licenses` is empty, so the NZ market table is omitted in favor of a one-line statement.
- Finasteride is not an antineoplastic agent, so the Cytotoxicity section is omitted entirely.
- All safety fields are `[Data Gap]`, so the fallback safety sentence is used.

---

# Finasteride: From Androgenetic Alopecia to Ambras Type Hypertrichosis Universalis Congenita

## One-Sentence Summary

> Finasteride is a 5α-reductase inhibitor whose established clinical use — referenced within this evidence pack's own mechanistic notes — is androgenetic alopecia (male pattern hair loss); no independently sourced TFDA/DrugBank record of its original indication is present in this pack.
> The TxGNN model's top-ranked prediction is **Ambras type hypertrichosis universalis congenita**, a rare congenital chromosomal-rearrangement disorder, with a prediction score of **99.99%**.
> This prediction is currently supported by **zero clinical trials** and **zero publications**, and the evidence pack's own rationale flags it as a likely embedding-space artifact rather than a genuine mechanistic signal.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Androgenetic alopecia (male pattern hair loss) — inferred from in-pack mechanistic notes; not formally sourced (Data Gap DG001/DG002) |
| Predicted New Indication | Ambras type hypertrichosis universalis congenita |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L5 (model prediction only, no supporting studies) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism-of-action data from an authoritative source (DrugBank/TFDA) is not available in this evidence pack (Data Gap DG002). Based on the mechanistic descriptions embedded in this pack's own rationale notes, Finasteride inhibits 5α-reductase, reducing conversion of testosterone to dihydrotestosterone (DHT), and is clinically established for androgen-dependent hair loss (androgenetic alopecia).

**This top-ranked prediction does not fit that mechanism well.** Ambras type hypertrichosis universalis congenita is a rare congenital disorder linked to chromosomal rearrangement, producing generalized excess hair growth through a pathway that is not androgen/DHT-driven. Finasteride has no known molecular target in this disease process. The evidence pack's own rationale is explicit on this point: the high TxGNN score is attributed to semantic proximity between "hair/follicle" concepts in the model's embedding space rather than to any real mechanistic or pharmacological linkage — i.e., a likely false-positive association.

It is also worth noting that Finasteride's true pharmacological direction runs counter to hypertrichosis-type indications generally: it is used to *reduce* androgen-driven hair growth (or promote scalp hair retention), not to *induce* hair growth in non-androgen-dependent contexts. The next-ranked candidate in this pack, "hypertrichosis (disease)" (rank 2, score 99.99%), shares this same directional mismatch, and even the drug's lower-ranked candidates (ranks 4–6, all L5) show no trial or literature support. Across all six candidates in this pack, TxGNN consistently returned a "Hold" recommendation, reinforcing that no candidate here currently rises above hypothesis-generation level.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Finasteride is not currently marketed in New Zealand under this evidence pack's regulatory query (0 authorizations on file). No product license records are available to summarize.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: retrieval of TFDA-sourced warnings and contraindications is flagged as a **Blocking** data gap (DG001) in this evidence pack — it explicitly prevents this candidate from progressing past the S1 safety pre-screen stage until resolved.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked predicted indication (Ambras type hypertrichosis universalis congenita) has no supporting clinical trials or literature, an L5 evidence level, and the evidence pack's own mechanistic analysis identifies it as a probable model artifact rather than a biologically plausible repurposing signal. All six candidates in this pack independently resolved to "Hold," and a Blocking-severity safety data gap (missing TFDA warnings/contraindications) further prevents any candidate from advancing to safety pre-screening.

**To proceed, the following is needed:**
- TFDA package insert (warnings, contraindications) — required to clear the Blocking gap (DG001) before any S1 safety evaluation can begin
- Authoritative mechanism-of-action and original-indication documentation from DrugBank (DG002), to replace the currently inferred/unsourced MOA context
- If pursuing rank 2 ("hypertrichosis (disease)") instead of rank 1, a manual review of the single low-relevance Phase 4 trial (NCT04293822, graded "C" relevance — does not actually test Finasteride) and the four Tier-3 review-level publications, none of which involve Finasteride directly
- Expert pharmacological review to confirm or refute the embedding-artifact hypothesis before allocating further evaluation resources to this drug-indication pair
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


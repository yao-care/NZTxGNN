---
layout: default
title: Leflunomide
parent: 僅模型預測 (L5)
nav_order: 198
evidence_level: L5
indication_count: 2
---

# Leflunomide
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
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

# Leflunomide: From an Undocumented Original Indication to Brachydactyly-Syndactyly Syndrome

## One-Sentence Summary

Leflunomide (DrugBank DB01097) currently has no original indication or approval data on file for New Zealand, and it is not marketed there. The TxGNN model predicts a possible link to **Brachydactyly-Syndactyly Syndrome**, but this prediction is supported by **zero clinical trials** and **zero publications** — it rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in the current data set (no TFDA/regulatory license record available) |
| Predicted New Indication | Brachydactyly-Syndactyly Syndrome |
| TxGNN Prediction Score | 99.93% |
| Evidence Level | L5 (model prediction only, no supporting studies) |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available for leflunomide in this evidence pack. Based on the repurposing rationale supplied alongside the prediction, leflunomide is understood to be a DHODH (dihydroorotate dehydrogenase) inhibitor that blocks de novo pyrimidine synthesis, a mechanism associated with immune modulation (e.g., in rheumatoid arthritis).

Brachydactyly-syndactyly syndrome is a congenital limb malformation disorder rooted in skeletal morphogenesis defects. There is no established biological connection between pyrimidine-synthesis inhibition/immune modulation and congenital skeletal patterning genes. The high TxGNN score (0.9993) most likely reflects **topological proximity within the knowledge graph** — for example, clustering with other rare-disease nodes — rather than a genuine mechanistic relationship.

Given that the drug is not marketed in New Zealand and both the original indication and MOA data are currently missing, there is no independent clinical or regulatory basis to corroborate this prediction. This should be treated as a candidate for further mechanistic screening, not as an actionable repurposing signal.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## New Zealand Market Information

Leflunomide is not currently marketed in New Zealand, and no product authorizations are on file (0 licenses).

## Safety Considerations

Please refer to the package insert for safety information.

## Other Predicted Indication (Secondary Candidate)

A second candidate was also flagged by the model, with the same evidence gaps and outcome:

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Clinical Trials | Literature | Decision |
|------|----------------------|-------------|-----------------|------------------|------------|----------|
| 2 | Colobomatous Microphthalmia-Rhizomelic Dysplasia Syndrome | 99.93% | L5 | 0 | 0 | Hold |

Like the primary candidate, this is a congenital developmental malformation syndrome with no known biological link to leflunomide's pyrimidine-synthesis/immune-modulation mechanism, and is likely a graph-topology artifact rather than a true signal.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Both predicted indications are supported only by TxGNN model scores (L5), with no clinical trials, no literature, no confirmed mechanistic rationale, and no market or safety data available — including a blocking gap in TFDA/package-insert safety data (warnings and contraindications). There is no basis to advance either candidate at this time.

**To proceed, the following is needed:**
- TFDA/regulatory package insert data (warnings, contraindications) — currently a blocking data gap
- Confirmed mechanism of action (MOA) data from DrugBank or another authoritative source
- Documented original indication(s) for leflunomide to establish a baseline for similarity comparison
- Preclinical or mechanistic studies specifically linking DHODH inhibition/pyrimidine synthesis to skeletal/craniofacial developmental pathways, to rule out a knowledge-graph false positive
- Any real-world case reports, registries, or off-label use data for either predicted indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


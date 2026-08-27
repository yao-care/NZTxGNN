---
layout: default
title: Sulfasalazine
parent: 僅模型預測 (L5)
nav_order: 326
evidence_level: L5
indication_count: 10
---

# Sulfasalazine
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

# Sulfasalazine: From Rheumatoid Arthritis/Inflammatory Bowel Disease to Brachydactyly-Syndactyly Syndrome

## One-Sentence Summary

Sulfasalazine is a long-established anti-inflammatory/immunomodulatory agent (NF-κB inhibitor combining 5-ASA and sulfapyridine), best known for rheumatoid arthritis and inflammatory bowel disease, though its formal original-indication record is not available in this evidence pack.
The TxGNN model's top-ranked prediction is **Brachydactyly-Syndactyly Syndrome**, a rare congenital skeletal disorder, but this direction is currently supported by **0 clinical trials** and **0 publications** — it is a pure model output with no mechanistic or empirical backing.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in structured NZ regulatory data (drug not marketed); per known pharmacology, sulfasalazine is used for rheumatoid arthritis and inflammatory bowel disease/ankylosing spondylitis |
| Predicted New Indication | Brachydactyly-Syndactyly Syndrome |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (blocking data gap). Based on known information, sulfasalazine is a DMARD-class agent whose antineoplastic/anti-inflammatory efficacy has been demonstrated in rheumatologic and gastrointestinal inflammatory conditions.

Brachydactyly-syndactyly syndrome is a rare congenital skeletal developmental disorder with no established inflammatory or immune-mediated pathology. According to the evidence pack's own repurposing rationale, there is **no known mechanistic relationship** between sulfasalazine's anti-inflammatory/immunomodulatory activity and this disease's underlying (developmental/genetic) biology.

This top-ranked prediction should be treated as a pure network-embedding signal (TxGNN score, rank 826 among all disease nodes) rather than a biologically grounded hypothesis. It has not been validated by any clinical trial or literature search — both returned zero results.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## New Zealand Market Information

Sulfasalazine currently holds no marketing authorization in New Zealand (0 licenses on record); no product/dosage-form data is available.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked TxGNN prediction (Brachydactyly-Syndactyly Syndrome) has no supporting clinical trials, no literature, and no plausible mechanistic link — it is Evidence Level L5, model-prediction-only. There is no basis to advance this specific indication beyond hypothesis generation.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently blocking (DG001)
- Sulfasalazine mechanism of action data from DrugBank — currently high-priority gap (DG002)
- Preclinical or mechanistic studies specifically linking sulfasalazine's pharmacology to skeletal/connective-tissue developmental pathways, if this indication is to be pursued further

**Note:** This evidence pack also contains two other TxGNN predictions with materially stronger support that may warrant separate evaluation — **Osteoarthritis** (rank 5, L3, multiple preclinical studies, Research Question stage) and **Spondyloarthropathy susceptibility** (rank 8, L3, Proceed with Guardrails, supported by NAT2-pharmacogenomic and clinical-guideline context for peripheral spondyloarthritis). Given sulfasalazine's established role in inflammatory arthritis, these two candidates are more actionable repurposing directions than the top-ranked rare-disease prediction reported above.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


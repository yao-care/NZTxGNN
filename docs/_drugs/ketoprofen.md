---
layout: default
title: Ketoprofen
parent: 僅模型預測 (L5)
nav_order: 188
evidence_level: L5
indication_count: 10
---

# Ketoprofen
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

# Ketoprofen: From NSAID Anti-Inflammatory Therapy to Acromesomelic Dysplasia, Hunter-Thompson Type

## One-Sentence Summary

Ketoprofen is a non-steroidal anti-inflammatory drug (NSAID) with a non-selective COX-1/COX-2 inhibitory mechanism, classically used for pain and inflammation (e.g., arthritis, musculoskeletal pain). The TxGNN model's top-ranked prediction for a new indication — **Acromesomelic Dysplasia, Hunter-Thompson Type** — is supported by **0 clinical trials** and **0 publications**, and the model's own rationale flags it as a likely embedding-space artifact unrelated to ketoprofen's anti-inflammatory mechanism. Two lower-ranked candidates (spondyloarthropathy susceptibility and LACC1-deficient juvenile arthritis) have a more defensible class-level mechanistic rationale and warrant follow-up instead.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from local license records (drug not marketed in New Zealand). Ketoprofen is generically classified as an NSAID for pain/inflammation, per its known non-selective COX-1/COX-2 inhibitory mechanism cited in the evidence pack's rationale text. |
| Predicted New Indication | Acromesomelic Dysplasia, Hunter-Thompson Type |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L5 (model prediction only, no supporting studies) |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data for ketoprofen is not available in this evidence pack (flagged as a Data Gap). Based on the evidence pack's own rationale annotations, ketoprofen is a non-selective COX-1/COX-2 inhibitor, a mechanism grounded in classical anti-inflammatory and analgesic pharmacology, consistent with its known role as an NSAID.

For the top-ranked prediction — Acromesomelic Dysplasia, Hunter-Thompson Type — the evidence pack explicitly states there is **no mechanistic link**: this is an NPR2-gene skeletal dysplasia driven by abnormal endochondral ossification, a pathway unrelated to COX inhibition or NSAID pharmacology. The high TxGNN score is attributed to clustering with other skeletal dysplasias in the model's embedding space rather than genuine pharmacological relevance, and is corroborated by zero clinical trials and zero literature hits across all queried sources.

By contrast, two other candidates in this evidence pack carry a more plausible class-level rationale: **spondyloarthropathy, susceptibility to** (rank 8) and **juvenile arthritis due to defect in LACC1** (rank 10) are both inflammatory joint conditions for which NSAIDs, including ketoprofen, are an established symptomatic treatment class. These are marked "Research Question" (decision stage S1) rather than "Hold," and are discussed further below.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available for Acromesomelic Dysplasia, Hunter-Thompson Type.

---

## New Zealand Market Information

Ketoprofen currently holds no marketing authorization in New Zealand (0 licenses on file; market status: Not Marketed).

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and drug-interaction data are not currently available; TFDA package insert retrieval is flagged as a Blocking data gap — DG001.)

---

## Other Predicted Indications Worth Noting

Since this evidence pack covers 10 ranked predictions for ketoprofen, the two with any research signal beyond raw model score are summarized here for context:

| Rank | Disease | TxGNN Score | Evidence Level | Decision Stage | Recommendation | Notes |
|------|---------|-------------|-----------------|-----------------|-----------------|-------|
| 8 | Spondyloarthropathy, susceptibility to | 99.86% | L4 | S1 | Research Question | 1 review-type literature hit (PMID [20470931](https://pubmed.ncbi.nlm.nih.gov/20470931/)); NSAIDs are standard symptomatic therapy for this disease class, but the indication is "susceptibility," not the disease itself. |
| 10 | Juvenile arthritis due to defect in LACC1 | 99.74% | L4 | S1 | Research Question | No disease-specific literature/trials; class-level NSAID rationale only (monogenic inflammatory arthritis). |

All remaining ranks (1–7, 9) are flagged in their own rationale as lacking any inflammatory/COX-related mechanism and are recommended **Hold**.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (Acromesomelic Dysplasia, Hunter-Thompson Type) has zero clinical trials, zero literature support, and its own rationale identifies it as a likely model artifact with no plausible mechanistic connection to ketoprofen's COX-inhibitory action.

**To proceed, the following is needed:**
- TFDA package insert (warnings/contraindications) — currently a Blocking gap (DG001)
- Ketoprofen's confirmed mechanism of action (DG002)
- If pursuing this drug further, redirect research effort toward the two "Research Question" candidates (spondyloarthropathy susceptibility, LACC1-deficient juvenile arthritis) rather than the rank-1 prediction, and seek disease-specific NSAID trial/literature evidence for those instead
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


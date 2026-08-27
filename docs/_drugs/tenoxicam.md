---
layout: default
title: Tenoxicam
parent: 僅模型預測 (L5)
nav_order: 334
evidence_level: L5
indication_count: 10
---

# Tenoxicam
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

# Tenoxicam: From NSAID Musculoskeletal Pain Management to Rheumatoid Arthritis

## One-Sentence Summary

Tenoxicam is an oxicam-class NSAID with long-established use in osteoarthritis, ankylosing spondylitis, and inflammatory/postoperative pain.
The TxGNN model ranks **Rheumatoid Arthritis** as its top predicted indication, but this is not a novel repurposing signal —
it is corroborated by **1 clinical trial** and **20 publications**, including several decades-old RCTs that already demonstrate efficacy in RA specifically.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in New Zealand market data (drug unmarketed); based on established oxicam/NSAID use, tenoxicam is indicated for osteoarthritis, ankylosing spondylitis, and other inflammatory/postoperative pain conditions |
| Predicted New Indication | Rheumatoid Arthritis |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (data gap). Based on known information, tenoxicam belongs to the oxicam class of NSAIDs (related to piroxicam), acting via inhibition of COX-1/COX-2 to reduce prostaglandin-mediated inflammation and pain. This anti-inflammatory mechanism is directly applicable to rheumatoid arthritis, an inflammatory joint disease.

Notably, the literature evidence for this prediction is not exploratory extrapolation — tenoxicam has been directly studied and compared against other NSAIDs (piroxicam, aceclofenac, naproxen) specifically in RA populations since the 1980s–1990s, with several completed RCTs. This means the TxGNN model's high score for RA largely reflects an already-established, well-documented use of the drug rather than a genuinely new therapeutic hypothesis. The value of this candidate lies less in novelty and more in confirming the model correctly recovers known pharmacology.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT05508451](https://clinicaltrials.gov/study/NCT05508451) | Phase NA | Completed | 80 | Compared tenoxicam, paracetamol, and tenoxicam-paracetamol combination for postoperative pain following double-jaw surgery. Not RA-specific; supports general analgesic/anti-inflammatory profile only (relevance grade B). |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8894360](https://pubmed.ncbi.nlm.nih.gov/8894360/) | 1996 | RCT | Clinical Rheumatology | Multicentre double-blind RCT (n=292) comparing aceclofenac vs. tenoxicam in RA; both groups showed clinical improvement with comparable efficacy and safety |
| [1593574](https://pubmed.ncbi.nlm.nih.gov/1593574/) | 1992 | RCT | The Journal of Rheumatology | RCT (n=102) comparing tenoxicam 20mg OD vs. piroxicam 20mg OD in RA; no difference in efficacy or adverse event rates |
| [2695152](https://pubmed.ncbi.nlm.nih.gov/2695152/) | 1989 | RCT | The British Journal of Clinical Practice | Large double-blind parallel-group trial (n=1,328) in OA and RA comparing tenoxicam and piroxicam; tenoxicam showed slightly greater global assessment improvement |
| [2292331](https://pubmed.ncbi.nlm.nih.gov/2292331/) | 1990 | RCT | The Journal of International Medical Research | Multicentre general-practice study (n=2,963) of oral tenoxicam 20mg/day for 12 weeks in OA/RA; symptom reduction sustained through long-term follow-up |
| [1711963](https://pubmed.ncbi.nlm.nih.gov/1711963/) | 1991 | Review | Drugs | Pharmacology update: tenoxicam efficacy in RA, OA, ankylosing spondylitis and other rheumatic conditions is at least equivalent to other NSAIDs, tolerability comparable to or better than piroxicam |
| [3315620](https://pubmed.ncbi.nlm.nih.gov/3315620/) | 1987 | Review | Drugs | Preliminary review of pharmacodynamics/pharmacokinetics; once-daily 20mg tenoxicam shown effective across RA, OA, ankylosing spondylitis, gout |
| [8137596](https://pubmed.ncbi.nlm.nih.gov/8137596/) | 1994 | Review | Clinical Pharmacokinetics | Clinical pharmacokinetic review: complete oral absorption, ~99% protein binding, supports once-daily dosing in rheumatic disease |
| [3915885](https://pubmed.ncbi.nlm.nih.gov/3915885/) | 1985 | Cohort | European Journal of Rheumatology and Inflammation | Double-blind parallel trials in OA, RA and ankylosing spondylitis; tenoxicam 20mg at least as effective as piroxicam with good tolerance |
| [2512637](https://pubmed.ncbi.nlm.nih.gov/2512637/) | 1989 | Cohort | Scandinavian Journal of Rheumatology Supplement | 4-year long-term trial (n=20) of tenoxicam plus basis therapy (gold salts/D-penicillamine) in RA; sustained analgesic/anti-inflammatory benefit |
| [3915889](https://pubmed.ncbi.nlm.nih.gov/3915889/) | 1985 | Cohort | European Journal of Rheumatology and Inflammation | Open multicentric study (n=79) of rectal tenoxicam suppository (20-40mg/day) for 6 weeks in RA and arthrosis |

## New Zealand Market Information

Tenoxicam currently has no marketing authorizations in New Zealand (market status: not marketed, 0 licenses on record).

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and drug interaction data are not currently available in the evidence pack — flagged as a blocking data gap for TFDA/Medsafe label review.)

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Efficacy in RA is well-supported by multiple historical RCTs and reviews (Evidence Level L2), but the only registered clinical trial for this indication is not RA-specific, the drug is unregistered in New Zealand, and core safety/MOA data are missing — so this should advance cautiously rather than as a confirmed go.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert with warnings, contraindications, and drug interaction data (currently a Blocking data gap, DG001)
- Confirmed mechanism of action detail (High-priority data gap, DG002)
- Documentation of tenoxicam's actual approved indications in markets where it is registered, to properly characterize "original indication"
- Assessment of whether pursuing RA is worthwhile given it largely confirms known NSAID class efficacy rather than revealing a novel therapeutic use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


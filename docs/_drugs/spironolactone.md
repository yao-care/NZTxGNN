---
layout: default
title: Spironolactone
parent: 僅模型預測 (L5)
nav_order: 324
evidence_level: L5
indication_count: 2
---

# Spironolactone
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

# Spironolactone: From Edema/Hypertension to Hypotrichosis Simplex of the Scalp

## One-Sentence Summary

Spironolactone is a long-established aldosterone receptor antagonist used clinically for fluid retention, hypertension, and related conditions (specific original indication text unavailable — drug is not marketed in New Zealand). The TxGNN model predicts it may be effective for **Hypotrichosis Simplex of the Scalp**, but this prediction is currently supported by **0 clinical trials** and **0 publications** — it is a model-only signal with no corroborating real-world evidence.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available (drug not marketed in New Zealand; no license text on file) |
| Predicted New Indication | Hypotrichosis Simplex of the Scalp |
| TxGNN Prediction Score | 99.26% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this candidate. Based on known pharmacology, spironolactone is an aldosterone receptor antagonist with secondary anti-androgenic activity; this anti-androgenic effect is the basis for its established off-label use in female-pattern androgenetic alopecia (androgen-driven hair loss).

However, the predicted indication here — hypotrichosis simplex of the scalp — is a distinct, genetically driven condition (typically linked to APCDD1 mutations disrupting Wnt signaling in hair follicle development) with no known hormonal or androgen-pathway component. The second-ranked prediction, congenital hypotrichosis with milia, is similarly a congenital keratinization/follicle-development disorder (associated with HR-gene or related pathways), again without an established androgen or mineralocorticoid link.

The evidence pack's own mechanistic rationale flags this directly: the high TxGNN score most likely reflects proximity between "hypotrichosis" and "alopecia" phenotype nodes within the knowledge graph, rather than a genuine shared biological mechanism. Without any supporting trials or literature, this prediction should be treated as a hypothesis-generating artifact rather than a validated mechanistic link.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## New Zealand Market Information

Spironolactone is not currently marketed in New Zealand, and no product authorizations are on record (0 licenses).

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests solely on a TxGNN graph-similarity score (L5) with zero corroborating clinical trials or literature, and the proposed indications (genetic/congenital hypotrichosis) lack a plausible mechanistic connection to spironolactone's known antimineralocorticoid/anti-androgenic activity. Compounding this, the drug is not marketed in New Zealand and core safety data (warnings, contraindications, DDI) are unavailable, blocking any S1 safety review.

**To proceed, the following is needed:**
- TFDA/official package insert data (warnings, contraindications) — currently a blocking data gap
- Confirmed mechanism of action data from DrugBank or primary literature
- Preclinical or mechanistic evidence directly linking spironolactone's pathway to APCDD1/Wnt or HR-gene/keratinization biology, to rule out graph-proximity artifact
- Drug-drug interaction data
- Reassessment if any clinical trials or case literature for these specific indications emerge
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


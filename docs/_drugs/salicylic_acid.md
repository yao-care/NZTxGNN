---
layout: default
title: Salicylic Acid
parent: Model Prediction Only (L5)
nav_order: 419
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Salicylic Acid: predicted new indication Papillary conjunctivitis (evidence level L5). Approved use: Acne, seborrheic dermatitis, tinea infections, rheumatoid arthritis…"
monitor_inn: "SALICYLIC ACID"
predicted_indications: ["papillary conjunctivitis", "colobomatous microphthalmia-rhizomelic dysplasia syndrome", "brachydactyly-syndactyly syndrome", "brachyolmia-amelogenesis imperfecta syndrome", "rosacea conjunctivitis", "acromesomelic dysplasia, Hunter-Thompson type", "pseudoachondroplasia", "myosclerosis", "brachyolmia", "spondyloarthropathy, susceptibility to"]
---

# Salicylic Acid
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Salicylic Acid: From Topical Dermatological and Inflammatory Conditions to Papillary Conjunctivitis
{: .text-alpha }

## One-Sentence Summary

Salicylic acid is a long-established agent used mainly in skin care and for inflammatory conditions, including acne, warts, corns, seborrheic dermatitis and fungal skin infections.
The TxGNN model predicts it may be effective for **papillary conjunctivitis**, but **no clinical trials and no publications** were found to support this prediction.
It is a computational prediction only (evidence level L5).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Acne, seborrheic dermatitis, tinea infections, rheumatoid arthritis, allergic rhinitis and others (from the knowledge graph; no approved indication text is available for the New Zealand licence) |
| Predicted New Indication | Papillary conjunctivitis |
| TxGNN Prediction Score | 99.88% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Salicylic acid is generally proposed to act through inhibition of COX enzymes, which reduces prostaglandin synthesis, but no public bioactivity data were found to confirm this at a specific molecular target. It also has keratolytic activity, which explains its use on the skin.

The knowledge graph links salicylic acid to several inflammatory and allergic conditions, including allergic rhinitis, rheumatoid arthritis and synovitis. Papillary conjunctivitis is an allergic or inflammatory disease of the ocular surface, so an anti-inflammatory effect could in theory be relevant. This is speculative.

There is also a practical concern. Topical salicylic acid is irritating and is generally unsuitable for application to the eye. The high score probably reflects proximity in the knowledge graph rather than demonstrated therapeutic benefit.

Other top-ranked predictions are weaker still. Most are rare genetic skeletal or developmental syndromes (for example brachyolmia, pseudoachondroplasia and acromesomelic dysplasia) with no plausible mechanism. The most biologically plausible alternative is susceptibility to spondyloarthropathy (score 99.34%), because of the overlap with the drug's rheumatoid arthritis and synovitis indications. It is also unsupported by trials or literature, and other NSAIDs are the usual standard of care.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2574136 (cancelled) | Midwest | Powder | Not available |

## Safety Considerations

- **Drug Interactions / Pharmacology**: The data compiled for this report list one pharmacological association, with the target ASIC3 (acid-sensing ion channel 3). No interaction severity level is available. The compiled data note that no public bioactivity data confirm the proposed COX-inhibition mechanism.
- **Site-specific concern**: Topical salicylic acid carries an irritation risk, which is particularly relevant to any ocular use.

Please refer to the package insert for further safety information, including warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone, with no clinical trials or literature. Ocular irritation risk argues against the proposed route, and the only New Zealand licence has been cancelled.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data (for example from DrugBank)
- Preclinical or clinical evidence for papillary conjunctivitis, together with an assessment of ocular tolerability and route compatibility
- Confirmation of whether any product is currently marketed in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


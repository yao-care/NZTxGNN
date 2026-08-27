---
layout: default
title: Naproxen
parent: 僅模型預測 (L5)
nav_order: 238
evidence_level: L5
indication_count: 4
---

# Naproxen
{: .fs-9 }

證據等級: **L5** | 預測適應症: **4** 個
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

# Naproxen: From NSAID Therapy to Brachydactyly-Syndactyly Syndrome

## One-Sentence Summary

Naproxen is a well-known nonsteroidal anti-inflammatory drug (NSAID); however, this evidence pack does not contain recorded data on its original approved indication. TxGNN predicts a possible association with **Brachydactyly-Syndactyly Syndrome**, a rare congenital skeletal disorder, but this prediction is currently supported by **0 clinical trials** and **0 publications**, and no biologically plausible mechanistic link has been identified.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in evidence pack (Naproxen is a propionic-acid class NSAID generally used for pain/inflammation) |
| Predicted New Indication | Brachydactyly-Syndactyly Syndrome |
| TxGNN Prediction Score | 99.35% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack (original_moa = Data Gap). Naproxen is generally known as a propionic acid derivative NSAID that inhibits COX-1/COX-2 to reduce prostaglandin synthesis, producing analgesic, anti-inflammatory, and antipyretic effects. This information comes from general pharmacological knowledge and is not sourced from the evidence pack itself.

Brachydactyly-syndactyly syndrome is a congenital skeletal malformation associated with mutations in developmental pathways such as GDF5/ROR2, resulting in structural limb abnormalities from birth. This is fundamentally a developmental/structural genetic condition, not an inflammatory or pain-mediated pathology — there is no established pharmacological pathway connecting COX inhibition to correction of congenital limb malformation.

The repurposing rationale explicitly flags this as having **no direct mechanistic link**: the high TxGNN score likely reflects co-occurrence of "skeletal/joint"-related nodes within the knowledge graph rather than a genuine causal or therapeutic signal. Notably, all four top-ranked predictions for Naproxen in this pack (brachydactyly-syndactyly syndrome, colobomatous microphthalmia-rhizomelic dysplasia syndrome, acromesomelic dysplasia Hunter-Thompson type, brachyolmia-amelogenesis imperfecta syndrome) are rare congenital skeletal/developmental syndromes with the same absence-of-mechanism pattern — this consistency across the top ranks further supports the interpretation that these are structural artifacts of the graph embedding rather than independent, credible repurposing signals.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

No authorization records are available — Naproxen is currently not marketed in this jurisdiction per the evidence pack (0 licenses on file).

---

## Safety Considerations

Please refer to the package insert for safety information.

> Note: Warnings/contraindications and drug-drug interaction data could not be retrieved for this evidence pack (query status: not found). This is flagged as a **Blocking** data gap (DG001), as it prevents this candidate from entering the S1 safety pre-screening stage.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication has an Evidence Level of L5 (model prediction only, no supporting clinical trials or literature), and the repurposing rationale itself identifies no plausible mechanistic connection between Naproxen's NSAID activity and this congenital skeletal disorder. Combined with a Blocking data gap on safety information (package insert warnings/contraindications not yet retrieved), this candidate does not currently meet the bar to advance.

**To proceed, the following is needed:**
- Retrieve TFDA/regulatory package insert warnings and contraindications (resolves Blocking gap DG001, required for S1 safety pre-screening)
- Confirm Naproxen's original approved indication(s) and mechanism of action (DG002) to properly ground original-vs-predicted indication comparison
- Independent literature/preclinical review to assess whether any plausible biological pathway links NSAID pharmacology to skeletal developmental disorders
- Given the pattern across all top-4 predictions (rare congenital skeletal syndromes with zero trials/literature and no mechanistic rationale), consider re-evaluating whether this candidate-disease pairing reflects a genuine signal or a knowledge-graph artifact before committing further review resources
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


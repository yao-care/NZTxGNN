---
layout: default
title: Lactulose
parent: 僅模型預測 (L5)
nav_order: 191
evidence_level: L5
indication_count: 8
---

# Lactulose
{: .fs-9 }

證據等級: **L5** | 預測適應症: **8** 個
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

# Lactulose: From Hepatic Encephalopathy/Constipation to Acute Urate Nephropathy

## One-Sentence Summary

> Lactulose is a non-absorbable disaccharide generally used as an osmotic laxative and for hepatic encephalopathy (general pharmacological knowledge; not confirmed by New Zealand regulatory data in this evidence pack, as the drug is not currently marketed there). The TxGNN model predicts it may be effective for **Acute Urate Nephropathy**, but **0 clinical trials** and **0 publications** currently support this specific direction — the score reflects the model's embedding similarity alone, not any documented biological or clinical link.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in evidence pack (no New Zealand license records; drug is not marketed) |
| Predicted New Indication | Acute Urate Nephropathy |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on general pharmacological knowledge, lactulose is a non-absorbable synthetic disaccharide that is metabolized by colonic bacteria into short-chain organic acids, producing an osmotic laxative effect and acidifying the colonic lumen (which traps ammonia as ammonium and reduces its systemic absorption — the basis for its use in hepatic encephalopathy).

There is no established or plausible mechanistic pathway connecting lactulose's colonic osmotic/acidifying action to acute urate nephropathy, which arises from renal tubular precipitation of uric acid crystals (typically in tumour lysis syndrome or severe hyperuricemia). This assessment is corroborated by the evidence pack itself: no clinical trials, literature, or preclinical studies were found linking the two, and the source rationale explicitly characterizes the association as a high-scoring but low-confidence model artifact, likely reflecting embedding-space noise rather than a genuine biological signal.

Given the absence of any supporting mechanism or evidence, this specific prediction should not be treated as clinically actionable at this time.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

Lactulose currently has no marketing authorization on file in New Zealand (market status: Not Marketed; 0 authorizations recorded in this evidence pack).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Despite a high TxGNN prediction score (99.89%), there is zero clinical trial or literature evidence, and no plausible mechanistic link between lactulose's known pharmacology and acute urate nephropathy. This candidate should be treated as unvalidated model output pending independent confirmation.

**To proceed, the following is needed:**
- Preclinical or mechanistic studies establishing any pathway between lactulose and uric acid/renal tubular handling before further investment
- Confirmed mechanism of action (MOA) data (currently marked as a data gap)
- Regulatory safety data — key warnings, contraindications, and drug interactions (currently unavailable; classified as a Blocking data gap for New Zealand labeling information)
- Note: this evidence pack also contains a considerably better-supported candidate for the same drug — **Obstructive Jaundice** (rank 3, evidence level L2, 1 completed multicentre RCT + 19 supporting publications, recommendation "Proceed with Guardrails") — which may warrant a separate, dedicated evaluation report rather than further pursuit of the acute urate nephropathy signal.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


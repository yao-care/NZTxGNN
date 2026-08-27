---
layout: default
title: Olopatadine
parent: 僅模型預測 (L5)
nav_order: 255
evidence_level: L5
indication_count: 1
---

# Olopatadine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Olopatadine: From Allergic Conjunctivitis to Rosacea Conjunctivitis

## One-Sentence Summary

Olopatadine is an H1-antihistamine / mast cell stabilizer known primarily for treating allergic conjunctivitis. The TxGNN model predicts it may be effective for **Rosacea Conjunctivitis**, but this direction is currently supported by **0 clinical trials** and **0 publications** — the prediction is model-only at this stage.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Allergic conjunctivitis (referenced in the model's mechanistic rationale; not confirmed by regulatory license data — `original_indications` is empty in this evidence pack) |
| Predicted New Indication | Rosacea Conjunctivitis |
| TxGNN Prediction Score | 99.41% (rank 5020) |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this drug (marked as a data gap in DrugBank). Based on the model's own rationale, olopatadine is an H1-antihistamine / mast cell stabilizer, formulated as an ophthalmic product (e.g. Patanol/Pataday), and its efficacy in allergic conjunctivitis is well established — the mechanism centers on inhibiting histamine release and mast cell degranulation.

Rosacea conjunctivitis and allergic conjunctivitis both fall under ocular surface inflammatory disease, which is the basis of the TxGNN association. However, rosacea conjunctivitis is pathophysiologically distinct: it is driven primarily by vascular/neurovascular dysregulation, meibomian gland dysfunction, and chronic inflammation, rather than a classic IgE-mediated allergic response. The overlap with olopatadine's known mechanism is therefore inferential (shared "ocular surface inflammation" category) rather than direct mechanistic evidence, and should be treated as a hypothesis requiring further mechanistic and clinical confirmation.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## New Zealand Market Information

This drug is not currently marketed in New Zealand and has no registered authorizations in this evidence pack (`total_licenses` = 0).

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high, but evidence is at the lowest tier (L5) — no clinical trials, no literature, and the drug is not currently marketed in New Zealand. A blocking data gap also exists on TFDA package insert / warnings, which prevents any safety pre-screening (S1).

**To proceed, the following is needed:**
- TFDA/regulatory package insert data (warnings, contraindications) — currently a Blocking gap
- Confirmed mechanism of action (MOA) from DrugBank — currently a High-severity gap
- Preclinical or mechanistic studies linking olopatadine to rosacea conjunctivitis specifically (vascular/inflammatory pathway, not just shared "ocular surface" category)
- Confirmation of the drug's original approved indication(s) via regulatory license records, since `original_indications` is currently empty
- New Zealand market/regulatory status update if commercialization is being considered
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


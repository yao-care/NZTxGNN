---
layout: default
title: Ramipril
parent: 僅模型預測 (L5)
nav_order: 299
evidence_level: L5
indication_count: 10
---

# Ramipril
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

# Ramipril: From Hypertension to Pulmonary Hypertension with Unclear Multifactorial Mechanism

## One-Sentence Summary

Ramipril is a long-established ACE inhibitor, classically used to treat hypertension and reduce cardiovascular risk. The TxGNN model predicts it may be effective for **pulmonary hypertension with unclear multifactorial mechanism**, but this direction currently has **no supporting clinical trials and no supporting literature** — the prediction stands on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension (general ACE-inhibitor class knowledge; the evidence pack itself has no `original_indications` recorded) |
| Predicted New Indication | Pulmonary hypertension with unclear multifactorial mechanism |
| TxGNN Prediction Score | 99.93% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the evidence pack (`original_moa: [Data Gap]`). Based on general pharmacological knowledge, ramipril is a member of the ACE inhibitor class, which blocks conversion of angiotensin I to angiotensin II, lowering systemic vascular resistance and blood pressure. Its efficacy in hypertension and cardiovascular risk reduction is well established.

Pulmonary hypertension "with unclear multifactorial mechanism" corresponds to WHO Group 5 PH — a heterogeneous catch-all category (e.g., metabolic, haematologic, or systemic disorders) rather than a single well-defined pathway. Because RAAS activation can contribute to pulmonary vascular remodeling in some PH subtypes, an ACE inhibitor mechanism is not implausible in principle. However, Group 5 PH's mechanistic diversity makes a specific, reliable link to ACE inhibition speculative rather than established.

Critically, this is not corroborated by any external evidence in the pack: no clinical trials, no literature, and no ICTRP records were found for ramipril in this indication. The high TxGNN score alone should be read as a computational signal, not as confirmation of biological plausibility.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: `safety.ddi.query_status` is `not_found`, and TFDA/Medsafe package-insert warnings and contraindications are recorded as a **Blocking** data gap — DG001 — meaning safety cannot yet be formally assessed at Stage 1.)*

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked predicted indication (pulmonary hypertension with unclear multifactorial mechanism) has zero clinical trial or literature support and is not marketed in New Zealand — this is a pure L5 model prediction with no independent evidence to act on.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently a Blocking data gap (DG001)
- Confirmed mechanism of action data from DrugBank (DG002)
- Formal original indication history for ramipril (not present in this evidence pack)
- Targeted literature/trial search specifically on ramipril in WHO Group 5 pulmonary hypertension, since the general PubMed pull returned no hits

**Additional note:** other predictions in this evidence pack carry materially more evidence than the top rank — e.g., rank 8 (intracerebral hemorrhage, 4 literature incl. a ramipril-specific rat model study) and rank 10 (cerebral artery occlusion, 1 completed Phase 2 trial + 5 literature items directly involving ramipril). If further investigation is prioritized, these may warrant a separate evaluation ahead of the top-ranked pulmonary hypertension prediction.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


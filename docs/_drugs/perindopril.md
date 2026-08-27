---
layout: default
title: Perindopril
parent: 僅模型預測 (L5)
nav_order: 273
evidence_level: L5
indication_count: 5
---

# Perindopril
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

# Perindopril: From Hypertension to Malignant Hypertensive Renal Disease

## One-Sentence Summary

Perindopril is an angiotensin-converting enzyme (ACE) inhibitor originally used to treat hypertension and heart failure. The TxGNN model predicts it may be effective for **malignant hypertensive renal disease**, but this direction is currently supported by **0 clinical trials** and only **1 publication** — and that publication does not directly discuss perindopril in this condition.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension / heart failure (ACE inhibitor class; no New Zealand license record available to confirm the exact approved wording) |
| Predicted New Indication | Malignant Hypertensive Renal Disease |
| TxGNN Prediction Score | 99.77% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (data gap). Based on known information, perindopril is an ACE inhibitor: it blocks conversion of angiotensin I to angiotensin II, lowering systemic blood pressure and reducing intraglomerular pressure, which is why the class is used broadly across hypertension, heart failure, and hypertensive nephropathy.

Malignant hypertensive renal disease is, by definition, severe hypertension causing acute renal injury. Since renin-angiotensin system (RAS) blockade is a cornerstone of blood-pressure control and renal protection in hypertensive kidney disease, there is a plausible pharmacological rationale for perindopril in this setting.

However, this dataset does not contain any clinical trial directly testing perindopril in malignant hypertensive renal disease, and the single retrieved literature record is about renal function after nephrectomy for renal cancer — not about perindopril or malignant hypertension. There is therefore a gap between the mechanistic plausibility and the actual evidence base: the TxGNN score reflects pattern-based prediction, not confirmed clinical benefit.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36382821](https://pubmed.ncbi.nlm.nih.gov/36382821/) | 2022 | Review/Case series | Urologiia (Moscow, Russia: 1999) | Discusses functional state of a solitary kidney after nephrectomy for renal cancer; does not directly evaluate perindopril or malignant hypertensive renal disease — relevance to this candidate indication is unconfirmed |

---

## New Zealand Market Information

Perindopril is not currently marketed in New Zealand and no product license records are available in this dataset.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on plausible ACE-inhibitor pharmacology but has no supporting clinical trial and only one tangentially related publication (evidence level L4). Combined with the missing TFDA/package-insert safety data (a blocking data gap) and the drug's unmarketed status in New Zealand, this is not yet ready to advance past a research question.

**To proceed, the following is needed:**
- Official package insert / warnings and contraindications data (currently blocking — DG001)
- Confirmed mechanism of action from DrugBank or another primary source (DG002)
- Dedicated studies or trials evaluating perindopril specifically in malignant hypertensive renal disease
- Regulatory pathway assessment if market entry in New Zealand is being considered
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Labetalol
parent: 僅模型預測 (L5)
nav_order: 189
evidence_level: L5
indication_count: 4
---

# Labetalol
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

# Labetalol: From Hypertension to Malignant Renovascular Hypertension

## One-Sentence Summary

Labetalol is a combined alpha1/non-selective beta-adrenergic blocker traditionally used to treat hypertension, including hypertensive emergencies. The TxGNN model predicts it may be effective for **Malignant Renovascular Hypertension**, but this direction is currently supported only by **0 clinical trials** and **2 case-report publications**, making the evidence base weak and preliminary.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from New Zealand licensing data (drug is not marketed in NZ); pharmacologically, labetalol is an α1/non-selective β-blocker used for hypertension, including hypertensive emergencies |
| Predicted New Indication | Malignant Renovascular Hypertension |
| TxGNN Prediction Score | 99.08% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, labetalol is a combined alpha1/non-selective beta-adrenergic blocker; its efficacy in hypertension, including hypertensive emergencies, has been well established, and mechanistically it may be applicable to malignant renovascular hypertension, where rapid, effective blood pressure control is the central therapeutic goal.

Labetalol's established use in hypertensive emergencies gives it a plausible mechanistic link to malignant renovascular hypertension, since both involve severe, acute blood pressure elevation requiring aggressive pharmacologic control. However, this predicted indication is a specific **etiological subtype** — hypertension driven by renovascular disease — rather than hypertension in general. The very high TxGNN score (0.99) is nearly identical across several other predicted "hypertension" and "pulmonary hypertension" indications in this evidence pack, which raises the possibility that the model is responding to a broad "hypertension" semantic cluster rather than evidence specific to the renal-vascular etiology.

This caveat is reinforced by the supporting literature: both available publications are older case reports (1981, 2004) describing labetalol's use for blood pressure control in the context of malignant hypertension, not renovascular hypertension specifically or dedicated efficacy studies. The mechanistic rationale is credible, but specificity to the renovascular subtype remains unproven.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7242419](https://pubmed.ncbi.nlm.nih.gov/7242419/) | 1981 | Case Report | The Medical Journal of Australia | Case of malignant hypertension with renal arteritic changes in a 20-year-old man; minoxidil and labetalol were used for initial blood pressure control |
| [15113447](https://pubmed.ncbi.nlm.nih.gov/15113447/) | 2004 | Case Report | BMC Nephrology | Hyponatremic hypertensive syndrome in an 18-month-old child presenting as malignant renovascular hypertension |

---

## New Zealand Market Information

Labetalol is not currently marketed in New Zealand — no product authorizations, dosage forms, or approved indication text are available in the regulatory dataset.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication is supported only by two older case reports (no clinical trials, no systematic reviews), and the near-identical TxGNN scores across related "hypertension" predictions suggest the model may be generalizing from a broad hypertension association rather than renovascular-specific evidence. Combined with the absence of NZ market authorization and safety/labeling data, the evidence does not yet support advancing beyond a research question.

**To proceed, the following is needed:**
- TFDA/manufacturer package insert data (warnings, contraindications, DDI) — currently a blocking data gap
- Confirmed mechanism of action detail from DrugBank or primary literature
- Targeted literature or case-series search specific to labetalol in renovascular (not general) malignant hypertension, to test whether the TxGNN score reflects etiology-specific signal or a general hypertension cluster
- Assessment of route/formulation availability (IV vs. oral) relevant to malignant hypertension management, given NZ has no current market presence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


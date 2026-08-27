---
layout: default
title: Isosorbide Mononitrate
parent: 僅模型預測 (L5)
nav_order: 182
evidence_level: L5
indication_count: 10
---

# Isosorbide Mononitrate
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

# Isosorbide Mononitrate: From Angina Pectoris Prophylaxis to Hypertrichosis

## One-Sentence Summary

Isosorbide mononitrate (ISMN) is a long-acting organic nitrate conventionally used as a vasodilator for angina pectoris prophylaxis; no original-indication or registration data for this drug is present in the current evidence pack. The TxGNN model's top-ranked prediction is **Hypertrichosis (disease)**, with a prediction score of **99.995%**, but this is currently supported by **zero clinical trials** and **zero publications**.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in current evidence pack (no New Zealand license records) |
| Predicted New Indication | Hypertrichosis (disease) |
| TxGNN Prediction Score | 99.995% |
| Evidence Level | L5 |
| New Zealand Market Status | 未上市 (Not marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for this drug in the evidence pack. Based on general pharmacological knowledge, isosorbide mononitrate is a direct nitric oxide (NO) donor belonging to the organic nitrate/vasodilator class, conventionally used for angina pectoris prophylaxis through peripheral and coronary vasodilation.

The rationale linking ISMN to hypertrichosis is a **mechanistic analogy to minoxidil**: minoxidil is a vasodilator known to cause hypertrichosis as a side effect, and the model appears to be extrapolating that vasodilation could similarly over-activate hair follicles when applied to ISMN. However, this is explicitly a pure mechanistic hypothesis — there is no clinical, preclinical, or literature evidence specific to isosorbide mononitrate supporting an effect on hair growth.

It is worth noting that 7 of the top 10 predicted indications for this drug (ranks 1, 2, 5, 6, 7, 8, 9) all cluster around hair growth/loss disorders with no supporting evidence whatsoever, and one additional high-ranking candidate (rank 3) was explicitly flagged by the evidence-gathering process as a likely false positive (20 retrieved periodontal-disease papers, none of which mention isosorbide mononitrate). This pattern suggests the model's embedding space may be grouping ISMN near hair-related diseases due to its structural/mechanistic similarity to minoxidil in the training data, rather than reflecting a genuine drug-specific signal.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## Safety Considerations

Please refer to the package insert for safety information. Note: TFDA/label warnings and contraindications data for this drug is currently a **Blocking** data gap (DG001) — this prevents the candidate from entering the S1 safety pre-assessment stage until remediated.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication (hypertrichosis) has only an L5 evidence level — a high TxGNN model score with no corroborating clinical trials or literature — and the mechanistic link is a speculative analogy to minoxidil rather than drug-specific evidence. In addition, a Blocking data gap on TFDA safety labelling means the candidate cannot yet proceed through safety pre-assessment.

**To proceed, the following is needed:**
- TFDA package insert data (warnings, contraindications) — currently Blocking (DG001)
- Detailed mechanism of action (MOA) data from DrugBank — currently High severity gap (DG002)
- Original indication / registration history for the drug
- Preclinical or clinical evidence specific to ISMN's effect on hair follicles, if this indication is to be pursued further
- Consider re-scoping evaluation toward **pulmonary arterial hypertension** (rank 10 in this evidence pack), which has a stronger mechanistic rationale (shared NO–sGC–cGMP pathway with approved PAH therapies) and 6 supporting publications, reaching L4/S1 ("Research Question") — a more defensible starting point than the current top-ranked hypertrichosis prediction.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


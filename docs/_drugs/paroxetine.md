---
layout: default
title: Paroxetine
parent: 僅模型預測 (L5)
nav_order: 267
evidence_level: L5
indication_count: 1
---

# Paroxetine
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

# Paroxetine: From SSRI Antidepressant to Ohdo Syndrome and Variants

## One-Sentence Summary

Paroxetine (DB00715) is a widely used selective serotonin reuptake inhibitor (SSRI); the evidence pack does not include a specific original-indication text or DrugBank-sourced original MOA. The TxGNN model predicts a possible link to **Ohdo syndrome and variants**, an ultra-rare chromatin-modification genetic disorder, but this prediction is supported by **zero clinical trials** and **zero publications**, and the model's own rationale flags it as a likely false positive.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not specified in evidence pack (no `original_indications` or license text available) |
| Predicted New Indication | Ohdo syndrome and variants |
| TxGNN Prediction Score | 99.11% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (data gap DG002, High severity). No `original_indications` were provided in the evidence pack either, so the drug's established clinical use cannot be characterized from this data source alone.

Based on the repurposing rationale supplied with this candidate, Ohdo syndrome (including the SBBYS/Say-Barber-Biesecker-Young-Simpson variant) is a monogenic developmental disorder caused by dysfunction of chromatin-modifying genes such as *KAT6A*/*KAT6B* (histone acetyltransferases). Paroxetine's known pharmacological target is the serotonin transporter (SERT), which has no established mechanistic connection to *KAT6A*/*KAT6B* chromatin regulation.

Given the combination of missing original-MOA data, an unmarketed status in New Zealand, and the absence of any supporting trials or literature, the evidence pack itself assesses this TxGNN score as most likely a **model false positive** arising from a sparsely connected region of the knowledge graph, rather than a biologically grounded signal.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## New Zealand Market Information

Paroxetine is currently not marketed in New Zealand under this evidence pack, and no product authorizations are on file (total licenses: 0).

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high, but evidence level is L5 (model prediction only) — no clinical trials, no literature, no established mechanistic link between SERT inhibition and the *KAT6A*/*KAT6B* pathway underlying Ohdo syndrome. The evidence pack's own rationale flags this as a probable false positive.

**To proceed, the following is needed:**
- Regulatory/package-insert warnings and contraindications (DG001, Blocking — required before any S1 safety screening)
- Confirmed mechanism of action from DrugBank or equivalent source (DG002)
- Any preclinical or mechanistic evidence connecting serotonergic modulation to *KAT6A*/*KAT6B*-related chromatin disorders
- Re-evaluation once literature/clinical-trial evidence emerges, or reprioritization toward higher-confidence candidates in this drug's pipeline
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


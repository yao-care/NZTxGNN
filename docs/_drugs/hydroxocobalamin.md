---
layout: default
title: Hydroxocobalamin
parent: 僅模型預測 (L5)
nav_order: 167
evidence_level: L5
indication_count: 2
---

# Hydroxocobalamin
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

# Hydroxocobalamin: From Vitamin B12 Deficiency / Cyanide Poisoning to Esophageal Varices Without Bleeding

## One-Sentence Summary

Hydroxocobalamin is the active form of vitamin B12, established for treating B12 deficiency and as a cyanide-poisoning antidote. The TxGNN model predicts potential efficacy for **Esophageal Varices Without Bleeding** with a **99.23%** prediction score, but currently **no clinical trials and no literature** support this direction — the prediction is based purely on knowledge-graph inference.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Vitamin B12 deficiency / cyanide poisoning (based on known pharmacology; no formal Taiwan license data available) |
| Predicted New Indication | Esophageal Varices Without Bleeding |
| TxGNN Prediction Score | 99.23% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

*Note: A near-identical prediction exists for **Esophageal Varices With Bleeding** (score 99.23%, rank 6146), also with no supporting trials or literature and a Hold recommendation.*

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (marked as a High-severity data gap). Based on known pharmacology, hydroxocobalamin's established mechanisms are cyanide detoxification (binding cyanide ions to form cyanocobalamin, excreted renally) and correction of B12 deficiency (coenzyme function in methionine/succinyl-CoA metabolism).

Esophageal varices arise from portal hypertension secondary to cirrhosis, involving changes in vessel wall tension and fragility. No pharmacological literature currently establishes a direct mechanistic link between the B12/cobalamin pathway and portal pressure regulation or esophageal vessel wall integrity.

The high TxGNN score (99.23%) most likely reflects an **indirect** knowledge-graph association — for example, the comorbidity relationship between B12 deficiency (e.g., pernicious anemia) and liver disease — rather than a causal pharmacological mechanism. This is a model-generated hypothesis that requires manual biological plausibility review before further investment.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

This drug is currently **not marketed** in Taiwan (0 authorizations on record), so no license/product information is available.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: TFDA package insert warnings/contraindications are flagged as a Blocking data gap (DG001) — this must be resolved before any S1 safety evaluation can proceed.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a TxGNN model score (L5, decision stage S0) with zero clinical trials, zero literature, and no established mechanistic rationale linking B12/cyanide pathways to portal hypertension or variceal pathology. Combined with a Blocking safety data gap and the drug's unmarketed status in Taiwan, there is insufficient basis to advance.

**To proceed, the following is needed:**
- TFDA package insert (warnings, contraindications) — resolves Blocking gap DG001
- Confirmed mechanism of action via DrugBank — resolves High-severity gap DG002
- Preclinical/mechanistic studies exploring any B12–portal hypertension or vascular wall relationship
- Ongoing surveillance for emerging trials or literature on either esophageal varices indication (with/without bleeding)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


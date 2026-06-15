---
layout: default
title: Clobetasol Propionate
parent: 僅模型預測 (L5)
nav_order: 79
evidence_level: L5
indication_count: 7
---

# Clobetasol Propionate
{: .fs-9 }

證據等級: **L5** | 預測適應症: **7** 個
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

# Clobetasol Propionate: From Inflammatory Dermatoses to Vulvar Inverted Follicular Keratosis

## One-Sentence Summary

Clobetasol propionate is a Class I (ultra-potent) topical corticosteroid, widely used for moderate-to-severe inflammatory skin conditions including psoriasis, eczema, and lichen sclerosus.
The TxGNN model predicts it may be effective for **Vulvar Inverted Follicular Keratosis**, an exceedingly rare benign follicular epithelial tumor of the vulva.
Currently, **no clinical trials** and **no publications** directly supporting this specific indication have been identified, placing this prediction at the lowest evidence tier.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Inflammatory dermatoses (psoriasis, eczema, lichen sclerosus) |
| Predicted New Indication | Vulvar Inverted Follicular Keratosis |
| TxGNN Prediction Score | 99.46% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the Evidence Pack. Based on known pharmacology, clobetasol propionate is a synthetic glucocorticoid that acts through glucocorticoid receptor (GR) activation, suppressing the NF-κB and AP-1 transcription factor pathways and thereby downregulating key pro-inflammatory cytokines (IL-1β, TNF-α, IL-6). It also inhibits keratinocyte proliferation and suppresses mast cell and T-cell activation. These anti-inflammatory and anti-proliferative properties underpin its broad clinical use across inflammatory dermatoses.

Inverted follicular keratosis (IFK) is a rare benign follicular epithelial neoplasm. The vulvar variant is exceptionally uncommon, with only isolated case reports in the dermatopathology literature. While clobetasol's anti-proliferative effects via GR signaling could theoretically suppress keratinocyte proliferation, there is no established pathomechanistic link between GR modulation and IFK pathogenesis. Typical management of IFK is surgical excision, not pharmacological suppression of inflammation.

The TxGNN high score (0.9946) most likely reflects a broad dermatological-corticosteroid graph association rather than disease-specific predictive accuracy. Unlike lichen sclerosus or lichen planus — where clobetasol has substantial Phase 2/3 RCT evidence — vulvar IFK lacks an inflammatory disease mechanism that would make a corticosteroid the logical therapeutic choice.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No clinical trial or published literature evidence supports clobetasol propionate for vulvar inverted follicular keratosis. Despite a high TxGNN prediction score (99.46%), the mechanistic link is speculative and the condition is typically managed surgically, not pharmacologically with topical corticosteroids.

**To proceed, the following is needed:**
- Histopathological and mechanistic studies establishing GR signaling involvement in IFK pathogenesis
- Case reports or case series documenting any clinical response of IFK to topical corticosteroids
- Verification of whether the TxGNN prediction reflects disease-specific biology or a class-level artifact (broad steroid–skin disease association in the knowledge graph)
- MOA data retrieval from DrugBank API (currently a High-severity data gap)
- Safety data retrieval from the TFDA package insert (currently a Blocking data gap)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


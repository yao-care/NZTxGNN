---
layout: default
title: Clotrimazole
parent: 僅模型預測 (L5)
nav_order: 84
evidence_level: L5
indication_count: 3
---

# Clotrimazole
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

# Clotrimazole: From Fungal Infections to Acne

## One-Sentence Summary

Clotrimazole is a broad-spectrum antifungal agent widely used globally for the treatment of tinea pedis, oral candidiasis, and vulvovaginal candidiasis.
The TxGNN model predicts it may be effective for **Acne**, though only **1 clinical trial** (currently suspended) and **no directly relevant publications** currently support this specific application.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Fungal infections (tinea pedis, candidiasis) |
| Predicted New Indication | Acne (disease) |
| TxGNN Prediction Score | 99.86% |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this Evidence Pack. Based on known information, clotrimazole is a synthetic azole antifungal agent whose efficacy in treating fungal skin infections — including tinea pedis, cutaneous candidiasis, and mucosal candidiasis (oral and vaginal) — is well established across multiple global markets. It is understood to act by inhibiting fungal CYP51, thereby blocking ergosterol biosynthesis and disrupting fungal cell membrane integrity.

The connection to conventional acne is mechanistically indirect. Acne vulgaris is predominantly driven by *Cutibacterium acnes*, a bacterium against which clotrimazole has no meaningful direct activity. A plausible but narrow mechanistic bridge exists through *Malassezia* folliculitis (pityrosporum folliculitis) — a fungal condition that clinically mimics acne — where clotrimazole's antifungal activity against *Malassezia* species could be relevant. However, this is a distinct disease entity from acne vulgaris and would require separate clinical validation.

The TxGNN's high prediction score for acne most likely arises from knowledge graph proximity effects, where shared dermatological disease nodes and antifungal drug nodes create indirect neighbours, rather than from a direct mechanistic causal relationship. This prediction warrants mechanistic validation before any clinical development pathway is considered.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01244256](https://clinicaltrials.gov/study/NCT01244256) | Phase 2/3 | Suspended | 80 | Evaluated a triple combination cream (clotrimazole 1% + gentamicin 0.1% + beclomethasone 0.025%) in patients with contaminated dermatosis showing bilateral symmetrical acne-like lesions; trial was suspended before completion and clotrimazole was not studied as a monotherapy — independent efficacy for acne cannot be assessed from this data |

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Clotrimazole currently has no registered products in New Zealand (0 authorizations on record).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only available clinical trial was suspended and evaluated a triple-drug combination, making it impossible to attribute any acne-related effect to clotrimazole alone; combined with the absence of supporting literature and a mechanistically weak link to conventional acne pathophysiology, the current evidence base is insufficient to proceed.

**To proceed, the following is needed:**
- Mechanistic validation: determine whether clotrimazole has meaningful activity against *Cutibacterium acnes* or *Malassezia furfur* in relevant acne models
- Disease scope clarification: assess whether the TxGNN indication targets acne vulgaris or *Malassezia* folliculitis, as the latter would have a more defensible mechanistic basis
- Dedicated preclinical or Phase 1/2 study evaluating clotrimazole monotherapy in acne or fungal folliculitis
- Retrieval of Medsafe package insert and DrugBank MOA data to address current safety and mechanism data gaps
- Safety profile review with specific attention to topical tolerability for facial/acne-prone skin applications
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


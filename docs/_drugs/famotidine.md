---
layout: default
title: Famotidine
parent: 僅模型預測 (L5)
nav_order: 146
evidence_level: L5
indication_count: 10
---

# Famotidine
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

Using this Evidence Pack for FAMOTIDINE (DB00927), the primary predicted indication is **duodenogastric reflux** (`predicted_indications[0]`), which per the pack itself carries relatively thin, exploratory-stage evidence (L3, no clinical trials). I've written the report following the required section order, and flagged the mechanistic caveat and blocking safety data gap explicitly noted in the pack, rather than glossing over them.

---

# Famotidine: From Peptic Ulcer Disease to Duodenogastric Reflux

## One-Sentence Summary

Famotidine is a histamine H2-receptor antagonist historically used to treat acid-related conditions such as peptic ulcer disease and gastroesophageal reflux disease.
The TxGNN model predicts it may be effective for **Duodenogastric Reflux**, with **0 clinical trials** and **2 publications** currently supporting this direction.
Evidence at this stage is limited and largely mechanistic/observational, placing this candidate at an early "Research Question" stage rather than a validated repurposing opportunity.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not present in New Zealand regulatory data (drug is unmarketed there); based on established pharmacology, famotidine's classic indication is peptic ulcer disease / acid-related GI disorders |
| Predicted New Indication | Duodenogastric Reflux |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the Evidence Pack. Based on known pharmacology, famotidine belongs to the histamine H2-receptor antagonist (H2RA) class; its efficacy in acid-related upper GI conditions such as peptic ulcer disease has been proven over decades of clinical use, and mechanistically it may be applicable to duodenogastric reflux (DGER) by reducing the acidic component of refluxed gastric content.

However, the repurposing rationale captured in the pack raises an important caveat: duodenogastric reflux is primarily driven by bile and pancreatic secretions (an alkaline mixture), not gastric acid. This means famotidine's core mechanism — acid suppression — addresses only a secondary component of DGER pathophysiology. The drug may offer indirect symptomatic relief by reducing acid-related irritation of refluxed material, but it is unlikely to directly treat the underlying reflux mechanism.

Given this mechanistic mismatch, the high TxGNN similarity score should be interpreted cautiously — it likely reflects famotidine's strong embedding association with the broader gastroduodenal disease space (where it has extensive, well-established evidence) rather than a specific, validated therapeutic pathway for DGER itself.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12532466](https://pubmed.ncbi.nlm.nih.gov/12532466/) | 2003 | Cohort | World Journal of Gastroenterology | Investigated famotidine's effect on gastroesophageal reflux (GER) and duodeno-gastro-esophageal reflux (DGER) in critically ill patients, exploring possible mechanisms and relevant contributing factors. |
| [16259441](https://pubmed.ncbi.nlm.nih.gov/16259441/) | 2004 | Review | Eksperimental'naia i klinicheskaia gastroenterologiia | Evaluated famotidine 20 mg BID in early-stage gastroduodenal reflux disease (Savary-Miller grade 0–1), based on clinical and endoscopic findings. |

## New Zealand Market Information

Famotidine is currently **not marketed** in New Zealand (market status: 未上市), and there are no active authorizations on record. No product-level licensing data is available for this indication.

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: the Evidence Pack flags TFDA package insert warnings/contraindications as a Blocking data gap (DG001), meaning a formal safety review (S1) cannot currently be completed for this drug.)*

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for the duodenogastric reflux indication is limited to two lower-tier publications (a cohort study and a review) with no registered clinical trials, and the drug's core acid-suppressive mechanism only partially addresses DGER's largely bile/pancreatic-driven pathophysiology. Combined with the blocking gap in TFDA safety/label data, this candidate is not yet ready to advance past the research-question stage.

**To proceed, the following is needed:**
- TFDA/manufacturer package insert data (warnings, contraindications) to clear the current Blocking data gap (DG001)
- Confirmed mechanism-of-action documentation (DG002) to properly assess relevance to DGER's bile-mediated pathophysiology
- Prospective clinical evidence specifically evaluating famotidine in DGER populations (current literature is observational/review-level only)
- Consider prioritizing famotidine's other TxGNN-predicted indications in this same Evidence Pack that show substantially stronger evidence — notably "active peptic ulcer disease" and "peptic ulcer disease" (both L1, "Proceed with Guardrails," backed by multiple completed Phase 3/4 RCTs) — as more actionable near-term repurposing candidates
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


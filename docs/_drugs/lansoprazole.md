---
layout: default
title: Lansoprazole
parent: 僅模型預測 (L5)
nav_order: 195
evidence_level: L5
indication_count: 2
---

# Lansoprazole
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

# Lansoprazole: From Proton Pump Inhibitor Use to Duodenogastric Reflux

## One-Sentence Summary

Lansoprazole is a proton pump inhibitor (PPI); specific original-indication and mechanism-of-action data for this drug are not available in the current evidence pack (data gap). The TxGNN model predicts a mechanistic association with **Duodenogastric Reflux**, but this is currently supported only by preclinical/mechanistic literature — **0 clinical trials** and **2 publications**, one of which reports a potential *carcinogenic* signal rather than a therapeutic benefit.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in evidence pack (no Taiwan registration records; PPI class is generally indicated for peptic ulcer disease, GERD, H. pylori eradication per background literature) |
| Predicted New Indication | Duodenogastric Reflux |
| TxGNN Prediction Score | 99.69% |
| Evidence Level | L4 (preclinical/mechanistic study only; no clinical trials) |
| Taiwan Market Status | Not Marketed |
| Number of Taiwan Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data for lansoprazole is not available (data gap). Based on known information, lansoprazole belongs to the proton pump inhibitor (PPI) class, whose efficacy in acid-related disorders has been established, and mechanistically PPIs alter gastric acid dynamics — a pathway plausibly linked to duodenogastric reflux (DGR), where reflux of bile/duodenal contents interacts with gastric acid secretion.

However, the single disease-specific piece of literature evidence available (PMID 15052437) is a rat model study, and its finding runs counter to a simple "treats" relationship: acid suppression combined with duodenogastric reflux was associated with **promotion of gastric carcinogenesis**, not therapeutic benefit. This suggests the TxGNN link may reflect a *mechanistic/risk association* between lansoprazole and DGR pathophysiology rather than confirmed therapeutic efficacy for treating DGR. This distinction should be treated as material to the repurposing decision.

The second literature item (PMID 18679668) is a general PPI clinical-pharmacology review and does not address duodenogastric reflux specifically — it is background context only.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15052437](https://pubmed.ncbi.nlm.nih.gov/15052437/) | 2004 | Animal/Preclinical Study | Gastric Cancer | In a rat model, lansoprazole-mediated acid inhibition combined with duodenogastric reflux **promoted gastric carcinogenesis** — a cautionary mechanistic signal, not evidence of therapeutic benefit for DGR |
| [18679668](https://pubmed.ncbi.nlm.nih.gov/18679668/) | 2008 | Review | European Journal of Clinical Pharmacology | General review of PPI clinical use/pharmacokinetics across peptic ulcer, H. pylori infection, GERD, NSAID-induced GI lesions, and Zollinger-Ellison syndrome; not specific to duodenogastric reflux |

---

## Taiwan Market Information

Lansoprazole is currently **not marketed in Taiwan** — 0 authorizations on record.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: TFDA package insert warnings/contraindications and DDI data could not be retrieved — this is flagged as a blocking data gap; see Conclusion.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- Evidence for duodenogastric reflux is limited to one preclinical rat study that reports a potential carcinogenesis-promoting signal rather than therapeutic benefit, with no supporting clinical trials.
- Core safety data (TFDA warnings/contraindications) is a **blocking** gap, and mechanism-of-action data is a **high-severity** gap — both preclude a sound S1 safety assessment.
- The drug is not currently marketed in Taiwan.

**To proceed, the following is needed:**
- TFDA package insert (warnings, contraindications) — blocking gap (DG001)
- Confirmed mechanism of action data via DrugBank — high-severity gap (DG002)
- Additional studies clarifying whether the TxGNN-flagged association reflects therapeutic potential or disease-risk association, given the cautionary preclinical finding
- Clinical or higher-tier preclinical evidence specifically evaluating a therapeutic (not merely mechanistic) role for lansoprazole in duodenogastric reflux
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Propylene Glycol
parent: 僅模型預測 (L5)
nav_order: 294
evidence_level: L5
indication_count: 10
---

# Propylene Glycol
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

# Propylene Glycol: From Pharmaceutical Excipient to Bronchitis

## One-Sentence Summary

Propylene glycol has no approved original therapeutic indication on file — it is primarily used in pharmaceutical products as a solvent/excipient rather than as an active drug. The TxGNN model predicts a possible association with **Bronchitis**, but the supporting evidence (4 clinical trials, 3 publications) does not actually demonstrate a therapeutic effect of propylene glycol itself, and part of the literature points in the opposite direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — no approved indications on file (propylene glycol is primarily used as a pharmaceutical excipient/solvent, not marketed as a standalone active drug) |
| Predicted New Indication | Bronchitis |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for propylene glycol. Based on known information, propylene glycol is not typically developed or approved as an independent active pharmaceutical ingredient — it functions almost exclusively as a solvent, humectant, or vehicle within other drug formulations (including inhalation solutions), which explains why no original indication is on file.

Because propylene glycol lacks a defined independent pharmacology, the TxGNN association with bronchitis likely reflects its frequent co-occurrence as a formulation component in respiratory drug products, rather than a direct pharmacological effect on airway disease.

Critically, the underlying evidence does not support this link as an efficacy signal. All four bronchitis-related trials investigate **Cyclosporine Inhalation Solution (CIS)** for bronchiolitis obliterans after lung/hematopoietic stem cell transplant — cyclosporine is the active therapeutic agent, and propylene glycol, if present, is only a formulation excipient, not the treatment under study. The literature evidence is more concerning still: two of the three publications discuss propylene glycol as a constituent of e-cigarette aerosols and associate chronic exposure with an *increased* risk of airway/lung disease (including asthma and bronchitis-like pathology), i.e., evidence pointing toward harm rather than treatment benefit.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01273207](https://clinicaltrials.gov/study/NCT01273207) | Phase 2 | Completed | 7 | Extension study of Cyclosporine Inhalation Solution (CIS) in lung/HSCT transplant recipients for bronchiolitis obliterans; propylene glycol is not the study drug |
| [NCT00755781](https://clinicaltrials.gov/study/NCT00755781) | Phase 3 | Completed | 284 | Multi-center RCT of CIS to improve bronchiolitis obliterans syndrome-free survival after lung transplant; active agent is cyclosporine, propylene glycol only a possible formulation solvent |
| [NCT00938236](https://clinicaltrials.gov/study/NCT00938236) | Phase 3 | Terminated | 17 | Open-label extension of CIS in patients from prior CIS001 study; trial terminated, propylene glycol not the study agent |
| [NCT01287078](https://clinicaltrials.gov/study/NCT01287078) | Phase 2 | Completed | 25 | CIS trial for bronchiolitis obliterans syndrome after lung/HSCT transplant; same limitation — cyclosporine is the treatment, not propylene glycol |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [26408554](https://pubmed.ncbi.nlm.nih.gov/26408554/) | 2015 | Review | Am J Physiol Lung Cell Mol Physiol | Reviews chronic e-cigarette use (propylene glycol is a major e-liquid constituent) as a potential cause of chronic lung disease, including chronic bronchitis |
| [28983782](https://pubmed.ncbi.nlm.nih.gov/28983782/) | 2017 | Review | Curr Allergy Asthma Rep | Discusses e-cigarette constituents, including propylene glycol, as potential contributors to airway irritation and asthma pathogenesis |
| [20920189](https://pubmed.ncbi.nlm.nih.gov/20920189/) | 2010 | Animal study | Respir Res | Mouse model of elastase/LPS-induced COPD; active compound studied is quercetin, not propylene glycol |

## New Zealand Market Information

Propylene glycol currently has no marketing authorizations on file for New Zealand (0 licenses registered).

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The clinical trial evidence for bronchitis does not actually test propylene glycol — it tests cyclosporine inhalation solution, with propylene glycol at most a formulation excipient — and part of the literature suggests propylene glycol exposure (via e-cigarettes) may contribute to airway disease rather than treat it. Combined with missing MOA and safety/warning data, this candidate does not meet the bar to proceed. The other nine ranked candidates (diabetic retinopathy, various cataract subtypes, etc.) are weaker still — most have Evidence Level L5 with no supporting trials or literature at all.

**To proceed, the following is needed:**
- TFDA/regulatory package insert with warnings and contraindications (currently blocking — DG001)
- Confirmed mechanism of action data for propylene glycol (DG002)
- A trial or study design that isolates propylene glycol's own effect from co-administered active drugs, since existing "supporting" trials confound it with cyclosporine
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


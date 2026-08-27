---
layout: default
title: Salbutamol
parent: 僅模型預測 (L5)
nav_order: 316
evidence_level: L5
indication_count: 10
---

# Salbutamol
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

Using superpowers:using-superpowers — checked for relevant skills; this is a template-driven report-writing task fully specified by the prompt, no domain skill applies, so proceeding directly.

# Salbutamol: From Asthma/COPD Bronchodilation to Papillary Conjunctivitis

## One-Sentence Summary

Salbutamol is a short-acting β2-adrenergic receptor agonist (SABA) bronchodilator, established as a mainstay therapy for asthma and COPD-related bronchospasm.
The TxGNN model's top-ranked prediction for this drug is **Papillary Conjunctivitis**,
but currently **0 clinical trials** and **0 publications** directly support this specific indication — the prediction rests on model score alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Asthma / COPD (bronchodilator) — no NZ Medsafe license record found; drug is currently not marketed in New Zealand |
| Predicted New Indication | Papillary Conjunctivitis |
| TxGNN Prediction Score | 99.9964% |
| Evidence Level | L5 |
| New Zealand Market Status | 未上市 (Not Marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action (MOA) data for salbutamol is not available in this evidence pack (flagged as a High-severity data gap, DG002). Based on generally known pharmacology, salbutamol is a selective β2-adrenoceptor agonist that relaxes bronchial smooth muscle; its efficacy in reversible airway obstruction (asthma, COPD, bronchospasm) is well established, and this is the mechanistic basis for essentially all of its approved and near-approved uses.

Papillary conjunctivitis, however, is an ocular surface condition, not an airway disease — the connective tissue between original indication and this predicted indication is not direct. The evidence pack itself acknowledges this: the repurposing rationale for this candidate states there is no clinical trial or literature support, and the mechanistic link is only an **indirect analogy** to allergic conjunctivitis pathophysiology.

That analogy has some basis elsewhere in this same evidence pack: for the separately-ranked candidate "atopic conjunctivitis" (rank 8), preclinical literature shows topically applied salbutamol suppressed immediate allergic conjunctivitis in an animal model ([PMID 3666475](https://pubmed.ncbi.nlm.nih.gov/3666475/)) and that β2-agonists exert topical anti-inflammatory activity on conjunctival tissue ([PMID 2906082](https://pubmed.ncbi.nlm.nih.gov/2906082/)). These findings are for allergic/atopic conjunctivitis, not papillary conjunctivitis specifically, and remain preclinical (animal/in vitro) — they should be read as a plausibility signal for the broader mechanistic class, not as direct evidence for this candidate.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

Salbutamol currently has no marketing authorization on record in New Zealand (0 licenses; market status: 未上市 / Not Marketed).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This is the TxGNN model's highest-scoring predicted indication for salbutamol, but it has zero supporting clinical trials or literature, and the mechanistic connection to the original bronchodilator indication is only an indirect analogy — this places it at evidence level L5 (model prediction only).

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data (warnings, contraindications — currently a Blocking data gap, DG001)
- Confirmed drug mechanism of action (DG002)
- Preclinical or clinical evidence specific to papillary conjunctivitis (current supporting literature only covers atopic/allergic conjunctivitis and is preclinical)
- If pursued, an initial pharmacology/mechanism study bridging β2-agonist activity to papillary conjunctivitis pathophysiology before considering any clinical investigation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


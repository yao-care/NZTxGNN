---
layout: default
title: Oxybutynin
parent: 僅模型預測 (L5)
nav_order: 260
evidence_level: L5
indication_count: 3
---

# Oxybutynin
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

# Oxybutynin: From Overactive Bladder to Restless Legs Syndrome

## One-Sentence Summary

> Oxybutynin is an M3-predominant antimuscarinic/antispasmodic agent, traditionally used for overactive bladder and urinary urgency incontinence.
> The TxGNN model predicts it may be effective for **Restless Legs Syndrome**,
> but currently **0 clinical trials** and **0 publications** support this specific direction — the prediction is model-score-driven only.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Overactive bladder / urinary urgency incontinence (based on known pharmacological classification; official NZ label text unavailable — see below) |
| Predicted New Indication | Restless Legs Syndrome |
| TxGNN Prediction Score | 99.74% |
| Evidence Level | L5 |
| New Zealand Market Status | 未上市 (Not Marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action documentation is currently a data gap (DG002, High severity). Based on known pharmacology, oxybutynin is an M3-predominant antimuscarinic ("musculotropic") antispasmodic that acts on bladder detrusor and gastrointestinal smooth muscle, and it also has central nervous system-penetrant anticholinergic activity.

Restless Legs Syndrome pathophysiology is primarily linked to dopaminergic system dysfunction and disturbed brain iron metabolism — there is no established pathway connecting muscarinic receptor antagonism to RLS symptom control. The repurposing rationale in the evidence pack explicitly notes this prediction "lacks mechanistic support" and is purely driven by the TxGNN model score (network rank 2803), not by any known biological pathway.

Given the absence of any supporting clinical trials, ICTRP registrations, or PubMed literature for this drug–disease pair (query log IDs 5–7, all zero results), this candidate should be treated as an early-stage computational hypothesis only, not a mechanistically or clinically substantiated signal.

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
The prediction carries a high TxGNN score but no mechanistic, preclinical, or clinical evidence, and the drug is not currently marketed in New Zealand (0 authorizations). This is an L5, decision-stage S0 candidate — insufficient to advance.

**To proceed, the following is needed:**
- Resolve DG001 (Blocking): official label warnings/contraindications, required before any S1 safety screen
- Resolve DG002 (High): confirmed mechanism-of-action data to evaluate biological plausibility for RLS
- Preclinical or mechanistic studies exploring any muscarinic–RLS pathway
- At least preliminary clinical or case-level evidence before moving beyond S0

*Note: The evidence pack also lists two additional candidates for oxybutynin — gastroduodenitis (rank 2, L5, Hold) and peptic ulcer disease (rank 3, L4/S1, Hold — supported by 3 older publications, though one reports oxybutynin-induced reflux esophagitis, a mechanism-conflicting adverse signal). Both were evaluated but are outside the scope of this primary report, which follows the template's use of the top-ranked prediction.*
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


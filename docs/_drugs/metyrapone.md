---
layout: default
title: Metyrapone
parent: 僅模型預測 (L5)
nav_order: 224
evidence_level: L5
indication_count: 10
---

# Metyrapone
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

# Metyrapone: From Adrenocortical Steroidogenesis Inhibition to Familial Periodic Paralysis

> **Note on indication selection:** TxGNN's top-ranked hit (*exercise-induced malignant hyperthermia*, score 99.95%) is explicitly flagged in the evidence pack's own rationale as a likely embedding-cluster artifact — no clinical trials, no literature, and "no mechanistic basis" per the model's own annotation. Of the 10 candidates, only **familial periodic paralysis** (rank 8) has supporting literature and a plausible mechanistic pathway, so this report treats it as the substantive candidate. The other 9 (including all malignant-hyperthermia/RYR1-myopathy predictions) remain at Hold/L5 with no independent evidence.

## One-Sentence Summary

Metyrapone is a CYP11B1 (11β-hydroxylase) inhibitor classically used for adrenocortical function testing and short-term management of Cushing's syndrome. TxGNN's highest-scoring predictions cluster around malignant hyperthermia-related myopathies but carry no supporting evidence and are judged mechanistically implausible by the evidence pack itself; the one candidate with actual literature support is **Familial Periodic Paralysis**, backed by **2 historical physiology publications** and an indirect electrolyte-regulation hypothesis — still hypothesis-generating only, with **zero clinical trials**.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in the evidence pack's regulatory records (drug not marketed in New Zealand, no license data). General pharmacological literature identifies adrenocortical (HPA-axis) function testing and Cushing's syndrome management as metyrapone's established uses. |
| Predicted New Indication | Familial Periodic Paralysis (hyperkalemic/hypokalemic) |
| TxGNN Prediction Score | 99.40% (rank 5089 of model output) |
| Evidence Level | L4 (preclinical/mechanistic studies only) |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Metyrapone competitively inhibits 11β-hydroxylase (CYP11B1), the adrenal enzyme catalyzing the final step of cortisol synthesis. Blocking this step causes compensatory ACTH rise and accumulation of upstream steroid precursors — notably 11-deoxycorticosterone (DOC), which retains mineralocorticoid activity and can influence renal potassium handling.

Familial periodic paralysis (hyperkalemic and hypokalemic forms) is driven by acute shifts in serum potassium that trigger transient muscle weakness. Because DOC accumulation under metyrapone could theoretically perturb potassium regulation, there is an indirect electrolyte-pathway rationale connecting the drug to this disease — distinct from, and more grounded than, the RYR1/calcium-channel-related myopathy predictions that dominate the model's top ranks.

A 1971 physiology study (PMID 4322666) adds a specific and testable observation: in a patient with hyperkalemic periodic paralysis, glucocorticoids reliably induced paralytic attacks, but **ACTH-gel combined with metyrapone did not**. This suggests cortisol synthesis itself may be mechanistically required for glucocorticoid/ACTH-triggered attacks — raising a prophylactic hypothesis (blocking cortisol synthesis to blunt steroid-triggered attacks) rather than a treatment-of-active-disease hypothesis. This is a meaningfully different, and more defensible, repurposing angle than the high-score/no-evidence MH-related predictions.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [4322666](https://pubmed.ncbi.nlm.nih.gov/4322666/) | 1971 | Cohort/Physiology Study | The Journal of Clinical Investigation | In hyperkalemic periodic paralysis, glucocorticoids consistently induced paralytic attacks, but ACTH-gel plus metyrapone did not — suggesting cortisol synthesis is mechanistically required for steroid-triggered attacks. |
| [4301626](https://pubmed.ncbi.nlm.nih.gov/4301626/) | 1968 | Cohort/Case Series | European Neurology | Case series of primary periodic paralysis (hyperkalemic and hypokalemic attacks) with hemolytic bilirubinemia; abstract text not available in evidence pack. |

## New Zealand Market Information

Metyrapone is not currently marketed in New Zealand — no authorization records are available.

## Safety Considerations

Please refer to the package insert for safety information. (TFDA/Medsafe label warnings, contraindications, and DDI data are flagged as a **blocking data gap [DG001]** in the evidence pack — this must be resolved before any safety evaluation can proceed.)

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only mechanistically coherent, literature-supported candidate (familial periodic paralysis) rests on two 1960s–70s physiology-era papers and no direct interventional data — evidence level L4 with no clinical trials. The drug is unmarketed in New Zealand, and core safety data (label warnings/contraindications/DDI) is a blocking gap.

**To proceed, the following is needed:**
- Resolve DG001: obtain official label/package-insert warnings and contraindications (TFDA/Medsafe or manufacturer source)
- Resolve DG002: confirm mechanism of action and original approved indication from DrugBank/regulatory source (currently unavailable in the evidence pack)
- Preclinical validation of the DOC-mediated potassium hypothesis (does metyrapone measurably shift serum K⁺ or blunt steroid-triggered attacks in periodic paralysis models)
- If preclinical signal confirms, a small hypothesis-generating clinical study before any Go/Guardrails decision
- Re-examine the 9 malignant-hyperthermia/RYR1-myopathy predictions only if independent trial or literature evidence emerges — current evidence pack rationale considers them likely false signals
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


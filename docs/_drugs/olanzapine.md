---
layout: default
title: Olanzapine
parent: 僅模型預測 (L5)
nav_order: 253
evidence_level: L5
indication_count: 3
---

# Olanzapine
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

# Olanzapine: From Schizophrenia/Bipolar Disorder to Benign Paroxysmal Torticollis of Infancy

## One-Sentence Summary

> Olanzapine is a second-generation antipsychotic internationally indicated for schizophrenia and bipolar disorder; it is not currently marketed in New Zealand.
> The TxGNN model predicts it may be effective for **Benign Paroxysmal Torticollis of Infancy**,
> but this pairing currently has **no registered clinical trials** and **no supporting literature** — the prediction rests on the model score alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from New Zealand regulatory records (drug not marketed); internationally indicated for schizophrenia and bipolar disorder |
| Predicted New Indication | Benign paroxysmal torticollis of infancy |
| TxGNN Prediction Score | 99.54% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for this candidate pairing. Based on known information, olanzapine is a second-generation (atypical) antipsychotic — a combined dopamine (D2) and serotonin (5-HT2A) receptor antagonist — whose efficacy in schizophrenia and bipolar disorder is well established internationally.

Benign paroxysmal torticollis of infancy is a rare, self-limiting episodic movement disorder in infants, classified as a periodic syndrome related to migraine rather than a primary dopaminergic or psychotic condition. Without MOA data linking olanzapine's receptor pharmacology to this disorder's pathophysiology, the mechanistic rationale for this specific prediction is unclear, and no clinical trials, registry entries, or published literature currently corroborate it.

This prediction should therefore be treated as a pure model output at this stage — plausible as a hypothesis-generating signal, but not yet supported by any independent evidence.

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

## Additional TxGNN-Predicted Indications (Supplementary)

This evidence pack also contains two other candidate indications for olanzapine with meaningfully stronger evidence bases, worth noting alongside the primary candidate above:

| Predicted Indication | TxGNN Score | Evidence Summary | Indicative Evidence Level |
|---|---|---|---|
| Agoraphobia | 99.47% | 7 publications, incl. a 12-week open-label fixed-dose trial of olanzapine augmentation in SSRI-resistant panic disorder/agoraphobia ([16415705](https://pubmed.ncbi.nlm.nih.gov/16415705/)), case reports ([10739446](https://pubmed.ncbi.nlm.nih.gov/10739446/), [15470803](https://pubmed.ncbi.nlm.nih.gov/15470803/)), and systematic reviews on treatment-resistant anxiety ([40946318](https://pubmed.ncbi.nlm.nih.gov/40946318/), [26635099](https://pubmed.ncbi.nlm.nih.gov/26635099/)) | L3 (no RCT; open-label trial + systematic reviews) |
| Dysthymic disorder | 99.28% | 5 publications, incl. a Cochrane systematic review of second-generation antipsychotics for major depressive disorder and dysthymia ([21154393](https://pubmed.ncbi.nlm.nih.gov/21154393/)) and an open-label olanzapine trial in comorbid borderline personality disorder/dysthymia ([10578457](https://pubmed.ncbi.nlm.nih.gov/10578457/)) | L3 (systematic review, no dedicated RCT) |

Both candidates have no registered clinical trials on ClinicalTrials.gov or ICTRP. Given their stronger literature base relative to the primary candidate, they may warrant separate evaluation as **Proceed with Guardrails** candidates once MOA and safety data gaps are closed.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The primary predicted indication (benign paroxysmal torticollis of infancy) has no clinical trial or literature support and no MOA data to establish biological plausibility — evidence is at the model-prediction-only level (L5).

**To proceed, the following is needed:**
- Mechanism of action (MOA) data for olanzapine (DrugBank query, per DG002)
- TFDA/Medsafe package insert warnings and contraindications (per DG001, currently blocking safety review)
- A dedicated literature and trial search for olanzapine in pediatric paroxysmal/periodic movement disorders to confirm the current zero-result finding
- If pursuing repurposing further, consider prioritizing agoraphobia or dysthymic disorder instead, given their stronger existing evidence base
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


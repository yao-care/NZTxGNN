---
layout: default
title: Carvedilol
parent: 僅模型預測 (L5)
nav_order: 66
evidence_level: L5
indication_count: 5
---

# Carvedilol
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

The txgnn-pipeline skill is for pipeline management — the report format is defined directly in my system prompt. Proceeding with report generation now.

---

# Carvedilol: From Heart Failure & Hypertension to Malignant Renovascular Hypertension

## One-Sentence Summary

Carvedilol is a non-selective beta-blocker with additional α1-adrenergic blocking activity, internationally established for chronic heart failure, hypertension, and post-myocardial infarction management.
The TxGNN model predicts it may be effective for **Malignant Renovascular Hypertension**, with **0 clinical trials** and **0 publications** directly supporting this specific indication.
The prediction is mechanistically plausible given carvedilol's multi-target renin–sympathetic axis suppression profile, but currently remains at the hypothesis-generation stage only.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Heart failure, hypertension (internationally established; no New Zealand registrations found in regulatory database) |
| Predicted New Indication | Malignant Renovascular Hypertension |
| TxGNN Prediction Score | 99.55% |
| Evidence Level | L4 — Mechanistic/preclinical reasoning only; no clinical trials or direct literature |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why Is This Prediction Reasonable?

Carvedilol is pharmacologically distinct from other beta-blockers because it combines non-selective β-adrenergic blockade with α1-adrenergic blocking activity. Through β-blockade it reduces cardiac output and suppresses renin secretion from the juxtaglomerular apparatus; through α1-blockade it lowers peripheral vascular resistance. This dual mechanism makes it one of the most hemodynamically versatile antihypertensive agents available.

Malignant renovascular hypertension arises when renal artery stenosis triggers runaway activation of the renin–angiotensin–aldosterone system (RAAS), producing a hypertensive emergency with end-organ damage. Because the β1 component of carvedilol suppresses renin release, it theoretically interrupts this cascade at the upstream sympathetic–renal interface — a mechanistic angle distinct from, and potentially complementary to, ACE inhibitors or ARBs that act further downstream. The TxGNN graph-based model likely captured this RAAS-adjacent network proximity between carvedilol and this disease phenotype.

That said, clinicians have long observed that beta-blockade alone performs suboptimally in high-renin, renovascular hypertension. First-line management guidelines favour ACEi/ARB ± calcium channel blockers, with revascularisation for the anatomical lesion. There are currently no clinical trials or focused publications examining carvedilol in this specific malignant phenotype, and MOA data from DrugBank was not retrievable in this evidence pack. The prediction is therefore best interpreted as a research question warranting mechanistic pre-clinical investigation before any clinical study design.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for carvedilol in malignant renovascular hypertension.

---

## Literature Evidence

Currently no related literature available for carvedilol in malignant renovascular hypertension.

---

## New Zealand Market Information

No carvedilol products are registered in the New Zealand regulatory database. Carvedilol (brand names Dilatrend, Coreg, and others) is approved and widely marketed in the United States, European Union, Japan, and many other jurisdictions for heart failure and hypertension, but no Medsafe authorisations were returned by this evidence pack's regulatory query.

> **Note for assessors:** If a New Zealand study or expanded access programme is contemplated, an import licence or provisional consent under the Medicines Act 1981 would be required.

---

## Safety Considerations

Detailed package insert warnings, contraindications, and drug–drug interactions were not retrieved in this evidence pack.

Please refer to the package insert and established clinical references for full safety information. Key areas to review before any clinical use in the target population include:

- **Beta-blockade contraindications**: decompensated heart failure, significant bradycardia, high-degree AV block, severe bronchospastic disease
- **Renal dosing**: carvedilol is primarily hepatically metabolised, but haemodynamic effects on renal perfusion are relevant in renovascular disease
- **Drug interactions**: CYP2D6 inhibitors may increase carvedilol exposure; co-administration with other antihypertensives or antiarrhythmics requires monitoring

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score (99.55%) reflects strong network-level proximity between carvedilol and malignant renovascular hypertension, and the mechanistic reasoning — RAAS suppression via renin secretion inhibition — is internally consistent. However, there are zero clinical trials, zero direct publications, no MOA data confirmed from DrugBank, and no New Zealand regulatory baseline. Current evidence supports only an L4 classification (mechanistic hypothesis), which falls below the threshold for clinical development without further foundational work.

**To proceed, the following is needed:**

- **MOA confirmation**: Retrieve full DrugBank pharmacology entry for carvedilol (DB01136) to formally document β1/β2/α1 receptor binding profile and any renal-specific pharmacodynamic data
- **Pre-clinical evidence search**: Targeted PubMed search specifically combining "carvedilol" with "renovascular hypertension" or "malignant hypertension" to check whether any animal-model or mechanistic studies exist
- **Safety gap closure**: Obtain New Zealand-equivalent package insert (Medsafe monograph or recognised international SmPC) to complete contraindication and DDI assessment before any S1 safety gate review
- **RAAS interaction mapping**: Assess whether carvedilol's renin-suppression mechanism adds clinical value over or alongside established ACEi/ARB therapy in the renovascular phenotype — this is the key scientific question the model raises
- **Consideration of rank 2 co-indication**: Malignant hypertensive renal disease (rank 2, identical score) shares the same mechanistic basis; any future study design should address both phenotypes in parallel
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


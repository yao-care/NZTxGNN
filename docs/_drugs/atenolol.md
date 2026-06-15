---
layout: default
title: Atenolol
parent: 僅模型預測 (L5)
nav_order: 36
evidence_level: L5
indication_count: 9
---

# Atenolol
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
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

# Atenolol: From Hypertension to Posteroinferior Myocardial Infarction

## One-Sentence Summary

Atenolol is a cardioselective β1-adrenergic receptor blocker widely used in the treatment of hypertension and angina pectoris. The TxGNN model predicts it may be effective for **Posteroinferior Myocardial Infarction**, with **0 registered clinical trials** and **1 publication** directly supporting this specific indication. Notably, among all 9 predicted indications in this pack, **Chronic Pulmonary Heart Disease** (rank 9) carries the strongest repurposing evidence (L3, 1 Phase 4 trial, 15 publications) and merits a separate "Proceed with Guardrails" pathway.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension / Angina Pectoris (pharmacological class inference; no New Zealand marketing authorization on file) |
| Predicted New Indication | Posteroinferior Myocardial Infarction |
| TxGNN Prediction Score | 99.87% |
| Evidence Level | L3 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the current evidence pack. Based on established pharmacological classification, Atenolol is a β1-selective adrenergic receptor antagonist (cardioselective beta-blocker). It competitively blocks β1 receptors in cardiac tissue, reducing heart rate, myocardial contractility, and myocardial oxygen demand — with minimal off-target effect on β2 receptors in bronchial or vascular smooth muscle at therapeutic doses.

Posteroinferior myocardial infarction (inferior wall MI) typically results from occlusion of the right coronary artery or, less commonly, the left circumflex artery. This creates a substrate of intense sympathetic activation, catecholamine surge, and sustained myocardial stress. Beta-blockers have been a cornerstone of post-MI standard of care per ACC/AHA guidelines for decades: they limit infarct extension, reduce ventricular arrhythmia risk, and attenuate adverse remodeling. The TxGNN prediction therefore reflects a pharmacologically coherent class-effect rationale extended to this anatomical MI subtype.

The supporting literature is limited to a single 1985 crossover randomized study (PMID 3901170) comparing atenolol against diltiazem in post-MI patients with residual ischemia on exercise stress testing. While the mechanistic logic is sound and aligns with current guidelines for general MI, there are no dedicated clinical trials enrolling patients with the specific "posteroinferior MI" designation, leaving a formal evidence gap for this precise indication.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for posteroinferior myocardial infarction specifically.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3901170](https://pubmed.ncbi.nlm.nih.gov/3901170/) | 1985 | Crossover Single-blind RCT | La Revue de médecine interne | Compared anti-ischemic activity of atenolol (200 mg) vs diltiazem (240 mg) in 23 patients undergoing cardiac rehabilitation 4 weeks after a limited posteroinferior or anterior MI with signs of residual ischemia; computerized bicycle ergometry (Case-Marquette system) used to quantify ischemic response |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
TxGNN assigns atenolol a very high prediction score (99.87%) for posteroinferior MI, grounded in a well-established beta-blocker class effect in post-MI care. However, the only available evidence is a single 40-year-old crossover RCT without dedicated enrollment for the posteroinferior anatomical subtype — insufficient to progress beyond a research question. Mechanism of action documentation and formal regulatory safety data for the New Zealand market are both absent.

**To proceed, the following is needed:**
- Mechanism of action (MOA) data from DrugBank or published pharmacology sources
- Safety data: package insert warnings, contraindications, and drug-drug interactions (DDI query returned no results — requires fresh search)
- At least one dedicated clinical trial or prospective cohort study with a posteroinferior MI patient subgroup
- New Zealand regulatory pathway assessment (TGA/Medsafe feasibility for a new indication submission)

**Additional recommendation — Chronic Pulmonary Heart Disease (rank 9):**
Among all 9 predicted indications in this pack, chronic pulmonary heart disease (cor pulmonale) presents the most actionable near-term development case: it carries L3 evidence (1 Phase 4 REDUCE-SWEDEHEART trial with 5,000 patients, plus a direct 1978 atenolol study in chronic lung disease with airway obstruction — PMID 31524), multiple supporting cohort studies on cardioselective beta-blockers in COPD-comorbid patients, and a scoring recommendation of **Proceed with Guardrails**. A focused evidence dossier for this indication is recommended as the priority pathway before revisiting the specific MI subtypes.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


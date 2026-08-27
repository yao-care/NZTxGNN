---
layout: default
title: Lisinopril
parent: 僅模型預測 (L5)
nav_order: 207
evidence_level: L5
indication_count: 10
---

# Lisinopril
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

此 Evidence Pack 含 10 個 TxGNN 預測適應症；其中 8 個為 L5/S0（僅模型分數、無臨床試驗或文獻），僅 rank 7（septal MI）與 rank 9（chronic pulmonary heart disease）達 L3/S1。以下報告聚焦證據最完整的 **rank 9：Chronic Pulmonary Heart Disease**（唯一有 lisinopril 專屬文獻支持者）。

---

# Lisinopril: From Hypertension/Heart Failure to Chronic Pulmonary Heart Disease

## One-Sentence Summary

Lisinopril is an ACE inhibitor whose established uses are hypertension, heart failure and post-MI management (this Evidence Pack itself contains no jurisdiction-specific regulatory record of the original indication). The TxGNN model predicts potential efficacy for **Chronic Pulmonary Heart Disease (chronic cor pulmonale)**, with **5 loosely-related clinical trials** and **8 publications** — including two drug-specific cohort studies on lisinopril in pulmonary hypertension/cor pulmonale — currently supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in the regulatory data provided (New Zealand: not marketed); generically known as hypertension / heart failure / post-MI (ACE inhibitor class) |
| Predicted New Indication | Chronic Pulmonary Heart Disease |
| TxGNN Prediction Score | 99.68% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this Evidence Pack (flagged as a High-severity data gap). Based on generally known pharmacology, lisinopril is an ACE inhibitor (ACEi): it blocks conversion of angiotensin I to angiotensin II, reducing systemic and pulmonary vascular resistance, afterload, and neurohormonal activation.

Chronic pulmonary heart disease (cor pulmonale) arises from sustained pulmonary hypertension secondary to chronic lung disease, leading to right ventricular strain. Because ACEi reduce vascular resistance and afterload — the same mechanism underlying their approved use in systemic hypertension and heart failure — extrapolation to the pulmonary vasculature is mechanistically plausible.

This plausibility is reinforced by two lisinopril-specific cohort studies in the evidence set (Pribylov 2006; Verbitskiĭ 2003), both reporting attenuation of pulmonary hypertension and improved right ventricular function in patients with chronic cor pulmonale/COPD. This is a materially stronger evidentiary basis than the other 9 candidates in this pack, which are supported only by TxGNN's prediction score with no drug-specific trials or literature (L5).

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04486118](https://clinicaltrials.gov/study/NCT04486118) | Phase 2 | Active, not recruiting | 36 | Centrally-acting ACE inhibition for cognitive impairment in SLE — relevance graded C, not a lisinopril-specific or cor pulmonale trial |
| [NCT03967496](https://clinicaltrials.gov/study/NCT03967496) | N/A | Completed | 402 | Postoperative delirium incidence study — no direct relevance to lisinopril or cor pulmonale |
| [NCT00982423](https://clinicaltrials.gov/study/NCT00982423) | Phase 1/2 | Completed | 41 | Furosemide dose effects on cardiorenal/humoral function in CHF — not a lisinopril trial |
| [NCT06697353](https://clinicaltrials.gov/study/NCT06697353) | N/A | Completed | 4936 | Real-world vericiguat outcomes in Japanese HFrEF patients — not a lisinopril trial |
| [NCT00292162](https://clinicaltrials.gov/study/NCT00292162) | N/A | Completed | 41 | Radiofrequency ablation for AF in advanced CHF — not a drug intervention trial |

**Note:** None of the retrieved trials directly test lisinopril in chronic pulmonary heart disease; all are graded "C" (low direct relevance) in the source query.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [17047621](https://pubmed.ncbi.nlm.nih.gov/17047621/) | 2006 | Cohort | Kardiologiia | Lisinopril (10 mg/day) attenuated pulmonary hypertension and improved right ventricular systolic/diastolic function in heart failure patients with combined ischemic heart disease and COPD |
| [14524095](https://pubmed.ncbi.nlm.nih.gov/14524095/) | 2003 | Cohort | Problemy tuberkuleza i boleznei legkikh | Lisinopril used in treatment of patients with chronic cor pulmonale |
| [26895877](https://pubmed.ncbi.nlm.nih.gov/26895877/) | 2016 | Review | European journal of pediatrics | Reviews underuse of ACEi/ARB/mineralocorticoid antagonists in pediatric chronic heart failure |
| [19393838](https://pubmed.ncbi.nlm.nih.gov/19393838/) | 2009 | Review | Clinical therapeutics | Review of nebivolol (beta-blocker), not lisinopril-specific |
| [19592143](https://pubmed.ncbi.nlm.nih.gov/19592143/) | 2009 | Review | American journal of kidney diseases | Depression in CKD patients; case mentions ACEi use, not disease-focused |
| [11170787](https://pubmed.ncbi.nlm.nih.gov/11170787/) | 2001 | Preclinical | Experimental and molecular pathology | Rat models of hypertension (aortic constriction, Goldblatt) and cardiopulmonary protein synthesis |
| [20852161](https://pubmed.ncbi.nlm.nih.gov/20852161/) | 2010 | Case Report | American journal of health-system pharmacy | Hypotension/bradycardia from concomitant tizanidine + lisinopril — safety signal, not efficacy |
| [32404370](https://pubmed.ncbi.nlm.nih.gov/32404370/) | 2020 | Case Report | BMJ case reports | Rare case of biventricular non-compaction; no direct lisinopril efficacy data |

## New Zealand Market Information

No marketing authorizations are currently on record — lisinopril is listed as **not marketed** in New Zealand in this dataset (0 licenses).

## Safety Considerations

Please refer to the package insert for safety information. (No warnings, contraindications, or drug interaction data are available in this Evidence Pack; Medsafe/TFDA package-insert retrieval is flagged as a **Blocking** data gap — DG001.)

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence level is L3, based on two older cohort studies (not RCTs) directly linking lisinopril to improved pulmonary hemodynamics in cor pulmonale, with no clinical trials specifically testing this drug-indication pair. A Blocking data gap (missing package-insert safety data) also prevents any S1 safety pre-assessment from being completed.

**To proceed, the following is needed:**
- Medsafe/TFDA package insert (warnings, contraindications) — currently Blocking (DG001)
- DrugBank-sourced mechanism of action confirmation — currently High-severity gap (DG002)
- A prospective RCT or larger controlled cohort of lisinopril in chronic pulmonary heart disease/COPD-associated pulmonary hypertension
- Route/formulation compatibility assessment, pending market authorization data
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Propranolol
parent: 僅模型預測 (L5)
nav_order: 293
evidence_level: L5
indication_count: 6
---

# Propranolol
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
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

# Propranolol: From Hypertension/Arrhythmia to Cardiomyopathy (Hypertrophic Obstructive Subtype)

## One-Sentence Summary

> Propranolol is a classic non-selective β-adrenergic receptor blocker, historically used to treat hypertension, angina, and cardiac arrhythmias.
> Among six TxGNN-predicted indications for this drug, **Cardiomyopathy** (specifically hypertrophic obstructive cardiomyopathy, HOCM) has by far the strongest evidence base,
> with **3 registered clinical trials** and **20 publications**, several dating back decades of documented clinical use in this exact population.
> Note: the single highest-scoring TxGNN prediction ("distal myopathy, Tateyama type", 99.40%) has **no supporting evidence** and is flagged by the model rationale itself as a likely knowledge-graph structural artifact rather than a genuine mechanistic signal — it is excluded from this report's focus and held at recommendation "Hold."

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension, angina pectoris, cardiac arrhythmia (classic non-selective β-blocker indications; no drug-specific TFDA label text available — see Data Gap below) |
| Predicted New Indication | Cardiomyopathy (hypertrophic obstructive cardiomyopathy, HOCM) |
| TxGNN Prediction Score | 99.12% (rank 6,856 of full candidate list) |
| Evidence Level | L2 |
| Taiwan Market Status | 未上市 (Not currently marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in this Evidence Pack (Data Gap DG002). Based on well-established pharmacological knowledge, propranolol is a non-selective β1/β2-adrenergic receptor antagonist: it reduces heart rate, myocardial contractility, and myocardial oxygen demand, and blunts catecholamine-driven arrhythmogenesis. These are the same properties that underlie its classic use in hypertension, angina, and arrhythmia control.

Hypertrophic obstructive cardiomyopathy (HOCM) is characterized by dynamic left ventricular outflow tract (LVOT) obstruction that worsens with increased contractility and heart rate — precisely the physiological state propranolol's negative inotropic/chronotropic action is designed to counteract. This mechanistic fit is not merely theoretical: propranolol has been used off-label in HOCM since the 1970s–80s, as reflected in the decades of hemodynamic and combination-therapy literature below.

The caveat is that "cardiomyopathy" in this Evidence Pack is a broad disease-ontology bucket. The three registered clinical trials retrieved are not efficacy trials in HOCM but rather modern *deprescribing* (N-of-1, stop-vs-continue) studies in HFpEF and cardiac amyloidosis populations — a different clinical question (whether long-term β-blockade is still needed) rather than validation of new use. The strongest support for this repurposing signal comes from older hemodynamic/observational literature specific to HOCM, not from the retrieved trial registry.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04767061](https://clinicaltrials.gov/study/NCT04767061) | Phase 4 | Completed | 9 | N-of-1 deprescribing trial evaluating whether stopping β-blockers affects physical function in older adults with HFpEF — tests discontinuation, not efficacy for cardiomyopathy. |
| [NCT05427474](https://clinicaltrials.gov/study/NCT05427474) | Phase 3 | Unknown | 90 | Propranolol + gabapentin for paroxysmal sympathetic hyperactivity after traumatic brain injury — unrelated indication (not cardiomyopathy), status unknown. |
| [NCT05019027](https://clinicaltrials.gov/study/NCT05019027) | Phase 4 | Enrolling by invitation | 20 | N-of-1 deprescribing trial in older adults with transthyretin cardiac amyloidosis, testing feasibility of stopping β-blockers — again a discontinuation study, not efficacy evidence. |

**Note:** None of the three trials directly support initiating propranolol for cardiomyopathy; two are deprescribing studies and one targets an unrelated indication.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [4586631](https://pubmed.ncbi.nlm.nih.gov/4586631/) | 1973 | Double-blind trial | British Heart Journal | Double-blind comparison of propranolol vs. practolol in hypertrophic cardiomyopathy. |
| [7200796](https://pubmed.ncbi.nlm.nih.gov/7200796/) | 1982 | Cohort | British Heart Journal | Hemodynamic effects of nifedipine and propranolol in hypertrophic obstructive cardiomyopathy; combination superior to nifedipine alone, reduced LV peak systolic pressure. |
| [7192151](https://pubmed.ncbi.nlm.nih.gov/7192151/) | 1980 | Cohort | British Heart Journal | Propranolol's effect on myocardial oxygen consumption and hemodynamics in HOCM during cardiac catheterization. |
| [6686544](https://pubmed.ncbi.nlm.nih.gov/6686544/) | 1983 | Cohort | European Heart Journal | Propranolol vs. verapamil effects on LV diastolic stiffness in HOCM patients. |
| [1611637](https://pubmed.ncbi.nlm.nih.gov/1611637/) | 1992 | Cohort | Cardiology | Propranolol and disopyramide effects on LV function at rest/exercise in HOCM. |
| [2920304](https://pubmed.ncbi.nlm.nih.gov/2920304/) | 1989 | Cohort | Canadian Journal of Cardiology | Combination of disopyramide and propranolol reduces outflow tract obstruction markers in HOCM. |
| [3433863](https://pubmed.ncbi.nlm.nih.gov/3433863/) | 1987 | Cohort | Zeitschrift für Kardiologie | Nifedipine + propranolol combination therapy in HOCM over 6–24 months; some patients discontinued due to deterioration/side effects. |
| [11300365](https://pubmed.ncbi.nlm.nih.gov/11300365/) | 2000 | Cohort | Cardiovascular Drugs and Therapy | Verapamil vs. propranolol effects on coronary vasomotor response to cold pressor test in symptomatic HOCM. |
| [7191199](https://pubmed.ncbi.nlm.nih.gov/7191199/) | 1980 | Observational | American Journal of Cardiology | Propranolol and arrhythmia control in hypertrophic cardiomyopathy. |
| [8989641](https://pubmed.ncbi.nlm.nih.gov/8989641/) | 1996 | Cohort | Journal of Cardiac Failure | Hemodynamic predictors of early intolerance and long-term propranolol effects in dilated cardiomyopathy. |

---

## Taiwan Market Information

Propranolol is currently **not marketed** in Taiwan under this Evidence Pack (`market_status: 未上市`), and no TFDA license records were retrieved (`total_licenses: 0`). No authorization table can be produced at this time.

---

## Safety Considerations

Please refer to the package insert for safety information.

**Critical gap:** TFDA package-insert warnings, contraindications, and drug-interaction data could not be retrieved for propranolol (Data Gap DG001, severity: **Blocking**). This blocks entry into the S1 safety pre-assessment stage and must be resolved before any clinical repurposing pathway proceeds — non-selective β-blockade carries well-known class risks (bronchospasm in reactive airway disease, bradycardia/heart block, masking of hypoglycemia) that cannot be confirmed against the local label without this data.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Decades of hemodynamic and observational literature support propranolol's mechanistic and clinical plausibility in hypertrophic obstructive cardiomyopathy (L2), but the drug is unmarketed in Taiwan and a Blocking-severity safety data gap (TFDA label unavailable) prevents formal safety pre-assessment. The other five TxGNN-predicted indications for propranolol are either low-evidence (L5, "Hold") or carry a double-edged safety signal in cirrhotic cardiomyopathy (L3) and should not be pursued in parallel without dedicated review.

**To proceed, the following is needed:**
- Retrieve and parse the TFDA package insert (warnings, contraindications, DDI) — DG001, Blocking
- Confirm detailed mechanism-of-action documentation via DrugBank — DG002, High
- Define the target subtype precisely as HOCM (not broad "cardiomyopathy") before any protocol design, given the retrieved trials address deprescribing rather than initiation
- If pursuing a Taiwan market pathway, obtain a licensing/importation assessment given current "未上市" status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


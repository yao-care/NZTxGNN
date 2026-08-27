---
layout: default
title: Felodipine
parent: 僅模型預測 (L5)
nav_order: 148
evidence_level: L5
indication_count: 7
---

# Felodipine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **7** 個
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

# Felodipine: From Hypertension/Angina to Prinzmetal Angina (Lead Candidate Among 7 TxGNN Predictions)

## One-Sentence Summary

Felodipine is a dihydropyridine calcium-channel blocker (calcium antagonist) with vascular selectivity, referenced in the evidence base for its use in hypertension, angina, and heart failure hemodynamics. The TxGNN model generated **7 candidate new indications** for this drug (candidate pack `TW-DB01023-multi`); the most credible finding is **Prinzmetal (variant) angina**, supported by **3 RCTs** and **6 additional publications**, while the remaining 6 candidates range from moderate mechanistic plausibility to likely knowledge-graph noise with no supporting evidence.

---

## Quick Overview (Lead Candidate: Prinzmetal Angina)

| Item | Content |
|------|------|
| Original Indication | Not stated in structured regulatory data (`original_indications` empty; MOA marked as data gap DG002). Cardiovascular/antihypertensive use is inferable only from the cited literature (calcium antagonist). |
| Predicted New Indication | Prinzmetal Angina |
| TxGNN Prediction Score | 99.07% (rank 7,125) |
| Evidence Level | L2 (1 completed RCT class evidence; 3 RCTs total identified) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

**Note:** This evidence pack contains 7 ranked candidate indications for felodipine, not one. A full breakdown of all 7 is provided below because TxGNN score alone (rank 1–5 all score >99.8%) does not track with actual evidence quality — the highest-scoring candidates (rank 1–3, 5) have **zero** supporting trials or literature.

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Decision Stage | Recommendation |
|------|----------------------|-------------|-----------------|-----------------|-----------------|
| 1 | Pulmonary hypertension, unclear multifactorial mechanism | 99.91% | L5 | S0 | Hold |
| 2 | Pulmonary hypertension owing to lung disease/hypoxia | 99.91% | L5 | S0 | Hold |
| 3 | Malignant hypertensive renal disease | 99.90% | L5 | S0 | Hold |
| 4 | Malignant renovascular hypertension | 99.90% | L4 | S0 | Hold |
| 5 | Braddock syndrome | 99.88% | L5 | S0 | Hold |
| 6 | Chronic pulmonary heart disease (cor pulmonale) | 99.19% | L3 | S2 | Research Question |
| **7** | **Prinzmetal angina** | **99.07%** | **L2** | **S3** | **Proceed with Guardrails** |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data (`original_moa`) is flagged as a data gap (DG002) and could not be retrieved from DrugBank in this pull. However, the literature captured in this evidence pack consistently describes felodipine as a **dihydropyridine calcium-channel blocker with vascular selectivity**, acting by blocking L-type calcium channels in vascular smooth muscle (see PMIDs 2487551, 2838319, 3154329, 7728649).

For **Prinzmetal (variant) angina**, the pathophysiology is coronary artery vasospasm — a mechanism directly addressed by dihydropyridine CCBs, which are already standard first-line therapy for this condition. Felodipine specifically has direct RCT evidence preventing ergonovine-induced and hyperventilation-induced coronary spasm, and head-to-head non-inferiority against nifedipine (the established comparator in this indication). This is the strongest mechanistic and clinical link among the 7 candidates.

For **chronic pulmonary heart disease** (rank 6), felodipine's systemic and pulmonary vasodilation reduced pulmonary vascular resistance and increased cardiac output in small 1980s hemodynamic studies of COPD and severe CHF patients — a plausible but dated and non-outcome-based signal.

For the four weakest candidates (ranks 1, 2, 3, 5), the TxGNN scores are similarly high (>99.8%) but the underlying evidence is either absent or off-target: rank 2's 20 retrieved PubMed articles are generic hypoxia-biology papers (brain aging, cancer metabolism) with no direct link to felodipine or pulmonary hypertension, and rank 5 (Braddock/CHOPS syndrome, a COG1-related congenital disorder) has no plausible calcium-channel pathophysiology at all. These are most consistent with knowledge-graph false positives rather than genuine repurposing signals.

---

## Clinical Trial Evidence

No registered clinical trials (ClinicalTrials.gov or ICTRP) were found for felodipine in **any** of the 7 predicted indications.

> Currently no related clinical trials registered for felodipine in these predicted indications.

---

## Literature Evidence

### Lead Candidate — Prinzmetal Angina (L2, 9 publications)

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|---------|
| [8013514](https://pubmed.ncbi.nlm.nih.gov/8013514/) | 1994 | RCT | European Heart Journal | Once-daily felodipine ER prevented ergonovine-induced myocardial ischemia in 14 Prinzmetal angina patients |
| [1746458](https://pubmed.ncbi.nlm.nih.gov/1746458/) | 1991 | RCT | American Journal of Cardiology | Once-daily felodipine matched four-times-daily nifedipine in controlling Prinzmetal angina in 30 patients |
| [7744087](https://pubmed.ncbi.nlm.nih.gov/7744087/) | 1995 | RCT | European Heart Journal | Double-blind crossover vs nifedipine SR and placebo; felodipine ER improved exercise duration by 66s in exercise-induced angina |
| [14689111](https://pubmed.ncbi.nlm.nih.gov/14689111/) | 2003 | Review | Herz | Reviews differential efficacy of calcium antagonists across hypertension and angina subtypes |
| [7728649](https://pubmed.ncbi.nlm.nih.gov/7728649/) | 1995 | Review | Canadian Journal of Cardiology | CCBs are first-choice therapy in Prinzmetal angina via antivasospastic action; felodipine discussed specifically |
| [3345765](https://pubmed.ncbi.nlm.nih.gov/3345765/) | 1988 | Cohort | European Heart Journal | Case series documenting exercise-induced ST elevation consistent with coronary spasm mechanism |
| [2909138](https://pubmed.ncbi.nlm.nih.gov/2909138/) | 1989 | Cohort/small clinical study | American Journal of Cardiology | Felodipine reduced hyperventilation-induced ischemic attacks in variant angina |
| [15222138](https://pubmed.ncbi.nlm.nih.gov/15222138/) | 2004 | Case report | Orvosi Hetilap | Nicergoline-induced Prinzmetal angina case (background pathophysiology, not felodipine-specific) |
| [19052677](https://pubmed.ncbi.nlm.nih.gov/19052677/) | 2008 | Case report | Canadian Journal of Cardiology | Vasospasm-induced polymorphic ventricular tachycardia case (background, not felodipine-specific) |

### Secondary Candidate — Chronic Pulmonary Heart Disease (L3, 3 publications)

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|---------|
| [2487551](https://pubmed.ncbi.nlm.nih.gov/2487551/) | 1989 | Open-label hemodynamic study | Cardiovascular Drugs and Therapy | Acute felodipine infusion assessed in severe chronic congestive heart failure; regional blood flow effects measured |
| [2838319](https://pubmed.ncbi.nlm.nih.gov/2838319/) | 1988 | Open-label hemodynamic study | European Respiratory Journal | Felodipine infusion reduced pulmonary vascular resistance 18% and systemic vascular resistance 33%, increased cardiac output 33% in severe COPD |
| [3154329](https://pubmed.ncbi.nlm.nih.gov/3154329/) | 1988 | Review | Cardiovascular Drugs and Therapy | Reviews calcium antagonists' minor/secondary indications in hypertension and arrhythmias |

### Weak/Unsupported Candidates — Not Tabulated

- **Malignant renovascular hypertension (rank 4, L4):** only 1 retrieved publication ([8893190](https://pubmed.ncbi.nlm.nih.gov/8893190/), 1996 case report) — describes renovascular hypertension after adrenalectomy, **not** a felodipine efficacy study.
- **Pulmonary hypertension owing to lung disease/hypoxia (rank 2, L5):** 20 publications retrieved, but all are generic hypoxia-biology papers (brain aging, cancer metabolism, altitude physiology) with no direct relevance to felodipine or this indication — treated as evidentiary noise.
- **Pulmonary hypertension, unclear multifactorial mechanism (rank 1, L5)** and **malignant hypertensive renal disease (rank 3, L5)**: no clinical trials or literature retrieved.
- **Braddock syndrome (rank 5, L5):** no clinical trials or literature retrieved; no known pathophysiological link to calcium-channel modulation. Flagged as a likely false-positive association and recommended for exclusion from further follow-up.

---

## New Zealand Market Information

Felodipine currently holds **0 authorizations** and is **not marketed** in New Zealand under this evidence pack. No product license records are available to summarize.

---

## Safety Considerations

Please refer to the package insert for safety information. Structured safety data (key warnings, contraindications, drug-drug interactions) could not be retrieved for this evidence pack — the TFDA/regulatory package insert query is flagged as a **Blocking** data gap (DG001), and the DDI database query returned no results.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails** (for Prinzmetal angina, the lead candidate) — **Hold** for the remaining 6 candidates.

**Rationale:**
- Prinzmetal angina has direct RCT-level evidence (3 trials) and a well-established mechanistic basis (CCBs are already standard therapy for coronary vasospasm), supporting cautious advancement (L2/S3).
- Chronic pulmonary heart disease has plausible but dated (1980s), non-outcome hemodynamic evidence only (L3/S2) — warrants a research question rather than action.
- The four remaining candidates (pulmonary hypertension ×2, malignant hypertensive/renovascular hypertension, Braddock syndrome) have no or off-target evidence (L4–L5) and should remain on Hold; Braddock syndrome in particular should be deprioritized as a probable knowledge-graph false positive.

**To proceed, the following is needed:**
- Resolve **DG001 (Blocking)**: obtain the TFDA/manufacturer package insert to complete the S1 safety screen — this is currently blocking any indication from advancing past S0/S1.
- Resolve **DG002 (High)**: confirm felodipine's formal mechanism of action via DrugBank API rather than relying on literature-inferred descriptions.
- For Prinzmetal angina: update the evidence base with contemporary (post-1995) trials or guideline citations, since all 3 RCTs are from 1991–1995; confirm current DDI profile before any clinical use.
- Since felodipine is not currently marketed in New Zealand (0 licenses), a regulatory pathway/market-entry assessment would be required before any repurposing indication could be operationalized locally.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Levocarnitine
parent: 僅模型預測 (L5)
nav_order: 204
evidence_level: L5
indication_count: 10
---

# Levocarnitine
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

# Levocarnitine: From Carnitine Deficiency to Congestive Heart Failure

## One-Sentence Summary

> Levocarnitine (L-carnitine) is an endogenous compound essential for mitochondrial fatty-acid transport, classically used to treat primary and secondary carnitine deficiency.
> Among 10 TxGNN-predicted new indications in this evidence pack, **Congestive Heart Failure** is the only candidate reaching **L2 evidence** and a **Proceed with Guardrails** recommendation,
> supported by **12 clinical trials** (including a completed Phase 2/3 RCT with 268 patients) and **20 publications** linking myocardial fatty-acid metabolism to carnitine status.

*Note on candidate selection*: this evidence pack is a multi-indication screen (`TW-DB00583-multi`). The TxGNN-highest-scoring items (rank 1, 2, 7, 8) are explicitly flagged in their own `repurposing_rationale` as score-only artefacts with **zero** trial/literature support and no plausible mechanistic link (e.g., rare collagen-gene or skeletal syndromes). This report focuses on the indication with the strongest actual evidence — Congestive Heart Failure (rank 9) — rather than the top raw model score. A portfolio overview of all 10 candidates is provided at the end.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Primary and secondary L-carnitine (carnitine) deficiency — based on established pharmacology; no Taiwan/NZ license record exists in this data pull to cite an official approved-indication text |
| Predicted New Indication | Congestive Heart Failure |
| TxGNN Prediction Score | 99.47% |
| Evidence Level | L2 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (flagged as a High-severity data gap, DG002). Based on known pharmacology, levocarnitine's core physiological role is shuttling long-chain fatty acids across the inner mitochondrial membrane for β-oxidation — the same pathway its established use (carnitine deficiency) depends on.

The failing heart relies heavily on fatty-acid oxidation to meet its energy demand, and myocardial carnitine depletion together with abnormal metabolic substrate switching has long been documented in heart failure, paralleling the mechanism of established metabolic-modulator drugs such as perhexiline and trimetazidine (both act on the carnitine palmitoyltransferase/fatty-acid oxidation axis).

This mechanistic continuity — from correcting carnitine deficiency to supporting myocardial energetic substrate handling — is why, among the 10 TxGNN predictions in this pack, congestive heart failure carries the strongest and most literature-consistent mechanistic rationale, rather than being a pure statistical artifact of the model.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01580553](https://clinicaltrials.gov/study/NCT01580553) | Phase 2/3 | Completed | 268 | Prospective, multicenter, randomized, double-blind, placebo-controlled trial of L-carnitine injection in Chinese heart failure patients — the largest completed direct trial in this set |
| [NCT00247975](https://clinicaltrials.gov/study/NCT00247975) | Phase 2/3 | Terminated | 36 | L-carnitine for primary prevention of anthracycline-induced cardiotoxicity/heart failure in breast cancer patients |
| [NCT01904396](https://clinicaltrials.gov/study/NCT01904396) | Phase 4 | Unknown | 30 | Identifies carnitine-responsive cardiomyopathy/myopathy in adults with dilated/hypertrophic cardiomyopathy |
| [NCT04913805](https://clinicaltrials.gov/study/NCT04913805) | Phase 2 | Recruiting | 53 | Propionyl-L-carnitine + nicotinamide riboside vs. KNO3 for exercise endurance/mitochondrial function in HFpEF |
| [NCT02862600](https://clinicaltrials.gov/study/NCT02862600) | Phase 2 | Terminated | 35 | Perhexiline (carnitine-pathway-related CPT inhibitor) for exercise capacity in hypertrophic cardiomyopathy with HFpEF |
| [NCT06256276](https://clinicaltrials.gov/study/NCT06256276) | N/A | Completed | 21 | Protein/nutritional supplementation for autonomic dysfunction in elderly heart failure patients |
| [NCT04426578](https://clinicaltrials.gov/study/NCT04426578) | Phase 2 | Unknown | 60 | Perhexiline for regression of LV hypertrophy in symptomatic hypertrophic cardiomyopathy (RESOLVE-HCM) |
| [NCT01524861](https://clinicaltrials.gov/study/NCT01524861) | Phase 4 | Completed | 90 | α-lipoic acid + L-acetylcarnitine on sympathetic heart innervation in stress (tako-tsubo) cardiomyopathy |
| [NCT03994874](https://clinicaltrials.gov/study/NCT03994874) | Phase 1/2 | Recruiting | 84 | PolyCore (polydextrin, L-carnitine, D-xylitol) peritoneal ultrafiltration in HFrEF with cardiorenal syndrome |
| [NCT07201714](https://clinicaltrials.gov/study/NCT07201714) | Early Phase 1 | Not yet recruiting | 20 | Oral L-carnitine supplementation for symptoms/quality of life in cardiorenal heart failure |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [30195728](https://pubmed.ncbi.nlm.nih.gov/30195728/) | 2020 | Clinical study | Hellenic J Cardiol | Levocarnitine improved cardiac function and reduced urinary albumin, hs-CRP, BNP and troponin in patients with coronary heart disease and heart failure |
| [17445089](https://pubmed.ncbi.nlm.nih.gov/17445089/) | 2007 | Review | Cardiovasc Drug Rev | Reviews perhexiline, which reduces fatty-acid metabolism via carnitine palmitoyltransferase inhibition — mechanistic analogue supporting the carnitine/FAO-heart failure link |
| [19275645](https://pubmed.ncbi.nlm.nih.gov/19275645/) | 2009 | Review | Curr Pharm Des | Reviews metabolic (fatty-acid oxidation-targeted) treatment approaches for coronary artery disease and heart failure |
| [36632072](https://pubmed.ncbi.nlm.nih.gov/36632072/) | 2023 | Review | Saudi J Biol Sci | Reviews L-carnitine nutrition/pathology; notes carnitine deficits contribute to cardiomyopathy and related conditions |
| [12695723](https://pubmed.ncbi.nlm.nih.gov/12695723/) | 2003 | Review | Am J Med Sci | Notes L-carnitine deficiency may contribute to CHF in end-stage renal disease patients |
| [11298185](https://pubmed.ncbi.nlm.nih.gov/11298185/) | 2000 | Review | Int J Exp Pathol | Reviews metabolic cardiomyopathies caused by disturbed fatty-acid/mitochondrial beta-oxidation metabolism, including heart failure |
| [40593619](https://pubmed.ncbi.nlm.nih.gov/40593619/) | 2025 | Basic/Mechanistic | Nature Communications | Shows mitophagy mitigates cardiomyopathy caused by deficient mitochondrial fatty-acid β-oxidation |
| [39197425](https://pubmed.ncbi.nlm.nih.gov/39197425/) | 2024 | RCT protocol | Kidney Blood Press Res | Design paper for the PURE RCT evaluating PolyCore (incl. L-carnitine) peritoneal ultrafiltration in refractory CHF |
| [40287103](https://pubmed.ncbi.nlm.nih.gov/40287103/) | 2025 | Umbrella review | Complement Ther Med | Umbrella review of systematic reviews/meta-analyses on integrative medicine (including metabolic supplements) for chronic heart failure |
| [21561431](https://pubmed.ncbi.nlm.nih.gov/21561431/) | 2011 | Review | Curr Drug Metab | General review of L-carnitine's metabolic functions, including its role in normal cardiac energy production |

---

## New Zealand Market Information

Currently no marketing authorization on file — market status is "Not Marketed" with 0 registered licenses in this data pull.

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and DDI data are currently unavailable — TFDA package-insert warnings/contraindications are flagged as a **Blocking** data gap, DG001, and must be resolved before any S1 safety review.)

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
- Congestive heart failure is the only TxGNN-predicted indication in this pack backed by a completed, adequately-powered RCT (n=268) plus a mechanistically coherent narrative (myocardial fatty-acid oxidation/carnitine depletion), reaching L2/S2 — clearly above the noise-level, zero-evidence predictions that dominate the raw score ranking.

**To proceed, the following is needed:**
- Resolve DG001 (TFDA/Medsafe package-insert warnings and contraindications) — currently blocking any safety pre-screen
- Resolve DG002 (formal MOA data from DrugBank) to substantiate the mechanistic rationale beyond general pharmacology
- A larger, non-combination (single-agent), internationally replicated RCT specifically in HFrEF/HFpEF populations, since several supporting trials are terminated, small, or use levocarnitine only as part of a combination product (e.g., PolyCore)
- Regulatory pathway assessment given the drug is currently unmarketed in New Zealand (0 licenses)
- Safety monitoring plan for renal-impairment and cardiorenal syndrome subpopulations, which feature prominently in the supporting trials

---

## Appendix: Full TxGNN Prediction Portfolio (Screening Overview)

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Recommendation |
|------|----------------------|-------------|-----------------|-----------------|
| 1 | Autosomal dominant familial hematuria–retinal arteriolar tortuosity–contractures syndrome | 99.94% | L5 | Hold (no evidence, no mechanistic link) |
| 2 | Brain small vessel disease 1 with/without ocular anomalies | 99.94% | L5 | Hold (literature is keyword-mismatch noise) |
| 3 | Diabetic nephropathy | 99.91% | L3 | Research Question |
| 4 | Rheumatoid arthritis | 99.87% | L2 | Research Question |
| 5 | Sclerosing cholangitis | 99.75% | L4 | Hold |
| 6 | Gout | 99.74% | L4 | Hold |
| 7 | Brachydactyly-syndactyly syndrome | 99.66% | L5 | Hold (no evidence, no mechanistic link) |
| 8 | Colobomatous microphthalmia-rhizomelic dysplasia syndrome | 99.63% | L5 | Hold (no evidence, no mechanistic link) |
| **9** | **Congestive heart failure** | **99.47%** | **L2** | **Proceed with Guardrails** (this report) |
| 10 | Hypoalphalipoproteinemia | 99.45% | L4 | Hold |

Rheumatoid arthritis (rank 4) is a secondary candidate worth monitoring: it also reaches L2/S2 with a completed, direct RCT (NCT03953703, grade A, though n=15) plus two ongoing Phase 2/3 trials targeting the JAK/STAT–TGF-β1 pathway, and could be escalated in a future evidence-pack cycle once those trials read out.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


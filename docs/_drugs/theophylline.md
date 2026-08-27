---
layout: default
title: Theophylline
parent: 僅模型預測 (L5)
nav_order: 339
evidence_level: L5
indication_count: 7
---

# Theophylline
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

# Theophylline: From Asthma/COPD (Bronchodilator) to Thrombotic Disease

## One-Sentence Summary

Theophylline is classically used as a bronchodilator/anti-inflammatory agent for asthma and COPD via non-selective phosphodiesterase (PDE) inhibition and adenosine receptor antagonism (original indication and detailed MOA are not recorded in this evidence pack — Data Gaps DG001/DG002). The TxGNN model's top-ranked prediction for this drug is **Thrombotic Disease**, but this direction is currently supported by **0 clinical trials** and **19 publications**, none of which directly test theophylline's antithrombotic effect. This is a high-score, low-evidence model prediction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in evidence pack (no NZ license data); classically a bronchodilator for asthma/COPD |
| Predicted New Indication | Thrombotic Disease |
| TxGNN Prediction Score | 99.62% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (Data Gap DG002). Based on known pharmacology, theophylline is a methylxanthine that acts as a non-selective phosphodiesterase (PDE) inhibitor and adenosine receptor antagonist, raising intracellular cAMP — a mechanism established for airway smooth-muscle relaxation and anti-inflammatory effects in obstructive lung disease.

The link between this known respiratory mechanism and the model's top-ranked prediction of "Thrombotic Disease" is not well established. cAMP elevation is a pathway shared with some antiplatelet mechanisms (e.g., adenosine/milrinone-mediated platelet inhibition), which offers a theoretical rationale for exploring antiplatelet activity. However, the 19 literature results returned for this pairing are largely indirect: platelet/neutrophil activation biomarker studies, pharmacokinetics of an unrelated antiplatelet drug (ticlopidine), microRNA quantification methodology, and blood-sample-processing papers in which theophylline appears only as an anticoagulant additive rather than a therapeutic agent under study.

In short, this is a high TxGNN confidence score without corroborating direct clinical or mechanistic evidence — a pattern that warrants a Hold rather than active pursuit at this time.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8055680](https://pubmed.ncbi.nlm.nih.gov/8055680/) | 1994 | Review | Clinical Pharmacokinetics | Review of antiplatelet agent ticlopidine; does not study theophylline |
| [6771102](https://pubmed.ncbi.nlm.nih.gov/6771102/) | 1980 | Review | CRC Crit Rev Biochem | Background review of prostaglandin/thromboxane pathways in platelet aggregation and atherosclerosis |
| [749930](https://pubmed.ncbi.nlm.nih.gov/749930/) | 1978 | Other | Br J Haematol | Radioimmunoassay for platelet factor 4; theophylline used only as an anticoagulant additive in sample prep |
| [21719422](https://pubmed.ncbi.nlm.nih.gov/21719422/) | 2011 | Cohort | Rheumatology (Oxford) | Platelet/neutrophil activation studied in Behçet's disease; no theophylline intervention |
| [15475744](https://pubmed.ncbi.nlm.nih.gov/15475744/) | 2004 | Other | Inflamm Bowel Dis | Platelet-leukocyte aggregate formation in IBD; not a theophylline study |
| [29956444](https://pubmed.ncbi.nlm.nih.gov/29956444/) | 2018 | Other | J Thromb Haemost | Endothelial Weibel-Palade body exocytosis and hemostasis mechanisms; no theophylline data |
| [8981060](https://pubmed.ncbi.nlm.nih.gov/8981060/) | 1996 | Other | Gen Pharmacol | cAMP-mediated platelet inhibition by milrinone/adenosine — mechanistically adjacent pathway, not theophylline itself |
| [26764324](https://pubmed.ncbi.nlm.nih.gov/26764324/) | 2016 | Other | J Nutrition | Aged garlic extract inhibits platelet aggregation via cAMP/cGMP signaling; theophylline not tested |
| [6241135](https://pubmed.ncbi.nlm.nih.gov/6241135/) | 1984 | Other | Cor et Vasa | T-lymphocyte subset study in vascular disease using "theophylline-resistant" cell classification (assay term, not therapeutic use) |
| [14231672](https://pubmed.ncbi.nlm.nih.gov/14231672/) | 1964 | Other | Z Gesamte Inn Med | Historical case discussion of chronic cor pulmonale from thromboembolic disease; no theophylline data (abstract unavailable) |

Overall, none of the 19 returned publications directly evaluate theophylline's efficacy in thrombotic disease — most are background platelet-biology, methodology, or unrelated-drug studies.

---

## New Zealand Market Information

Theophylline currently holds no New Zealand market authorization (0 licenses on record in this evidence pack).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score (99.62%) is high, but there are zero clinical trials and no literature directly demonstrating an antithrombotic effect for theophylline — the supporting publications are indirect background studies. This does not meet the bar to progress past model-prediction-only status (L5).

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data — warnings and contraindications (DG001, Blocking)
- Detailed mechanism of action data from DrugBank (DG002, High)
- Targeted preclinical or mechanistic studies on theophylline's effect on platelet function/coagulation
- Drug interaction (DDI) data, currently not found

**Note:** Within this same evidence pack, two other TxGNN-predicted indications for theophylline have materially stronger support and may warrant separate evaluation — *Obstructive Lung Disease* (L2, Proceed with Guardrails; multiple cohort studies/reviews consistent with theophylline's known bronchodilator/anti-inflammatory mechanism) and *Nasal Cavity Disease* (L2; a completed Phase 2 RCT of nasal theophylline irrigation for post-viral olfactory dysfunction, n=27).
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Simvastatin
parent: 僅模型預測 (L5)
nav_order: 321
evidence_level: L5
indication_count: 8
---

# Simvastatin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **8** 個
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

# Simvastatin: From Hypercholesterolemia to Familial Hypercholesterolemia

## One-Sentence Summary

Simvastatin is an HMG-CoA reductase inhibitor (statin) established worldwide for lowering LDL cholesterol in hypercholesterolemia and mixed dyslipidemia. The TxGNN model assigns its highest-ranked new-indication signal to **Familial Hypercholesterolemia (FH)**, backed by **19 clinical trials** and **18 publications** in this evidence pack — though, as the underlying rationale itself notes, this largely reflects an *already-established* statin indication rather than a genuinely novel repurposing signal, and it comes with a blocking data gap on New Zealand-specific safety labeling.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — no approved indication text on file (drug is not currently marketed in New Zealand; 0 licenses) |
| Predicted New Indication | Familial Hypercholesterolemia |
| TxGNN Prediction Score | 99.63% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (MOA data gap, severity: High). Based on known pharmacological classification, simvastatin belongs to the statin (HMG-CoA reductase inhibitor) class; its cholesterol-lowering efficacy in hypercholesterolemia and mixed dyslipidemia is well established, and mechanistically this class action extends directly to familial hypercholesterolemia.

The evidence pack's own mechanistic rationale states: simvastatin inhibits HMG-CoA reductase, reducing hepatic cholesterol synthesis and compensatorily upregulating LDL receptor expression, which lowers circulating LDL-C. FH (both heterozygous and homozygous forms) is characterized by reduced or absent LDL receptor function, and statins directly counteract this defect — a mechanism that is broadly guideline-endorsed and used across pediatric and adult FH populations.

Importantly, the rationale text explicitly flags that this is **not a narrow "drug repurposing" case but an already-established standard-of-care indication** for statins as a class. The high TxGNN score and L1 evidence level here likely reflect confirmation of known pharmacology rather than discovery of a new therapeutic use — this distinction should inform how the "new indication" claim is communicated downstream.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00552097](https://clinicaltrials.gov/study/NCT00552097) | Phase 3 | Completed | 720 | ENHANCE trial: ezetimibe + high-dose simvastatin vs. simvastatin alone on carotid atherosclerosis progression in HeFH |
| [NCT03885921](https://clinicaltrials.gov/study/NCT03885921) | Phase 3 | Completed | 44 | Long-term open-label safety/tolerability of ezetimibe added to atorvastatin or simvastatin in homozygous FH |
| [NCT03884452](https://clinicaltrials.gov/study/NCT03884452) | Phase 3 | Completed | 50 | Efficacy and safety of ezetimibe co-administered with atorvastatin or simvastatin in homozygous FH |
| [NCT00654446](https://clinicaltrials.gov/study/NCT00654446) | Phase 3b | Completed | 442 | Renal effects of rosuvastatin vs. simvastatin in Fredrickson type IIa/IIb dyslipidaemia, including HeFH |
| [NCT00465088](https://clinicaltrials.gov/study/NCT00465088) | Phase 3 | Completed | 199 | SUPREME study: niacin ER + simvastatin vs. atorvastatin on HDL-C effects in hyperlipidemia/mixed dyslipidemia |
| [NCT01070966](https://clinicaltrials.gov/study/NCT01070966) | N/A (post-marketing) | Completed | 2089 | Re-examination survey of VYTORIN (ezetimibe/simvastatin) safety and efficacy in routine practice |
| [NCT00129402](https://clinicaltrials.gov/study/NCT00129402) | Phase 3 | Completed | 248 | Efficacy, safety, and tolerability of ezetimibe co-administered with simvastatin in adolescents with HeFH |
| [NCT01414192](https://clinicaltrials.gov/study/NCT01414192) | N/A (cohort) | Completed | 3215 | Model-observation bridging study for Ezetrol/Inegy (ezetimibe ± statin, incl. simvastatin) in CVD risk modeling |
| [NCT00475826](https://clinicaltrials.gov/study/NCT00475826) | N/A | Unknown | N/A | Chylomicron metabolism in sub-clinical atherosclerosis, HeFH patients treated with statin plus ezetimibe |
| [NCT02107898](https://clinicaltrials.gov/study/NCT02107898) | Phase 3 | Completed | 216 | Alirocumab as add-on to stable statin therapy in HeFH/high cardiovascular risk hypercholesterolemia (disease-context trial) |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [41824590](https://pubmed.ncbi.nlm.nih.gov/41824590/) | 2026 | Guideline | J Am Coll Cardiol | 2026 ACC/AHA dyslipidemia management guideline, replacing the 2018 cholesterol guideline |
| [31696945](https://pubmed.ncbi.nlm.nih.gov/31696945/) | 2019 | Review | Cochrane Database Syst Rev | Cochrane review: statins (including simvastatin) for children with familial hypercholesterolemia |
| [28685504](https://pubmed.ncbi.nlm.nih.gov/28685504/) | 2017 | Review | Cochrane Database Syst Rev | Earlier Cochrane review update: statins for children with FH |
| [15794711](https://pubmed.ncbi.nlm.nih.gov/15794711/) | 2005 | Review | Expert Opin Drug Saf | Benefits and risks assessment of simvastatin specifically in FH |
| [35629051](https://pubmed.ncbi.nlm.nih.gov/35629051/) | 2022 | Cohort | J Clin Med | Cross-sectional study: simvastatin's effect on cellular immunity parameters in children with FH |
| [18376000](https://pubmed.ncbi.nlm.nih.gov/18376000/) | 2008 | Pending classification | N Engl J Med | ENHANCE trial publication: simvastatin with or without ezetimibe in FH, effect on atherosclerosis progression |
| [21173733](https://pubmed.ncbi.nlm.nih.gov/21173733/) | 2010 | Pending classification | Int Angiol | Long-term efficacy and safety of ezetimibe/simvastatin treatment in FH |
| [12908847](https://pubmed.ncbi.nlm.nih.gov/12908847/) | 2003 | Pending classification | Drug Safety | Benefits and risks of simvastatin in patients with FH |
| [11383320](https://pubmed.ncbi.nlm.nih.gov/11383320/) | 2001 | Pending classification | Nutr Metab Cardiovasc Dis | Comparison of atorvastatin vs. simvastatin in attaining NCEP LDL-C goals in heterozygous FH |
| [25054950](https://pubmed.ncbi.nlm.nih.gov/25054950/) | 2014 | Pending classification | Cochrane Database Syst Rev | Earlier Cochrane review edition: statins for children with FH |

---

## New Zealand Market Information

Simvastatin is not currently marketed in New Zealand — no authorizations, licenses, or approved product information are on file (total_licenses = 0).

---

## Safety Considerations

Please refer to the package insert for safety information. No key warnings, contraindications, or drug interaction data are currently available in this evidence pack (TFDA package insert retrieval is flagged as a **Blocking** data gap — DG001).

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The clinical trial and literature base meets L1 evidence criteria (multiple completed Phase 3 RCTs, including the landmark ENHANCE trial), and FH is a well-established statin indication mechanistically consistent with simvastatin's action. However, the drug has no current New Zealand market presence, no MOA documentation on file, and no TFDA-equivalent safety labeling — and the underlying rationale itself indicates this is closer to confirming an already-standard use than to a novel repurposing finding, so claims should be scoped accordingly.

**To proceed, the following is needed:**
- TFDA/manufacturer package insert (warnings, contraindications, DDI) — currently a Blocking gap (DG001)
- Detailed mechanism of action documentation — currently a High-severity gap (DG002)
- Confirmation of whether this indication should be framed as "repurposing" or "established use" before external communication
- Assessment of the New Zealand regulatory pathway, given the drug is not currently licensed there
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


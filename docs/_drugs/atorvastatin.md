---
layout: default
title: Atorvastatin
parent: 僅模型預測 (L5)
nav_order: 39
evidence_level: L5
indication_count: 6
---

# Atorvastatin
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

# Atorvastatin: From Primary Hypercholesterolemia to Familial Hypercholesterolemia

## One-Sentence Summary

Atorvastatin is an HMG-CoA reductase inhibitor (statin) established globally as the cornerstone treatment for primary hypercholesterolemia and cardiovascular risk reduction, though it currently holds no market authorisation in New Zealand.
The TxGNN model predicts it may be effective for **Familial Hypercholesterolemia (FH)**, a genetically distinct and severe form of hypercholesterolemia,
with **35 clinical trials** and **19 publications** supporting this direction at the highest evidence level.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Primary hypercholesterolemia and mixed dyslipidemia (global standard of care; no current New Zealand registration) |
| Predicted New Indication | Familial Hypercholesterolemia |
| TxGNN Prediction Score | 99.42% |
| Evidence Level | L1 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Atorvastatin competitively and reversibly inhibits HMG-CoA reductase, the rate-limiting enzyme in the mevalonate pathway for hepatic cholesterol biosynthesis. By reducing intracellular free cholesterol, the liver compensatorily upregulates LDL receptor (LDLR) expression on hepatocyte surfaces, dramatically accelerating LDL-C clearance from the bloodstream. This dual mechanism — less cholesterol produced, more cholesterol removed — makes it uniquely effective in conditions of LDL-C excess.

Familial Hypercholesterolemia is caused by loss-of-function mutations in the *LDLR* gene (most commonly), or in *APOB* or *PCSK9*, resulting in severely impaired LDL-C clearance from birth and a cardiovascular risk 13-fold above the general population. In heterozygous FH (HeFH), where some residual LDLR activity persists, atorvastatin directly targets this pathological bottleneck: it reduces the LDL burden entering the LDLR pathway while simultaneously upregulating whatever functional receptor capacity remains. The mechanistic chain is direct and complete — this is not an extrapolation but a pharmacological match. In homozygous FH (HoFH), where LDLR activity is near-absent, the response is attenuated but not absent, and atorvastatin still forms the backbone of combination therapy.

In practice, high-intensity statin therapy — atorvastatin 40–80 mg daily being the prototype — is the first-line pharmacological treatment for FH in every major international guideline (ACC/AHA, EAS, AACE/ACE). The TxGNN model prediction aligns precisely with established clinical science. The New Zealand context reflects a registration gap, not an evidence gap.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00136981](https://clinicaltrials.gov/study/NCT00136981) | Phase 3 | Completed | 800 | Torcetrapib/Atorvastatin vs. maximally tolerated Atorvastatin alone in HeFH over 24 months; carotid artery B-mode ultrasound used as primary endpoint. Torcetrapib arm terminated for safety reasons; the Atorvastatin monotherapy arm constitutes a landmark dataset for vascular outcomes in HeFH. |
| [NCT01623115](https://clinicaltrials.gov/study/NCT01623115) | Phase 3 | Completed | 486 | Alirocumab vs. placebo added to atorvastatin background therapy in HeFH: demonstrated significant LDL-C reduction at 24 weeks in a large, multinational, double-blind RCT. Establishes atorvastatin as the essential foundation on which newer therapies are built. |
| [NCT03867318](https://clinicaltrials.gov/study/NCT03867318) | Phase 3 | Completed | 621 | Ezetimibe 10 mg + Atorvastatin vs. Atorvastatin alone in HeFH or CHD/multiple-risk patients with primary hypercholesterolemia not controlled by atorvastatin 10 mg: efficacy and safety evaluated directly. |
| [NCT01709500](https://clinicaltrials.gov/study/NCT01709500) | Phase 3 | Completed | 249 | Alirocumab vs. placebo add-on to lipid-modifying therapy in HeFH with LDL-C ≥ 160 mg/dL: randomised, double-blind, placebo-controlled parallel-group trial supporting atorvastatin as standard background. |
| [NCT02107898](https://clinicaltrials.gov/study/NCT02107898) | Phase 3 | Completed | 216 | Alirocumab as add-on to stable daily statin therapy in HeFH or high CV-risk hypercholesterolemia: statistically significant LDL-C reduction vs. placebo at 24 weeks across multiple secondary lipid parameters. |
| [NCT03882996](https://clinicaltrials.gov/study/NCT03882996) | Phase 3 | Completed | 432 | Long-term (12-month) safety and tolerability of Ezetimibe + Atorvastatin in HeFH or CHD/multiple risk factors with primary hypercholesterolemia; supports sustained combination use in chronic care. |
| [NCT03885921](https://clinicaltrials.gov/study/NCT03885921) | Phase 3 | Completed | 44 | 24-month open-label extension: Ezetimibe + Atorvastatin or Simvastatin in HoFH; provides long-term safety data in this high-risk population with near-absent LDLR function. |
| [NCT00827606](https://clinicaltrials.gov/study/NCT00827606) | Phase 3 | Completed | 272 | Three-year open-label Atorvastatin in children and adolescents with HeFH: characterised efficacy of cholesterol reduction and growth/development (height, weight, Tanner stage) over extended treatment. |
| [NCT03884452](https://clinicaltrials.gov/study/NCT03884452) | Phase 3 | Completed | 50 | Ezetimibe co-administered with Atorvastatin or Simvastatin in HoFH: evaluated efficacy and safety in patients with the most severe form of FH where monotherapy is typically insufficient. |
| [NCT00145574](https://clinicaltrials.gov/study/NCT00145574) | Phase 4 | Completed | 194 | Colesevelam in paediatric HeFH (aged 10–17) on stable background statin (including atorvastatin, lovastatin, simvastatin, pravastatin) or treatment-naïve: lipid-lowering effect and safety assessed in a well-designed RCT. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [9793596](https://pubmed.ncbi.nlm.nih.gov/9793596/) | 1998 | Clinical Review | Ann Pharmacother | Foundational review of atorvastatin efficacy and safety in primary hypercholesterolemia and mixed dyslipidemias; established dose-response relationship and favourable tolerability. |
| [28437620](https://pubmed.ncbi.nlm.nih.gov/28437620/) | 2017 | Clinical Guideline | Endocrine Practice | AACE/ACE comprehensive guidelines for dyslipidemia management and CVD prevention; recommends high-intensity statins (atorvastatin, rosuvastatin) as first-line for FH across age groups. |
| [27417002](https://pubmed.ncbi.nlm.nih.gov/27417002/) | 2016 | Observational Cohort | J Am Coll Cardiol | Statin therapy in HeFH: quantified significant reduction in coronary artery disease events and all-cause mortality in statin-treated vs. untreated FH patients in a large Dutch cohort. |
| [26988948](https://pubmed.ncbi.nlm.nih.gov/26988948/) | 2016 | Clinical Review | J Am Coll Cardiol | FH monitoring and care: identified gaps in cascade screening, LDL-C target achievement, and treatment intensification; reinforces atorvastatin as the therapeutic backbone. |
| [39751968](https://pubmed.ncbi.nlm.nih.gov/39751968/) | 2025 | Review | Curr Atheroscler Rep | Novel pharmacological therapies for HoFH: reviews emerging agents (inclisiran, evinacumab, lomitapide) and frames statins as indispensable foundation on which all newer therapies are layered. |
| [27678432](https://pubmed.ncbi.nlm.nih.gov/27678432/) | 2016 | Clinical Study | J Clin Lipidology | Three-year atorvastatin in children and adolescents (aged 6–17) with HeFH: demonstrated sustained LDL-C reduction and acceptable growth and safety profile over extended paediatric treatment. |
| [11383320](https://pubmed.ncbi.nlm.nih.gov/11383320/) | 2001 | Comparative Clinical | Nutr Metab Cardiovasc Dis | Atorvastatin vs. simvastatin in HeFH: atorvastatin achieved LDL-C NCEP targets more frequently and showed additional favourable effects on fibrinogen and coagulation variables. |
| [22957727](https://pubmed.ncbi.nlm.nih.gov/22957727/) | 2013 | Clinical Study | Echocardiography | Atorvastatin improves myocardial and peripheral blood flow reserve in FH patients without overt coronary atherosclerosis, demonstrating early pleiotropic vascular benefits beyond LDL-C lowering. |
| [35361995](https://pubmed.ncbi.nlm.nih.gov/35361995/) | 2022 | Genetic/Cohort | Pharmacogenomics J | Combined FH gene panel and statin pharmacogenomics strategy: supports tailored atorvastatin dosing in FH based on *SLCO1B1* and *CYP2C8* variant screening to minimise myopathy risk. |
| [36928267](https://pubmed.ncbi.nlm.nih.gov/36928267/) | 2023 | Observational Cohort | J Atheroscler Thromb | Real-world LDL-C goal achievement rates in high-risk patients in Japan: documents persistent under-treatment despite statin availability, supporting need for intensified therapy including higher-dose atorvastatin. |

---

## New Zealand Market Information

Atorvastatin is currently not registered with Medsafe and holds no market authorisations in New Zealand. No approved product data is available to tabulate.

> Note: Atorvastatin is approved in numerous other jurisdictions (USA, EU, Japan, Australia, Taiwan) across multiple indications including heterozygous and homozygous FH, primary hypercholesterolemia, mixed dyslipidemia, and cardiovascular risk reduction. A New Zealand Medsafe submission would be the appropriate next step.

---

## Safety Considerations

Please refer to the package insert for safety information.

> Note: Detailed warnings, contraindications, and drug-drug interaction data were not retrievable from the New Zealand regulatory database for this review. For clinical use, key monitoring considerations based on the statin class include: liver function tests at baseline, creatine kinase monitoring if myopathy symptoms develop, renal function assessment, and caution with CYP3A4 inhibitors (azole antifungals, macrolide antibiotics, protease inhibitors) which can markedly increase atorvastatin plasma exposure.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple completed Phase 3 RCTs, decades of real-world use, and endorsement from every major international lipid guideline confirm that atorvastatin is the first-line treatment for familial hypercholesterolemia. The evidence base is at the ceiling of clinical confidence (L1). The primary barrier to use in New Zealand is regulatory registration, not scientific uncertainty.

**To proceed, the following is needed:**
- Medsafe registration application, with a full regulatory dossier addressing New Zealand-specific requirements
- New Zealand-compliant package insert with local safety warnings and contraindications
- Mechanism of action documentation sourced from DrugBank for dossier completeness
- Formal drug-drug interaction review, particularly for concurrent use with CYP3A4 inhibitors (including HIV protease inhibitors, given the broad patient population who may require both statins and antiretrovirals)
- Paediatric prescribing guidance for HeFH in children (aged 6+), given available Phase 3 data
- Post-marketing pharmacovigilance plan for the New Zealand patient population, with LDL-C target monitoring aligned to current ACC/AHA or ESC/EAS goals
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


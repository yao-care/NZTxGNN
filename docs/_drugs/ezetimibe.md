---
layout: default
title: Ezetimibe
parent: 僅模型預測 (L5)
nav_order: 145
evidence_level: L5
indication_count: 4
---

# Ezetimibe
{: .fs-9 }

證據等級: **L5** | 預測適應症: **4** 個
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

# Ezetimibe: From Hypercholesterolemia to Hyperlipoproteinemia

## One-Sentence Summary

Ezetimibe is a selective intestinal cholesterol absorption inhibitor, globally established as a key lipid-lowering agent for hypercholesterolemia, though it has not yet obtained market registration in Taiwan.
The TxGNN model predicts it may be effective for **Hyperlipoproteinemia** — encompassing mixed hyperlipidemia with elevated LDL-cholesterol and triglycerides —
with **50 clinical trials** and **19 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypercholesterolemia (established global indication; no TFDA registration record available) |
| Predicted New Indication | Hyperlipoproteinemia |
| TxGNN Prediction Score | 99.63% |
| Evidence Level | L1 |
| Taiwan Market Status | 未上市 (Not marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Ezetimibe selectively inhibits the Niemann-Pick C1-Like 1 (NPC1L1) protein at the intestinal brush border, blocking the reabsorption of both dietary and biliary cholesterol. This mechanism reduces intestinal cholesterol absorption by approximately 50%, which lowers hepatic cholesterol stores, triggers compensatory upregulation of LDL receptors (LDL-R), and ultimately decreases plasma LDL-C by 15–20% as monotherapy. When combined with a statin — which inhibits cholesterol synthesis via HMG-CoA reductase — the additive effect yields a further 15–20% LDL-C reduction, making Ezetimibe particularly valuable for patients unable to reach lipid targets on statin monotherapy alone. This NPC1L1/LDL-R axis is the core mechanistic rationale underlying all evidence presented below.

Hyperlipoproteinemia is a broad diagnostic category defined by pathologically elevated blood lipoproteins, encompassing mixed hyperlipidemia (raised LDL-C plus raised triglycerides; Type IIb), primary hypercholesterolemia (Type IIa), and genetic subtypes such as heterozygous and homozygous familial hypercholesterolemia (HeFH, HoFH). Ezetimibe's intestinal absorption-blocking mechanism directly targets the elevated LDL-C component shared across all these subtypes, while its combination with fenofibrate has been explicitly trialled in mixed dyslipidemia patients with elevated both LDL-C and triglycerides — precisely the population captured by "hyperlipoproteinemia." The TxGNN prediction score of 99.63% reflects this strong mechanistic and clinical alignment.

The evidence base is exceptionally robust by repurposing standards: multiple large-scale completed Phase 3 RCTs directly evaluate Ezetimibe as the primary intervention or active comparator across the hyperlipoproteinemia spectrum, and the drug is embedded as a standard second-line agent in ESC/EAS, ACC/AHA, and Japanese lipid guidelines. The current gap — no Taiwan TFDA registration — reflects a regulatory rather than a scientific uncertainty.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00093899](https://clinicaltrials.gov/study/NCT00093899) | Phase 3 | Completed | 611 | Large-scale RCT evaluating efficacy and safety of Ezetimibe/Simvastatin in combination with fenofibrate in patients with mixed hyperlipidemia (elevated cholesterol and triglycerides); highest-grade direct evidence for the hyperlipoproteinemia indication |
| [NCT01763827](https://clinicaltrials.gov/study/NCT01763827) | Phase 3 | Completed | 615 | Double-blind, randomized, placebo- and ezetimibe-controlled trial evaluating 12 weeks of evolocumab monotherapy; ezetimibe served as the primary active comparator for LDL-C lowering in hypercholesterolemic adults |
| [NCT01043380](https://clinicaltrials.gov/study/NCT01043380) | Phase 4 | Completed | 245 | IVUS (Plaque REgression study): head-to-head comparison of cholesterol absorption inhibitor (ezetimibe) vs. cholesterol synthesis inhibitor on coronary plaque regression; provides clinically meaningful surrogate endpoint data |
| [NCT00092573](https://clinicaltrials.gov/study/NCT00092573) | Phase 3 | Completed | 587 | Evaluated cholesterol-lowering safety and effectiveness of fenofibrate + ezetimibe coadministration in patients with mixed hyperlipidemia; direct evidence for combination approach in Type IIb dyslipidemia |
| [NCT00552097](https://clinicaltrials.gov/study/NCT00552097) | Phase 3 | Completed | 720 | ENHANCE Trial: Ezetimibe + high-dose simvastatin vs. simvastatin alone on carotid intima-media thickness (CIMT) progression in HeFH subjects; landmark carotid imaging RCT |
| [NCT00349284](https://clinicaltrials.gov/study/NCT00349284) | Phase 3 | Completed | 181 | Double-blind RCT comparing fenofibrate 145 mg, ezetimibe 10 mg, and their combination in patients with Type IIb dyslipidemia and metabolic syndrome; directly addresses the lipid triad (elevated TG, LDL-C, low HDL-C) |
| [NCT06005597](https://clinicaltrials.gov/study/NCT06005597) | Phase 3 | Completed | 407 | Placebo-controlled RCT of obicetrapib 10 mg + ezetimibe 10 mg fixed-dose combination on top of maximally tolerated lipid therapy in HeFH and/or ASCVD patients; recent evidence for ezetimibe in combination strategies |
| [NCT03884452](https://clinicaltrials.gov/study/NCT03884452) | Phase 3 | Completed | 50 | Efficacy and safety study of ezetimibe (SCH58235) 10 mg added to atorvastatin or simvastatin in homozygous familial hypercholesterolemia; foundational Phase 3 trial establishing ezetimibe's FH evidence base |
| [NCT00092833](https://clinicaltrials.gov/study/NCT00092833) | Phase 3 | Terminated | 49 | Treatment-use study providing ezetimibe 10 mg/day to HoFH and sitosterolemia patients; despite termination, provides mechanistic and safety reference data for severe hyperlipoproteinemia |
| [NCT04433533](https://clinicaltrials.gov/study/NCT04433533) | Phase 4 | Unknown | 200 | Randomized head-to-head study comparing rosuvastatin/ezetimibe combination vs. rosuvastatin monotherapy in Korean patients with LV diastolic dysfunction and hyperlipidemia; real-world combination therapy evaluation |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [40347969](https://pubmed.ncbi.nlm.nih.gov/40347969/) | 2025 | Phase 2/3 RCT | Lancet | TANDEM trial: fixed-dose combination of obicetrapib (CETP inhibitor) and ezetimibe 10 mg demonstrated significant LDL-C reduction vs. placebo in patients with hypercholesterolemia on background statin therapy |
| [41206969](https://pubmed.ncbi.nlm.nih.gov/41206969/) | 2026 | Phase 3 RCT | JAMA | Oral PCSK9 inhibitor enlicitide RCT in HeFH; highlights persistent unmet need where ezetimibe is standard background therapy and benchmark for LDL-C lowering |
| [23956253](https://pubmed.ncbi.nlm.nih.gov/23956253/) | 2013 | Guideline/Consensus | European Heart Journal | EAS consensus on familial hypercholesterolemia: formally establishes ezetimibe as the recommended second-line lipid-lowering agent in combination with statins for coronary heart disease prevention |
| [25939291](https://pubmed.ncbi.nlm.nih.gov/25939291/) | 2015 | Review | Cardiology Clinics | Comprehensive FH review listing ezetimibe alongside statins, bile acid sequestrants, and LDL apheresis as evidence-based treatment options; early treatment reduces cardiovascular events |
| [29219151](https://pubmed.ncbi.nlm.nih.gov/29219151/) | 2017 | Review | Nature Reviews Disease Primers | Disease primer on familial hypercholesterolaemia covering genetics (LDLR, APOB, PCSK9 mutations), pathophysiology, and full treatment landscape including ezetimibe's mechanistic role |
| [34480646](https://pubmed.ncbi.nlm.nih.gov/34480646/) | 2021 | Review | Current Cardiology Reports | Global burden and management of FH; emphasizes ezetimibe's role in achieving LDL-C targets in both heterozygous and homozygous patients in line with recent guideline recommendations |
| [37762244](https://pubmed.ncbi.nlm.nih.gov/37762244/) | 2023 | Review | Int J Mol Sci | Postprandial hyperlipidemia pathophysiology and treatments; discusses ezetimibe's effect on postprandial lipemia, particularly relevant for mixed dyslipidemia patients |
| [40682836](https://pubmed.ncbi.nlm.nih.gov/40682836/) | 2025 | Review | Molecular Medicine Reports | Current drug targets for hyperlipidemia including ezetimibe; covers NPC1L1-mediated mechanism and its positioning alongside statins, PCSK9 inhibitors, and bempedoic acid |
| [35593194](https://pubmed.ncbi.nlm.nih.gov/35593194/) | 2022 | Review | J Cardiovasc Pharmacol Ther | Comprehensive PCSK9 inhibitor review; positions ezetimibe as the standard bridge therapy for statin-intolerant and inadequately controlled hypercholesterolemia patients |
| [18376001](https://pubmed.ncbi.nlm.nih.gov/18376001/) | 2008 | Editorial | N Engl J Med | Landmark NEJM editorial on cholesterol lowering and ezetimibe following early ENHANCE controversy; pivotal in shaping clinical interpretation of ezetimibe's LDL-C vs. outcomes data |

---

## Taiwan Market Information

Ezetimibe is currently not registered in Taiwan (台灣未上市; TFDA authorization count: 0). No approved product licenses or package inserts are available from the TFDA database at this time.

For reference, Ezetimibe is approved and commercially available in numerous major regulatory jurisdictions under the following product names:

| Jurisdiction | Brand Name | Dosage Form |
|------|------|------|
| USA (FDA) | Zetia® | 10 mg oral tablet |
| USA/Global (FDA/EMA) | Vytorin® (ezetimibe/simvastatin FDC) | 10/10, 10/20, 10/40, 10/80 mg tablets |
| EU/Global (EMA) | Ezetrol® | 10 mg oral tablet |
| Japan (PMDA) | Zetia® (ゼチーア) | 10 mg oral tablet |
| Korea | Ezetrol® | 10 mg oral tablet |

These approvals cover primary hypercholesterolemia, mixed hyperlipidemia, homozygous familial hypercholesterolemia, and homozygous sitosterolemia.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The evidence base for Ezetimibe in hyperlipoproteinemia is exceptionally strong (L1: multiple completed Phase 3 RCTs, international guideline endorsement, decades of post-marketing real-world data), and the TxGNN prediction score of 99.63% aligns with the drug's well-established clinical role; the primary barrier to use in Taiwan is the absence of TFDA registration rather than any scientific uncertainty about efficacy or safety.

**To proceed, the following is needed:**
- **TFDA regulatory filing**: Initiate Taiwan new drug registration referencing existing FDA and EMA approvals; cross-reference Phase 3 data from NCT00093899, NCT00552097, and NCT03884452 as core submission evidence
- **Taiwan-bridging pharmacokinetics**: Determine whether TFDA requires a bridging PK study in Taiwanese patients or accepts reference country data equivalence
- **Package insert (仿單) preparation**: Compile complete TFDA-format warnings, contraindications, drug interaction profile (particularly cyclosporine, fibrates, and colesevelam interactions), and special population guidance (hepatic impairment, pregnancy)
- **Formal MOA documentation**: Provide structured NPC1L1 mechanistic summary for TFDA scientific review dossier
- **Safety monitoring plan**: Establish protocol for liver function tests, myopathy surveillance (especially with concomitant statin), and post-marketing pharmacovigilance in the Taiwan population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


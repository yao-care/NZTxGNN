---
layout: default
title: Levocarnitine
parent: Model Prediction Only (L5)
nav_order: 269
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Levocarnitine: predicted new indication Autosomal dominant familial hematuria-retinal arteriolar tortuosity-c… (evidence level L5). Approved use: Not recorded in the New Zealand license data (all ind…"
monitor_inn: "LEVOCARNITINE"
predicted_indications: ["autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome", "brain small vessel disease 1 with or without ocular anomalies", "diabetic nephropathy", "rheumatoid arthritis", "sclerosing cholangitis", "gout", "brachydactyly-syndactyly syndrome", "colobomatous microphthalmia-rhizomelic dysplasia syndrome", "congestive heart failure", "hypoalphalipoproteinemia"]
---

# Levocarnitine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Levocarnitine: From Carnitine Supplementation to Autosomal Dominant Familial Hematuria-Retinal Arteriolar Tortuosity-Contractures Syndrome
{: .text-alpha }

## One-Sentence Summary

Levocarnitine is the active form of carnitine, which carries fatty acids into mitochondria for energy production. The data compiled for this report do not record an approved indication.
The TxGNN model's top-ranked prediction is **autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome**, a rare COL4A1-related disorder, but it has **0 clinical trials** and **0 publications** behind it, so it is a model prediction only. Among the other ten predicted indications, **congestive heart failure** has the strongest support, with **2 clinical trials** and **11 publications**.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the New Zealand license data (all indication fields are empty) |
| Predicted New Indication | Autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licenses) |
| Number of Authorizations | 6 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the compiled data. Based on general pharmacology, levocarnitine transports long-chain fatty acids into mitochondria for beta-oxidation and supports cellular energy metabolism.

The top-ranked predicted disease is a rare vascular basement membrane disorder caused by COL4A1 variants. It has no clear link to carnitine-mediated fatty acid transport or mitochondrial energy metabolism. The high score is likely a knowledge-graph artifact driven by shared gene or phenotype neighbors rather than a true pharmacological relationship. The second-ranked prediction, brain small vessel disease 1 (also COL4A1-related), has the same weakness. A mitochondrial-support or antioxidant rationale there is speculative.

## Clinical Trial Evidence

Currently no related clinical trials registered for the top-ranked indication.

## Literature Evidence

Currently no related literature available for the top-ranked indication.

## Other Predicted Indications With Evidence

Because the top prediction has no supporting evidence, the compiled data were also reviewed for the other nine predictions. Only three have any evidence.

| Rank | Indication | Score | Evidence Level | Data-Based Recommendation |
|------|------|------|------|------|
| 9 | Congestive heart failure | 99.47% | L2 | Proceed with Guardrails |
| 4 | Rheumatoid arthritis | 99.87% | L2 | Research Question |
| 3 | Diabetic nephropathy | 99.91% | L4 | Research Question |

The remaining predictions have no clinical or literature evidence (L5, Hold): brain small vessel disease 1, sclerosing cholangitis, gout, brachydactyly-syndactyly syndrome, colobomatous microphthalmia-rhizomelic dysplasia syndrome, and hypoalphalipoproteinemia.

### Congestive Heart Failure (strongest evidence)

Carnitine moves long-chain fatty acids into mitochondria, and myocardial carnitine depletion is associated with impaired cardiac energy metabolism. Supplementation may therefore improve myocardial energy supply and cardiac function.

**Clinical trials**

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01580553](https://clinicaltrials.gov/study/NCT01580553) | Phase 2/3 | Completed | 268 | Randomized, double-blind, placebo-controlled multicenter study of L-carnitine injection in Chinese heart failure patients (7-day treatment, 1-month follow-up). Results were not provided. |
| [NCT07201714](https://clinicaltrials.gov/study/NCT07201714) | Early Phase 1 | Recruiting | 20 | Oral L-carnitine in cardiorenal heart failure, assessing symptoms, heart function and quality of life. Exploratory; no results yet. |

**Literature**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [29468383](https://pubmed.ncbi.nlm.nih.gov/29468383/) | 2018 | RCT | Paediatric Drugs | Levocarnitine added to conventional therapy in 29 children with dilated cardiomyopathy |
| [40172612](https://pubmed.ncbi.nlm.nih.gov/40172612/) | 2025 | Meta-analysis | Int Urol Nephrol | Safety and efficacy of L-carnitine for cardiac function in hemodialysis patients |
| [30195728](https://pubmed.ncbi.nlm.nih.gov/30195728/) | 2020 | Clinical study | Hellenic J Cardiol | Effects on cardiac function, urinary albumin, hs-CRP, BNP and troponin in coronary heart disease with heart failure |
| [24125620](https://pubmed.ncbi.nlm.nih.gov/24125620/) | 2013 | Clinical study | Zhonghua Liu Xing Bing Xue Za Zhi | Effects on heart function and endocrine markers in elderly patients with chronic heart failure |
| [30041211](https://pubmed.ncbi.nlm.nih.gov/30041211/) | 2018 | Clinical study | Contrib Nephrol | Levocarnitine and cardiac function and renal anemia in hemodialysis patients |
| [15591005](https://pubmed.ncbi.nlm.nih.gov/15591005/) | 2004 | Review | Ann N Y Acad Sci | Carnitine and propionyl-L-carnitine in cardiovascular disease; reduces ischemia-reperfusion injury and improves carbohydrate metabolism |
| [30646314](https://pubmed.ncbi.nlm.nih.gov/30646314/) | 2018 | RCT | JAMA Netw Open | Levocarnitine in septic shock (RACE trial); indirect relevance to heart failure |

Several other retrieved papers are off-topic: a stem-cell cardiomyopathy model and metabolic disease case reports. They are not counted as supporting evidence. Much of the relevant literature comes from small or regional studies, and outcome data for the Phase 2/3 trial are not available. Level L2 is therefore assigned conservatively, and the effect direction and size remain unverified.

### Rheumatoid Arthritis and Diabetic Nephropathy

- **Rheumatoid arthritis:** The only evidence is [NCT03953703](https://clinicaltrials.gov/study/NCT03953703), a completed Phase 2 randomized, placebo-controlled crossover pilot (n=15). Its title refers to dry eye in Sjögren's syndrome, so its direct relevance to rheumatoid arthritis should be checked. No results were provided.
- **Diabetic nephropathy:** The only evidence is [PMID 41884608](https://pubmed.ncbi.nlm.nih.gov/41884608/) (2026), which tested a levocarnitine-alprostadil combination in end-stage diabetic nephropathy. The effect of levocarnitine alone cannot be isolated, and no trials are registered.

## New Zealand Market Information

All six licenses are cancelled, and none are currently valid.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2609266 | Balance | Capsule 500 mg | Cancelled |
| 2502151 | Solgar | Tablet 500 mg | Cancelled |
| 2609274 | Solgar | Capsule 250 mg | Cancelled |
| 2439816 | Balance | Oral liquid 500 mg per 10 ml | Cancelled |
| 2711281 | Lacuna | Oral liquid 1 g per 10 ml | Cancelled |

The sixth license is 2683059 (Metabolics, Capsule 500 mg, cancelled). Approved indication text is not recorded for any license. Available forms are oral only. The heart failure trial above used an injectable form, and no injectable product appears in the New Zealand license data.

## Safety Considerations

- **Drug Interactions:** 68 interactions were identified. Two are rated Moderate: **warfarin** and **dicoumarol**, so anticoagulation monitoring is warranted. The remaining interactions listed have unclassified severity, including calcitriol, folic acid, ramipril, nicotine, lorazepam, fluconazole, pantoprazole, citalopram, metoprolol, topiramate, valproic acid, acetaminophen, omeprazole, diltiazem, sucralfate, mirtazapine, amlodipine and digoxin.
- **Knowledge-graph contraindication signals** (computational associations, not label text): epilepsy, kidney disease, pyoureter, liver disease, gallbladder disease, hypotensive disorder, blood coagulation disease, cholestasis, benign recurrent intrahepatic cholestasis and familial intrahepatic cholestasis. These need confirmation against the package insert.

Please refer to the package insert for label warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction has no clinical or literature evidence (L5) and no plausible mechanistic link to carnitine metabolism, so it is most likely a knowledge-graph artifact. No New Zealand license is currently valid, and the package insert safety data are missing.

Congestive heart failure is the most promising alternative direction (L2, Proceed with Guardrails on the data alone). It would still need verified trial results before any advance.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications, which block safety screening
- Mechanism-of-action data from DrugBank
- Published results and design details (blinding, endpoints) for NCT01580553, to decide whether heart failure can be upgraded to L1
- Confirmation of the original approved indication
- A route and formulation plan, since an injectable product would be needed and none is currently marketed in New Zealand
- A warfarin and anticoagulant interaction management plan
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


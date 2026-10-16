---
layout: default
title: Rosuvastatin
parent: Model Prediction Only (L5)
nav_order: 413
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Rosuvastatin: predicted new indication Cholesterol-ester transfer protein deficiency (evidence level L5). Approved use: Hypertriglyceridaemia, stroke, cerebrovascular disorder…"
---

# Rosuvastatin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Rosuvastatin: From Hyperlipidaemia and Atherosclerotic Disease to Cholesterol-Ester Transfer Protein Deficiency

## One-Sentence Summary

Rosuvastatin is a statin originally used for lipid disorders and atherosclerotic disease, including hypertriglyceridaemia, familial hypercholesterolaemia and stroke prevention.
The TxGNN model's top-ranked prediction is **cholesterol-ester transfer protein (CETP) deficiency**, but this prediction has **no clinical trials and no publications** behind it (Evidence Level L5).
The better-supported predictions are **familial hypercholesterolaemia** (13 trials, 12 publications) and **hyperlipidaemia** (more than 35 trials, 20 publications), although both largely overlap with the drug's existing indications.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertriglyceridaemia, stroke, cerebrovascular disorder, cerebral infarction, atherosclerosis, familial combined hyperlipidaemia, hyperlipoproteinaemia, homozygous familial hypercholesterolaemia (the New Zealand licence records contain no approved-indication text) |
| Predicted New Indication | Cholesterol-ester transfer protein deficiency (top-ranked prediction) |
| TxGNN Prediction Score | 99.54% |
| Evidence Level | L5 (top prediction); L1 for familial hypercholesterolaemia and hyperlipidaemia |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 8 (all cancelled) |
| Recommended Decision | Hold (top prediction); Proceed with Guardrails for familial hypercholesterolaemia and hyperlipidaemia |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the data compiled for this report. Rosuvastatin is, however, a well-known statin (HMG-CoA reductase inhibitor). It reduces hepatic cholesterol synthesis and upregulates LDL receptors, which lowers LDL cholesterol.

**CETP deficiency (rank 1):** CETP deficiency raises HDL cholesterol and alters LDL metabolism, so a lipid-pathway link with rosuvastatin is plausible. However, it is generally a benign, or even potentially protective, lipid phenotype that rarely needs treatment, and no trials or literature exist. This is a model prediction only.

**Other predictions worth noting:**
- **Familial hypercholesterolaemia (rank 2) and hyperlipidaemia (rank 10):** These are mechanistically direct and well supported, but they overlap with the drug's existing indications. They are closer to established use than true repurposing.
- **HIV infection (rank 5):** Rosuvastatin would act as a cardiovascular-risk and inflammation-reducing therapy in people with HIV, not as an antiviral. Evidence is mixed.
- **Hypercholesterolaemia due to CYP7A1 deficiency, brain stem infarction, hypoalphalipoproteinaemia, hepatic lipase deficiency:** The links are weak or indirect, and the evidence is limited to mechanistic or preclinical studies, reviews and case reports.
- **Neurodevelopmental disorder with ataxic gait and ABri amyloidosis:** No plausible mechanistic link is evident.

---

## Clinical Trial Evidence

The top-ranked prediction (CETP deficiency) has no registered clinical trials. The table below covers the best-supported prediction, **familial hypercholesterolaemia (rank 2)**, showing the most relevant trials.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00654602](https://clinicaltrials.gov/study/NCT00654602) | Phase 3 | Completed | 1500 | 48-week open-label study of rosuvastatin efficacy and safety in Type IIa/IIb dyslipidaemia, including heterozygous FH |
| [NCT02226198](https://clinicaltrials.gov/study/NCT02226198) | Phase 3 | Completed | 20 | Randomised, double-blind, placebo-controlled crossover study in children with homozygous FH |
| [NCT02434497](https://clinicaltrials.gov/study/NCT02434497) | Phase 3 | Completed | 9 | Open-label long-term extension of the paediatric homozygous FH study, assessing safety |
| [NCT01078675](https://clinicaltrials.gov/study/NCT01078675) | Phase 3 | Completed | 315 | Open-label 2-year efficacy and safety study in children and adolescents with FH |
| [NCT00355615](https://clinicaltrials.gov/study/NCT00355615) | Phase 3 | Completed | 173 | Double-blind, placebo-controlled study in children aged 10–17 with heterozygous FH, followed by 40 weeks open-label |
| [NCT00654446](https://clinicaltrials.gov/study/NCT00654446) | Phase 3 | Completed | 442 | 6-week randomised comparison of rosuvastatin and simvastatin on renal effects in dyslipidaemia, including heterozygous FH |
| [NCT06910098](https://clinicaltrials.gov/study/NCT06910098) | N/A | Completed | 195 | Dose effects of rosuvastatin on LDL, CK and AST |
| [NCT02748057](https://clinicaltrials.gov/study/NCT02748057) | Phase 3 | Completed | 135 | Long-term safety of ezetimibe plus low-dose rosuvastatin in Japanese patients with hypercholesterolaemia |
| [NCT07474649](https://clinicaltrials.gov/study/NCT07474649) | Phase 3 | Not yet recruiting | 103 | Bempedoic acid/ezetimibe/high-intensity statin on coronary plaque; rosuvastatin is a background component |

Note that NCT01709500, NCT03510884 and NCT03510715 are alirocumab trials, and NCT01890967 tests LY3015014, so they are not direct evidence for rosuvastatin.

---

## Literature Evidence

Evidence for **familial hypercholesterolaemia (rank 2)**:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [20223367](https://pubmed.ncbi.nlm.nih.gov/20223367/) | 2010 | RCT | J Am Coll Cardiol | Efficacy and safety of rosuvastatin in children with familial hypercholesterolaemia |
| [28838366](https://pubmed.ncbi.nlm.nih.gov/28838366/) | 2017 | Cohort | J Am Coll Cardiol | Rosuvastatin efficacy in children with homozygous FH and its association with underlying genetic mutations |
| [12269853](https://pubmed.ncbi.nlm.nih.gov/12269853/) | 2002 | Review | Drugs | Rosuvastatin superior to atorvastatin, simvastatin and pravastatin in lipid improvement; low propensity for drug interactions |
| [15531000](https://pubmed.ncbi.nlm.nih.gov/15531000/) | 2004 | Review | Clin Ther | Rosuvastatin in the management of hyperlipidaemia, including homozygous FH |
| [26387811](https://pubmed.ncbi.nlm.nih.gov/26387811/) | 2016 | Pharmacokinetic study | Eur J Clin Pharmacol | Population pharmacokinetics in children with heterozygous FH |
| [40254247](https://pubmed.ncbi.nlm.nih.gov/40254247/) | 2025 | Preclinical (iPSC) | Toxicology | Lipophilic versus hydrophilic statin myotoxicity in muscle cells from FH patients |
| [20531065](https://pubmed.ncbi.nlm.nih.gov/20531065/) | 2010 | Case report | South Med J | Rosuvastatin-induced severe thrombocytopenia |
| [24849689](https://pubmed.ncbi.nlm.nih.gov/24849689/) | 2014 | Review | Consult Pharm | Statin-associated incident diabetes |

For **hyperlipidaemia (rank 10)**, key sources include the STELLAR trial ([12860216](https://pubmed.ncbi.nlm.nih.gov/12860216/), 2003, RCT, rosuvastatin versus atorvastatin, simvastatin and pravastatin across doses) and a Cochrane review of rosuvastatin's lipid-lowering efficacy ([25415541](https://pubmed.ncbi.nlm.nih.gov/25415541/), 2014).

---

## New Zealand Market Information

All eight authorisations are cancelled. No approved-indication text is recorded for any of them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2716712 | Rosuvastatin - Sandoz | Tab 5 mg | Not recorded |
| 2716720 | Rosuvastatin - Sandoz | Tab 10 mg | Not recorded |
| 2716739 | Rosuvastatin - Sandoz | Tab 20 mg | Not recorded |
| 2716747 | Rosuvastatin - Sandoz | Tab 40 mg | Not recorded |
| 2651130 | Rosuvastatin Viatris | Tab 5 mg | Not recorded |

The remaining three authorisations (Rosuvastatin Viatris 10 mg, 20 mg and 40 mg tablets) are also cancelled.

---

## Safety Considerations

- **Contraindication-related conditions** (from the knowledge graph, not package insert text): liver disease, gallbladder disease, hypothyroidism, myxedema, congenital hypothyroidism, myopathy, muscular disease, skeletal muscle disease, familial periodic paralysis, substance abuse/dependence, rhabdomyolysis, nodular fasciitis, injury, proteinuria.
- **Drug Interactions:** 100 interactions are known. Examples:
  - **Major:** fenofibrate, atazanavir.
  - **Moderate:** atorvastatin, ezetimibe, bempedoic acid, amiodarone, paclitaxel, adalimumab, alpelisib, apalutamide, ethanol, zidovudine, among others.
  - **Minor:** levonorgestrel, norethisterone, desogestrel.
- **Literature-reported concerns:** rare severe thrombocytopenia (case report), a possible association with new-onset diabetes, and muscle symptoms. One RCT in men with HIV found no arterial stiffness benefit and possible cardiometabolic adverse events.
- **Antiretroviral interactions:** Phase 1 trials with darunavir/ritonavir, lopinavir/ritonavir, tipranavir/ritonavir and fostemsavir are relevant to dosing in HIV.

Package insert warnings and contraindications were not available for this report; please consult the package insert.

---

## Conclusion and Next Steps

**Decision: Hold** (for the top-ranked prediction, CETP deficiency); **Proceed with Guardrails** for familial hypercholesterolaemia and hyperlipidaemia.

**Rationale:**
- The top prediction (CETP deficiency, score 99.54%) is supported only by the model. It has no trials or publications, and the condition rarely needs treatment.
- Familial hypercholesterolaemia and hyperlipidaemia have strong evidence (Level L1: multiple completed Phase 3 trials, including paediatric RCTs), but they overlap with the drug's established uses rather than representing new repurposing. HIV-related cardiovascular risk reduction is a reasonable research question (L2), with mixed results.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking data gap for safety screening).
- Mechanism-of-action data from DrugBank.
- Verification of product availability in New Zealand, since all eight authorisations are cancelled.
- Safety guardrails: myopathy and CK monitoring, renal and hepatic function checks, paediatric dosing and growth monitoring, consideration of Asian-ancestry dosing, and a review of drug interactions.
- Before any evaluation of CETP deficiency, a clinical rationale showing that treatment is needed and a mechanistic assessment.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


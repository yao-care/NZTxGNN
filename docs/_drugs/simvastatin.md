---
layout: default
title: Simvastatin
parent: Model Prediction Only (L5)
nav_order: 424
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
monitor_inn: "SIMVASTATIN"
predicted_indications: ["brain stem infarction", "cholesterol-ester transfer protein deficiency", "HIV infectious disease", "ABri amyloidosis", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter"]
description: "Simvastatin: predicted new indication Brain stem infarction (evidence level L5). Approved use: Not recorded in the local data"
---

# Simvastatin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

## Simvastatin: Brain stem infarction
{: .text-alpha }

## One-Sentence Summary

Simvastatin is a statin originally used for lipid disorders (such as hypertriglyceridemia, homozygous familial hypercholesterolemia and hyperlipoproteinemia) and for atherosclerosis-related conditions such as stroke and cerebral infarction.
Note that this prediction overlaps heavily with the existing labelled use, so it is closer to an on-label or near-label use than a true repurposing.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Brain stem infarction |
| TxGNN Prediction Score | 0.9941 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 5 of 5) |
| Number of Authorizations | 5 (valid: 5) |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, simvastatin is an HMG-CoA reductase inhibitor. It lowers cholesterol synthesis in the liver and upregulates LDL receptors, which increases clearance of LDL cholesterol. This mechanism is directly relevant to familial hypercholesterolemia, where LDL clearance is impaired.

The original indications already include homozygous familial hypercholesterolemia and several related hyperlipidaemias. Many patients still need add-on therapy (for example ezetimibe or PCSK9 inhibitors) to reach LDL-C targets, which is reflected in most of the trials below.

Other predictions from the model are weaker. Brain stem infarction is a subtype of an existing indication with no trials. Several rare conditions (CETP deficiency, cholesterol 7alpha-hydroxylase deficiency, ABri amyloidosis, a rare neurodevelopmental disorder) are model predictions only, with no trials or literature. The HIV prediction concerns drug interactions and HIV-associated dyslipidaemia rather than antiviral activity, and one pilot study found no anti-HIV effect of simvastatin.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00552097](https://clinicaltrials.gov/study/NCT00552097) | Phase 3 | Completed | 720 | ENHANCE: ezetimibe plus high-dose simvastatin vs simvastatin alone on carotid atherosclerosis progression in heterozygous FH |
| [NCT00129402](https://clinicaltrials.gov/study/NCT00129402) | Phase 3 | Completed | 248 | Ezetimibe coadministered with simvastatin vs simvastatin alone in adolescents (10-17 years) with heterozygous FH |
| [NCT03884452](https://clinicaltrials.gov/study/NCT03884452) | Phase 3 | Completed | 50 | Efficacy and safety of ezetimibe 10 mg added to atorvastatin or simvastatin in homozygous FH |
| [NCT03885921](https://clinicaltrials.gov/study/NCT03885921) | Phase 3 | Completed | 44 | 24-month open-label safety extension of ezetimibe added to atorvastatin or simvastatin in homozygous FH |
| [NCT01709500](https://clinicaltrials.gov/study/NCT01709500) | Phase 3 | Completed | 249 | Alirocumab vs placebo in heterozygous FH on lipid-modifying therapy (simvastatin role unclear, likely background) |
| [NCT00654446](https://clinicaltrials.gov/study/NCT00654446) | Phase 3 | Completed | 442 | Renal effects of rosuvastatin vs simvastatin over 6 weeks in type IIa/IIb dyslipidaemia, including heterozygous FH |
| [NCT03510884](https://clinicaltrials.gov/study/NCT03510884) | Phase 3 | Completed | 153 | Alirocumab in children and adolescents with heterozygous FH on statin background (simvastatin not the study drug) |
| [NCT03510715](https://clinicaltrials.gov/study/NCT03510715) | Phase 3 | Completed | 18 | Open-label alirocumab in children and adolescents with homozygous FH on background treatment |
| [NCT01890967](https://clinicaltrials.gov/study/NCT01890967) | Phase 2 | Completed | 527 | Dose-ranging study of LY3015014 in primary hypercholesterolemia on stable statin therapy |

Only two trials (NCT00552097 and NCT00129402) directly evaluate simvastatin as part of the tested regimen; the others use simvastatin as background therapy.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [18376000](https://pubmed.ncbi.nlm.nih.gov/18376000/) | 2008 | RCT | N Engl J Med | Simvastatin with or without ezetimibe in FH (ENHANCE), examining progression of atherosclerosis |
| [31696945](https://pubmed.ncbi.nlm.nih.gov/31696945/) | 2019 | Cochrane systematic review | Cochrane Database Syst Rev | Statins for children with familial hypercholesterolemia |
| [28685504](https://pubmed.ncbi.nlm.nih.gov/28685504/) | 2017 | Cochrane systematic review | Cochrane Database Syst Rev | Earlier update of the Cochrane review on statins in children with FH |
| [12908847](https://pubmed.ncbi.nlm.nih.gov/12908847/) | 2003 | Review | Drug Safety | Benefits and risks of simvastatin in FH, with emphasis on long-term safety |
| [15794711](https://pubmed.ncbi.nlm.nih.gov/15794711/) | 2005 | Review | Expert Opin Drug Saf | Benefits and risk assessment of simvastatin in FH |
| [18940534](https://pubmed.ncbi.nlm.nih.gov/18940534/) | 2008 | Clinical study | J Am Coll Cardiol | Efficacy and safety of long-term ezetimibe plus simvastatin in adolescents with heterozygous FH |
| [27417002](https://pubmed.ncbi.nlm.nih.gov/27417002/) | 2016 | Clinical study | J Am Coll Cardiol | Statin-related reduction in coronary artery disease events and mortality in heterozygous FH |
| [41824590](https://pubmed.ncbi.nlm.nih.gov/41824590/) | 2026 | Guideline | J Am Coll Cardiol | 2026 ACC/AHA multisociety dyslipidemia guideline, replacing the 2018 cholesterol guideline |
| [11383320](https://pubmed.ncbi.nlm.nih.gov/11383320/) | 2001 | Clinical study | Nutr Metab Cardiovasc Dis | Atorvastatin vs simvastatin in heterozygous FH for reaching LDL-C goals |
| [2679083](https://pubmed.ncbi.nlm.nih.gov/2679083/) | 1989 | Clinical study | Am J Med | Early efficacy data: about 38% LDL-C reduction with 20 mg and 42% with 40 mg in FH |

---

## New Zealand Market Information


| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2643596 | Simvastatin Viatris | Tab 80 mg | Not recorded (valid) |
| 2520087 | Simvastatin Mylan | Tab 10 mg | Not recorded (valid) |
| 2643588 | Simvastatin Viatris | Tab 40 mg | Not recorded (valid) |
| 2643561 | Simvastatin Viatris | Tab 20 mg | Not recorded (valid) |
| 2643553 | Simvastatin Viatris | Tab 10 mg | Not recorded (valid) |

---

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: The knowledge graph links simvastatin to caution in liver disease, pancreatitis, gallbladder disease, myopathy and other muscle diseases, rhabdomyolysis, diabetes and hyperglycemia, thrombocytopenia, blood coagulation disease, gout, peptic ulcer disease, and hypotensive disorder, among others. These should be verified against the official label.
- **Drug Interactions**: The interaction query returned 100 entries. Major-level interactions include **amiodarone**, **amlodipine** and **fenofibrate**. Moderate-level interactions include zidovudine, paclitaxel, nifedipine, adalimumab, omeprazole, aprepitant, apalutamide, alpelisib and ethanol.
- **HIV-related interaction risk**: Protease inhibitors and cobicistat-boosted regimens inhibit CYP3A4 and markedly raise simvastatin exposure, increasing the risk of myopathy and rhabdomyolysis (a case report describes biopsy-proven myopathy with Genvoya plus simvastatin). Simvastatin is generally avoided with these regimens.

Please refer to the package insert for full warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple completed Phase 3 trials and a Cochrane review support simvastatin-based therapy in familial hypercholesterolemia, and the use largely overlaps with existing indications.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (currently missing and a blocking gap)
- Detailed mechanism of action data
- A drug-interaction management plan, particularly for amiodarone, amlodipine, fenofibrate and CYP3A4 inhibitors
- Confirmation of which trials contribute simvastatin-specific efficacy data
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


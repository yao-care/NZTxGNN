---
layout: default
title: Rosuvastatin
parent: 僅模型預測 (L5)
nav_order: 313
evidence_level: L5
indication_count: 10
---

# Rosuvastatin
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

# Rosuvastatin: From Hyperlipidemia to Familial Hypercholesterolemia

## One-Sentence Summary

Rosuvastatin is a high-potency HMG-CoA reductase inhibitor (statin) whose core, well-established use is hyperlipidemia/hypercholesterolemia. This evidence pack screened **10 TxGNN-predicted indications**; among them, **Familial Hypercholesterolemia (FH)** shows by far the strongest and most clinically coherent signal, supported by **24 clinical trials** (including multiple completed Phase 3 RCTs of rosuvastatin itself in pediatric HoFH/HeFH) and **13 publications**. The TxGNN top-ranked candidate (cholesterol-ester transfer protein deficiency) was not selected as the headline finding here — its cited literature concerns unrelated ApoA-I/hepatic lipase deficiency case reports and the pack's own rationale flags it as likely knowledge-graph noise.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hyperlipidemia / hypercholesterolemia (rosuvastatin's core statin indication — no Taiwan/NZ license record is available since the drug is not currently marketed there) |
| Predicted New Indication | Familial Hypercholesterolemia |
| TxGNN Prediction Score | 99.54% |
| Evidence Level | L1 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed structured mechanism-of-action data for rosuvastatin (e.g., a DrugBank MOA export) is currently a data gap in this pack. However, the mechanistic rationale can be derived directly from the evidence: familial hypercholesterolemia is caused by defective or absent LDL receptor (LDLR) function, which impairs hepatic clearance of LDL cholesterol. Rosuvastatin, a high-potency HMG-CoA reductase inhibitor, upregulates residual LDL receptor activity and suppresses endogenous cholesterol synthesis — the same mechanism underlying its core hyperlipidemia indication.

This is not a distant repurposing hypothesis but a direct extension of the drug's established pharmacology: rosuvastatin is already a cornerstone of standard therapy for FH, particularly heterozygous FH (HeFH), and has been formally studied in homozygous FH (HoFH) pediatric populations. The strength of this prediction is corroborated by the volume of dedicated Phase 3 trials of rosuvastatin specifically in FH/HoFH populations (see below), which is unusual for a "predicted" indication and suggests the knowledge graph captured a genuine, near-label-adjacent relationship rather than a novel biological hypothesis.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06686615](https://clinicaltrials.gov/study/NCT06686615) | N/A (Observational) | Recruiting | 2000 | Real-world effectiveness/safety of bempedoic acid + ezetimibe combined with rosuvastatin or atorvastatin in primary hypercholesterolemia/mixed dyslipidemia (includes FH-relevant regimens) |
| [NCT02434497](https://clinicaltrials.gov/study/NCT02434497) | Phase 3 | Completed | 9 | Open-label long-term extension evaluating safety of rosuvastatin in children/adolescents (6–<18y) with homozygous FH (HoFH) |
| [NCT02226198](https://clinicaltrials.gov/study/NCT02226198) | Phase 3 | Completed | 20 | Randomized, double-blind, placebo-controlled cross-over study establishing efficacy, safety and tolerability of rosuvastatin in children/adolescents with HoFH |
| [NCT00654602](https://clinicaltrials.gov/study/NCT00654602) | Phase 3b | Completed | 1500 | 48-week, open-label, non-comparative study of rosuvastatin efficacy/safety in Fredrickson Type IIa/IIb dyslipidaemia, including heterozygous FH |
| [NCT00355615](https://clinicaltrials.gov/study/NCT00355615) | Phase 3 | Completed | 173 | 12-week double-blind RCT plus 40-week open-label follow-up of once-daily rosuvastatin reducing LDL-C in children 10–17y with HeFH |
| [NCT01078675](https://clinicaltrials.gov/study/NCT01078675) | Phase 3 | Completed | 315 | Efficacy and 2-year safety/tolerability/PK study of open-label rosuvastatin in children/adolescents with FH |
| [NCT00654446](https://clinicaltrials.gov/study/NCT00654446) | Phase 3b | Completed | 442 | Open-label, randomised study of renal effects of rosuvastatin vs. simvastatin in Fredrickson Type IIa/IIb dyslipidaemia, including heterozygous FH |
| [NCT01507831](https://clinicaltrials.gov/study/NCT01507831) | Phase 3 | Completed | 2341 | Long-term safety/tolerability of alirocumab (PCSK9i) added on top of background statin therapy in high-CV-risk hypercholesterolemia patients |
| [NCT02107898](https://clinicaltrials.gov/study/NCT02107898) | Phase 3 | Completed | 216 | RCT of alirocumab vs. placebo added to stable statin therapy in heterozygous FH or high-CV-risk hypercholesterolemia inadequately controlled on lipid-modifying therapy |
| [NCT04656028](https://clinicaltrials.gov/study/NCT04656028) | N/A | Active, not recruiting | 180 | Impact of genetic testing and motivational counseling on adherence to lifestyle/lipid-lowering therapy and cascade screening efficiency in FH patients |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28437620](https://pubmed.ncbi.nlm.nih.gov/28437620/) | 2017 | Guideline | Endocr Pract | AACE/ACE guidelines for management of dyslipidemia and cardiovascular disease prevention |
| [28838366](https://pubmed.ncbi.nlm.nih.gov/28838366/) | 2017 | RCT | J Am Coll Cardiol | Efficacy of rosuvastatin in children with homozygous FH and association with underlying LDLR genetic mutations |
| [20223367](https://pubmed.ncbi.nlm.nih.gov/20223367/) | 2010 | RCT | J Am Coll Cardiol | Efficacy and safety of rosuvastatin therapy for children with familial hypercholesterolemia |
| [26988948](https://pubmed.ncbi.nlm.nih.gov/26988948/) | 2016 | Review | J Am Coll Cardiol | Improving the monitoring and care of patients with familial hypercholesterolemia |
| [28838367](https://pubmed.ncbi.nlm.nih.gov/28838367/) | 2017 | Review | J Am Coll Cardiol | Managing patients with homozygous familial hypercholesterolemia |
| [15256766](https://pubmed.ncbi.nlm.nih.gov/15256766/) | 2004 | Open-label dose-titration trial | J Atheroscler Thromb | Clinical efficacy and safety of rosuvastatin in Japanese patients with heterozygous FH |
| [34640319](https://pubmed.ncbi.nlm.nih.gov/34640319/) | 2021 | Observational | J Clin Med | Clinical features of FH in children and adults from a EAS-FHSC regional rare-disease center in Poland |
| [30829592](https://pubmed.ncbi.nlm.nih.gov/30829592/) | 2019 | Clinical study | Georgian Med News | Rosuvastatin 20 mg/day plus hepatoprotector in patients with heterozygous FH and NASH |
| [30270066](https://pubmed.ncbi.nlm.nih.gov/30270066/) | 2018 | Retrospective study | Atherosclerosis | Treatment patterns, LDL-C goal attainment, and treatment obstacles for FH in Slovakia |
| [12269853](https://pubmed.ncbi.nlm.nih.gov/12269853/) | 2002 | Review | Drugs | Overview of rosuvastatin pharmacology, efficacy vs. other statins across hypercholesterolemic populations |

## New Zealand Market Information

Rosuvastatin currently has **no license record** in the evidence pack — market status is "Not Marketed" with 0 total authorizations. No product name, dosage form, or approved indication text is available for New Zealand.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
- Multiple completed Phase 3 RCTs of rosuvastatin itself in pediatric and adult HoFH/HeFH populations, plus supportive PCSK9-inhibitor add-on-to-statin trials, meet the L1 evidence bar (≥2 completed Phase 3 RCTs), and the mechanism (LDL receptor upregulation via HMG-CoA reductase inhibition) is directly applicable to FH pathophysiology.
- However, this evidence pack has a **Blocking** data gap (DG001: TFDA/NZ package insert warnings and contraindications not yet retrieved), which prevents a formal S1 safety assessment, and the drug is not currently marketed in New Zealand (0 licenses).

**To proceed, the following is needed:**
- Retrieve TFDA/NZ Medsafe package insert (warnings, contraindications, DDI) to complete the initial safety assessment (resolves DG001)
- Obtain structured DrugBank mechanism-of-action data to formally document the pharmacological rationale (resolves DG002)
- Confirm regulatory pathway/timeline for New Zealand market entry, since no local license currently exists
- Clarify whether FH would be pursued as a label extension (given rosuvastatin is already global standard-of-care for FH) versus a formal repurposing submission

---

### Other TxGNN-Predicted Indications Screened (Supplementary)

This evidence pack (`TW-DB01098-multi`) evaluated 10 candidate indications. For transparency, the remaining candidates are summarized below; none were selected as the report headline due to weaker evidence or lack of novelty.

| Rank | Disease | Evidence Level | Recommendation | Note |
|------|---------|------|------|------|
| 1 | Cholesterol-ester transfer protein deficiency | L5 | Hold | Cited literature (ApoA-I/hepatic lipase deficiency case reports) is unrelated to CETP deficiency or rosuvastatin; likely knowledge-graph noise |
| 3 | Hypercholesterolemia due to cholesterol 7α-hydroxylase deficiency | L4 | Research Question | Only one mechanistic/basic-science paper (nephrotic syndrome model); no clinical trials |
| 4 | Brain stem infarction | L4 | Hold | Only animal/biomarker studies; no direct human efficacy evidence for this stroke subtype |
| 5 | HIV infectious disease | L2 | Research Question | 19 trials / 20 publications, but evidence supports cardiovascular/inflammatory comorbidity management in HIV, not antiviral effect — indication framing needs clarification |
| 6 | Hypoalphalipoproteinemia | L4 | Hold | Sole relevant trial targets prostate cancer lipid reprogramming, not this indication; weak mechanistic fit |
| 7 | Neurodevelopmental disorder with ataxic gait, absent speech, decreased cortical white matter | L5 | Hold | No trials or literature; no biological rationale |
| 8 | Hyperlipidemia due to hepatic triglyceride lipase deficiency | L5 | Hold | No trials or literature |
| 9 | ABri amyloidosis | L5 | Hold | No trials or literature; no known pathophysiological link to statins |
| 10 | Hyperlipidemia | L1 | Proceed with Guardrails | This is rosuvastatin's existing core/label indication, not a repurposing hypothesis — excluded from the headline for that reason |
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


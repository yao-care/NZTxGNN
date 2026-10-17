---
layout: default
title: Tranylcypromine
parent: Model Prediction Only (L5)
nav_order: 461
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Tranylcypromine: predicted new indication Benign paroxysmal torticollis of infancy (evidence level L5). Approved use: Unipolar depression, endogenous depression, major depressive disorder…"
monitor_inn: "tranylcypromine"
predicted_indications: ["benign paroxysmal torticollis of infancy", "Ohdo syndrome and variants", "agoraphobia", "blepharophimosis - intellectual disability syndrome, Ohdo type", "ligneous conjunctivitis", "Keppen-Lubinsky syndrome"]
---

# Tranylcypromine
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

# Tranylcypromine: From Depression to Benign Paroxysmal Torticollis of Infancy

## One-Sentence Summary

Tranylcypromine (brand name Parnate) is an irreversible monoamine oxidase inhibitor (MAOI) originally used to treat depression and anxiety disorders.
The TxGNN model predicts it may be effective for **benign paroxysmal torticollis of infancy**, but **no clinical trials and no publications** currently support this prediction.
The prediction comes from the graph model alone and has no mechanistic or clinical backing.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Unipolar depression, endogenous depression, major depressive disorder, anxiety disorder (the New Zealand licence record does not state an indication, so these come from the knowledge graph) |
| Predicted New Indication | Benign paroxysmal torticollis of infancy |
| TxGNN Prediction Score | 99.67% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only (no currently valid authorisation) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the data compiled for this report. Tranylcypromine is known as an irreversible, non-selective MAOI. It raises brain levels of serotonin, noradrenaline and dopamine, and its efficacy in depression and anxiety is well established. It also inhibits LSD1 (KDM1A), an epigenetic enzyme, which is relevant to some of the other predicted conditions but not to this one.

Benign paroxysmal torticollis of infancy is a self-limiting movement disorder of infants. It is related to the migraine spectrum and to channelopathies (for example CACNA1A variants). The review found no plausible mechanistic link between MAO inhibition and this condition. The high score of 0.997 reflects network proximity in the knowledge graph, not biological or clinical evidence.

Giving an irreversible MAOI to infants is also hard to justify. The drug carries serious dietary (tyramine) and drug-interaction risks, with 151 interactions recorded. Without supporting evidence, this prediction is not credible as a repurposing opportunity.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2315076 | Parnate | Tab 10 mg (oral tablet) | Not stated in the licence record (status: cancelled) |

---

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (taken from drug–disease graph edges, not from product label text): hypertension and hypertensive disorders; orthostatic hypotension and related autonomic conditions (multiple system atrophy, postural orthostatic tachycardia syndrome); epilepsy; kidney, liver and gallbladder disease; bipolar disorder and major affective disorder; diabetes mellitus and diabetic ketoacidosis; substance abuse or dependence; coronary artery disease, heart conduction disease, sudden cardiac arrest and other cardiovascular disease; phaeochromocytoma and related adrenal or paraganglioma tumours; hyperthyroidism and Graves disease.
- **Drug interactions**: 151 interactions are recorded in total. The major ones among those retrieved include isometheptene, bupropion, morphine, lorcaserin, diethylpropion, phentermine, dolasetron and palonosetron. Moderate interactions include epinephrine, several antidiabetic agents (chlorpropamide, glimepiride, repaglinide, insulin aspart) and bowel-preparation agents.
- **Other risks noted in the literature compiled for this report**: dietary tyramine reactions (including a historical fatal case with cheese), serotonin syndrome when combined with serotonergic drugs, and withdrawal or discontinuation phenomena after abrupt stopping.

Please refer to the package insert for the full warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a graph-based score, with no trials, no publications and no mechanistic link. The target population is infants, and an irreversible MAOI with major dietary and interaction hazards is a poor fit for them. The product is also not currently authorised in New Zealand.

**To proceed, the following is needed:**
- Any mechanistic or clinical evidence linking MAO or LSD1 inhibition to this condition
- Detailed mechanism-of-action data
- The package insert (warnings and contraindications), which was not available for review
- A paediatric safety assessment, including dietary and drug-interaction risk
- A route to supply, since the only New Zealand licence is cancelled

**Note on other candidates in the same analysis:** Several other predicted indications have much stronger support. Neurotic depression (evidence level L2) and melancholia (L3) both fall within tranylcypromine's existing depressive-disorder scope and were rated "Proceed with Guardrails". Agoraphobia, dysthymic disorder and neurotic disorder were rated "Research Question". Any further evaluation effort would be better spent on these than on the rank 1 prediction.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


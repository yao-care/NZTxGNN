---
layout: default
title: Atenolol
parent: Moderate Evidence (L3-L4)
nav_order: 48
evidence_level: L4
indication_count: 9
last_modified_at: 2026-10-15
description: "Atenolol: predicted new indication Posteroinferior myocardial infarction (evidence level L4). Approved use: Hypertension, myocardial infarction"
monitor_inn: "ATENOLOL"
predicted_indications: ["malignant hypertensive renal disease", "chronic pulmonary heart disease"]
---

# Atenolol
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **9** 
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

# Atenolol: From Hypertension and Myocardial Infarction to Posteroinferior Myocardial Infarction

## One-Sentence Summary

Atenolol is a beta-1 selective blocker originally used for hypertension and myocardial infarction.
The TxGNN model predicts it may be effective for **posteroinferior myocardial infarction**, but this is an anatomical subtype of an already established indication rather than a true repurposing signal.
Evidence is thin: **0 clinical trials** and **1 publication** (a small 1985 crossover study) support this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension, myocardial infarction (the New Zealand licence records contain no indication text, so this comes from the knowledge-graph indication data) |
| Predicted New Indication | Posteroinferior myocardial infarction |
| TxGNN Prediction Score | 99.87% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, atenolol is a beta-1 selective adrenergic blocker. Its efficacy in hypertension and myocardial infarction is established, and it is mechanistically plausible that it applies to posteroinferior myocardial infarction.

Posteroinferior myocardial infarction is a subtype of myocardial infarction, which is already an approved indication. The high TxGNN score most likely reflects this close relationship in the disease ontology, not a new therapeutic discovery. Beta-1 blockade reduces heart rate, myocardial oxygen demand and arrhythmic risk, which is the rationale behind its use after infarction in general.

Other predictions in the list include posterolateral and septal myocardial infarction (also subtypes of the approved indication), malignant hypertensive renal disease, malignant renovascular hypertension, pulmonary hypertension, chronic pulmonary heart disease and Braddock syndrome. For these, the mechanistic link is weak or unsupported. Beta-blockers are generally not recommended in pulmonary arterial hypertension, and the Braddock syndrome score is likely a graph artifact.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3901170](https://pubmed.ncbi.nlm.nih.gov/3901170/) | 1985 | Randomized crossover (single-blind), small | La Revue de médecine interne | Compared the anti-ischaemic activity of atenolol 200 mg and diltiazem 240 mg in 23 patients undergoing retraining 4 weeks after a limited posteroinferior or anterior myocardial infarction, using computerized exercise testing. This gives only indirect support. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2627140 | Viatris (cancelled) | Tab 50 mg | Not recorded |
| 2403366 | Atenolol AFT (cancelled) | Oral liq 25 mg per 5 ml | Not recorded |
| 2627159 | Atenolol Viatris (cancelled) | Tab 100 mg | Not recorded |

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (drug-disease edges, not package insert text): atrioventricular block, asthma (intrinsic and allergic), Raynaud disease, anaphylaxis, diabetes mellitus (type 1, type 2) and diabetic ketoacidosis, liver failure, anuria, pancreatitis, systemic lupus erythematosus, psoriasis, gout and hyperuricaemia, hypercalcaemia and nephrocalcinosis, potassium deficiency, phosphorus metabolism disease, and anxiety, neurotic and dysthymic disorders.
- **Drug interactions**: 100 interactions were found. The ones listed include moderate interactions with epinephrine, corticosteroids (hydrocortisone, triamcinolone, betamethasone, budesonide), bupropion, morphine, atropine, hyoscyamine, acetohexamide and several calcium salts (acetate, carbonate, citrate, gluconate, lactate and others). Minor interactions include acetylsalicylic acid, aluminium hydroxide and attapulgite.
- **Other points raised in the evidence**: atenolol is renally cleared and needs dose adjustment in renal impairment. Beta-blockers carry a theoretical risk of reduced right ventricular reserve and are generally not recommended in pulmonary arterial hypertension.

Please refer to the package insert for the official warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Posteroinferior myocardial infarction is a subtype of an indication atenolol already has, so the high TxGNN score is not evidence of a new use. There are no registered trials and only one small, indirect 1985 study. All three New Zealand licences are cancelled, so there is no currently marketed product.

**To proceed, the following is needed:**
- Confirmation of whether this prediction adds anything beyond the existing myocardial infarction indication
- Package insert warnings and contraindications (for example from Medsafe), since safety screening cannot proceed without them
- Detailed mechanism of action data
- Clarification of the current New Zealand supply and licensing status, given that all licences are cancelled
- For the non-MI predictions (pulmonary hypertension, chronic pulmonary heart disease, renal hypertension), human clinical evidence and a risk assessment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


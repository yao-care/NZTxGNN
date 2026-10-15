---
layout: default
title: Quinapril
parent: Moderate Evidence (L3-L4)
nav_order: 396
evidence_level: L4
indication_count: 5
---

# Quinapril
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **5** 
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

# Quinapril: From Hypertension to Malignant Renovascular Hypertension

## One-Sentence Summary

Quinapril is an ACE inhibitor originally used to treat hypertension.
The TxGNN model predicts it may be effective for **malignant renovascular hypertension**,
but there are currently **0 clinical trials** and **0 publications** supporting this specific prediction. The evidence is model prediction plus indirect class-level reasoning only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension / hypertensive disorder (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Malignant renovascular hypertension |
| TxGNN Prediction Score | 99.86% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, quinapril belongs to the ACE inhibitor class, its efficacy in hypertension is established, and mechanistically it may be applicable to renovascular hypertension, which is strongly driven by activation of the renin-angiotensin system.

The predicted indication is essentially an extension of the approved hypertension indication rather than a true repurposing. The high TxGNN score likely reflects closeness to hypertension in the disease ontology rather than new biological insight. ACE inhibitors also carry a known risk of acute renal function decline in bilateral renal artery stenosis, which is directly relevant to this indication.

Other predicted indications are weaker or similar in nature:

| Rank | Predicted Indication | Score | Evidence Level | Note |
|------|------|------|------|------|
| 2 | Malignant hypertensive renal disease | 99.86% | L4 | RAS blockade is plausible (class-level, indirect); close to the approved indication |
| 3 | Pulmonary hypertension, unclear multifactorial mechanism | 99.85% | L5 | Prediction only; ACE inhibitors have not shown reliable clinical benefit |
| 4 | Pulmonary hypertension owing to lung disease and/or hypoxia | 99.85% | L4 | One animal study; flagged as a research question |
| 5 | Braddock syndrome | 99.79% | L5 | No mechanistic rationale or evidence identified |

For rank 4, a 1996 animal study (PMID 8873672) reported that quinapril reduced hypoxic pulmonary hypertension and vascular remodeling. It is preclinical only, and vasodilators can worsen gas exchange in hypoxic lung disease.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the top predicted indication (malignant renovascular hypertension).

For the rank 4 indication (pulmonary hypertension owing to lung disease and/or hypoxia), the following preclinical publications were found:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8873672](https://pubmed.ncbi.nlm.nih.gov/8873672/) | 1996 | Animal study | Circulation | Inhibiting tissue ACE with quinapril reduced hypoxic pulmonary hypertension and pulmonary vascular remodeling |
| [8720418](https://pubmed.ncbi.nlm.nih.gov/8720418/) | 1996 | Animal study | J Cardiovasc Pharmacol | Quinapril preserved left ventricular function and energy metabolism in rats after myocardial infarction (not directly relevant to pulmonary hypertension) |

## New Zealand Market Information

All three licences are cancelled; no approved indication text is recorded.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2427877 | Arrow-Quinapril 5 | Tab 5 mg | Not recorded (cancelled) |
| 2427893 | Arrow-Quinapril 20 | Tab 20 mg | Not recorded (cancelled) |
| 2427885 | Arrow-Quinapril 10 | Tab 10 mg | Not recorded (cancelled) |

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report; please refer to the package insert for label safety information.

- **Contraindication-related conditions (from the knowledge graph, not label text)**: kidney disease, anuria, angioedema (including hereditary, acquired and RAAS-blocker-induced forms), pancreatitis, liver disease, hepatic encephalopathy/coma, gallbladder disease, systemic lupus erythematosus, connective tissue disease, diabetes mellitus and diabetic ketoacidosis, potassium deficiency, hypotensive disorder, neutropenia, agranulocytosis, hyperparathyroidism, gout, neonatal jaundice, and bone and cartilage disease.
- **Drug Interactions**: 100 interactions were recorded; the first 20 retrieved are mostly Moderate. The one **Major** interaction listed is with **potassium citrate** (hyperkalaemia risk). Moderate interactions include metformin, aspirin, corticosteroids (hydrocortisone, dexamethasone, betamethasone, budesonide, triamcinolone), SGLT2 inhibitors (canagliflozin, dapagliflozin), chlorpropamide, morphine, bupropion, atropine, tetracyclines (doxycycline, tetracycline), and several laxatives and bowel preparations. The remaining 80 were not itemized in the data.
- **Indication-specific concern**: ACE inhibitors may cause acute renal function decline in bilateral renal artery stenosis, which is relevant to the top predicted indication.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction is a closely related extension of the approved hypertension use, with no trials or publications for quinapril in this indication, and the high score likely reflects ontology proximity. All New Zealand licences are cancelled, and label safety data are missing, so a safety screen cannot proceed.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking data gap)
- Mechanism of action data (for example from DrugBank)
- Quinapril-specific clinical evidence in renovascular hypertension, including renal safety in renal artery stenosis
- Clarification of the market situation (reason for cancellation, availability of other ACE inhibitors) and whether a genuine unmet need exists
- For pulmonary hypertension, human data beyond the preclinical studies, with attention to gas exchange in hypoxic lung disease
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


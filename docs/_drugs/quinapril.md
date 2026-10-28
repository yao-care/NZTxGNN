---
layout: default
title: Quinapril
parent: Model Prediction Only (L5)
nav_order: 396
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-15
monitor_inn: "QUINAPRIL"
predicted_indications: ["malignant hypertensive renal disease", "pulmonary hypertension with unclear multifactorial mechanism", "pulmonary hypertension owing to lung disease and/or hypoxia", "Braddock syndrome"]
description: "Quinapril: predicted new indication Malignant hypertensive renal disease (evidence level L5). Approved use: Not recorded in the local data"
---

# Quinapril
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

## Quinapril: Malignant hypertensive renal disease
{: .text-alpha }

## One-Sentence Summary

Quinapril is an ACE inhibitor originally used to treat hypertension.
but there are currently **0 clinical trials** and **0 publications** supporting this specific prediction. The evidence is model prediction plus indirect class-level reasoning only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Malignant hypertensive renal disease |
| TxGNN Prediction Score | 0.9986 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 3 of 3) |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, quinapril belongs to the ACE inhibitor class, its efficacy in hypertension is established, and mechanistically it may be applicable to renovascular hypertension, which is strongly driven by activation of the renin-angiotensin system.

The predicted indication is essentially an extension of the approved hypertension indication rather than a true repurposing. The high TxGNN score likely reflects closeness to hypertension in the disease ontology rather than new biological insight. ACE inhibitors also carry a known risk of acute renal function decline in bilateral renal artery stenosis, which is directly relevant to this indication.

Other predicted indications are weaker or similar in nature:

| Rank | Predicted Indication | Score | Evidence Level | Note |
|------|------|------|------|------|
| 2 | Malignant hypertensive renal disease | 0.9986 | L5 | RAS blockade is plausible (class-level, indirect); close to the approved indication |
| 3 | Pulmonary hypertension, unclear multifactorial mechanism | 0.9985 | L5 | Prediction only; ACE inhibitors have not shown reliable clinical benefit |
| 4 | Pulmonary hypertension owing to lung disease and/or hypoxia | 0.9985 | L5 | One animal study; flagged as a research question |
| 5 | Braddock syndrome | 0.9979 | L5 | No mechanistic rationale or evidence identified |

For rank 4, a 1996 animal study (PMID 8873672) reported that quinapril reduced hypoxic pulmonary hypertension and vascular remodeling. It is preclinical only, and vasodilators can worsen gas exchange in hypoxic lung disease.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence


For the rank 4 indication (pulmonary hypertension owing to lung disease and/or hypoxia), the following preclinical publications were found:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8873672](https://pubmed.ncbi.nlm.nih.gov/8873672/) | 1996 | Animal study | Circulation | Inhibiting tissue ACE with quinapril reduced hypoxic pulmonary hypertension and pulmonary vascular remodeling |
| [8720418](https://pubmed.ncbi.nlm.nih.gov/8720418/) | 1996 | Animal study | J Cardiovasc Pharmacol | Quinapril preserved left ventricular function and energy metabolism in rats after myocardial infarction (not directly relevant to pulmonary hypertension) |

## New Zealand Market Information


| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2427877 | Arrow-Quinapril 5 | Tab 5 mg | Not recorded (valid) |
| 2427893 | Arrow-Quinapril 20 | Tab 20 mg | Not recorded (valid) |
| 2427885 | Arrow-Quinapril 10 | Tab 10 mg | Not recorded (valid) |

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report; please refer to the package insert for label safety information.

- **Contraindication-related conditions (from the knowledge graph, not label text)**: kidney disease, anuria, angioedema (including hereditary, acquired and RAAS-blocker-induced forms), pancreatitis, liver disease, hepatic encephalopathy/coma, gallbladder disease, systemic lupus erythematosus, connective tissue disease, diabetes mellitus and diabetic ketoacidosis, potassium deficiency, hypotensive disorder, neutropenia, agranulocytosis, hyperparathyroidism, gout, neonatal jaundice, and bone and cartilage disease.
- **Drug Interactions**: 100 interactions were recorded; the first 20 retrieved are mostly Moderate. The one **Major** interaction listed is with **potassium citrate** (hyperkalaemia risk). Moderate interactions include metformin, aspirin, corticosteroids (hydrocortisone, dexamethasone, betamethasone, budesonide, triamcinolone), SGLT2 inhibitors (canagliflozin, dapagliflozin), chlorpropamide, morphine, bupropion, atropine, tetracyclines (doxycycline, tetracycline), and several laxatives and bowel preparations. The remaining 80 were not itemized in the data.
- **Indication-specific concern**: ACE inhibitors may cause acute renal function decline in bilateral renal artery stenosis, which is relevant to the top predicted indication.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction is a closely related extension of the approved hypertension use, with no trials or publications for quinapril in this indication, and the high score likely reflects ontology proximity.

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


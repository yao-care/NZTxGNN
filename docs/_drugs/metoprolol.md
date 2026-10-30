---
layout: default
title: Metoprolol
parent: Model Prediction Only (L5)
nav_order: 296
evidence_level: L5
indication_count: 6
monitor_inn: "Metoprolol"
predicted_indications: ["malignant hypertensive renal disease", "pulmonary hypertension with unclear multifactorial mechanism", "pulmonary hypertension owing to lung disease and/or hypoxia", "Braddock syndrome", "chronic pulmonary heart disease", "trichotillomania"]
last_modified_at: 2026-10-30
description: "Metoprolol: predicted new indication Malignant hypertensive renal disease (evidence level L5). Approved use: Hypertension, acute coronary syndrome, myocardial infarction"
---

# Metoprolol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Metoprolol: From Hypertension to Malignant Hypertensive Renal Disease
{: .text-alpha }

## One-Sentence Summary

Metoprolol is a beta-1 selective blocker, with a relation recorded in the knowledge graph for hypertension, acute coronary syndrome and myocardial infarction.
The TxGNN model predicts it may be effective for **malignant hypertensive renal disease**, with a very high score (0.9991).
However, **no clinical trials and no publications** directly study this disease, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension, acute coronary syndrome, myocardial infarction (relations recorded in the knowledge graph; the local licence records contain no indication text) |
| Predicted New Indication | Malignant hypertensive renal disease |
| TxGNN Prediction Score | 0.9991 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (9 of 9 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 9 (valid: 9) |
| Number of Approvals | 9 (single-ingredient licences) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Pharmacology records show that metoprolol acts on the beta-1 and beta-2 adrenoceptors, and it is a beta-1 selective blocker. Reduced heart rate, lower cardiac output and lower renin release are the usual explanations for its blood pressure lowering effect.

Malignant hypertensive renal disease is a severe kidney complication of very high blood pressure. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. Hypertension is a relation recorded for metoprolol.

This link is weak in practice. Acute management of malignant hypertension generally relies on parenteral agents rather than oral beta-blockers. No study in the data compiled for this report tests metoprolol in this condition, so a repurposing claim is not supported.

Other predictions in the same run were also reviewed. None has a direct study of the predicted disease:

| Predicted Disease | Score | Evidence Level | Comment |
|------|------|------|------|
| Pulmonary hypertension, unclear multifactorial mechanism | 0.9991 | L5 | No studies; beta-blockers are used cautiously because they can reduce right ventricular reserve |
| Pulmonary hypertension owing to lung disease and/or hypoxia | 0.9991 | L5 | 20 publications retrieved, none addressing this disease |
| Braddock syndrome | 0.9989 | L5 | No studies; the score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence |
| Chronic pulmonary heart disease | 0.9940 | L5 | Only studies in COPD and heart failure; not the disease itself |
| Trichotillomania | 0.9905 | L5 | No studies |

## Clinical Trial Evidence

Currently no related clinical trials registered for malignant hypertensive renal disease.

## Literature Evidence

Currently no related literature available for malignant hypertensive renal disease.

## New Zealand Market Information

At least the following authorisations are listed, all single-ingredient products. Approved indication text is not included in the licence records.

| Authorization Number | Product Name | Dosage Form |
|---------|------|------|
| 2550369 | Metoprolol IV Mylan | Injection 1 mg per ml, 5 ml vial |
| 2639777 | Metoprolol IV Viatris | Injection 1 mg per ml, 5 ml vial |
| 2618680 | IPCA-Metoprolol | Tablet 100 mg |
| 2618672 | IPCA-Metoprolol | Tablet 50 mg |
| 2108682 | Slow-Lopresor | Long-acting tablet 200 mg |

Four further licences (Myloc CR long-acting tablets at 23.75, 47.5, 95 and 190 mg) are also valid, bringing the total to 9.

## Safety Considerations

- **Relations recorded in the knowledge graph as contraindications** (not label text): anxiety disorder, pancreatitis, liver disease, gallbladder disease, systemic lupus erythematosus, psoriasis, diabetes mellitus, diabetic ketoacidosis, hypotensive disorder, gout, hepatic encephalopathy, anuria, myasthenia gravis and phaeochromocytoma, among others.
- **Pharmacology targets**: beta-1 adrenoceptor (ADRB1) and beta-2 adrenoceptor (ADRB2).

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no supporting trials or publications for malignant hypertensive renal disease, so the evidence level is L5. Standard acute treatment of malignant hypertension uses parenteral agents, and the high score most likely reflects the known antihypertensive use of metoprolol.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications, to allow safety screening
- Mechanism of action data
- Any clinical or observational study of metoprolol in malignant hypertensive renal disease
- Clinical review of the place of an oral beta-blocker relative to standard parenteral therapy in this setting
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


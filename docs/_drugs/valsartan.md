---
layout: default
title: Valsartan
parent: Moderate Evidence (L3-L4)
nav_order: 475
evidence_level: L4
indication_count: 7
last_modified_at: 2026-10-15
description: "Valsartan: predicted new indication Malignant renovascular hypertension (evidence level L4). Approved use: Hypertension (hypertensive disorder)"
---

# Valsartan
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **7** 
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

# Valsartan: From Hypertension to Malignant Renovascular Hypertension

## One-Sentence Summary

Valsartan is an angiotensin II receptor blocker (ARB) originally used to treat hypertension, and it is also a component of the sacubitril/valsartan combination (Entresto).
The TxGNN model predicts it may be effective for **malignant renovascular hypertension**, but there are **0 registered clinical trials** and only **1 preclinical publication** directly supporting this prediction.
The indication overlaps heavily with the original hypertension indication, so the prediction is plausible but largely unproven in humans.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension (hypertensive disorder) |
| Predicted New Indication | Malignant renovascular hypertension |
| TxGNN Prediction Score | 99.97% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available in the data compiled for this report. Based on known pharmacology, valsartan selectively blocks the angiotensin II type 1 (AT1) receptor, and its efficacy in hypertension is well established.

Angiotensin II-driven vascular injury is central to malignant hypertension, particularly in renovascular forms where renin-angiotensin system (RAS) activation is prominent. Blocking the AT1 receptor is therefore mechanistically sensible. The only supporting paper (PMID 11560862, 2001) is an animal study showing that AT1 receptor blockade prevented lethal malignant hypertension, even without a blood-pressure-lowering effect, and was linked to reduced kidney inflammation.

Note that this predicted indication is essentially a severe subtype of the original indication. The prediction is mechanistically reasonable, but there is no human trial evidence for this subtype.

**Other predictions from the model (lower priority):**

| Predicted Indication | TxGNN Score | Evidence Level | Comment |
|------|------|------|------|
| Malignant hypertensive renal disease | 99.97% | L4 | Only indirect support (a paper on avosentan, a different drug) |
| Pulmonary hypertension, unclear multifactorial mechanism | 99.97% | L5 | Model prediction only |
| Pulmonary hypertension due to lung disease/hypoxia | 99.97% | L4 | Mostly preclinical; many studies involve sacubitril/valsartan, so the valsartan contribution is confounded |
| Braddock syndrome | 99.96% | L5 | Model prediction only |
| Chronic pulmonary heart disease | 99.58% | L3 | Observational and post hoc heart failure data, not cor pulmonale itself |
| Prinzmetal angina | 99.45% | L5 | No plausible mechanistic link |

---

## Clinical Trial Evidence

Currently no related clinical trials are registered for malignant renovascular hypertension.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [11560862](https://pubmed.ncbi.nlm.nih.gov/11560862/) | 2001 | Preclinical (animal model) | Circulation | AT1 receptor blockade prevented lethal malignant hypertension in an animal model, with effects related to reduced kidney inflammation |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2554720 | Entresto 24/26 | Tablet (sacubitril 24.3 mg / valsartan 25.7 mg) | Not available (cancelled) |
| 2554747 | Entresto 97/103 | Tablet (sacubitril 97.2 mg / valsartan 102.8 mg) | Not available (cancelled) |
| 2554739 | Entresto 49/51 | Tablet (sacubitril 48.6 mg / valsartan 51.4 mg) | Not available (cancelled) |

All three licences are cancelled, and no valid single-ingredient valsartan licences are listed.

---

## Safety Considerations

- **Contraindications (knowledge-graph-derived, not label text):** The drug-disease contraindication data include angioedema (including hereditary, acquired, and RAAS-blocker-induced forms), liver failure and liver disease, hepatic encephalopathy, pancreatitis, gallbladder disease, impaired renal function, renal artery obstruction, anuria, hypotensive disorder, diabetes mellitus and diabetic ketoacidosis, potassium deficiency, hyperuricemia and gout, hypercalcemia and nephrocalcinosis, hyperparathyroidism, neutropenia and agranulocytosis, and systemic lupus erythematosus. These were not verified against package insert text.
- **Important for this prediction:** Renal artery obstruction and impaired renal function appear among the contraindications. These are directly relevant to a renovascular hypertension indication and need careful clinical review (for example, concern in bilateral renal artery stenosis).
- **Drug Interactions:** 254 interactions were identified. Major interactions include **potassium chloride** and **potassium bicarbonate** (risk of hyperkalaemia). Moderate interactions include insulins and exenatide (glycaemic effects), hydrocortisone, eluxadoline, and polyethylene glycol with electrolytes.

Please refer to the package insert for full label warnings.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is mechanistically plausible and strongly related to valsartan's existing hypertension use, but the only supporting evidence is a single 2001 animal study, with no clinical trials. New Zealand licences are all cancelled, and label-based safety data have not been obtained, so safety screening cannot proceed.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data (for example from DrugBank)
- Human clinical evidence in malignant or renovascular hypertension, or confirmation that existing hypertension guidance already covers this use
- A review of renal artery stenosis and renal function risk specific to renovascular hypertension
- Clarification of the regulatory pathway given that all NZ licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


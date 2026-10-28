---
layout: default
title: Atenolol
parent: Model Prediction Only (L5)
nav_order: 48
evidence_level: L5
indication_count: 5
monitor_inn: "ATENOLOL"
predicted_indications: ["malignant hypertensive renal disease", "pulmonary hypertension with unclear multifactorial mechanism", "pulmonary hypertension owing to lung disease and/or hypoxia", "Braddock syndrome", "chronic pulmonary heart disease"]
last_modified_at: 2026-10-28
description: "Atenolol: predicted new indication Malignant hypertensive renal disease (evidence level L5). Approved use: Hypertension and myocardial infarction"
---

# Atenolol
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

## Atenolol: From Hypertension to Malignant Hypertensive Renal Disease
{: .text-alpha }

## One-Sentence Summary

Atenolol is a beta-1 selective blocker whose recorded uses in the knowledge graph include hypertension and myocardial infarction.
The TxGNN model predicts it may be useful for **malignant hypertensive renal disease**, but this rests on a model prediction only, with **0 clinical trials** and **0 publications** directly studying this disease.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension and myocardial infarction (relations recorded in the knowledge graph; no approved indication text is available in the New Zealand licence records) |
| Predicted New Indication | Malignant hypertensive renal disease |
| TxGNN Prediction Score | 0.9985 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 3 of 3) |
| Number of Authorizations | 3 (valid: 3) |
| Number of Approvals | 3 (all single-ingredient products) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Based on general pharmacology, atenolol is a beta-1 selective blocker that lowers blood pressure and reduces renin release. This gives a plausible link to hypertension-related kidney injury.

Malignant hypertensive renal disease is a severe consequence of uncontrolled hypertension. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

The prediction should be treated cautiously. Malignant hypertension is usually managed with rapidly titratable intravenous agents rather than oral atenolol. No trials or publications were found for this disease, so the score is a hypothesis only.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available for malignant hypertensive renal disease.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2627140 | Viatris | Tab 50 mg | Not stated in the licence records |
| 2403366 | Atenolol AFT | Oral liq 25 mg per 5 ml | Not stated in the licence records |
| 2627159 | Atenolol Viatris | Tab 100 mg | Not stated in the licence records |

---

## Safety Considerations

- **Relations recorded in the knowledge graph (not label text)**: Atenolol is linked to contraindication relations with several conditions, including:
  - Respiratory and allergic: asthma (intrinsic and allergic), anaphylaxis
  - Cardiovascular: atrioventricular block, Raynaud disease
  - Metabolic and endocrine: type 1 and type 2 diabetes mellitus, diabetic ketoacidosis, gout, hypercalcemia, potassium deficiency
  - Other: liver failure, pancreatitis, anuria, systemic lupus erythematosus, psoriasis, anxiety and related disorders
- **Drug Interactions**: 100 interactions were identified in the data compiled for this report. The first 20 listed are mostly Moderate, for example epinephrine, hydrocortisone and other corticosteroids, bupropion, morphine, atropine, hyoscyamine, acetohexamide, and calcium salts. Minor ones include acetylsalicylic acid, aluminum hydroxide and attapulgite.

Package insert warnings and contraindications have not been retrieved, so please refer to the package insert for authoritative safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score, with no clinical trials or publications on this disease (Evidence Level L5). Malignant hypertension is normally treated with intravenous agents, and package insert safety data have not yet been reviewed.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from the Medsafe data sheet
- Mechanism of action data
- Clinical or observational studies directly examining atenolol in malignant hypertensive renal disease
- A comparison with standard treatment for malignant hypertension to establish whether oral atenolol has a role
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


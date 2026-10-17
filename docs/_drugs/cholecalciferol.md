---
layout: default
title: Cholecalciferol
parent: Model Prediction Only (L5)
nav_order: 97
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Cholecalciferol: predicted new indication Familial isolated hypoparathyroidism due to impaired PTH secretion (evidence level L5). Approved use: Vitamin D deficiency, rickets, osteomalacia, osteoporos…"
monitor_inn: "Cholecalciferol"
predicted_indications: ["acromesomelic dysplasia, Campailla Martinelli type", "craniofacial conodysplasia", "Dahlberg-Borer-Newcomer syndrome", "renal tubular acidosis"]
---

# Cholecalciferol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

# Cholecalciferol: From Vitamin D Deficiency and Bone Disorders to Familial Isolated Hypoparathyroidism (Impaired PTH Secretion)

## One-Sentence Summary

Cholecalciferol (vitamin D3) is originally used for vitamin D deficiency and related bone and mineral disorders such as rickets, osteomalacia and osteoporosis.
The TxGNN model predicts it may be effective for **familial isolated hypoparathyroidism due to impaired PTH secretion**,
but there are currently **0 clinical trials** and **0 publications** specific to this indication, so the prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Vitamin D deficiency, rickets, osteomalacia, osteoporosis, hypoparathyroidism (New Zealand licence texts list no indication wording, so these come from the knowledge-graph indication list) |
| Predicted New Indication | Familial isolated hypoparathyroidism due to impaired PTH secretion |
| TxGNN Prediction Score | 99.79% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Based on known information, cholecalciferol is a vitamin D prohormone. It is converted in the body to active vitamin D metabolites that regulate calcium and phosphate balance. Its efficacy in vitamin D deficiency and bone mineral disorders is well established, and mechanistically it may be applicable to hypocalcaemia caused by low parathyroid hormone (PTH).

The predicted disease is a narrower, genetic subtype of hypoparathyroidism, which is already among cholecalciferol's known indications. The high TxGNN score most likely reflects this overlap rather than new biology. Vitamin D metabolites are used to manage low calcium in PTH deficiency, but no data compiled for this report show a benefit specific to impaired PTH secretion. In practice, the active metabolites (such as calcitriol) are usually favoured over native cholecalciferol in PTH deficiency, because conversion to the active form depends partly on PTH-driven kidney activation.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Status | Approved Indication |
|---------|------|------|------|-----------|
| 2658429 | Clinicians | Oral liquid, 188 mcg/mL (7,500 IU/mL) | Cancelled | Not specified |
| 2523590 | Vit.D3 | Capsule, 1.25 mg (50,000 IU) | Cancelled | Not specified |

## Safety Considerations

- **Disease associations flagged in the knowledge graph (not label text):** The TxGNN knowledge graph links cholecalciferol to contraindication or caution signals in the following groups. These need verification against the official product information.
  - **Calcium and mineral disorders:** hypercalcaemia, nephrocalcinosis, phosphorus metabolism disease, hyperparathyroidism, osteitis fibrosa, sarcoidosis
  - **Kidney, liver and gallbladder disease:** kidney disease, liver disease, gallbladder disease, pyoureter, primary hyperoxaluria
  - **Cardiovascular and metabolic disease:** hypertension, atrial fibrillation, diabetes mellitus, diabetic ketoacidosis, hyperlipidaemia (including familial)
  - **Haematological and iron disorders:** blood coagulation disease, sickle cell anaemia, hereditary persistence of fetal haemoglobin, hereditary haemochromatosis, haemosiderosis, iron metabolism disease
  - **Other conditions:** gastritis, Wilson disease, female breast carcinoma, prostate cancer/neoplasm/carcinoma
- **Drug Interactions:** No drug-drug interactions are recorded. The only entry is a pharmacology annotation linking vitamin D3 to the farnesoid X receptor-β, a mouse target, which has no established clinical interaction implication.

Please refer to the package insert for official warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This prediction has no trials or literature behind it, and it largely restates an existing use (hypoparathyroidism in general). It is therefore a model-only signal with little added value for this specific subtype. Cholecalciferol is also currently not marketed in New Zealand, since both licences are cancelled.

Among the other predicted indications, **renal osteodystrophy** has the most supporting evidence. It has a Phase 3 trial (NCT00752401, status unknown, in vitamin D-deficient kidney transplant recipients) and a Phase-NA comparison study (NCT00285467) in CKD-related secondary hyperparathyroidism. That evidence is an indirect match, and the Phase 3 trial is not completed, so it does not meet the L1 criterion of two completed Phase 3 RCTs. It would be the better candidate for a separate evaluation, with calcium, phosphate and PTH monitoring as guardrails.

**To proceed, the following is needed:**
- Disease-specific trials or case series of cholecalciferol versus active vitamin D metabolites in familial hypoparathyroidism with impaired PTH secretion
- Detailed mechanism of action data
- Official New Zealand package insert warnings and contraindications, and clarification of the cancelled licence status and any currently available supply
- A calcium, phosphate and PTH monitoring plan, with attention to hypercalcaemia and nephrocalcinosis risk
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Octreotide
parent: Model Prediction Only (L5)
nav_order: 333
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Octreotide: predicted new indication Vulvar inverted follicular keratosis (evidence level L5). Approved use: Acromegaly, hyperpituitarism, carcinoid syndrome"
---

# Octreotide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

# Octreotide: From Acromegaly to Vulvar Inverted Follicular Keratosis

## One-Sentence Summary

Octreotide is a somatostatin analogue, originally used to treat acromegaly, hyperpituitarism and carcinoid syndrome.
The TxGNN model predicts it may be effective for **vulvar inverted follicular keratosis**, but there are **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Acromegaly, hyperpituitarism, carcinoid syndrome (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Vulvar inverted follicular keratosis |
| TxGNN Prediction Score | 99.58% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 11 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, octreotide is a somatostatin analogue that acts mainly on somatostatin receptors (SSTR2 and SSTR5). Its efficacy in acromegaly and carcinoid syndrome is established, which is consistent with suppressing hormone secretion and cell growth signalling.

A link to vulvar inverted follicular keratosis, a benign follicular keratinocyte lesion, is speculative. It would depend on somatostatin receptor expression in skin or follicular epithelium and on antiproliferative or anti-IGF-1 effects. Neither is supported by the data compiled for this report. The high score may reflect proximity to related keratotic conditions in the knowledge graph rather than a true biological relationship.

A second predicted indication, **seborrheic keratosis** (score 99.55%), has the same problem. It also has no trials or literature, and it is a common benign lesion with low-risk standard treatments such as cryotherapy and curettage. A systemic peptide drug would have an unfavourable risk-benefit balance without supporting evidence. The two scores are nearly identical, which suggests a shared graph-based artefact rather than independent evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

All 11 licences are cancelled, and no approved indication text is recorded. The table lists 5 of them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2595885 | Octreotide GH | Inj 100 mcg per ml, 1 ml ampoule | Not recorded (cancelled) |
| 2595893 | Octreotide GH | Inj 500 mcg per ml, 1 ml ampoule | Not recorded (cancelled) |
| 2595907 | Octreotide GH | Inj 50 mcg per ml, 1 ml ampoule | Not recorded (cancelled) |
| 2629534 | Max Health | Inj 50 mcg per ml, 1 ml ampoule | Not recorded (cancelled) |
| 2669390 | Sun Pharma | Inj 500 mcg per ml, 1 ml ampoule | Not recorded (cancelled) |

## Safety Considerations

- **Contraindication-related conditions (knowledge graph, not label text)**: The knowledge graph links octreotide to caution or contraindication in these conditions:
  - Metabolic: hyperglycaemia, hypoglycaemia, diabetes mellitus, diabetic ketoacidosis, vitamin B12 deficiency
  - Thyroid: hypothyroidism, myxedema, congenital hypothyroidism
  - Liver, biliary and gastrointestinal: cirrhosis of the liver, liver disease, gallbladder disease, cholelithiasis, cholecystolithiasis, choledocholithiasis, ileus, intestinal obstruction
  - Cardiac: sudden cardiac arrest, heart conduction disease
  - Renal: kidney failure, uremia
- **Drug Interactions**: 100 interactions were found, and the 20 returned are all rated Moderate. They are mainly glucose-lowering drugs, which fits octreotide's effect on blood glucose. Examples include:
  - Insulins (aspart, degludec, detemir, glargine, glulisine, human)
  - Metformin
  - Sulfonylureas (chlorpropamide, glimepiride)
  - GLP-1 agonists (albiglutide, dulaglutide, semaglutide)
  - SGLT2 inhibitors (canagliflozin, dapagliflozin, empagliflozin)
  - DPP-4 inhibitors (alogliptin, linagliptin)
  - Acarbose

Please refer to the package insert for warnings and formal contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only support for either predicted indication is the model score. There are no clinical trials or publications, the mechanism of action is not available, and both indications are benign conditions with low-risk standard care. All New Zealand licences for octreotide are also cancelled.

**To proceed, the following is needed:**
- Mechanism of action data (for example from DrugBank) and an assessment of somatostatin receptor expression in vulvar or follicular epithelium
- Package insert warnings and contraindications from Medsafe, which are required before safety screening
- Preclinical or case-level evidence of benefit in inverted follicular keratosis or seborrheic keratosis
- Confirmation of current New Zealand supply, since all licences are cancelled
- Route and formulation compatibility assessment (for example, whether a topical approach is plausible instead of an injection)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


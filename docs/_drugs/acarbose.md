---
layout: default
title: Acarbose
parent: Model Prediction Only (L5)
nav_order: 13
evidence_level: L5
indication_count: 9
monitor_inn: "ACARBOSE"
predicted_indications: ["classic stiff person syndrome", "focal stiff limb syndrome", "thiamine-responsive dysfunction syndrome", "opsismodysplasia", "drug-induced localized lipodystrophy", "centrifugal lipodystrophy", "pressure-induced localized lipoatrophy", "idiopathic localized lipodystrophy", "pancreatic agenesis"]
last_modified_at: 2026-10-27
description: "Acarbose: predicted new indication Classic stiff person syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Acarbose
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

## Acarbose: alpha-glucosidase inhibitor → classic stiff person syndrome
{: .text-alpha }

## One-Sentence Summary

Acarbose is an intestinal alpha-glucosidase inhibitor that slows carbohydrate digestion and is used to lower blood sugar after meals.
The TxGNN model predicts it may be relevant to **classic stiff person syndrome**, with a score of 0.9965.
There are **no clinical trials and no publications** on this disease, so the prediction rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Classic stiff person syndrome |
| TxGNN Prediction Score | 0.9965 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the data compiled for this report. Acarbose is generally understood to act locally in the gut, inhibiting alpha-glucosidase enzymes and so reducing the absorption of carbohydrates after meals.

Stiff person syndrome is a rare autoimmune neurological disorder, typically linked to anti-GAD antibodies, that causes muscle stiffness and spasms. Nothing connects the action of acarbose to this autoimmune mechanism. The high score most likely reflects associations between diabetes and GAD autoimmunity in the model's training data rather than a therapeutic rationale. The same score appears for focal stiff limb syndrome, a related variant, which suggests the result comes from shared model associations rather than separate evidence. The prediction is therefore not mechanistically supported at this time.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for classic stiff person syndrome. Publications retrieved for other predictions in the model output concern diabetes in general and do not address this disease.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2402416 | Accarb | Tab 100 mg | Not recorded |
| 2402408 | Accarb | Tab 50 mg | Not recorded |

## Safety Considerations

- **Drug Interactions**: The interaction query returned 100 records. Examples at moderate level include acetazolamide, salbutamol, aloe vera leaf, alpelisib, amprenavir, aripiprazole, asenapine, atazanavir, bedaquiline, bendroflumethiazide and betamethasone. Minor-level examples include albiglutide, alclometasone (topical), lipoic acid and amcinonide.

Please refer to the package insert for warnings and contraindications, which are not available in the data compiled for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no supporting trials or publications, and no plausible mechanistic link between acarbose and stiff person syndrome was identified. The evidence level is L5 (model prediction only).

**To proceed, the following is needed:**
- A plausible mechanistic hypothesis connecting alpha-glucosidase inhibition to the disease
- Any preclinical or clinical studies that directly address stiff person syndrome
- Detailed mechanism-of-action data for acarbose
- Package insert warnings and contraindications from Medsafe
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


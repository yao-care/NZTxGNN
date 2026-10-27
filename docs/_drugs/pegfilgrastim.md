---
layout: default
title: Pegfilgrastim
parent: Model Prediction Only (L5)
nav_order: 354
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Pegfilgrastim: predicted new indication Severe nonproliferative diabetic retinopathy (evidence level L5). Approved use: Not stated in the available licence records"
monitor_inn: "PEGFILGRASTIM"
predicted_indications: ["severe nonproliferative diabetic retinopathy", "diabetic retinopathy"]
---

# Pegfilgrastim
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

## Pegfilgrastim: From Neutropenia Support to Severe Nonproliferative Diabetic Retinopathy
{: .text-alpha }

## One-Sentence Summary

Pegfilgrastim is a long-acting (PEGylated) G-CSF that stimulates neutrophil production. The licence records compiled for this report do not state an approved indication.
The TxGNN model predicts it may be effective for **severe nonproliferative diabetic retinopathy** (and the broader category, diabetic retinopathy), but there are currently **0 clinical trials** and **0 publications** supporting this direction. This is a model prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the available licence records |
| Predicted New Indication | Severe nonproliferative diabetic retinopathy |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this report. Pegfilgrastim is generally known as a PEGylated G-CSF that stimulates neutrophil production and mobilizes hematopoietic progenitor cells.

A plausible but unverified link is that G-CSF-mobilized bone marrow-derived progenitor cells could support retinal vascular repair. However, the direction of effect is uncertain: neutrophil activation and inflammation are also implicated in diabetic retinal microvascular damage, and mobilized progenitor cells could promote pathological neovascularization. The effect could therefore be beneficial or harmful.

The second predicted indication, diabetic retinopathy (score 99.73%), is a broader parent category that overlaps with the severe nonproliferative entry. The two should not be counted as independent signals. No clinical or preclinical data support either prediction.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2724251 (cancelled) | Ziextenzo | Inj 6 mg per 0.6 ml syringe | Not stated |
| 2643340 (cancelled) | Ziextenzo | Inj 6 mg per 0.6 ml syringe | Not stated |

Both authorizations are cancelled, so no pegfilgrastim product is currently licensed in New Zealand according to the data compiled. The only dosage form listed is a prefilled injection syringe.

## Safety Considerations

- **Drug Interactions**: The interaction screening returned 100 entries. The 20 reviewed were all rated Moderate. Examples include:
  - Cytotoxic and antineoplastic agents: paclitaxel, mercaptopurine, azacitidine, arsenic trioxide, altretamine, trimetrexate
  - Targeted and immune agents: abemaciclib, acalabrutinib, avapritinib, aldesleukin, alemtuzumab, belantamab mafodotin
  - Radiopharmaceuticals and radioimmunotherapy: iobenguane (I-131), ibritumomab tiuxetan, tositumomab (with and without I-131), fludeoxyglucose (18F)
  - Others: pegvaliase, levamisole

Package insert warnings and contraindications were not available in the data compiled. Please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a high model score, with no clinical trials or publications. The direction of effect is mechanistically uncertain, as progenitor mobilization could help or worsen retinal vascular disease. The product is also not currently licensed in New Zealand.

**To proceed, the following is needed:**
- Mechanism of action data (e.g. from DrugBank) to cross-check the proposed link
- Package insert warnings and contraindications from Medsafe, which are required before any safety screening
- Preclinical or clinical evidence on G-CSF or pegfilgrastim in diabetic retinopathy, including a literature search to check for harm signals such as neovascularization
- Assessment of route compatibility, since the available systemic injection form has not been compared with the routes needed for retinal disease
- Clarification of the original approved indication and the regulatory pathway, given that all licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


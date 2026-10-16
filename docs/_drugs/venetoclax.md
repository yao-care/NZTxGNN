---
layout: default
title: Venetoclax
parent: Model Prediction Only (L5)
nav_order: 477
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Venetoclax: predicted new indication CLL/SLL with immunoglobulin heavy chain variable-region gene somatic… (evidence level L5). Approved use: Acute lymphoblastic/lymphocytic leukaemia"
---

# Venetoclax
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Venetoclax: From Acute Lymphoblastic Leukaemia and Richter Syndrome to CLL/SLL with Somatic Hypermutation of the IGHV Gene

## One-Sentence Summary

Venetoclax is an oral BCL-2 inhibitor. The compiled data list acute lymphoblastic leukaemia and Richter syndrome as its original indications.
The TxGNN model predicts it may be effective for **chronic lymphocytic leukaemia/small lymphocytic lymphoma (CLL/SLL) with immunoglobulin heavy chain variable-region (IGHV) gene somatic hypermutation**.
There are currently **0 clinical trials** and **0 publications** specific to this subtype, so the prediction rests on the model score and mechanistic plausibility alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Acute lymphoblastic/lymphocytic leukaemia; Richter syndrome (from the knowledge graph, because the New Zealand licence records contain no indication text) |
| Predicted New Indication | CLL/SLL with immunoglobulin heavy chain variable-region gene somatic hypermutation |
| TxGNN Prediction Score | 99.55% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only (no currently valid licences) |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on known information, venetoclax is a selective BCL-2 inhibitor. BCL-2 is a protein that helps cancer cells evade programmed cell death. Venetoclax is an established therapy in chronic lymphocytic leukaemia generally and in several other BCL-2-dependent blood cancers.

CLL/SLL cells depend heavily on BCL-2 for survival, which gives the predicted indication a sound mechanistic basis. The original indications, acute lymphoblastic leukaemia and Richter syndrome (a transformation of CLL), are also lymphoid malignancies.

The prediction is for a specific molecular subtype, CLL/SLL with mutated IGHV. Nothing in the compiled data addresses this subtype directly. Any benefit is inferred from CLL in general, and the high model score is a prediction, not clinical proof.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

All four licences for the single-ingredient product Venclexta are cancelled. No approved indication text is recorded for any of them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|------|
| 2555654 | Venclexta | Tab 50 mg | Cancelled |
| 2555638 | Venclexta | Tab 14 x 10 mg, 7 x 50 mg, 21 x 100 mg | Cancelled |
| 2555662 | Venclexta | Tab 100 mg | Cancelled |
| 2635054 | Venclexta | Tab 10 mg | Cancelled |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (BCL-2 inhibitor) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Drug Interactions**: 100 interactions are recorded. The 20 shown in the compiled data include these **Major**-level ones:
  - Aprepitant
  - Dexamethasone
  - Clarithromycin
  - Eliglustat
  - Rolapitant
  - Fidaxomicin
  - Cobicistat
  - Deferiprone
  - Samarium (153Sm) lexidronam

  Several other interactions are rated Moderate (e.g., alvimopan, loperamide, cimetidine, miconazole, rifaximin, clotrimazole, deferasirox) or Minor (naloxegol). Many of these involve CYP3A or transporter pathways, so dose review is needed when co-prescribing.

For warnings and contraindications, please refer to the package insert.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high and BCL-2 dependence in CLL/SLL makes it plausible. However, the compiled data contain no trials or publications for the IGHV-mutated subtype, so the evidence is model prediction only (L5). All New Zealand licences are also cancelled, and package insert safety information is missing.

**To proceed, the following is needed:**
- Subtype-specific clinical evidence, for example a review of venetoclax trial data in CLL stratified by IGHV mutation status
- Detailed mechanism of action data (MOA)
- Package insert warnings and contraindications from Medsafe
- Clarification of current New Zealand regulatory and supply status
- A drug-interaction management plan, especially for CYP3A-related Major interactions

**Other predicted indications:** The same data show stronger support for other indications. Myeloid leukaemia (AML) is rated L1 with a "Proceed with Guardrails" recommendation. Chronic myelogenous leukaemia (BCR-ABL1 positive) and follicular lymphoma are both L2. If the goal is to prioritise repurposing candidates, these deserve review ahead of the IGHV-mutated CLL/SLL subtype.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


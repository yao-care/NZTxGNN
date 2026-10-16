---
layout: default
title: Periciazine
parent: Model Prediction Only (L5)
nav_order: 357
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-15
description: "Periciazine: predicted new indication Migraine disorder (evidence level L5). Approved use: Not recorded in the New Zealand licence data (the approved indication…"
monitor_inn: "periciazine"
predicted_indications: ["migraine disorder", "migraine with brainstem aura", "atrophoderma vermiculata", "dysthymic disorder", "migraine with or without aura, susceptibility to", "ulerythema ophryogenesis"]
---

# Periciazine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

# Periciazine: From Psychosis and Severe Behavioural Disturbance to Migraine Disorder

## One-Sentence Summary

Periciazine is a phenothiazine antipsychotic (brand name Neulactil), used for schizophrenia and other psychoses and for short-term management of severe anxiety, agitation and impulsive behaviour.
The TxGNN model predicts it may be effective for **migraine disorder**, but this is a computational prediction only, with **0 clinical trials** and **0 publications** currently supporting it.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the New Zealand licence data (the approved indication text is empty). Pharmacology reference data describes schizophrenia and other psychoses, and short-term adjunctive management of severe anxiety, psychomotor agitation and violent or dangerously impulsive behaviour |
| Predicted New Indication | Migraine disorder |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for periciazine. Based on known information, it is a phenothiazine antipsychotic with dopamine D2 antagonism, plus antihistaminic, anticholinergic and alpha-adrenergic blocking activity. The pharmacology data compiled for this report links it to the dopamine D1 receptor (DRD1), though the source notes that no peer-reviewed data could be found to map a primary molecular target.

Other phenothiazines, such as chlorpromazine and prochlorperazine, are used for acute migraine, mainly for headache and nausea relief through D2 blockade. A class-level mechanistic rationale for periciazine in migraine is therefore plausible. However, no periciazine-specific clinical or literature evidence was found, so the high score remains a model output.

**Other predictions in the top six (all L5, all Hold):**

| Rank | Predicted Indication | Score | Comment |
|------|------|------|------|
| 2 | Migraine with brainstem aura | 99.84% | A subtype of migraine. The score likely reflects ontology proximity to the parent term, not independent evidence |
| 3 | Atrophoderma vermiculata | 99.36% | No plausible mechanistic link. Likely a knowledge-graph artifact |
| 4 | Dysthymic disorder | 99.25% | Speculative. Periciazine has no established antidepressant role, and older antipsychotics carry a tolerability burden in chronic mood disorders |
| 5 | Migraine with or without aura, susceptibility to | 99.22% | Describes a genetic susceptibility, not a treatable condition, so it is not directly actionable |
| 6 | Ulerythema ophryogenesis | 99.16% | No pharmacological rationale. Likely driven by similarity to other follicular skin disorders |

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 254665 | Neulactil | Tab 2.5 mg | Not recorded (licence cancelled) |
| 254673 | Neulactil | Tab 10 mg | Not recorded (licence cancelled) |

---

## Safety Considerations

- **Drug Interactions**: The interaction query completed with one entry. It is a pharmacology record of periciazine acting on the dopamine D1 receptor, not a drug-drug interaction with a severity level.

Please refer to the package insert for warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the TxGNN score and a class-level argument from other phenothiazines. There are no trials or publications for periciazine in migraine, and both New Zealand licences are cancelled, so no product is currently marketed.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data for periciazine, for example from DrugBank
- Periciazine-specific clinical or literature evidence in migraine, and a comparison with established phenothiazine use
- Confirmation of the original approved indications, as the licence data holds none
- Route and formulation compatibility assessment, and a review of the market and supply situation given the cancelled licences
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


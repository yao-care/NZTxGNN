---
layout: default
title: Amorolfine
parent: Model Prediction Only (L5)
nav_order: 36
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Amorolfine: predicted new indication Drug-induced osteoporosis (evidence level L5). Approved use: Onychomycosis (fungal nail infection)…"
monitor_inn: "AMOROLFINE"
predicted_indications: ["drug-induced osteoporosis", "disease of retroperitoneum", "disease of uterine broad ligament", "lumbar spinal stenosis", "celiac trunk compression syndrome", "abdominal cystic lymphangioma", "abdominal ectopic pregnancy", "sacrum chordoma", "lymph node palisaded myofibroblastoma", "urethral disease"]
---

# Amorolfine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Amorolfine: From Fungal Nail Infection to Drug-Induced Osteoporosis

## One-Sentence Summary

Amorolfine is a topical morpholine antifungal, used as a 5% nail solution for fungal nail infections (onychomycosis).
The TxGNN model predicts it may be effective for **drug-induced osteoporosis**, but this is a computational prediction only, with **0 clinical trials** and **0 publications** supporting it.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Onychomycosis (fungal nail infection), based on pharmacology reference data; the New Zealand licence record has no indication text |
| Predicted New Indication | Drug-induced osteoporosis |
| TxGNN Prediction Score | 99.998% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, amorolfine is a morpholine antifungal that inhibits fungal sterol delta-14 reductase and delta-7,8 isomerase, which blocks ergosterol biosynthesis. Its efficacy is established for topical treatment of nail fungal infections.

No plausible mechanistic link to bone metabolism has been identified. The very high TxGNN score (0.99998) comes from knowledge-graph patterns alone and is not backed by clinical or literature data. Such scores can reflect network-topology artifacts rather than real biological relevance. Amorolfine is also a topical product with minimal systemic exposure, which makes a systemic bone indication even less plausible. Route compatibility has not yet been assessed.

Other top-ranked predictions (disease of retroperitoneum, disease of uterine broad ligament, lumbar spinal stenosis, celiac trunk compression syndrome, abdominal cystic lymphangioma, abdominal ectopic pregnancy, sacrum chordoma, lymph node palisaded myofibroblastoma, urethral disease) share the same weakness: no trials, no literature and no mechanistic rationale. All are rated L5 and Hold.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2461633 | MycoNail (cancelled) | Nail solution 5% | Not stated in the licence record |

## Safety Considerations

- **Drug Interactions**: The interaction query completed with one pharmacological entry. Amorolfine is listed as a ligand of the voltage-gated sodium channel Nav1.7 (SCN9A, human). No severity level is assigned, and no drug-drug interactions with specific medicines were identified.

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a graph-based score. There are no clinical trials, no literature and no plausible mechanistic link between a topical ergosterol-synthesis inhibitor and drug-induced osteoporosis. The only New Zealand licence is cancelled, so there is no active local product either. The same applies to all other predicted indications reviewed.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (currently a blocking gap for safety screening)
- Mechanism of action data from DrugBank, to enable mechanistic-link analysis
- Preclinical or mechanistic evidence connecting amorolfine to bone metabolism (for example, in vitro osteoclast/osteoblast studies)
- Route and formulation compatibility assessment, since only a topical nail solution has been marketed
- Literature and trial searches to confirm whether any supporting evidence exists
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


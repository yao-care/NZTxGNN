---
layout: default
title: Rasagiline
parent: 僅模型預測 (L5)
nav_order: 300
evidence_level: L5
indication_count: 6
---

# Rasagiline
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

Using the evidence-pack fields directly; where a field is empty/`[Data Gap]` I'm pulling the only sourced fact available (the mechanism note embedded in rank-4's `repurposing_rationale`) rather than inventing anything, and flagging the inconsistency as the pack itself does.

---

# Rasagiline: From Parkinson's Disease to PLA2G6-associated Neurodegeneration

## One-Sentence Summary

> Rasagiline is a selective, irreversible MAO-B inhibitor originally used for Parkinson's disease (noted in the evidence pack's rationale text, though the drug-level MOA field itself is flagged as a data gap).
> The TxGNN model's top-ranked prediction is **PLA2G6-associated Neurodegeneration**, with a prediction score of **99.71%**,
> but currently **0 clinical trials** and **0 publications** support this specific link — the connection is theoretical only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Parkinson's disease (per repurposing_rationale text; structured `original_moa`/`original_indications` fields are marked Data Gap — see DG002) |
| Predicted New Indication | PLA2G6-associated Neurodegeneration |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L5 (model prediction only, no trials or literature) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the structured drug record (`original_moa: [Data Gap]`). Based on the only textual information present in this evidence pack — embedded in the rationale for a lower-ranked candidate — Rasagiline is understood to be a selective, irreversible MAO-B inhibitor that reduces dopamine breakdown and may have neuroprotective/anti-apoptotic effects, with proven efficacy in Parkinson's disease.

The top-ranked prediction, PLA2G6-associated Neurodegeneration (PLAN), is a rare genetic disorder of membrane phospholipid metabolism (including infantile neuroaxonal dystrophy and PARK14-type dystonia-parkinsonism). Some PLAN subtypes do present with a dystonia-parkinsonism phenotype, which is the only theoretical bridge to MAO-B inhibition and dopaminergic pathways. However, per the model's own rationale, this is **not** a classic dopamine-deficiency mechanism — PLAN is fundamentally a metabolic/structural lipid disorder, so the causal inference is weak.

Notably, a lower-ranked candidate in this pack (rank 4, "paralysis agitans, juvenile, of Hunt" — an early-onset Parkinson's-spectrum variant) shows a mechanistically much closer overlap with Rasagiline's known MAO-B action, but scored lower than PLAN and also has zero supporting evidence. This suggests the ranking here should be read as a raw model signal rather than a mechanistically validated shortlist.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

No authorization records are currently available — Rasagiline is not marketed in New Zealand under this evidence pack (`total_licenses: 0`).

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: `key_warnings`, `contraindications`, and DDI query all returned no data (DG001 — TFDA label not yet obtained, severity: Blocking). This gap currently prevents any Stage-1 safety pre-assessment.)*

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction sits at Evidence Level L5 — a model score with zero corroborating trials or literature, and the underlying mechanistic link (metabolic/lipid disorder vs. MAO-B/dopaminergic action) is explicitly described as weak in the source rationale. Compounding this, core drug-level data (MOA, package insert, safety profile) are marked as gaps, with DG001 rated **Blocking** for safety review.

**To proceed, the following is needed:**
- TFDA/regulatory package insert (warnings, contraindications) — DG001, Blocking
- Confirmed mechanism of action via DrugBank API — DG002
- Preclinical or case-level evidence specifically linking Rasagiline to PLAN before considering any advancement past S0
- Re-evaluation of whether the mechanistically closer candidate (Hunt's juvenile paralysis agitans, rank 4) warrants separate prioritization despite its lower model score
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Alfacalcidol
parent: Model Prediction Only (L5)
nav_order: 26
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Alfacalcidol: predicted new indication Familial isolated hypoparathyroidism due to impaired PTH secretion (evidence level L5). Approved use: Hyperparathyroidism, osteitis fibrosa, hypoparathyroidism…"
monitor_inn: "ALFACALCIDOL"
predicted_indications: ["Dahlberg-Borer-Newcomer syndrome", "craniofacial conodysplasia", "acromesomelic dysplasia, Campailla Martinelli type", "renal tubular acidosis"]
---

# Alfacalcidol
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

# Alfacalcidol: From Hyperparathyroidism to Familial Isolated Hypoparathyroidism Due to Impaired PTH Secretion

## One-Sentence Summary

Alfacalcidol is an active vitamin D analogue, originally used for hyperparathyroidism, osteitis fibrosa, hypoparathyroidism and renal osteodystrophy.
The TxGNN model predicts it may be effective for **familial isolated hypoparathyroidism due to impaired PTH secretion**, but **no clinical trials and no publications** specific to this condition were found, so the prediction rests on model output and mechanistic reasoning alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hyperparathyroidism, osteitis fibrosa, hypoparathyroidism, renal osteodystrophy (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Familial isolated hypoparathyroidism due to impaired PTH secretion |
| TxGNN Prediction Score | 99.61% |
| Evidence Level | L5 (model prediction only, no specific studies) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. From known pharmacology, alfacalcidol is a 1-alpha-hydroxylated vitamin D analogue. It bypasses the renal 1-alpha-hydroxylation step, which is PTH-dependent, and is converted to active 1,25-dihydroxyvitamin D after hepatic 25-hydroxylation.

Hypoparathyroidism is already among alfacalcidol's listed original indications. In familial isolated hypoparathyroidism, PTH secretion is impaired, so a drug that does not rely on PTH to become active is biologically sensible. The high TxGNN score probably reflects how close this disease sits to the known indication in the knowledge graph.

No trials or literature were retrieved for this specific subtype. Support is by extension from the parent indication, not direct evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for this predicted indication.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 315761 | One-Alpha | Oral drops 2 mcg per ml | Not stated (cancelled) |
| 203092 | One-Alpha | Capsule 1 mcg | Not stated (cancelled) |
| 203084 | One-Alpha | Capsule 0.25 mcg | Not stated (cancelled) |

All three licences are cancelled, so there is currently no valid marketed alfacalcidol product in New Zealand.

## Safety Considerations

- **Drug Interactions**: No drug interaction records were found.
- **Calcium-related risk (from the retrieved literature)**: Case reports in hypoparathyroidism describe hypercalcaemia and calcium-alkali syndrome when high-dose calcium is combined with active vitamin D. Calcium and renal function monitoring would be needed.

Please refer to the package insert for further safety information. Warnings and contraindications from the New Zealand label have not yet been compiled.

## Other Predicted Indications (For Reference)

| Rank | Predicted Disease | Score | Evidence Level | Assessment |
|---|---|---|---|---|
| 2 | Dahlberg-Borer-Newcomer syndrome (Fanconi-Bickel syndrome) | 99.60% | L5 | Plausible indirect rationale (supporting bone and mineral metabolism), but the retrieved papers are mostly off-target and none studies alfacalcidol in this disease. Hold. |
| 3 | Craniofacial conodysplasia | 99.55% | L5 | No evidence or mechanistic link identified. Hold. |
| 4 | Acromesomelic dysplasia, Campailla-Martinelli type | 99.53% | L5 | Not a vitamin D or mineral metabolism defect, so the rationale is lacking. Hold. |
| 5 | Renal tubular acidosis | 99.27% | L4 | Five case reports/reviews describe osteomalacia improving with alfacalcidol alongside alkali therapy. They are small, uncontrolled and confounded, and address the bone complication rather than the acidosis itself. Research question. |

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction is mechanistically reasonable, since alfacalcidol does not need PTH for activation and hypoparathyroidism is already a listed use. However, there are no trials or literature for this subtype, and alfacalcidol has no currently valid New Zealand licence. The next-ranked predictions have even weaker support. Renal tubular acidosis is the only alternative with any clinical signal, limited to case reports.

**To proceed, the following is needed:**
- Retrieval and review of the Medsafe package insert for warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Targeted literature search on alfacalcidol in familial or genetic hypoparathyroidism, to move beyond extrapolation from the parent indication
- Assessment of whether an alfacalcidol product is obtainable in New Zealand, given that all licences are cancelled
- For renal tubular acidosis, a structured review of the case reports to judge whether a controlled study on bone outcomes is justified
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


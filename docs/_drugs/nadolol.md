---
layout: default
title: Nadolol
parent: 僅模型預測 (L5)
nav_order: 237
evidence_level: L5
indication_count: 5
---

# Nadolol
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

Now I'll generate the report directly from the Evidence Pack JSON provided.

# Nadolol: From Antihypertensive Therapy to Malignant Hypertensive Renal Disease

## One-Sentence Summary

Nadolol is a non-selective β-adrenergic blocker whose formal original-indication and mechanism-of-action records are not yet populated in this evidence pack (flagged as data gaps DG001/DG002). The TxGNN model predicts it may be effective for **Malignant Hypertensive Renal Disease**, but this direction is currently supported only by class-level pharmacological reasoning — **0 clinical trials** and **0 publications** have been identified for this specific drug–disease pair.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in this evidence pack — DrugBank/TFDA package-insert extraction is an open data gap (DG001 Blocking, DG002 High) |
| Predicted New Indication | Malignant Hypertensive Renal Disease |
| TxGNN Prediction Score | 99.59% |
| Evidence Level | L4 (mechanism/class-effect reasoning only) |
| New Zealand Market Status | ✗ Not marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism-of-action data for nadolol has not been extracted into this evidence pack (DG002). Based on the pharmacological class information available in the underlying rationale, nadolol is a non-selective β-blocker, and drugs in this class are known to suppress renin release by blocking β1 receptors on the juxtaglomerular apparatus (JGA). This is a well-established, class-level (not nadolol-specific) mechanism relevant to renin-dependent hypertensive states.

Malignant hypertensive renal disease is typically driven by severe, renin-angiotensin-aldosterone system (RAAS)-dependent hypertension causing renal microvascular injury. Because β-blockade at the JGA can reduce renin secretion, there is a plausible theoretical rationale for a non-selective β-blocker like nadolol to serve as adjunctive antihypertensive therapy in this setting. However, this reasoning is explicitly a **class-effect inference**, not evidence derived from nadolol itself — no nadolol-specific trial or publication currently supports this pairing, and standard-of-care for malignant hypertension/malignant hypertensive renal disease is generally built around ACE inhibitors/ARBs and rapid blood-pressure control rather than β-blockade as a primary agent.

A closely related, near-identical prediction (rank 2, "malignant renovascular hypertension," score 99.59%) shares the same mechanistic basis and the same evidentiary gap, reinforcing that this is a class-level signal rather than an nadolol-specific finding.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

Nadolol is currently **not marketed** in New Zealand and has **0** active authorizations on record. No product license entries are available to summarize in this section.

---

## Other TxGNN-Predicted Indications Screened

For completeness, four additional candidates from the same prediction run were reviewed. None met the bar for prioritization above the primary candidate:

| Rank | Predicted Indication | Score | Evidence Level | Decision | Why Held |
|------|----------------------|-------|-----------------|----------|----------|
| 2 | Malignant renovascular hypertension | 99.59% | L4 | Hold | Same JGA/renin mechanism as rank 1; no nadolol-specific evidence; renovascular hypertension is primarily managed by revascularization or ACEI/ARB, with β-blockade only adjunctive |
| 3 | Pulmonary hypertension with unclear multifactorial mechanism | 99.53% | L5 | Hold | No supporting evidence found; non-selective β-blockers are traditionally used cautiously (relatively contraindicated) in pulmonary arterial hypertension due to negative inotropic/chronotropic effects reducing right-heart compensation — risk plausibly exceeds benefit |
| 4 | Pulmonary hypertension owing to lung disease and/or hypoxia | 99.53% | L5 | Hold | 20 PubMed hits were retrieved by keyword ("hypoxia") but on review concern general hypoxia biology (brain aging, oncology, HIF-1α pathways) with no connection to nadolol or β-blocker therapy — this is a keyword-matching artifact, not real evidence. Additionally, non-selective β-blockade carries a theoretical bronchoconstriction risk (β2 blockade) in hypoxic/obstructive lung disease patients |
| 5 | Braddock syndrome | 99.43% | L5 | Hold | Ultra-rare congenital disorder with no known pathophysiological link to β-adrenergic blockade; no trials, no literature — likely a knowledge-graph embedding artifact rather than a real signal |

These are reported for transparency but are **not** recommended for further evaluation at this time.

---

## Safety Considerations

Please refer to the package insert for safety information.

*Note: TFDA/regulatory package-insert warnings and contraindications (DG001) are flagged as a **Blocking** data gap — this evidence pack cannot yet support even an initial (S1) safety screen for nadolol.*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The leading prediction (malignant hypertensive renal disease) is supported only by a plausible but generic β-blocker class-effect mechanism, with zero nadolol-specific clinical trials or publications identified. Combined with a **Blocking** data gap on TFDA package-insert warnings/contraindications — which prevents even a baseline (S1) safety assessment — and the drug's non-marketed status in New Zealand, there is currently insufficient evidence or regulatory footing to advance beyond a research hypothesis.

**To proceed, the following is needed:**
- TFDA package-insert data (warnings, contraindications) — resolve Blocking gap DG001 before any safety screening can begin
- Confirmed mechanism-of-action and original-indication data from DrugBank — resolve High-severity gap DG002
- Nadolol-specific preclinical or clinical evidence (trials or literature) directly addressing malignant hypertensive renal disease, rather than class-level β-blocker inference
- Route-of-administration compatibility assessment (currently marked "pending" for all candidate indications)
- Clarification of New Zealand regulatory pathway, given the drug is not currently marketed
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


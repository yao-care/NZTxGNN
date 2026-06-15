---
layout: default
title: Domperidone
parent: 僅模型預測 (L5)
nav_order: 125
evidence_level: L5
indication_count: 1
---

# Domperidone
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Domperidone: From Nausea & Vomiting to Nephrogenic Syndrome of Inappropriate Antidiuresis

## One-Sentence Summary

Domperidone is a dopamine D2 receptor antagonist widely used as an antiemetic and prokinetic agent for nausea, vomiting, and gastroparesis.
The TxGNN model predicts it may be effective for **Nephrogenic Syndrome of Inappropriate Antidiuresis (NSIAD)**,
however there are currently **no registered clinical trials** and **no published literature** specifically supporting this repurposing direction — making this a model-prediction-only candidate requiring foundational investigation.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Nausea, vomiting, gastroparesis (dopamine antagonist / prokinetic class) |
| Predicted New Indication | Nephrogenic Syndrome of Inappropriate Antidiuresis (NSIAD) |
| TxGNN Prediction Score | 99.08% |
| Evidence Level | L5 — Model prediction only, no actual studies |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known pharmacology, domperidone is a peripheral dopamine D2/D3 receptor antagonist. It does not readily cross the blood–brain barrier, and its primary clinical use has been in gastrointestinal motility disorders and chemotherapy-induced emesis. It has a well-characterised safety signal regarding QTc prolongation, which has led to restriction or withdrawal in some markets.

The proposed repurposing target — Nephrogenic Syndrome of Inappropriate Antidiuresis (NSIAD) — is a rare congenital condition caused by gain-of-function mutations in the V2 vasopressin receptor (AVPR2), leading to constitutive water reabsorption and chronic dilutional hyponatremia independent of arginine vasopressin (AVP) levels. The speculative mechanistic link lies in the role of dopamine in renal tubular function: renal dopamine (produced locally from circulating L-DOPA) acts via D1-like receptors to inhibit tubular sodium and water reabsorption, contributing to natriuresis and diuresis. Dopamine D2 receptors are also expressed in the kidney and may modulate collecting duct water permeability through cross-talk with vasopressin signalling pathways.

Whether domperidone — a peripherally-acting D2 antagonist — can meaningfully modulate renal water handling in NSIAD remains entirely speculative and biologically counterintuitive (D2 blockade would be expected to blunt rather than augment dopaminergic diuresis). The TxGNN graph neural network may have captured indirect network proximity between domperidone and NSIAD-related nodes in the knowledge graph, but no experimental or clinical data currently exists to support or refute this prediction. Independent mechanistic hypothesis generation and wet-lab validation would be the necessary first steps before any clinical consideration.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Domperidone is not currently authorised for sale in New Zealand. No Medsafe licenses are on record for this compound.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note for clinical teams:** Domperidone carries a known class risk of QTc interval prolongation and ventricular arrhythmia. This risk is dose-dependent and is heightened in patients with existing cardiac disease, electrolyte imbalances (including the hyponatraemia characteristic of NSIAD), or those receiving concomitant QT-prolonging agents. This interaction would need to be explicitly addressed in any future protocol design.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
There is no clinical trial evidence, no published literature, and no established mechanistic framework connecting domperidone to NSIAD. The TxGNN prediction score is high (99.08%), but the score alone cannot substitute for biological plausibility data at this stage. Furthermore, the QTc liability of domperidone poses a specific safety concern in the NSIAD patient population, who commonly present with electrolyte disturbances that independently increase arrhythmia risk.

**To proceed, the following is needed:**

- **Mechanistic validation:** Literature review or wet-lab assay to determine whether domperidone or D2 blockade has any measurable effect on renal collecting duct aquaporin-2 (AQP2) expression or V2 receptor signalling in NSIAD-relevant models
- **Full MOA data:** Retrieve complete DrugBank pharmacodynamic profile including targets, enzymes, transporters, and carriers
- **Safety data sheet:** Obtain TFDA / Medsafe / EMA package insert data on contraindications and warnings to complete the S1 safety screening
- **QTc risk assessment:** Formal cardiac risk stratification for domperidone in hyponatraemic patients before any in-human study design
- **Knowledge graph audit:** Review which TxGNN graph edges connected domperidone to NSIAD, to determine whether the prediction is mechanistically grounded or driven by indirect network artefacts
- **Orphan disease considerations:** Given NSIAD is an ultra-rare condition (fewer than ~100 published cases worldwide), any development pathway would need orphan drug designation strategy and patient registry linkage
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


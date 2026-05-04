---
layout: default
title: Cladribine
parent: 僅模型預測 (L5)
nav_order: 16
evidence_level: L5
indication_count: 7
---

# Cladribine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **7** 個
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

# Cladribine: Evidence Pack Incomplete — Repurposing Evaluation On Hold

## One-Sentence Summary

Cladribine (DrugBank: DB00242) is a purine nucleoside analogue with known cytotoxic activity, though original indication data is absent from the current dataset.
The TxGNN pipeline returned **no predicted indications** for this candidate, and two critical data gaps — mechanism of action and regulatory package insert — remain unresolved.
A full repurposing evaluation **cannot be completed** until these gaps are remediated.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not available in current dataset |
| Predicted New Indication | None — TxGNN predictions not returned |
| TxGNN Prediction Score | N/A |
| Evidence Level | L5 — model prediction not available; no supporting evidence |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## New Zealand Market Information

Cladribine currently holds **no regulatory authorizations** in New Zealand. No approved products, dosage forms, or licensed indications are on record in the dataset.

---

## Cytotoxicity

> Determination basis: Cladribine (2-chlorodeoxyadenosine) belongs to the purine nucleoside analogue class — a recognised category of conventional cytotoxic chemotherapy agents.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic — Purine nucleoside analogue |
| Myelosuppression Risk | High (profound and prolonged lymphocytopenia and neutropenia are class-defining toxicities) |
| Emetogenicity Classification | Low |
| Monitoring Items | CBC with differential, CD4⁺ lymphocyte count, renal function, hepatic function |
| Handling Protection | Must follow cytotoxic drug handling and disposal regulations |

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** The TFDA/regulatory package insert query returned a result (Query Log ID 4: `result_status: success`), but the content has not yet been parsed into structured fields. Extracting warnings and contraindications from that document is the highest-priority remediation action (see below).

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Evidence Pack for Cladribine is critically incomplete — the TxGNN model returned no predicted indications, original indication records are absent, mechanism of action data is missing, and regulatory safety content has not been extracted. There is no scientific basis on which to evaluate a repurposing hypothesis at this stage.

**To proceed, the following is needed:**

- **[Blocking — DG001]** Parse the TFDA package insert PDF already retrieved (Query Log ID 4) to extract warnings, contraindications, and approved indications
- **[High — DG002]** Query DrugBank API for mechanism of action, pharmacodynamics, and drug categories for DB00242
- **[Blocking]** Investigate why TxGNN returned zero predicted indications for this candidate — check whether the drug node exists in the knowledge graph and whether the model ran successfully
- **[Required]** Source drug-drug interaction data from an alternative DDI database (e.g., DrugBank interactions, Lexicomp) since the primary DDI query returned not found
- **[Recommended]** Cross-reference international market authorisations (FDA, EMA, TGA) to supplement missing original indication data and inform knowledge graph node attributes
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


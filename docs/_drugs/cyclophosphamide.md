---
layout: default
title: Cyclophosphamide
parent: 僅模型預測 (L5)
nav_order: 21
evidence_level: L5
indication_count: 5
---

# Cyclophosphamide
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

# Cyclophosphamide: Preliminary Assessment — TxGNN Predictions Pending

## One-Sentence Summary

Cyclophosphamide is a classical alkylating antineoplastic agent with established use across oncology and immunology.
The current Evidence Pack does not contain TxGNN model predictions for new indications, and key data fields — including mechanism of action, safety warnings, and regulatory authorizations — remain to be populated.
This report represents a **preliminary structural assessment only**; a full repurposing evaluation cannot proceed until the data pipeline is completed.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not retrieved in current data pipeline run |
| Predicted New Indication | No TxGNN predictions available |
| TxGNN Prediction Score | Not available |
| Evidence Level | Not assessable |
| New Zealand Market Status | Not marketed (0 authorizations found) |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** — critical data missing |

---

## Why is This Prediction Reasonable?

Mechanism of action data was not retrieved in the current Evidence Pack. Based on established pharmacological knowledge, Cyclophosphamide is a nitrogen mustard alkylating agent that covalently crosslinks DNA strands, thereby inhibiting replication and inducing apoptosis in rapidly dividing cells. It is also a prodrug requiring hepatic activation (primarily via CYP2B6 and CYP3A4) to its active metabolite 4-hydroxycyclophosphamide. This dual oncologic and immunosuppressive profile underlies its broad application across haematological malignancies, solid tumours, and autoimmune conditions.

Because the `predicted_indications` array is empty in this Evidence Pack, no TxGNN-driven repurposing candidate can be evaluated at this time. The TxGNN prediction step must be completed before a mechanistic linkage between Cyclophosphamide's known biology and any new candidate indication can be articulated.

---

## Cytotoxicity

Cyclophosphamide meets all criteria for antineoplastic classification: it belongs to the alkylating agent / nitrogen mustard class and has established use in cancer treatment.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic — Alkylating agent (nitrogen mustard class) |
| Myelosuppression Risk | **High** — bone marrow suppression is a primary dose-limiting toxicity; neutropenia nadir typically occurs 10–14 days post-administration, with thrombocytopenia and anaemia also common |
| Emetogenicity Classification | **Moderate to High** — dose-dependent; high-dose regimens (≥1,500 mg/m²) carry high emetogenic risk requiring prophylactic antiemetics |
| Monitoring Items | CBC with differential (weekly during active therapy), urinalysis and urine microscopy (hemorrhagic cystitis surveillance), serum creatinine and BUN, liver function tests (ALT, AST, bilirubin), electrolytes (especially sodium — SIADH risk at high doses) |
| Handling Protection | Must follow cytotoxic drug handling regulations; closed-system drug transfer devices (CSTD) required for preparation and administration |

---

## Safety Considerations

Please refer to the package insert for safety information.

> Note: Safety warnings and contraindications were listed as data gaps in this Evidence Pack (DG001). The TFDA package insert query returned a success status (Query ID 4), but parsed content was not included in the output. Retrieval and structured parsing of the package insert is required before a formal safety assessment can be completed.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Evidence Pack is structurally incomplete — the TxGNN prediction step has not produced candidate indications, and two blocking or high-severity data gaps (DG001: TFDA safety warnings; DG002: mechanism of action) remain unresolved. No repurposing evaluation can be conducted without at least one ranked candidate indication to anchor the analysis.

**To proceed, the following is needed:**

- **Run TxGNN predictions** for Cyclophosphamide (DB00531) to generate ranked candidate indications with scores
- **Retrieve MOA from DrugBank** (DG002 — listed as High severity): query DrugBank API for `DB00531` pharmacodynamics and mechanism fields
- **Parse TFDA package insert** (DG001 — listed as Blocking): the insert was successfully retrieved (Query ID 4) but content was not structured; extract warnings, contraindications, and special population guidance
- **Investigate zero-authorization result**: Cyclophosphamide has wide global availability; 0 TFDA licenses may reflect a search term mismatch (brand name vs. INN) rather than true non-registration — verify using brand names (e.g., Endoxan, Cytoxan)
- **Re-run evidence collection** (clinical trials via ClinicalTrials.gov, literature via PubMed) for the top 3–5 TxGNN-predicted indications once predictions are available
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


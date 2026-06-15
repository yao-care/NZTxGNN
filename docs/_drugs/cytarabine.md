---
layout: default
title: Cytarabine
parent: 僅模型預測 (L5)
nav_order: 95
evidence_level: L5
indication_count: 9
---

# Cytarabine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
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

# CYTARABINE: Repurposing Analysis — TxGNN Predictions Pending

## One-Sentence Summary

Cytarabine (ara-C) is a pyrimidine nucleoside analogue and cornerstone chemotherapy agent, classically used for acute myeloid leukaemia (AML) and other haematological malignancies.
This Evidence Pack is **critically incomplete** — no TxGNN repurposing predictions have been generated, and two data gaps (TFDA package insert warnings, DrugBank MOA) must be resolved before a repurposing evaluation can proceed.
A full report will be issued once predictions and missing data are available.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Acute myeloid leukaemia, acute lymphocytic leukaemia, meningeal leukaemia *(general knowledge; not recorded in this Evidence Pack)* |
| Predicted New Indication | Not yet available |
| TxGNN Prediction Score | Not yet available |
| Evidence Level | N/A — No predictions available |
| Taiwan Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why is This Prediction Reasonable?

No TxGNN repurposing predictions are present in this Evidence Pack, so a mechanism-bridging analysis cannot yet be performed. The following reflects established pharmacological knowledge of cytarabine as background context.

Cytarabine is a cell-cycle–specific (S-phase) antimetabolite. Once inside the cell, it is phosphorylated to its active triphosphate form (Ara-CTP), which competes with deoxycytidine triphosphate (dCTP) for incorporation into DNA. This terminates or stalls the replicating DNA strand and inhibits DNA polymerase α, triggering apoptosis in rapidly dividing cells. Its high activity against haematological malignancies stems from the elevated dCTP pool turnover in leukaemic blast cells.

Detailed MOA data from DrugBank was not successfully retrieved in this Evidence Pack (Data Gap DG002, severity: High). Once MOA data is available, a systematic mechanism-to-indication bridging analysis can identify biologically plausible repurposing targets — for example, exploring cytarabine's DNA-damaging properties in solid tumour contexts or its role in viral replication inhibition (a historically investigated direction).

---

## Cytotoxicity

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic — Pyrimidine antimetabolite (nucleoside analogue) |
| Myelosuppression Risk | **High** — bone marrow suppression is the dose-limiting toxicity; leukopenia, thrombocytopenia, and anaemia are expected at standard and high doses |
| Emetogenicity Classification | **Moderate** at standard doses; increases to moderate–high at high-dose consolidation regimens (≥ 1 g/m²) |
| Monitoring Items | CBC with differential (neutrophil and platelet nadir monitoring), liver function tests, renal function, neurological assessment (cerebellar toxicity screening at high-dose regimens), ophthalmologic exam (conjunctivitis prevention with steroid eye drops at high doses) |
| Handling Protection | Cytotoxic drug handling regulations apply — preparation in a biological safety cabinet, full PPE (gloves, gown, eye protection) required; follow institutional cytotoxic waste disposal protocols |

---

## Safety Considerations

Please refer to the package insert for safety information. The TFDA package insert was queried (query log ID 4, status: success), however key warnings and contraindications were not extracted into this Evidence Pack (Data Gap DG001, severity: **Blocking** — prevents progression to safety initial assessment S1). DDI data query returned no results.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This Evidence Pack contains two unresolved data gaps — no TxGNN predictions have been generated, and a Blocking-severity gap in TFDA safety data prevents completion of the standard evaluation framework. There is currently no repurposing candidate to evaluate.

**To proceed, the following is needed:**

- **[Blocking — DG001]** Extract TFDA package insert warnings, contraindications, and special population data; required to pass Safety Initial Assessment (S1)
- **[Blocking]** Run TxGNN prediction pipeline for `DB00987 / CYTARABINE` to generate ranked repurposing candidates; no evaluation is possible without predictions
- **[High — DG002]** Retrieve DrugBank MOA data via DrugBank API; required for mechanism-bridging analysis
- Once predictions are available, run automated evidence collection (ClinicalTrials.gov, PubMed) for top-ranked indications and re-issue a full evaluation report
- Confirm whether cytarabine is registered in any international market (e.g., FDA, EMA) to supplement the Taiwan non-marketed status and contextualize safety expectations
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


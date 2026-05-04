---
layout: default
title: Daunorubicin
parent: 僅模型預測 (L5)
nav_order: 32
evidence_level: L5
indication_count: 10
---

# Daunorubicin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
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

# Daunorubicin: Anthracycline Antineoplastic — Repurposing Evaluation (TxGNN Output Pending)

## One-Sentence Summary

Daunorubicin is a classical anthracycline antineoplastic antibiotic, widely used as a backbone agent in acute leukemia (AML/ALL) chemotherapy regimens.
This Evidence Pack does not yet contain TxGNN-predicted new indications; the evaluation below reflects only regulatory and safety data currently available.
A complete repurposing assessment — including evidence level rating and a Go/Hold decision — requires TxGNN prediction output before proceeding.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Acute leukemia (AML / ALL; standard clinical use) |
| Predicted New Indication | — (TxGNN output not yet available) |
| TxGNN Prediction Score | — |
| Evidence Level | — |
| Taiwan Market Status | 未上市 (Not marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Taiwan Market Information

Daunorubicin currently has **no registered products** in Taiwan (0 authorizations). No product name, dosage form, or approved indication data is available from TFDA records.

---

## Cytotoxicity

Daunorubicin is a canonical anthracycline cytotoxic agent. The following information is based on established pharmacological classification.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic — Anthracycline class (DNA intercalator / Topoisomerase II inhibitor) |
| Myelosuppression Risk | **High** — Dose-limiting myelosuppression; severe neutropenia and thrombocytopenia are expected; nadir typically at Days 10–14 |
| Emetogenicity Classification | Moderate to High |
| Monitoring Items | CBC with differential (at least weekly during treatment), cardiac function (baseline and serial ECHO or MUGA scan — cumulative cardiotoxicity risk), liver function tests, renal function, serum electrolytes |
| Handling Protection | **Yes — mandatory.** Daunorubicin is a **vesicant**: extravasation causes severe tissue necrosis. Must follow cytotoxic drug handling regulations (closed-system transfer devices, PPE, hazardous waste disposal) |

> ⚠️ **Cardiotoxicity note:** Cumulative dose-dependent cardiomyopathy is a class effect of anthracyclines. Lifetime cumulative dose limits must be tracked when planning any new indication use.

---

## Safety Considerations

Safety data (key warnings, contraindications, drug-drug interactions) could not be retrieved from TFDA package insert records or the DDI database for this Evidence Pack version. Please refer to the current package insert and authoritative clinical references for full safety information.

> **Outstanding data gaps requiring remediation before clinical use:**
> - **DG001 (Blocking):** TFDA package insert warnings and contraindications not yet parsed — blocks S1 safety screening
> - **DG002 (High):** Mechanism of action (MOA) not retrieved from DrugBank — limits mechanistic rationale analysis

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Evidence Pack is structurally incomplete. TxGNN predicted indications are absent (`predicted_indications: []`), meaning no repurposing candidate has been generated for evaluation. Additionally, a Blocking data gap (DG001) prevents safety screening from proceeding.

**To proceed, the following is needed:**

- [ ] **TxGNN model output** — run prediction pipeline for DB00694 and populate `predicted_indications` with scores, clinical trial links, and literature PMIDs
- [ ] **TFDA package insert** (DG001 — Blocking) — download and parse PDF to extract warnings, contraindications, and dosing limits
- [ ] **DrugBank MOA data** (DG002 — High) — query DrugBank API for mechanism of action, pharmacodynamics, and toxicity categories
- [ ] **DDI database** — re-query drug-drug interaction database (current status: `not_found`; verify query parameters)
- [ ] **Cumulative cardiotoxicity threshold review** — mandatory for any anthracycline repurposing proposal; establish lifetime dose ceiling for the proposed new indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


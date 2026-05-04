---
layout: default
title: Dasatinib
parent: 僅模型預測 (L5)
nav_order: 31
evidence_level: L5
indication_count: 10
---

# Dasatinib
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

# Dasatinib: TxGNN Repurposing Evaluation — Incomplete Evidence Pack

## One-Sentence Summary

Dasatinib (DrugBank ID: DB01254) is a second-generation BCR-ABL and Src family tyrosine kinase inhibitor (TKI) with established use in chronic myeloid leukemia (CML) and Philadelphia chromosome-positive acute lymphoblastic leukemia (Ph+ ALL).
The current evidence pack contains **no TxGNN repurposing predictions** for this drug, and critical mechanistic and safety data are absent.
This report is a **data-gap assessment only**; no repurposing recommendation can be issued until the missing inputs are resolved.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not available in current evidence pack |
| Predicted New Indication | No predictions returned by TxGNN |
| TxGNN Prediction Score | — |
| Evidence Level | L5 (model prediction only — no supporting studies identified) |
| New Zealand Market Status | Not marketed (0 authorizations) |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why is This Prediction Reasonable?

No TxGNN predictions are present in this evidence pack, so a mechanistic justification for a new indication cannot be provided at this time.

Currently, detailed mechanism of action data is not available in the evidence pack. Based on established pharmacological knowledge, Dasatinib is a second-generation BCR-ABL tyrosine kinase inhibitor that also inhibits Src family kinases, c-Kit, and PDGFR-β. Its efficacy in CML and Ph+ ALL has been extensively validated in pivotal clinical trials (DASISION, CA180-034). These broad kinase targets have independently been implicated in several solid tumours and inflammatory conditions, which may explain why TxGNN would be expected to generate predictions for this molecule once the model pipeline is re-run with complete input data.

Should TxGNN predictions become available, a mechanistic bridge analysis can be performed to assess whether the kinase inhibition profile is pharmacologically applicable to the predicted indication.

---

## New Zealand Market Information

Dasatinib currently holds **no regulatory authorizations** in New Zealand. No product licenses, dosage form data, or approved indication texts are available in the evidence pack.

---

## Cytotoxicity

Dasatinib is an antineoplastic agent (targeted therapy — tyrosine kinase inhibitor). The following cytotoxicity profile is based on class knowledge and published prescribing information.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Targeted therapy (BCR-ABL / Src family tyrosine kinase inhibitor) |
| Myelosuppression Risk | High — neutropenia, thrombocytopenia, and anaemia are frequent; Grade 3/4 cytopenias reported in >50% of CML patients in some trials |
| Emetogenicity Classification | Low |
| Monitoring Items | CBC with differential (weekly for first 2 months, then monthly), liver function tests, renal function, fluid status (pleural effusion risk), QT interval (baseline ECG) |
| Handling Protection | Must follow cytotoxic drug handling regulations; tablets should not be crushed |

---

## Safety Considerations

Please refer to the package insert for safety information.
Safety data (key warnings, contraindications, and drug-drug interactions) were not successfully retrieved in this evidence pack. The most clinically significant known risks include pleural effusion, pulmonary arterial hypertension, QT prolongation, and severe myelosuppression — these must be assessed before any repurposing study is initiated.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN prediction pipeline returned an empty `predicted_indications` array for Dasatinib, and both MOA data and regulatory safety information are absent; a repurposing evaluation cannot be conducted without these foundational inputs.

**To proceed, the following is needed:**

- **Re-run TxGNN pipeline** for Dasatinib (DB01254) and verify that the drug node is correctly mapped in the knowledge graph before the next prediction run
- **Retrieve MOA data** from DrugBank API (DrugBank ID: DB01254) — severity rated *High*, directly impacts mechanistic relevance analysis
- **Retrieve safety data** (key warnings and contraindications) by downloading and parsing the originator package insert PDF from the relevant regulatory authority — severity rated *Blocking* for S1 safety screening
- **Re-query DDI database** once safety data are in place
- **Verify New Zealand (Medsafe) registration status** independently, as Dasatinib (Sprycel®) may hold authorizations not yet captured in this evidence pack
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


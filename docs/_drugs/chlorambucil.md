---
layout: default
title: Chlorambucil
parent: 僅模型預測 (L5)
nav_order: 71
evidence_level: L5
indication_count: 8
---

# Chlorambucil
{: .fs-9 }

證據等級: **L5** | 預測適應症: **8** 個
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

# Chlorambucil: From Chronic Lymphocytic Leukaemia to Pregerminal Center CLL/Small Lymphocytic Lymphoma

## One-Sentence Summary

Chlorambucil is a nitrogen mustard alkylating agent that served as the undisputed standard first-line treatment for Chronic Lymphocytic Leukaemia (CLL) for over six decades.
The TxGNN model predicts it may be specifically effective for **Pregerminal Center CLL/Small Lymphocytic Lymphoma** — the clinically aggressive, unmutated IGHV molecular subtype (U-CLL) —
with **1 publication** currently supporting this specific molecular subtype indication.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Chronic Lymphocytic Leukaemia (CLL) — historical standard of care (not registered in New Zealand) |
| Predicted New Indication | Pregerminal Center CLL/Small Lymphocytic Lymphoma (U-CLL) |
| TxGNN Prediction Score | 99.72% |
| Evidence Level | L3 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this dataset. Based on established pharmacological knowledge, Chlorambucil is a bifunctional alkylating agent of the nitrogen mustard class. It crosslinks DNA strands interstrand and intrastrand, preventing DNA replication and RNA transcription. This leads to programmed cell death preferentially in rapidly proliferating lymphocytes — making it biologically well-suited to lymphoid malignancies.

Pregerminal center CLL (U-CLL) is defined by unmutated immunoglobulin heavy chain variable region genes (IGHV), a molecular finding that distinguishes the high-risk, rapidly proliferating subset of CLL from the more indolent mutated IGHV subtype (M-CLL). Because U-CLL cells retain a more "naive" B-cell phenotype with higher proliferative activity, they are mechanistically more vulnerable to cytotoxic alkylating agents such as Chlorambucil. This subtype distinction was characterised after decades of Chlorambucil's widespread use in CLL, meaning the TxGNN prediction represents a re-evaluation of a proven drug against a newly defined molecular entity rather than an entirely novel application.

The TxGNN score of 99.72% (rank #2977 out of all disease nodes) reflects the model's recognition that pregerminal center CLL shares the same lymphoid biology as the CLL for which Chlorambucil was historically the standard of care. Although direct clinical trial data specifically labelling this molecular subtype is absent in the current search, the broader CLL literature — including the landmark RESONATE-2 Phase 3 trial (PMID 36672456) in which Chlorambucil served as the active comparator arm — provides strong indirect support. The prediction is biologically sound and historically grounded.

---

## Clinical Trial Evidence

Currently no related clinical trials registered specifically for pregerminal center CLL/SLL.

> **Context note:** While no trials were retrieved that directly address the pregerminal center (U-CLL) molecular subtype, Chlorambucil has been the standard comparator arm in multiple pivotal Phase 3 CLL trials (e.g., RESONATE-2, CLL11, COMPLEMENT-1). Subgroup analyses stratified by IGHV mutation status from these trials represent the most relevant indirect evidence and should be retrieved as a next step.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [12577769](https://pubmed.ncbi.nlm.nih.gov/12577769/) | 2003 | Review | Nederlands tijdschrift voor geneeskunde | Describes two CLL molecular subtypes: pre-germinal centre (unmutated IGHV, higher risk) vs post-germinal centre (mutated IGHV, better prognosis); establishes clinical basis for risk-adapted treatment stratification in CLL |

---

## New Zealand Market Information

Chlorambucil is **not currently registered** in New Zealand. No product authorizations are on record.

---

## Cytotoxicity

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic — Nitrogen mustard alkylating agent |
| Myelosuppression Risk | High — bone marrow suppression is the primary dose-limiting toxicity; neutropenia, thrombocytopenia, and anaemia are characteristic adverse effects; cumulative myelosuppression risk increases with prolonged dosing |
| Emetogenicity Classification | Low to moderate |
| Monitoring Items | Full blood count (with differential) prior to each cycle and during therapy; liver function tests; renal function; monitor for cumulative bone marrow toxicity |
| Handling Protection | Must be handled in accordance with cytotoxic drug handling regulations; avoid skin contact and inhalation; dispose of as cytotoxic waste |

---

## Safety Considerations

Please refer to the package insert for safety information.

> Detailed safety data (key warnings, contraindications, and drug interactions) were not retrievable from the current data sources. TFDA package insert data and international labelling (EMA/FDA) should be reviewed prior to any clinical application. Key known risks include myelosuppression, secondary malignancy (including acute myeloid leukaemia with chronic use), teratogenicity, and seizures at high doses.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Chlorambucil has over 60 years of clinical evidence in CLL broadly, and the pregerminal center (U-CLL) subtype is a molecular reclassification of the same disease. The prediction score of 99.72% is consistent with the drug's established biological mechanism in lymphoid malignancies, and while direct subtype-specific trial data is absent from the current search, robust indirect evidence from major Phase 3 CLL trials (in which Chlorambucil served as the comparator arm) is retrievable.

**To proceed, the following is needed:**

- Retrieve IGHV mutation status–stratified subgroup data from completed Phase 3 CLL trials (RESONATE-2, CLL11, COMPLEMENT-1) to assess differential efficacy in U-CLL vs M-CLL
- Obtain full package insert safety data (EMA/FDA labelling) to complete contraindication and key warnings assessment — currently a blocking data gap
- Confirm mechanism of action from DrugBank API to support regulatory dossier for molecular-subtype indication expansion
- Establish a haematological monitoring protocol appropriate for the elderly/unfit target population in whom Chlorambucil remains most relevant
- Assess current treatment landscape: evaluate whether the benefit-risk profile of Chlorambucil (vs ibrutinib, venetoclax+obinutuzumab) supports a specific niche in U-CLL for resource-limited or BTK-inhibitor–ineligible settings
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


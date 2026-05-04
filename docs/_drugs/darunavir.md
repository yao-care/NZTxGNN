---
layout: default
title: Darunavir
parent: 僅模型預測 (L5)
nav_order: 30
evidence_level: L5
indication_count: 4
---

# Darunavir
{: .fs-9 }

證據等級: **L5** | 預測適應症: **4** 個
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

# DARUNAVIR: Repurposing Candidate – Evaluation Incomplete

## One-Sentence Summary

Darunavir is an internationally approved HIV-1 protease inhibitor used for the treatment of HIV infection in adults and paediatric patients.
This Evidence Pack contains **no TxGNN-predicted new indications** and has critical data gaps across safety, regulatory, and mechanistic domains, making a full repurposing evaluation impossible at this stage.
A **Hold** decision is warranted until the missing inputs are resolved.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | HIV-1 infection (international approval; no New Zealand authorization on record) |
| Predicted New Indication | Not available – TxGNN predictions not yet generated |
| TxGNN Prediction Score | Not available |
| Evidence Level | L5 (no repurposing studies; model output absent) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why a Full Prediction Cannot Be Assessed Yet

No entries exist in `predicted_indications`, so a repurposing rationale cannot be constructed from this Evidence Pack.

What is known from international prescribing information is that darunavir is a second-generation HIV-1 protease inhibitor. It binds with high affinity to the active site of HIV-1 protease, preventing cleavage of the Gag-Pol polyprotein and thus blocking maturation of viral particles. This mechanism has attracted interest in whether protease inhibitor scaffolds may apply to other viral targets or certain cancers—but until TxGNN scoring is available, any mechanistic bridge to a new indication would be speculative and outside the scope of this Evidence Pack.

Detailed mechanism of action data was flagged as a data gap (DG002) in this pack and must be loaded from DrugBank before the "Why is This Prediction Reasonable?" section can be properly completed.

---

## New Zealand Market Information

Darunavir has **0 active authorizations** in New Zealand. It is not currently marketed or registered in this jurisdiction.

> Note: Darunavir (brand name Prezista®) is approved by the FDA, EMA, and multiple other regulatory agencies for HIV-1 treatment. This international approval history may shorten the regulatory pathway if a repurposing indication is later identified.

---

## Safety Considerations

Package insert warnings, contraindications, and drug-drug interaction data were not retrieved in this evaluation cycle (data gaps DG001 and DDI query returned `not_found`).

As an interim reference, please consult:
- **Prezista® FDA label** or **EMA SmPC** for full contraindication and warning text
- Known drug class warnings for HIV protease inhibitors include hepatotoxicity risk, skin reactions (including Stevens-Johnson Syndrome), lipid and glucose metabolism effects, and extensive CYP3A4-mediated drug interactions

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This Evidence Pack is materially incomplete: there are no TxGNN-predicted indications, no safety data, no New Zealand regulatory approvals, and the mechanism of action has not been loaded. A repurposing evaluation cannot responsibly proceed without these inputs.

**To proceed, the following is needed:**

- **Run TxGNN model** for DARUNAVIR (DB01264) to generate predicted indications and confidence scores
- **Download and parse package insert** (TFDA PDF or international SmPC) to extract MOA, warnings, and contraindications — resolves DG001 (Blocking severity)
- **Retrieve MOA from DrugBank API** — resolves DG002 (High severity); required for mechanistic plausibility analysis
- **Re-query drug-drug interaction database** to populate DDI profile
- **Confirm international market status** (Prezista® is approved in 50+ countries) and document regulatory pathway implications for any future New Zealand submission
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


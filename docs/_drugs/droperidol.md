---
layout: default
title: Droperidol
parent: 僅模型預測 (L5)
nav_order: 128
evidence_level: L5
indication_count: 10
---

# Droperidol
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

# Droperidol: From Anaesthesia/Antiemetic Use to Tourette Syndrome

## One-Sentence Summary

Droperidol is a high-potency butyrophenone antipsychotic internationally used for preoperative sedation and postoperative nausea and vomiting (PONV) prevention, but holds no current New Zealand marketing authorization.
The TxGNN model predicts it may be effective for **Tourette Syndrome**, with **0 clinical trials** and **1 publication** currently supporting this direction.
The evidence base at this stage remains preclinical/mechanistic, placing confidence at Level 4.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | No New Zealand approval on record; internationally recognized for preoperative sedation and PONV prevention |
| Predicted New Indication | Tourette Syndrome |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the Evidence Pack. Based on known pharmacological information, droperidol belongs to the butyrophenone class of antipsychotics — the same family as haloperidol. Its primary pharmacodynamic action is potent dopamine D2 receptor antagonism, which underlies its established clinical utility in sedation, agitation control, and PONV prevention in anaesthesia settings.

Dopamine D2 receptor antagonism is precisely the established pharmacological mechanism for suppressing motor and vocal tics in Tourette syndrome. Haloperidol — droperidol's closest pharmacological relative — is one of the historically approved treatments for Tourette syndrome and shares the same butyrophenone receptor pharmacology. The TxGNN prediction therefore reflects a plausible class-effect hypothesis: if haloperidol reduces tic severity via D2 blockade, droperidol as a high-potency D2 antagonist may exert a mechanistically analogous effect.

However, a critical practical limitation must be acknowledged. Droperidol is typically administered parenterally (IV/IM) with a short duration of action, making it poorly suited for the long-term continuous treatment that Tourette syndrome requires. No direct clinical trials or case reports of droperidol specifically in Tourette syndrome currently exist. The TxGNN signal most likely captures pharmacological class membership rather than drug-specific clinical utility.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [791589](https://pubmed.ncbi.nlm.nih.gov/791589/) | 1976 | Clinical Study | Current Psychiatric Therapies | Describes haloperidol (not droperidol) in severe behaviour disorders; included due to butyrophenone class overlap — provides only indirect mechanistic relevance |

---

## New Zealand Market Information

Droperidol currently holds no Medsafe (New Zealand) marketing authorizations. No approved indications are on record.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The pharmacological class effect via D2 receptor antagonism provides a biologically coherent rationale — haloperidol's proven efficacy in Tourette syndrome supports the theoretical premise — but the evidence base for droperidol specifically is limited to L4 (a single indirectly relevant publication on a related compound). Furthermore, droperidol's short-acting parenteral formulation profile is fundamentally incompatible with the chronic oral therapy requirements of Tourette syndrome management.

**To proceed, the following is needed:**
- Formal MOA and pharmacokinetic data retrieval (DrugBank API, package insert)
- Direct clinical evidence for droperidol specifically in Tourette syndrome (case reports, pilot observational studies)
- Route compatibility assessment — evaluation of whether long-acting or alternative formulations could support chronic use
- Complete safety data including QTc prolongation risk profile and contraindication review, which are critical for neuropsychiatric patient populations
- Comparative analysis against currently approved Tourette syndrome agents (haloperidol, fluphenazine, aripiprazole) to assess whether droperidol offers any clinical advantage
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


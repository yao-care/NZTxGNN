---
layout: default
title: Venetoclax
parent: 僅模型預測 (L5)
nav_order: 361
evidence_level: L5
indication_count: 10
---

# Venetoclax
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

# Venetoclax: From Chronic Lymphocytic Leukemia to CLL/SLL with IGHV Somatic Hypermutation

## One-Sentence Summary

Venetoclax is a selective BCL-2 inhibitor originally developed and approved for chronic lymphocytic leukemia (CLL)/small lymphocytic lymphoma (SLL), and later extended to acute myeloid leukemia. The TxGNN model's top prediction for this drug is **chronic lymphocytic leukemia/small lymphocytic lymphoma with IGHV somatic hypermutation** — but this is a molecularly-defined subtype of venetoclax's own existing indication rather than a distinct new disease, and it currently has **zero clinical trials** and **zero publications** supporting it directly in this evidence pack.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Chronic Lymphocytic Leukemia (CLL) / Small Lymphocytic Lymphoma (SLL) — internationally established indication; not confirmed in New Zealand regulatory data (data gap, see below) |
| Predicted New Indication | Chronic lymphocytic leukemia/small lymphocytic lymphoma with immunoglobulin heavy chain variable-region gene (IGHV) somatic hypermutation |
| TxGNN Prediction Score | 99.55% |
| Evidence Level | L5 (model prediction only — no clinical trials or literature identified) |
| New Zealand Market Status | ✗ Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action (MOA) data is not available in this evidence pack (flagged as data gap **DG002**, High severity). Based on established public pharmacological knowledge, venetoclax is a selective, orally bioavailable small-molecule inhibitor of the anti-apoptotic protein BCL-2, which restores apoptosis in cancer cells that depend on BCL-2 overexpression to evade cell death.

The predicted "new" indication here — CLL/SLL with IGHV somatic hypermutation — is not a separate disease from venetoclax's already-established indication. IGHV mutation status is a well-known **prognostic biomarker within CLL/SLL** (mutated IGHV, or M-CLL, generally reflects a post-germinal-center origin and more indolent course), not a distinct oncologic entity. Venetoclax already has robust, mature clinical evidence in CLL/SLL broadly — for example, the Phase 3 MURANO trial (PMID 40009494) supporting venetoclax-rituximab in relapsed/refractory CLL appears elsewhere in this evidence pack, under the closely related rank-2 candidate ("pregerminal center CLL/SLL").

Because venetoclax's BCL-2-dependent mechanism is not thought to differ meaningfully by IGHV mutation status, the very high TxGNN score for this specific molecular subtype is mechanistically plausible but adds little beyond what is already known for the parent CLL/SLL indication. No clinical trials or publications specific to this IGHV-hypermutated subtype were retrieved, so this prediction is best interpreted as a knowledge-graph-level refinement of an already-approved indication rather than a genuine drug-repurposing opportunity. Analysts should note that several *other* candidates in this evidence pack (e.g., follicular lymphoma, chronic myelogenous leukemia) carry substantially stronger and more actionable evidence — see Conclusion.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Venetoclax is not currently marketed in New Zealand (0 authorizations on file). No product license records are available in this evidence pack.

---

## Cytotoxicity

Venetoclax is classified as antineoplastic (approved for hematologic malignancies including CLL/SLL and AML), so this section applies.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (selective BCL-2 inhibitor) — not a conventional cytotoxic chemotherapeutic |
| Myelosuppression Risk | High — no drug-specific toxicity data is available in this evidence pack; based on the drug's established class profile, significant neutropenia and thrombocytopenia are recognized risks requiring active monitoring |
| Emetogenicity Classification | Low — based on established class profile; not independently confirmed in this evidence pack |
| Monitoring Items | Complete blood count (CBC) with differential; renal function and electrolytes (potassium, phosphate, calcium, uric acid), particularly during initial dose ramp-up due to the known class-wide risk of Tumor Lysis Syndrome (TLS) |
| Handling Protection | Please refer to the package insert warnings and precautions — New Zealand/TFDA-specific package insert data is currently unavailable (blocking data gap, see DG001) |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked predicted indication carries no clinical trial or literature evidence (Evidence Level L5) and, on closer inspection, represents a molecularly-refined subtype of venetoclax's already-approved CLL/SLL indication rather than a genuinely novel repurposing candidate. Combined with a blocking-severity data gap in regulatory safety information (DG001 — TFDA/Medsafe package insert warnings and contraindications), this candidate cannot proceed past initial safety screening (S1) at this time.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) to resolve blocking data gap DG001
- Formal mechanism-of-action documentation (DrugBank or equivalent) to resolve data gap DG002
- If pursuing venetoclax repurposing further, redirect evaluation toward candidates in this evidence pack with substantially stronger support — notably **follicular lymphoma** (rank 7; Evidence Level L2, "Proceed with Guardrails," including a 2025 Phase 2 trial and the CONTRALTO study) and **chronic myelogenous leukemia, BCR-ABL1 positive** (rank 5; Evidence Level L2, "Research Question," with multiple Phase 2 combination trials) — rather than this IGHV-subtype candidate
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


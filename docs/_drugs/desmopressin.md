---
layout: default
title: Desmopressin
parent: 僅模型預測 (L5)
nav_order: 36
evidence_level: L5
indication_count: 7
---

# Desmopressin
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

# Desmopressin: Evidence Pack Incomplete — No Repurposing Predictions Available

## Summary

Desmopressin is a synthetic analogue of the antidiuretic hormone vasopressin (ADH), widely used internationally for central diabetes insipidus and nocturnal enuresis.
This Evidence Pack contains **no TxGNN repurposing predictions** (`predicted_indications: []`) and multiple critical data gaps, making a full repurposing evaluation impossible at this stage.
The recommended action is **Hold** pending data remediation.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not available (drug not registered in New Zealand; no license data) |
| Predicted New Indication | None (TxGNN returned 0 predictions) |
| TxGNN Prediction Score | N/A |
| Evidence Level | L5 — Model prediction only (predictions absent) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Safety Considerations

Please refer to the package insert for safety information.

> No drug interaction records were found in the DDI database query. TFDA label warnings and contraindications were not retrieved in this data collection cycle.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN pipeline returned zero predicted indications for Desmopressin, and two blocking/high-severity data gaps remain unresolved (MOA and regulatory label text), making it impossible to conduct mechanism-relevance analysis or safety pre-screening.

**To proceed, the following is needed:**

- **[DG001 — Blocking]** Retrieve full label text (warnings, contraindications) from the New Zealand Medsafe product monograph or equivalent package insert PDF to enable safety pre-screening
- **[DG002 — High]** Retrieve mechanism of action from DrugBank API (`DB00035`) to support mechanistic plausibility analysis
- **Re-run TxGNN pipeline** after confirming the disease–drug mapping is correctly configured; investigate why `predicted_indications` returned an empty array (possible upstream mapping issue for `DB00035`)
- Once predictions are available, re-issue a full Evidence Pack and regenerate this report using the standard template
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


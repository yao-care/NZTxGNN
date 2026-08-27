---
layout: default
title: Ursodeoxycholic Acid
parent: 僅模型預測 (L5)
nav_order: 357
evidence_level: L5
indication_count: 1
---

# Ursodeoxycholic Acid
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Ursodeoxycholic Acid: From Primary Biliary Cholangitis/Gallstones to Homozygous Familial Hypercholesterolemia

## One-Sentence Summary

Ursodeoxycholic acid (UDCA, DB01586) is a hydrophilic bile acid historically used to treat primary biliary cholangitis and to dissolve gallstones. The TxGNN model predicts it may be effective for **Homozygous Familial Hypercholesterolemia (HoFH)**, but this prediction is currently supported by **no clinical trials and no published literature** — it rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in structured license data (drug is unmarketed in New Zealand); per rationale notes, UDCA has historically been used for PBC and gallstone dissolution |
| Predicted New Indication | Homozygous Familial Hypercholesterolemia |
| TxGNN Prediction Score | 99.86% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | 未上市 (Not Marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (`original_moa` is a data gap). Based on known information, UDCA is a hydrophilic bile acid analogue whose established pharmacology centers on lowering biliary cholesterol saturation and exerting cytoprotective/anti-apoptotic effects, with possible weak activation of FXR and modulation of the enterohepatic bile acid circulation.

Theoretically, changes in the bile acid pool can feed back to suppress hepatic CYP7A1 and indirectly influence cholesterol synthesis and LDL receptor expression — a mechanism loosely analogous to bile acid sequestrants such as cholestyramine. However, this effect is far weaker than that of true bile acid-binding resins, and it does not target the core pathology of HoFH, which is loss-of-function from biallelic LDLR gene defects. UDCA does not act by upregulating LDLR or providing an LDLR-independent clearance pathway for LDL-C.

In short, the mechanistic link is indirect and speculative rather than targeted. Because the original MOA data is missing, it is also not possible to formally compare UDCA's established indications (PBC, gallstone dissolution) against HoFH for mechanistic overlap — this should be treated as a hypothesis-generating signal only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

UDCA is not currently marketed in New Zealand (market status: 未上市) and has 0 registered authorizations on file, so no product table is available.

## Safety Considerations

Please refer to the package insert for safety information. Note: TFDA/product-label warnings and contraindications are flagged as a **Blocking** data gap (DG001) — this must be resolved before the candidate can advance to the S1 safety review stage.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is evidence level L5 — supported only by the TxGNN model score, with zero clinical trials or literature, and a mechanistically indirect rationale that does not address HoFH's core LDLR defect. Combined with a Blocking safety data gap and missing MOA data, there is currently insufficient basis to proceed.

**To proceed, the following is needed:**
- TFDA/package-insert safety data (warnings, contraindications, DDI) — currently Blocking (DG001)
- Confirmed mechanism of action data from DrugBank or primary literature (DG002)
- At least preclinical or observational evidence directly linking UDCA to lipid-lowering effects in HoFH or LDLR-deficient models
- Formal documentation of original approved indications (currently absent from structured data)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


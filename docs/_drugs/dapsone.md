---
layout: default
title: Dapsone
parent: 僅模型預測 (L5)
nav_order: 100
evidence_level: L5
indication_count: 1
---

# Dapsone
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

The txgnn-pipeline skill is for pipeline management — not applicable here. Proceeding directly to generate the report from the Evidence Pack.

---

分析 Evidence Pack 的情況：

- `predicted_indications`：空陣列 → 無 TxGNN 預測結果
- `original_indications`：空陣列 → 無原始適應症資料
- `original_moa`：[Data Gap]
- 市場狀態：未上市，0 張許可證
- 安全性資料：全部 [Data Gap]
- 資料缺口：DG001（Blocking）、DG002（High）

由於 Evidence Pack 資料嚴重不足，以下為如實反映現況的評估報告：

---

# DAPSONE：Evidence Pack 資料不完整，無法執行老藥新用評估

## One-Sentence Summary

DAPSONE（DrugBank ID：DB00250）目前的 Evidence Pack 資料嚴重不足，無法完成完整的老藥新用評估。TxGNN 模型尚未產生任何新適應症預測，且原始適應症、作用機轉、安全性警語等關鍵欄位均存在資料缺口，需完成資料補充後方可進入正式評估流程。

---

## Quick Overview

| Item | Content |
|------|---------|
| Predicted New Indication | No TxGNN prediction available |
| Evidence Level | L5 — No predictions or studies available |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Safety Considerations

Please refer to the package insert for safety information.

> ⚠️ **Data Gap DG001 \[Blocking\]**: Package insert warnings and contraindications have not been retrieved. This blocks entry into the safety pre-screening stage (S1). Remediation required: download and parse the official package insert PDF.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The Evidence Pack for DAPSONE contains no TxGNN-predicted indications and two unresolved data gaps of Blocking and High severity. A repurposing assessment cannot be meaningfully conducted until these gaps are remediated.

**To proceed, the following is needed:**

- **\[Blocking — DG001\]** Retrieve package insert from the official regulatory authority website, extract warnings and contraindications, and re-run the safety pre-screening (S1) gate
- **\[High — DG002\]** Query DrugBank API for mechanism of action (MOA) to enable mechanism-relevance analysis for any future predicted indications
- **\[Required\]** Run TxGNN prediction pipeline for DB00250 to generate candidate new indications; without predictions, the repurposing rationale cannot be constructed
- **\[Required\]** Confirm original approved indications from authoritative sources (DrugBank, WHO, or approved regulatory filings) to establish the baseline for repurposing comparison
- **\[Optional\]** Conduct a drug interaction (DDI) query once the above data gaps are closed

---

> **Note:** This report reflects the state of the Evidence Pack as of 2026-04-20. The absence of data does not indicate that DAPSONE lacks repurposing potential — only that the current data pipeline has not yet produced the inputs required for evaluation. Please re-run the evidence collection pipeline and regenerate this report once remediation steps are completed.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


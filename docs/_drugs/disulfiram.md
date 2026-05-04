---
layout: default
title: Disulfiram
parent: 僅模型預測 (L5)
nav_order: 51
evidence_level: L5
indication_count: 0
---

# Disulfiram
{: .fs-9 }

證據等級: **L5** | 預測適應症: **0** 個
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

# DISULFIRAM：老藥新用評估報告（資料不足，無法完成完整分析）

---

## One-Sentence Summary

DISULFIRAM（DB00822）是一個臨床上已知用於酒精依賴戒治的藥物。
本次 Evidence Pack 中 **TxGNN 模型未返回任何預測適應症**，且缺少作用機轉（MOA）與安全性資料，
目前**無法進行正式的老藥新用評估**。

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Evidence Pack 中未提供（欄位為空） |
| Predicted New Indication | 無（`predicted_indications` 為空陣列） |
| TxGNN Prediction Score | 無 |
| Evidence Level | 無法評定（無預測結果） |
| Taiwan Market Status | 未上市 |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why This Evaluation Cannot Proceed

目前 Evidence Pack 缺少三項關鍵資料，導致評估流程中斷：

**1. 無 TxGNN 預測結果**
`predicted_indications` 陣列為空，代表 TxGNN 模型尚未對此藥物返回候選適應症。報告中所有核心分析章節（預測合理性、臨床試驗證據、文獻支持）均無法生成。

**2. 作用機轉（MOA）缺失**
DrugBank 查詢已完成（`query_log` 顯示 `result_status: success`），但 MOA 欄位仍標記為 Data Gap，代表尚未解析或寫入 Evidence Pack。沒有 MOA 資料，無法進行機制關聯性分析。

**3. 安全性資料缺失**
TFDA 仿單查詢已完成（`result_status: success`），但警語與禁忌欄位未從 PDF 解析填入。此為 Blocking 級別的 Data Gap，影響安全性初評。

---

## Taiwan Market Information

DISULFIRAM 目前在台灣**未上市**，無任何藥品許可證記錄。

---

## Safety Considerations

請參閱仿單（Package Insert）之警語與禁忌事項。

> **注意**：TFDA 仿單 PDF 已成功查詢（`query_log` ID 4），但尚未完成內容解析。需將警語、禁忌與交互作用欄位填入 Evidence Pack 後，方可進行正式安全性評估。

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence Pack 中缺少 TxGNN 預測結果，且作用機轉與安全性資料均未填入，無法對 DISULFIRAM 的老藥新用潛力進行任何實質評估。

**To proceed, the following is needed:**

- [ ] 執行 TxGNN 模型預測，對 DB00822 生成候選適應症列表，寫入 `predicted_indications`
- [ ] 解析 DrugBank 查詢結果，填入 `original_moa` 欄位
- [ ] 解析已取得的 TFDA 仿單 PDF，將警語（`key_warnings`）與禁忌（`contraindications`）結構化寫入 Evidence Pack
- [ ] 填入 `original_indications`（已知臨床用途：酒精依賴戒治）
- [ ] 待上述資料齊全後，重新生成 Evidence Pack v5 並啟動完整評估流程
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


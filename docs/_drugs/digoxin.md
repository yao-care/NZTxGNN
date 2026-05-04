---
layout: default
title: Digoxin
parent: 僅模型預測 (L5)
nav_order: 44
evidence_level: L5
indication_count: 6
---

# Digoxin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
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

# Digoxin: 證據包不完整 — 老藥新用評估待補充

## One-Sentence Summary

Digoxin（DrugBank: DB00390）為本次 Evidence Pack 所收錄之藥物，目前於 New Zealand 市場無上市紀錄。
TxGNN 模型在本次分析中**未產生任何新適應症預測**，
且作用機轉（MOA）與安全性資料均存在關鍵數據缺口，無法完成標準老藥新用評估。

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | 本次 Evidence Pack 無資料 |
| Predicted New Indication | 無預測結果產生 |
| TxGNN Prediction Score | N/A |
| Evidence Level | 無法評定（predicted_indications 為空） |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why This Evaluation Cannot Proceed

本次 Evidence Pack 存在以下三項根本性缺口，導致老藥新用評估無法進行：

**1. TxGNN 無預測輸出**
`predicted_indications` 陣列為空，代表 TxGNN 模型未針對 Digoxin 產生任何新適應症候選。這可能源於知識圖譜節點連結不足，或模型信心分數低於輸出門檻。

**2. 作用機轉（MOA）缺失**
`drug.original_moa` 為空缺，無法進行機轉關聯性分析，也無法評估新舊適應症的藥理相似度。

**3. 安全性資料完全缺失**
Key warnings、contraindications、DDI 均無可用資料，無法完成基礎安全性初評（S1 評估階段）。

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
TxGNN 模型未針對 Digoxin 產生任何適應症預測，且三項 Blocking/High 級別數據缺口同時存在，目前不具備進行老藥新用評估的最低數據條件。

**To proceed, the following is needed:**

- **\[Priority 1 — Blocking\]** 取得 TFDA 仿單 PDF 並解析 Key Warnings 與 Contraindications（缺口 DG001）
- **\[Priority 2 — High\]** 透過 DrugBank API 補齊作用機轉（MOA）資料（缺口 DG002）
- **\[Priority 3\]** 重新執行 TxGNN 預測流程，確認 Digoxin 節點是否正確映射至知識圖譜
- **\[Priority 4\]** 補充 DDI 查詢資料（當前狀態：not\_found，需確認查詢參數是否正確）
- 上述缺口補齊後，重新生成完整 Evidence Pack（目前版本：v4，data\_cutoff：2026-04-20）

---

> **注意：** 本報告因 Evidence Pack 數據不完整而無法輸出標準章節（Clinical Trial Evidence、Literature Evidence、New Zealand Market Information、Safety Considerations）。所有缺失章節將於數據補齊後的下一版本報告中完整呈現。
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


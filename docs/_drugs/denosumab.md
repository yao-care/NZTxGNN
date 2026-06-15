---
layout: default
title: Denosumab
parent: 僅模型預測 (L5)
nav_order: 106
evidence_level: L5
indication_count: 2
---

# Denosumab
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
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

# Denosumab：評估報告 — TxGNN 預測資料待補齊

## One-Sentence Summary

Denosumab 是一種靶向 RANKL 的全人源單株抗體，在國際上已核准用於骨質疏鬆症及癌症骨轉移之骨骼相關事件預防。
本次 Evidence Pack 中 **TxGNN 模型尚未產生任何老藥新用預測候選**，且作用機轉（MOA）及安全性警語等關鍵資料均標記為待補，
目前**無法完成完整的老藥新用適應症評估**。

---

## Quick Overview

| 項目 | 內容 |
|------|------|
| 原核准適應症 | Evidence Pack 未記錄（通用知識：骨質疏鬆症、癌症骨轉移骨骼相關事件預防） |
| TxGNN 預測新適應症 | 尚未產生 — 預測流程待執行 |
| TxGNN 預測分數 | 無資料 |
| 證據等級 | 無法評估 |
| 台灣市場狀態 | 未上市（核准字號：0） |
| 核准字號數量 | 0 |
| 建議決策 | **Hold（暫緩）** |

---

## 藥物背景說明

雖然 Evidence Pack 中 `original_moa` 標記為資料缺口，根據公開藥理知識補充如下：

Denosumab（品牌名：Prolia® / Xgeva®）是一種完全人源化 IgG₂ 單株抗體，能與 **RANK Ligand（RANKL）** 高親和性結合，阻斷其與破骨細胞表面 RANK 受體的交互作用，進而**抑制破骨細胞分化、活化與存活**，達到減少骨吸收的療效。

此機轉的臨床應用包括：
- **Prolia**：停經後女性骨質疏鬆症、男性骨質疏鬆症、糖皮質激素誘發骨質疏鬆症
- **Xgeva**：實體腫瘤骨轉移患者之骨骼相關事件預防；骨巨細胞瘤（Giant Cell Tumor of Bone）治療

然而，正式的老藥新用適應症評估**必須以 TxGNN 預測結果為基礎**，目前因預測資料缺失，無法進行機轉關聯性分析。

---

## 台灣市場資訊

目前台灣（TFDA）查詢結果顯示 Denosumab **核准字號數為 0，市場狀態為未上市**。

> ⚠️ 注意：全球多數市場（美國 FDA、歐盟 EMA、日本 PMDA）均已核准 Denosumab 上市，台灣未上市狀態可能反映查詢範圍或資料收集問題，建議重新確認 TFDA 查詢結果。

---

## 安全性注意事項

本 Evidence Pack 之安全警語與禁忌症資料尚未收集完整。

請參閱官方仿單（Prolia® 或 Xgeva®）取得完整安全性資訊，已知重要安全考量包括：低鈣血症、顎骨壞死（ONJ）、非典型股骨骨折、感染風險增加等。

---

## 結論與後續步驟

**決策：Hold（暫緩）**

**理由：**
本次 Evidence Pack 資料不完整——TxGNN 尚未產生任何老藥新用預測候選、作用機轉資料缺失（DG002），且安全性警語亦尚未收集（DG001）。在缺乏預測適應症的情況下，無法執行任何證據評估或決策分析。

**欲繼續推進，需補齊下列資料：**

1. **執行 TxGNN 預測流程**：為 Denosumab（DB06643）產生老藥新用候選適應症清單
2. **補齊 MOA 資料（DG002）**：透過 DrugBank API 查詢正式 MOA 描述，以利機轉關聯性分析
3. **補齊 TFDA 仿單安全資料（DG001）**：下載並解析 TFDA 仿單 PDF，提取警語與禁忌症（Blocking 等級，影響 S1 安全性初評）
4. **確認台灣市場狀態**：重新驗證 TFDA 查詢結果是否正確反映市場狀況
5. **補充原核准適應症欄位**：確保 `original_indications` 欄位正確填入後，方可執行完整的 From/To 適應症比對報告
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


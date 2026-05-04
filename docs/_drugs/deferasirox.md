---
layout: default
title: Deferasirox
parent: 僅模型預測 (L5)
nav_order: 33
evidence_level: L5
indication_count: 5
---

# Deferasirox
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

# DEFERASIROX：老藥新用候選評估報告

> ⚠️ **資料完整性警告**：本份 Evidence Pack 缺少關鍵欄位（TxGNN 預測適應症、原始適應症、MOA、安全性警語），以下報告依現有資料盡力呈現，並標示所有資料缺口。

---

## 摘要

Deferasirox 是一種口服鐵螯合劑（iron chelator），全球廣泛用於因長期輸血造成的慢性鐵過載（transfusional hemosiderosis）治療。本次 Evidence Pack（v4，建立於 2026-04-20）**未包含任何 TxGNN 老藥新用預測結果**（`predicted_indications` 為空），且藥物原始適應症、MOA 及安全性資料均未完整收錄。因此，本報告僅能呈現基本藥物資訊，**尚無法進行完整的新適應症評估**。

---

## 快速總覽

| 項目 | 內容 |
|------|------|
| 原始適應症 | 資料缺口（Evidence Pack 未提供） |
| 預測新適應症 | **無**（TxGNN 預測結果未收錄） |
| TxGNN 預測分數 | N/A |
| 證據等級 | **L5**（模型預測尚未執行） |
| 台灣市場狀態 | ✗ 未上市（TFDA 查無核准記錄） |
| 核准許可數量 | 0 |
| 建議決策 | **Hold** |

---

## 作用機轉

目前 Evidence Pack 未提供詳細作用機轉資料（DG002：High severity）。

根據公開知識，Deferasirox 為三牙配位基型鐵螯合劑（tridentate iron chelator），可高選擇性地與三價鐵離子（Fe³⁺）結合，形成穩定的 2:1 複合物並經糞便排出，從而降低體內鐵蓄積。其原開發適應症為：

- **長期輸血性鐵過載**（transfusional hemosiderosis）
- **非輸血依賴性地中海貧血之鐵過載**（non-transfusion-dependent thalassemia）

MOA 詳細資料需查詢 DrugBank API（DB01609）補全。

---

## TxGNN 預測結果

本次 Evidence Pack 的 `predicted_indications` 欄位為空陣列，**未收錄任何 TxGNN 預測新適應症**。

可能原因：
1. TxGNN 預測流程尚未針對 Deferasirox 執行
2. 預測結果未納入本次資料包
3. Pipeline 資料整合步驟遺漏

**建議行動**：重新執行 TxGNN 預測流程，確認 DB01609 已正確輸入知識圖譜。

---

## 台灣市場資訊

TFDA 查詢結果（查詢日期：2026-03-29）顯示 Deferasirox **在台灣無核准藥品許可證**。

| 項目 | 結果 |
|------|------|
| 許可證數量 | 0 |
| 市場狀態 | 未上市 |
| 劑型記錄 | 無 |

> 注意：Deferasirox 在台灣以外地區（如美國 FDA、EMA、日本 PMDA）已核准上市，品牌名稱包括 **Exjade**（膜衣錠/分散錠）及 **Jadenu**（膜衣錠），惟台灣目前尚無核准記錄。

---

## 安全性注意事項

本 Evidence Pack 的安全性資料均為資料缺口，無法從現有資料提取。

根據 Deferasirox 的全球已知安全性資訊（供參考，非來自本 Evidence Pack）：

- **腎毒性**：可能導致血清肌酸酐上升，需定期監測腎功能
- **肝毒性**：需監測肝功能（ALT/AST）
- **胃腸道副作用**：噁心、嘔吐、腹瀉常見
- **皮疹**：皮膚過敏反應

⚠️ 正式安全性評估需補全 TFDA 仿單警語（DG001：Blocking severity）後方可進行。

---

## 資料缺口清單

| 缺口 ID | 項目 | 嚴重程度 | 影響 | 建議補全方式 |
|---------|------|----------|------|-------------|
| DG001 | TFDA 仿單警語/禁忌 | 🔴 Blocking | 無法進行安全性初評 | 下載 TFDA 仿單 PDF 並解析 |
| DG002 | 作用機轉（MOA） | 🟠 High | 影響機轉關聯性分析 | 查詢 DrugBank API（DB01609） |
| DG003 | TxGNN 預測結果 | 🔴 Blocking | 無預測新適應症可評估 | 重新執行 TxGNN 預測流程 |

---

## 結論與後續步驟

**決策：Hold**

**理由：**
本次 Evidence Pack 缺乏 TxGNN 預測結果（`predicted_indications` 為空），且原始適應症、MOA 及安全性警語均未完整收錄，現階段無法進行有意義的老藥新用評估。

**繼續推進前需補全以下資料：**

1. **重新執行 TxGNN 預測**：確認 Deferasirox（DB01609）已正確進入知識圖譜，並取得預測新適應症清單
2. **補全 MOA 資料**：查詢 DrugBank API（DB01609），取得完整作用機轉描述
3. **收集安全性資料**：下載並解析 TFDA 仿單 PDF，補全警語與禁忌欄位
4. **確認台灣上市可行性**：評估是否需申請新藥查驗登記，或是否符合現行法規路徑
5. **補全 Evidence Pack**：待上述資料收齊後，重新產出 v5 版本 Evidence Pack 並重新生成完整評估報告

---

*本報告依 Evidence Pack v4（2026-04-20）資料產出，僅供研究參考，不構成醫療建議。正式臨床應用需經完整臨床驗證。*
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Metronidazole
parent: 僅模型預測 (L5)
nav_order: 223
evidence_level: L5
indication_count: 10
---

# Metronidazole
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

# 甲硝唑（Metronidazole）：從厭氧菌／原蟲感染到肺囊蟲肺炎（Pneumocystosis）

## 一句話摘要

甲硝唑（Metronidazole, DB00916）是硝基咪唑類抗菌藥物，本證據包內部文獻提及其原始用途涵蓋厭氧菌感染、阿米巴病、陰道滴蟲病等（詳細仿單適應症與原始 MOA 資料本身為缺口）。TxGNN 模型以 **99.99%** 的極高分數預測其可能對**肺囊蟲病（Pneumocystosis, PCP）**有效，但在納入 **24 篇臨床試驗**與 **10 篇文獻**逐筆審查後，**未發現任何直接支持證據**，且機轉學理上不成立（PCP 病原體 *Pneumocystis jirovecii* 為真菌，非甲硝唑作用之厭氧菌／原蟲範疇）。

---

## 總覽

| 項目 | 內容 |
|------|------|
| 原始適應症 | 未提供完整仿單資料（證據包內部文獻提及厭氧菌感染、阿米巴病、陰道滴蟲病等，MOA 亦為資料缺口） |
| 預測新適應症 | Pneumocystosis（肺囊蟲肺炎, PCP） |
| TxGNN 預測分數 | 99.99%（rank 250） |
| 證據等級 | L5（僅模型預測，無實質支持證據） |
| 紐西蘭市場狀態 | 未上市 |
| 許可證數量 | 0 |
| 建議決策 | **Hold（暫緩）** |

---

## 為何此預測值得檢視（及為何目前證據不支持）

甲硝唑的作用機轉為前驅藥（prodrug），需在厭氧或微需氧環境中被還原活化，產生自由基造成病原體 DNA 損傷，因此對厭氧菌與部分原蟲（如 *Entamoeba histolytica*、*Trichomonas vaginalis*）具殺菌／殺蟲活性。證據包中其他候選適應症（如外陰潰瘍、cap polyposis）確實引用了此一機轉作為理論基礎。

然而，本次預測的標的 pneumocystosis 病原體 *Pneumocystis jirovecii* 屬於**真菌**，並非厭氧菌或原蟲，甲硝唑的 nitroimidazole 還原活化機轉在此病原體上缺乏作用基礎。文獻 PMID 7355683（1980, *American Family Physician*）明確指出 PCP 的標準用藥為 **trimethoprim-sulfamethoxazole（TMP-SMX）**，而甲硝唑僅適用於阿米巴結腸炎與滴蟲病等厭氧／原蟲感染，兩者治療範疇並不重疊。因此本項預測雖然模型分數極高，但**機轉關聯性不成立**，屬於 TxGNN 潛在的偽陽性（false positive）案例。

---

## 臨床試驗證據

系統共檢索到 24 筆註冊試驗，經逐筆相關性分級後，已評分之 6 筆均為「不相關（Grade C）」，其餘 18 筆尚待分級但摘要主題（行為介入、糖尿病衛教、預立醫療等）與 PCP／甲硝唑治療亦無明顯關聯。以下列出已完成相關性評估之試驗：

| 試驗編號 | 期別 | 狀態 | 收案數 | 關鍵發現 |
|---------|------|------|------|---------|
| [NCT04463914](https://clinicaltrials.gov/study/NCT04463914) | NA | 已完成 | 649 | 行動應用程式介入憂鬱症試驗，與 PCP／甲硝唑無關 |
| [NCT02208947](https://clinicaltrials.gov/study/NCT02208947) | Phase 3 | 終止 | 77 | 預立醫療計畫財務誘因試驗，與本適應症無關 |
| [NCT05892666](https://clinicaltrials.gov/study/NCT05892666) | N/A | 招募中 | 4000 | 門診急重症分流價值比較研究，與本適應症無關 |
| [NCT03466866](https://clinicaltrials.gov/study/NCT03466866) | Phase 3 | 已完成 | 156 | 糖尿病急診減少衛教試驗，與本適應症無關 |
| [NCT05340426](https://clinicaltrials.gov/study/NCT05340426) | Phase 1 | 撤回 | 0 | 豬腎異種移植試驗，與本適應症無關 |

其餘 19 筆試驗（如 NCT06597123、NCT01909076 等）主題涵蓋 AI 問診訓練、鴉片類藥物風險管理、糖尿病照護模式等，與甲硝唑治療 PCP 之假說均無直接關聯，故未逐一列出。

**結論：目前無任何登記中之臨床試驗直接評估甲硝唑用於肺囊蟲病治療。**

---

## 文獻證據

| PMID | 年份 | 類型 | 期刊 | 關鍵發現 |
|------|-----|------|------|---------|
| [1545596](https://pubmed.ncbi.nlm.nih.gov/1545596/) | 1992 | Review | Mayo Clinic Proceedings | 抗寄生蟲藥物綜論，未提及甲硝唑用於 PCP |
| [26518395](https://pubmed.ncbi.nlm.nih.gov/26518395/) | 2015 | Review | Topics in Antiviral Medicine | HIV 相關機會性感染現況綜論，未特別支持甲硝唑於 PCP 之角色 |
| [2996829](https://pubmed.ncbi.nlm.nih.gov/2996829/) | 1985 | Review | Clinical Pharmacy | AIDS 感染併發症治療綜論，PCP 標準治療為 TMP-SMX 而非甲硝唑 |
| [7355683](https://pubmed.ncbi.nlm.nih.gov/7355683/) | 1980 | 未分類 | American Family Physician | 明確指出 PCP 首選藥物為 TMP-SMX；甲硝唑僅列為阿米巴結腸炎、滴蟲病用藥 |
| [1782741](https://pubmed.ncbi.nlm.nih.gov/1782741/) | 1991 | 未分類 | Clinical Pharmacokinetics | 抗原蟲藥物藥動學基礎綜論，未特別連結甲硝唑與 PCP |
| [6771863](https://pubmed.ncbi.nlm.nih.gov/6771863/) | 1980 | 未分類 | Reviews of Infectious Diseases | 抗生素預防性治療評論，非 PCP 治療相關 |
| [16496064](https://pubmed.ncbi.nlm.nih.gov/16496064/) | 2005 | 未分類 | J Formosan Medical Association | AIDS 患者 CMV 併阿米巴結腸炎致大腸穿孔個案，甲硝唑用於阿米巴感染而非 PCP |
| [6282154](https://pubmed.ncbi.nlm.nih.gov/6282154/) | 1982 | Case report | American Review of Respiratory Disease | PCP 併 CMV 感染個案，患者曾因腹瀉接受甲硝唑治療，非用於治療 PCP 本身 |
| [2338506](https://pubmed.ncbi.nlm.nih.gov/2338506/) | 1990 | Case report | 感染症學雜誌（日本） | AIDS 患者以甲硝唑治療阿米巴痢疾後併發 PCP，甲硝唑非用於 PCP 治療 |
| [2280469](https://pubmed.ncbi.nlm.nih.gov/2280469/) | 1990 | 未分類 | Nihon Rinsho | 抗原蟲藥物綜論，無摘要可供評估 |

**結論：現有 10 篇文獻均未證實甲硝唑對肺囊蟲病具治療效果；多篇明確指出 PCP 標準治療為 TMP-SMX，甲硝唑僅出現於同一患者的其他感染（阿米巴病）治療脈絡中，屬於共病巧合而非療效證據。**

---

## 紐西蘭市場資訊

甲硝唑目前**未於紐西蘭上市**，查無任何藥品許可證登記資料。

---

## 安全性考量

請參考藥品仿單以獲取安全性資訊（TFDA 仿單警語/禁忌、DDI 資料均為待補齊之關鍵資料缺口）。

---

## 結論與後續建議

**決策：Hold（暫緩）**

**理由：**
- 機轉學理不成立：*Pneumocystis jirovecii* 為真菌，甲硝唑之厭氧菌／原蟲還原活化機轉無作用基礎；PCP 標準治療為 TMP-SMX。
- 24 筆臨床試驗與 10 篇文獻經逐筆審查後，**均無直接支持證據**，TxGNN 高分應視為潛在偽陽性。
- 該藥品未於紐西蘭上市，安全性資料（仿單警語、禁忌、DDI）亦為缺口，S1 安全性初評無法進行。

**若欲繼續推進，需補充：**
- TFDA／原廠仿單完整警語與禁忌資料（DG001，Blocking）
- 甲硝唑完整作用機轉（MOA）資料（DG002，High）
- 若後續仍評估此配對，需針對 PCP 病原生物學另尋機轉假說或體外／動物實驗數據佐證

**附註：** 同批候選適應症中，rank 9（cap polyposis）與 rank 10（ulceration of vulva）具有較明確之機轉論述與間接文獻支持（分別達 L4／Research Question 階段），建議優先進行後續證據盤點。
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


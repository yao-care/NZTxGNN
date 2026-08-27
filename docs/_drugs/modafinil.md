---
layout: default
title: Modafinil
parent: 中證據等級 (L3-L4)
nav_order: 232
evidence_level: L4
indication_count: 1
---

# Modafinil
{: .fs-9 }

證據等級: **L4** | 預測適應症: **1** 個
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

# Modafinil: From Narcolepsy/Excessive Daytime Sleepiness to Insomnia

## 一句話摘要

Modafinil 是一種清醒促進劑（eugeroic），目前已知核准用途集中在嗜睡症(narcolepsy)、阻塞性睡眠呼吸中止症(OSA)殘餘嗜睡、輪班工作睡眠障礙(SWSD)等「促進清醒」的適應症。TxGNN 模型預測其可能對**失眠(Insomnia)**有效，評分高達 **99.85%**，但目前收集到的 **29 筆臨床試驗**與 **19 篇文獻**中，絕大多數探討的是 modafinil/armodafinil 用於治療「其他疾病伴隨的嗜睡與疲勞」，而非把它當作失眠的直接治療藥物——這是一個藥理方向可能相反的訊號，需謹慎解讀。

---

## 快速總覽

| 項目 | 內容 |
|------|------|
| 原始適應症 | 紐西蘭無上市/授權紀錄；依證據包所附機轉敘述，已知核准用途為嗜睡症(narcolepsy)、OSA 殘餘嗜睡、輪班工作睡眠障礙(SWSD) |
| 預測新適應症 | Insomnia (disease) |
| TxGNN 預測分數 | 99.85% |
| 證據等級 | L4 |
| 紐西蘭市場狀態 | ✗ 未上市 |
| 授權數量 | 0 |
| 建議決策 | Hold |

---

## 為什麼這個預測（可能）不合理？

目前尚無 modafinil 的詳細作用機轉(MOA)資料可供查詢（Data Gap）。但根據證據包內附的機轉關聯分析，modafinil 的核心藥理是抑制多巴胺再回收、活化下視丘食慾素(orexin)/組織胺喚醒通路，藥理效果是「促進清醒」，臨床已知用途包括嗜睡症、OSA 殘餘嗜睡、SWSD。

失眠(insomnia)的病理機轉方向相反——它是睡眠起始或維持困難，臨床上需要的是鎮靜/助眠藥物，而非促醒藥物。換言之，modafinil 的藥理作用與失眠治療需求在方向上互相矛盾。

檢視本次收錄的 29 筆試驗與 19 篇文獻，僅 1 筆試驗（NCT01091974，關聯度 B 級）標題直接提及 insomnia，且該試驗中 armodafinil 是用來處理化療後「疲勞」，失眠部分實際上是由 CBT-I（認知行為治療）處理，藥物僅為輔助角色。其餘試驗多為 narcolepsy、OSA、雙相情感障礙、帕金森氏症步態/嗜睡、化療後疲勞等主題，與失眠的直接治療證據薄弱。此高分預測很可能是 TxGNN 知識圖譜將 modafinil 與「睡眠障礙」這一大節點群聚連結所致的方向性誤判，應視為需要人工複核的假訊號，而非直接可行的老藥新用線索。

---

## 臨床試驗證據

| 試驗編號 | 期別 | 狀態 | 收案人數 | 重點發現 |
|---------|------|------|------|---------|
| [NCT00124384](https://clinicaltrials.gov/study/NCT00124384) | Phase 4 | Completed | 40 | 唯一以「原發性失眠(Primary Insomnia)」患者為對象，評估 modafinil 單獨或合併 CBT-I 對白天功能與失眠嚴重度的影響 |
| [NCT01091974](https://clinicaltrials.gov/study/NCT01091974) | Phase 2 | Completed | 138 | CBT-I 合併/不合併 armodafinil，處理乳癌化療後失眠與疲勞；armodafinil 主要處理疲勞而非失眠本身 |
| [NCT01019187](https://clinicaltrials.gov/study/NCT01019187) | Phase 2 | Completed | 226 | 與上述同設計之較大規模登記（CBT 合併/不合併 armodafinil），處理癌症存活者化療後失眠與疲勞 |
| [NCT02552303](https://clinicaltrials.gov/study/NCT02552303) | N/A | Completed | 39 | Armodafinil 及/或 CBT-I 用於合併睡眠呼吸障礙(OSA)之失眠患者 |
| [NCT01011218](https://clinicaltrials.gov/study/NCT01011218) | Phase 2 | Completed | 70 | 乳癌患者失眠管理先導研究，部分組別合併 armodafinil 150mg/day |
| [NCT01080807](https://clinicaltrials.gov/study/NCT01080807) | Phase 4 | Completed | 385 | Armodafinil 治療輪班工作障礙(SWSD)相關過度嗜睡，非失眠治療 |
| [NCT01305408](https://clinicaltrials.gov/study/NCT01305408) | Phase 3 | Completed | 399 | Armodafinil 作為雙相 I 型情感障礙重度憂鬱發作之輔助治療 |
| [NCT06404099](https://clinicaltrials.gov/study/NCT06404099) | Phase 2 | Active, not recruiting | 361 | RECOVER-SLEEP 平台試驗，評估新冠感染後(PASC)睡眠障礙的多種介入措施 |
| [NCT06404086](https://clinicaltrials.gov/study/NCT06404086) | Phase 2 | Completed | 830 | 同上 RECOVER-SLEEP 平台試驗之另一分支 |
| [NCT00233090](https://clinicaltrials.gov/study/NCT00233090) | Phase 2 | Terminated | 21 | Modafinil vs 安慰劑治療腦外傷後疲勞，非失眠治療，樣本小且已終止 |

---

## 文獻證據

| PMID | 年份 | 類型 | 期刊 | 重點發現 |
|------|-----|------|------|---------|
| [18729534](https://pubmed.ncbi.nlm.nih.gov/18729534/) | 2008 | Review | Drugs | Modafinil 核准與研究性用途的實證回顧，涵蓋嗜睡相關臨床試驗證據 |
| [24312590](https://pubmed.ncbi.nlm.nih.gov/24312590/) | 2013 | Systematic Review/Meta-analysis | PLoS One | Modafinil 對神經系統疾病相關疲勞與過度嗜睡療效之統合分析 |
| [39535843](https://pubmed.ncbi.nlm.nih.gov/39535843/) | 2024 | Review | Expert Opin Pharmacother | 帕金森氏症睡眠障礙的藥物與非藥物治療管理 |
| [27010071](https://pubmed.ncbi.nlm.nih.gov/27010071/) | 2016 | Systematic Review | Parkinsonism Relat Disord | 帕金森氏症白天嗜睡與睡眠障礙藥物介入之系統性回顧與統合分析 |
| [22021174](https://pubmed.ncbi.nlm.nih.gov/22021174/) | 2011 | Guideline/Evidence-Based Review | Mov Disord | MDS 實證醫學回顧：帕金森氏症非動作症狀治療更新 |
| [18805301](https://pubmed.ncbi.nlm.nih.gov/18805301/) | 2008 | Review | Rev Neurol | 猝睡症合併猝倒症之回顧，含睡眠維持型失眠為其伴隨症狀之一 |
| [17181377](https://pubmed.ncbi.nlm.nih.gov/17181377/) | 2006 | Review | Drugs | 輪班工作睡眠障礙(SWSD)之疾病負擔與治療方式回顧 |
| [20166851](https://pubmed.ncbi.nlm.nih.gov/20166851/) | 2010 | Review | Expert Opin Emerg Drugs | 猝睡症及相關疾病之新興治療藥物回顧 |
| [24272458](https://pubmed.ncbi.nlm.nih.gov/24272458/) | 2014 | 未分類 | Neurotherapeutics | 帕金森氏症睡眠障礙治療，指出失眠的最佳治療方式尚未確立 |
| [15824337](https://pubmed.ncbi.nlm.nih.gov/15824337/) | 2005 | 未分類（研究設計為隨機安慰劑對照雙盲試驗） | Neurology | Modafinil 治療多發性硬化症相關疲勞之隨機對照試驗 |

---

## 紐西蘭市場資訊

Modafinil 目前於紐西蘭**未上市**，查無任何授權許可證紀錄（total_licenses = 0），無法提供劑型、產品名稱或核准適應症等資訊。

---

## 安全性考量

目前查無 modafinil 的仿單警語、禁忌症或藥物交互作用資料（TFDA 仿單解析為 Blocking 等級資料缺口，DDI 查詢無結果）。

> 請參閱仿單完整資訊以確認安全性考量。

---

## 結論與後續行動

**決策：Hold**

**理由：**
證據包內部的機轉關聯分析已明確指出，modafinil 屬清醒促進劑，藥理方向與失眠治療需求相反；本次收錄的臨床試驗與文獻中，僅極少數直接涉及失眠治療，且多為輔助/合併角色而非主要療效證據。此預測評分雖高（99.85%），但很可能是知識圖譜群聚效應造成的方向性誤判，證據等級僅達 L4，不足以支持進入下一階段評估。

**若要繼續推進，需要補充：**
- Modafinil 完整作用機轉(MOA)資料，釐清是否存在任何鎮靜/助眠相關的藥理路徑
- TFDA/原廠仿單之警語、禁忌症與藥物交互作用完整資料（目前為 Blocking 等級缺口）
- 針對 TxGNN 此筆預測進行知識圖譜路徑回溯（path explanation），確認「insomnia」節點連結是否反映真實臨床情境（如「失眠合併症狀」而非「失眠主要治療」）
- 若後續仍評估此方向，需額外納入睡眠醫學專家人工複核，排除方向性誤判可能
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


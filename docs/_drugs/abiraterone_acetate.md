---
layout: default
title: Abiraterone Acetate
parent: 僅模型預測 (L5)
nav_order: 11
evidence_level: L5
indication_count: 0
---

# Abiraterone Acetate
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

# Abiraterone Acetate: Evaluation Pending — No Predicted Indication Available

## One-Sentence Summary

Abiraterone Acetate is a CYP17A1 inhibitor widely used internationally for the treatment of metastatic castration-resistant prostate cancer (mCRPC). Currently, the TxGNN model has **not generated any new indication predictions** for this drug, and no Taiwan (TFDA) marketing authorizations were found. This report serves as a baseline record; a full evaluation can proceed once prediction data becomes available.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in current dataset (known internationally: metastatic castration-resistant prostate cancer) |
| Predicted New Indication | — (No TxGNN prediction available) |
| TxGNN Prediction Score | — |
| Evidence Level | N/A — Insufficient data for assessment |
| Taiwan Market Status | ✗ Not marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

## Why is This Prediction Reasonable?

Currently, the TxGNN model has not produced any predicted new indications for Abiraterone Acetate. Without a prediction target, mechanism-based plausibility analysis cannot be conducted at this time.

From general pharmacological knowledge, Abiraterone Acetate is a prodrug of abiraterone, which selectively inhibits **CYP17A1** (17α-hydroxylase/C17,20-lyase), a key enzyme in the androgen biosynthesis pathway. By blocking androgen production at the adrenal and tumoral level, it reduces circulating testosterone to castrate levels. This mechanism has proven efficacy in castration-resistant prostate cancer and is approved in numerous international markets (FDA, EMA, etc.) under brand names such as Zytiga®.

Given its well-characterized mechanism targeting the androgen axis, potential repurposing directions could theoretically include other androgen-sensitive conditions. However, no model-driven prediction is available in this evidence pack to evaluate.

## Clinical Trial Evidence

No TxGNN-predicted indication is available; therefore, clinical trial evidence mapping was not performed.

> To proceed, a valid TxGNN prediction must first be generated for this drug.

## Literature Evidence

No TxGNN-predicted indication is available; therefore, literature evidence mapping was not performed.

> To proceed, a valid TxGNN prediction must first be generated for this drug.

## Taiwan Market Information

Abiraterone Acetate currently holds **no TFDA marketing authorizations** in Taiwan. No license records were returned from TFDA queries.

## Cytotoxicity

Abiraterone Acetate is an **antineoplastic agent** used in oncology; however, it is not a conventional cytotoxic chemotherapy drug.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (Androgen biosynthesis inhibitor — CYP17A1 inhibitor) |
| Myelosuppression Risk | Low (not a conventional cytotoxic; anaemia is a known adverse effect but direct myelosuppression is uncommon) |
| Emetogenicity Classification | Low |
| Monitoring Items | Liver function tests (ALT/AST — hepatotoxicity is a key risk), blood pressure (hypertension), serum potassium (hypokalaemia), cardiac function, CBC |
| Handling Protection | Standard handling; does **not** require cytotoxic drug handling precautions (non-cytotoxic mechanism), though pregnant women should avoid handling crushed/broken tablets |

## Safety Considerations

Detailed safety data from TFDA package inserts and DrugBank were not available in the current evidence pack. Based on internationally known safety information for Abiraterone Acetate:

- **Key Warnings**: Hepatotoxicity (ALT/AST elevations, including rare cases of fulminant hepatic failure); mineralocorticoid excess effects (hypertension, hypokalaemia, fluid retention) due to CYP17 blockade and compensatory ACTH rise; adrenocortical insufficiency risk if corticosteroid therapy is interrupted; must be co-administered with prednisone/prednisolone
- **Contraindications**: Pregnancy (Category X — may cause foetal harm); severe hepatic impairment (Child-Pugh Class C)
- **Drug Interactions**: Strong CYP3A4 inducer/inhibitor interactions should be evaluated; Abiraterone is a CYP2D6 inhibitor — caution with CYP2D6 substrates (e.g., dextromethorphan, thioridazine); co-administration with spironolactone should be avoided (may increase androgen receptor activation)

> ⚠️ The above safety information is based on internationally available data. Please refer to the official package insert once TFDA approval/import data becomes available.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No TxGNN-predicted new indication is currently available for Abiraterone Acetate. Additionally, the drug holds no TFDA marketing authorization in Taiwan, and critical data gaps exist for MOA details and local safety labelling. A meaningful drug repurposing evaluation cannot proceed without a prediction target.

**To proceed, the following is needed:**
- **TxGNN prediction output** — Re-run the TxGNN model to generate candidate new indications for Abiraterone Acetate
- **DrugBank ID mapping** — Resolve the missing DrugBank ID (likely [DB05812](https://go.drugbank.com/drugs/DB05812)) to enable automated MOA and safety data retrieval
- **TFDA regulatory data** — If the drug becomes marketed in Taiwan, retrieve package insert warnings and contraindications
- **MOA data gap closure** — Query DrugBank API with the confirmed DrugBank ID to populate mechanism of action details
- **TFDA package insert (仿單)** — Download and parse the PDF for local safety labelling (key warnings, contraindications)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


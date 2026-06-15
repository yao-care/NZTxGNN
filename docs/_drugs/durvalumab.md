---
layout: default
title: Durvalumab
parent: 僅模型預測 (L5)
nav_order: 129
evidence_level: L5
indication_count: 10
---

# Durvalumab
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

# Durvalumab: From Urothelial Carcinoma to Prostatic Urethra Urothelial Carcinoma

## One-Sentence Summary

Durvalumab is a humanized anti-PD-L1 monoclonal antibody (immune checkpoint inhibitor) approved globally for urothelial carcinoma and non-small cell lung cancer, but not yet marketed in Taiwan.
The TxGNN model predicts it may be effective for **Prostatic Urethra Urothelial Carcinoma** (rank 1 of 10 predicted indications, score: 99.98%), yet currently **no clinical trials or publications** specifically support this indication.
Across all 10 predicted indications, the strongest existing evidence is for infiltrating bladder urothelial carcinoma sarcomatoid variant (rank 3, L3 — 2 clinical trials identified).

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | No Taiwan approval; globally approved for urothelial carcinoma and NSCLC |
| Predicted New Indication | Prostatic Urethra Urothelial Carcinoma |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L5 |
| Taiwan Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this Evidence Pack. Based on known information, Durvalumab is an anti-PD-L1 immune checkpoint inhibitor — it blocks the interaction between PD-L1 on tumor cells and PD-1/CD80 receptors on T cells, thereby restoring the immune system's ability to recognize and attack the tumor. Its efficacy in urothelial carcinoma (bladder cancer and upper tract variants) has been demonstrated in multiple global trials (e.g., DANUBE study), and it is mechanistically applicable to any PD-L1-expressing urothelial subtype.

Prostatic urethra urothelial carcinoma is a rare anatomical subtype of urothelial carcinoma arising in the prostatic urethral segment. Like other urothelial carcinomas, tumor cells at this site frequently express PD-L1, and the local tumor microenvironment shares immunological features with bladder urothelial carcinoma. This biological similarity forms the mechanistic basis for the TxGNN prediction: the model infers applicability from the broader urothelial carcinoma immune profile.

However, this specific subtype is exceptionally rare. No clinical trial has specifically enrolled patients with prostatic urethral urothelial carcinoma for anti-PD-L1 treatment, and no published literature directly addresses Durvalumab in this context. This prediction should be regarded as hypothesis-generating at this stage, requiring dedicated prospective evidence before any clinical translation.

---

## Clinical Trial Evidence

Currently no clinical trials specifically registered for Prostatic Urethra Urothelial Carcinoma with Durvalumab.

---

## Literature Evidence

Currently no related literature available for Prostatic Urethra Urothelial Carcinoma with Durvalumab.

---

## Taiwan Market Information

Durvalumab has not received any marketing authorization in Taiwan (0 licenses). No product information is available from Taiwan regulatory records.

---

## Cytotoxicity

Durvalumab is an antineoplastic agent (immune checkpoint inhibitor targeting PD-L1) indicated for malignant tumors.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Immunotherapy — Anti-PD-L1 immune checkpoint inhibitor (not conventional cytotoxic) |
| Myelosuppression Risk | Low for direct myelosuppression; immune-mediated cytopenias (autoimmune hemolytic anemia, thrombocytopenia) may occur as immune-related adverse events (irAEs) |
| Emetogenicity Classification | Low (monoclonal antibodies are not directly emetogenic) |
| Monitoring Items | CBC with differential, liver function tests (AST/ALT/bilirubin), thyroid function (TSH, free T4), fasting blood glucose, serum creatinine; comprehensive monitoring for irAEs across all organ systems |
| Handling Protection | Standard biohazard precautions for IV monoclonal antibody preparations; conventional cytotoxic drug handling regulations are not required |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The rank 1 predicted indication (prostatic urethra urothelial carcinoma) is supported only by TxGNN model prediction with no clinical trials or literature (L5), making it insufficiently evidenced for drug repurposing development at this time. Among the 10 predicted indications in this pack, infiltrating bladder urothelial carcinoma sarcomatoid variant (rank 3) represents a more actionable research target, with L3 evidence including a Phase 2 trial (NCT03912818) specifically designed for variant histology bladder cancer.

**To proceed, the following is needed:**
- Mechanism of action data (MOA) from DrugBank API query (DG002 remediation)
- Taiwan package insert (仿單) or TFDA safety information for key warnings and contraindications (DG001 remediation)
- Reprioritise to rank 3 indication (infiltrating bladder urothelial carcinoma sarcomatoid variant) as the primary research candidate, given existing Phase 2 trial evidence
- PD-L1 expression prevalence data specific to prostatic urethra urothelial carcinoma before any dedicated trial design
- Case series or retrospective data on anti-PD-L1 response in upper urinary tract and prostatic urethra urothelial subtypes
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


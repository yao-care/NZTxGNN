---
layout: default
title: Methotrexate
parent: 僅模型預測 (L5)
nav_order: 220
evidence_level: L5
indication_count: 10
---

# Methotrexate
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

# Methotrexate: From Antifolate Chemotherapy/Immunosuppressant Therapy to Pulmonary Blastoma

## One-Sentence Summary

Methotrexate is a well-established antifolate (dihydrofolate reductase inhibitor) used internationally in oncology and autoimmune disease, though this drug is not currently marketed in New Zealand and no local approved-indication text is available in the evidence pack.
The TxGNN model predicts it may be effective for **Pulmonary Blastoma**,
but currently **0 clinical trials** and **0 publications** support this specific direction — the prediction rests on the model score alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from New Zealand licensing data (drug is not marketed in NZ); internationally, methotrexate is used across oncology (e.g., leukemia, osteosarcoma, choriocarcinoma) and autoimmune disease (e.g., rheumatoid arthritis, psoriasis) |
| Predicted New Indication | Pulmonary Blastoma |
| TxGNN Prediction Score | 99.45% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (drug-level data gap, high severity). Based on known information, methotrexate is an antifolate that inhibits dihydrofolate reductase, blocking DNA synthesis in rapidly dividing cells — the mechanistic basis for its established use in both cytotoxic chemotherapy and immunosuppression.

Pulmonary blastoma is an extremely rare, biphasic lung tumor. There is no clinical trial or literature evidence linking methotrexate to this specific disease in the evidence pack. Historically, pulmonary blastoma has been treated with sarcoma-type regimens such as VAC (vincristine/actinomycin D/cyclophosphamide), not antifolate-based chemotherapy. The TxGNN association therefore reflects a graph-model prediction only, without mechanistic or clinical corroboration at this time.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

Methotrexate is not currently marketed in New Zealand (0 authorizations on record), so no product license information is available.

---

## Cytotoxicity

Methotrexate is a conventional antimetabolite/antifolate chemotherapy agent, so this section applies.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (Antimetabolite / antifolate — dihydrofolate reductase inhibitor) |
| Myelosuppression Risk | High — myelosuppression is a well-recognized, dose-limiting toxicity of methotrexate, particularly with high-dose regimens |
| Emetogenicity Classification | Low (low-dose/oral use) to Moderate (high-dose IV regimens) |
| Monitoring Items | CBC with differential, liver function tests, renal function, and serum methotrexate levels (with leucovorin rescue for high-dose regimens) |
| Handling Protection | Yes — must follow cytotoxic/hazardous drug handling regulations |

Note: country-specific (TFDA-equivalent) package insert warnings could not be retrieved for this drug (blocking data gap), so the above reflects general, internationally recognized toxicity profile rather than a local label.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted link between methotrexate and pulmonary blastoma is supported only by the TxGNN model score (L5, no clinical trials or literature), and the drug is not currently marketed in New Zealand. There is insufficient evidence to advance this specific candidate.

**To proceed, the following is needed:**
- Local package insert / warnings and contraindications data (currently blocking — required before any S1 safety review)
- Detailed mechanism of action documentation from DrugBank or equivalent source
- Targeted literature/trial search specifically for methotrexate in biphasic pulmonary tumors, if this indication is to be pursued further

**Note:** Within this same evidence pack, other TxGNN-predicted indications for methotrexate carry materially stronger evidence and more favorable recommendations — notably **Hodgkin's lymphoma** (L3, Proceed with Guardrails, historical VBM-regimen precedent) and **rhabdomyosarcoma** (L2, Proceed with Guardrails, a completed Phase II trial in high-risk pediatric disease). These may be more productive candidates to prioritize ahead of pulmonary blastoma.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


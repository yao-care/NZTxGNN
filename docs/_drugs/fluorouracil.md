---
layout: default
title: Fluorouracil
parent: 僅模型預測 (L5)
nav_order: 156
evidence_level: L5
indication_count: 10
---

# Fluorouracil
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

# Fluorouracil: From Systemic Antineoplastic Therapy to Botryoid-Type Embryonal Rhabdomyosarcoma of the Vagina

## One-Sentence Summary

Fluorouracil (5-FU) is a long-established fluoropyrimidine antimetabolite used systemically across multiple solid tumours, but this evidence pack does not contain New Zealand-specific original indication data because the product is **not currently marketed in New Zealand**. The TxGNN model's top-ranked prediction for this drug is **Botryoid-Type Embryonal Rhabdomyosarcoma of the Vagina**, an ultra-rare paediatric sarcoma subtype, with a very high similarity score (**99.75%**) — however, this prediction is currently supported by **zero clinical trials** and **zero publications**, meaning it is a pure computational (knowledge-graph embedding) association with no direct evidentiary backing.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in this evidence pack — product is not marketed in New Zealand, so no approved indication text is on file |
| Predicted New Indication | Botryoid-Type Embryonal Rhabdomyosarcoma of the Vagina |
| TxGNN Prediction Score | 99.75% |
| Evidence Level | L5 (model prediction only, no supporting trials or literature) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for this drug in the evidence pack (flagged as a High-severity data gap requiring DrugBank lookup). Based on well-established general pharmacological knowledge, fluorouracil (5-FU) is a pyrimidine analogue that, after metabolic activation, inhibits thymidylate synthase, blocking DNA synthesis and exerting non-specific cytotoxicity against rapidly dividing cells — this is the basis of its long-standing use across a broad range of systemic chemotherapy regimens for solid tumours.

The proposed link to botryoid-type embryonal rhabdomyosarcoma of the vagina rests solely on this general "highly proliferative tumour → antimetabolite cytotoxicity" logic. Embryonal rhabdomyosarcoma cells do divide rapidly, so a theoretical rationale for antiproliferative activity exists. However, the standard-of-care regimen for this subtype (VAC/VAI: vincristine, actinomycin D, cyclophosphamide or ifosfamide) does **not** include 5-FU, and there is no clinical trial or published case series testing 5-FU in this indication. The mechanistic link is therefore inferential only, not corroborated by any disease-specific data — consistent with the TxGNN model surfacing a knowledge-graph similarity rather than a validated pharmacological signal.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Fluorouracil is **not currently marketed in New Zealand** — Medsafe records show 0 product authorizations, so no licensed product/indication information is available.

---

## Cytotoxicity

Fluorouracil is a conventional cytotoxic antineoplastic agent (fluoropyrimidine antimetabolite class), so this section applies.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (fluoropyrimidine antimetabolite) |
| Myelosuppression Risk | Moderate–High (class effect of fluoropyrimidines, typically neutropenia and thrombocytopenia) — please refer to the package insert warnings and precautions for product-specific data |
| Emetogenicity Classification | Low to Moderate (typical for fluoropyrimidine-based regimens) |
| Monitoring Items | Complete blood count with differential, liver and renal function, electrolytes; cardiac monitoring given known fluoropyrimidine cardiotoxicity risk; DPD deficiency status where feasible |
| Handling Protection | Yes — must be handled under standard cytotoxic/hazardous drug handling precautions |

*Note: No product-specific toxicity data was found in DrugBank or TFDA/Medsafe sources for this evidence pack; the above reflects established fluoropyrimidine class knowledge. Please refer to the package insert warnings and precautions for definitive guidance.*

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN similarity score is high, but this indication (botryoid-type embryonal rhabdomyosarcoma of the vagina) has **no clinical trials and no published literature** supporting 5-FU use, and it falls outside current standard-of-care regimens for this tumour subtype. This is an L5, model-prediction-only signal and does not meet the threshold to advance beyond initial screening.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data (warnings, contraindications) — currently a **Blocking** data gap preventing safety (S1) evaluation
- Confirmed mechanism-of-action data from DrugBank — currently a High-severity data gap
- Any preclinical or mechanistic studies specific to embryonal rhabdomyosarcoma (none currently identified)
- Consideration of re-prioritizing evaluation toward other candidates in this same evidence pack with materially stronger evidence — notably **liver sarcoma** (rank 7, evidence level L3, 6 clinical trials incl. one directly using fluorouracil-containing FOLFIRINOX, 20 publications, decision stage "Research Question"), which currently represents the most evidence-rich repurposing signal for this drug
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


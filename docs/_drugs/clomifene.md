---
layout: default
title: Clomifene
parent: Model Prediction Only (L5)
nav_order: 109
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Clomifene: predicted new indication 46… (evidence level L5). Approved use: Female infertility"
monitor_inn: "Clomifene"
predicted_indications: ["46,XY disorder of sex development due to testicular steroidogenesis defect", "transverse vaginal septum", "longitudinal vaginal septum", "symptomatic form of fragile X syndrome in female carrier", "blepharophimosis-epicanthus inversus-ptosis", "blepharophimosis-epicanthus inversus-ptosis due to 3q23 rearrangement syndrome", "partial trisomy/tetrasomy of the short arm of chromosome 18", "partial trisomy/tetrasomy of the short arm of chromosome 5", "partial trisomy/tetrasomy of the short arm of chromosome 12", "anovulation"]
---

# Clomifene
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

# Clomifene: From Female Infertility to 46,XY Disorder of Sex Development Due to Testicular Steroidogenesis Defect

## One-Sentence Summary

Clomifene was originally used to treat female infertility, mainly by inducing ovulation in women who do not ovulate.
The TxGNN model predicts it may be effective for **46,XY disorder of sex development due to testicular steroidogenesis defect**, but **no clinical trials and no publications** currently support this specific prediction. It rests on the model score alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Female infertility |
| Predicted New Indication | 46,XY disorder of sex development due to testicular steroidogenesis defect |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorization) |
| Number of Authorizations | 1 (0 valid, 1 cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Clomifene is known as a selective estrogen receptor modulator (SERM). It blocks estrogen receptors in the hypothalamus, which removes the normal negative feedback. This raises GnRH pulses and increases LH and FSH release. The compiled pharmacology data also lists estrogen receptor-alpha (ESR1) as a target.

The predicted link is theoretical. Higher LH could in principle stimulate whatever steroid-producing capacity remains in the testes. However, this condition is caused by a defect in a steroidogenic enzyme or in Leydig cell function, and that defect would probably limit any response. The high score is therefore best treated as a hypothesis, not as evidence of benefit.

**Other predictions in the data.** Most of the other high-scoring predictions are also model-only, with no supporting studies. They include vaginal septum anomalies, BPES and chromosomal imbalance syndromes, and several appear to be graph artifacts. The exception is **anovulation** (rank 10, score 99.52%). It has 3 clinical trials and 20 publications, including randomized comparisons of letrozole with clomifene and Cochrane-type reviews. Anovulation is essentially the drug's established use rather than true repurposing, so it does not represent a new indication.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2502666 (cancelled) | Mylan Clomiphen | Tab 50 mg | Not stated in the data compiled |

---

## Safety Considerations

- **Conditions linked to clomifene as contraindications in the knowledge graph** (graph-derived, not package insert text):
  - Liver disease and gallbladder disease
  - Thyroid disorders (hypothyroidism, hyperthyroidism)
  - Adrenal insufficiency
  - Pituitary tumours
  - Endometrial cancer
  - Endometriosis and polycystic ovary syndrome
  - Ovarian cyst and ovarian hyperstimulation syndrome
  - Thrombophlebitis
  - Vision disorders
  - Anxiety and mood disorders
- **Pharmacological targets** (not drug-drug interactions): estrogen receptor-alpha and the volume-regulated anion channel (VRAC).

Please refer to the package insert for label warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the model score (L5). No trials or literature address this condition, and the structural and enzymatic nature of the defect makes a clinical response doubtful. The only authorization in New Zealand is cancelled.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, which are currently missing and block safety screening
- Detailed mechanism of action data from DrugBank
- Any case reports or preclinical data showing a response to clomifene in testicular steroidogenesis defects
- Confirmation of whether any clomifene product is currently available in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


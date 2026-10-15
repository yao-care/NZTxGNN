---
layout: default
title: Lithium Carbonate
parent: Model Prediction Only (L5)
nav_order: 274
evidence_level: L5
indication_count: 10
---

# Lithium Carbonate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Lithium Carbonate: From Bipolar Disorder to Pseudoachondroplasia

## One-Sentence Summary

Lithium carbonate is a mood stabiliser originally used for bipolar disorder and related affective disorders. The TxGNN model predicts it may be effective for **pseudoachondroplasia**, a rare skeletal dysplasia. However, there are **0 clinical trials** and **0 publications** supporting this specific prediction, so it rests on the model score alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Bipolar disorder, major affective disorder, manic bipolar affective disorder (the approved-indication text in New Zealand licences is blank; these come from the knowledge graph) |
| Predicted New Indication | Pseudoachondroplasia |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this drug. Based on known information, lithium is an established mood stabiliser, and its efficacy in bipolar disorder is well recognised. Its knowledge-graph indications also include several joint and musculoskeletal conditions (such as rheumatoid arthritis, osteoarthritis and juvenile idiopathic arthritis), which probably contributes to the model linking it to skeletal disorders.

A speculative mechanistic link is lithium's inhibition of GSK-3β and modulation of Wnt signalling, which may affect chondrocyte function. Pseudoachondroplasia is caused by COMP gene mutations that lead to endoplasmic reticulum stress and chondrocyte death. No direct evidence connects lithium to this mechanism, so the prediction should be read as a hypothesis only.

The other top-ranked predictions are also rare skeletal or developmental conditions (for example acromesomelic dysplasia Hunter-Thompson type, brachyolmia and brachydactyly-syndactyly syndrome). They share the same pattern: high model scores, no trials, and only indirect Wnt/BMP signalling arguments. The model's high scores for this cluster likely reflect network proximity rather than a demonstrated biological basis.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available for pseudoachondroplasia.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 733393 | Douglas | Cap 250 mg | Not stated (licence cancelled) |
| 260851 | Priadel | Tab long-acting 400 mg | Not stated (licence cancelled) |

---

## Safety Considerations

- **Disease contraindications (from the knowledge graph, not label text)**: kidney disease, hypothyroidism (including myxedema and congenital hypothyroidism), diabetes insipidus (including central and neurohypophyseal forms), heart conduction disease, sudden cardiac arrest, cognitive disorder, psoriasis (including parapsoriasis), and pyoureter.
- **Drug Interactions**: 100 interactions were found in total. The first 20 retrieved include:
  - **Major**: bupropion, lorcaserin
  - **Moderate**: acarbose, famotidine, albiglutide, alogliptin, metformin, pioglitazone, loperamide, mesalazine, morphine, balsalazide, metronidazole, canagliflozin, chlorpropamide, potassium citrate, clarithromycin
  - **Minor**: doxycycline, epinephrine, acetylsalicylic acid

Package insert warnings were not available. Please refer to the package insert for further safety information. Lithium has a narrow therapeutic window, so any use would need careful monitoring.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score (99.98%), with no clinical trials or literature for pseudoachondroplasia and only a speculative mechanistic link. Lithium also has no currently valid licence in New Zealand, and the available safety data are incomplete.

**To proceed, the following is needed:**
- Preclinical evidence that lithium or GSK-3β/Wnt modulation affects COMP-related chondrocyte ER stress or growth plate function
- Detailed mechanism of action data
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Assessment of the regulatory pathway, given that both New Zealand licences are cancelled
- A risk-benefit assessment for a paediatric skeletal population, including renal, thyroid and serum-level monitoring
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


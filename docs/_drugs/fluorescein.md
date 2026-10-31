---
layout: default
title: Fluorescein
parent: Model Prediction Only (L5)
nav_order: 205
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-31
description: "Fluorescein: predicted new indication Prinzmetal angina (evidence level L5). Approved use: Not available"
monitor_inn: "fluorescein"
predicted_indications: ["Prinzmetal angina", "antithrombin deficiency type 2", "rheumatoid arthritis", "factor 5 excess with spontaneous thrombosis", "hemoglobinopathy", "heparin cofactor 2 deficiency", "thrombophilia", "hyperthyroidism", "partial deletion of the short arm of chromosome 16", "beta-thalassemia with other manifestations"]
---

# Fluorescein
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

## Fluorescein: From Diagnostic Dye (No Approved Indication on Record) to Prinzmetal Angina
{: .text-alpha }

## One-Sentence Summary

Fluorescein is a fluorescent dye used mainly as a diagnostic agent, for example in angiography and corneal staining. The only New Zealand licence on record for it is cancelled, and no approved indication text is available.
The TxGNN model (deep learning) ranks **Prinzmetal angina** as its top prediction, but there are **0 clinical trials** and **0 publications** for that disease, so the prediction is **model-only (L5)**. The score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available (the licence record contains no approved indication text) |
| Predicted New Indication | Prinzmetal angina |
| TxGNN Prediction Score | 0.9981 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Fluorescein is a diagnostic fluorescent dye used in angiography, and it has no known vasodilatory or anti-vasospastic activity.

The review of the top prediction found no identifiable mechanistic link between fluorescein and Prinzmetal angina, which is a coronary vasospasm disorder. The high TxGNN score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence, with no supporting trials or literature.

The same holds for the other top-ranked predictions:
- **Coagulation-related conditions** (antithrombin deficiency type 2, factor 5 excess with spontaneous thrombosis, heparin cofactor 2 deficiency): there is no plausible link to the coagulation cascade.
- **Rheumatoid arthritis, hemoglobinopathy, thrombophilia and hyperthyroidism**: the retrieved literature concerns ocular complications of these diseases, where fluorescein serves as a diagnostic stain or angiography agent. It does not show that fluorescein treats the underlying disease.
- **Chromosome 16 partial deletion and beta-thalassemia with other manifestations**: there is no mechanistic rationale and no evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for Prinzmetal angina.

Literature was retrieved for lower-ranked predictions (rheumatoid arthritis, hemoglobinopathy, thrombophilia, hyperthyroidism). It consists mostly of reviews, cohort studies and case reports on ocular manifestations of those diseases, in which fluorescein is used for diagnosis. None of it tests fluorescein as a treatment, so it is not presented as supporting evidence.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 307661 (cancelled) | Pinetarsol | Solution 2.3% with trolamine laurilsulfate and fluorescein sodium | Not recorded |

## Safety Considerations

Please refer to the package insert for safety information.

No drug interaction records were found for this drug in the data compiled for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score. There are no clinical trials or publications for Prinzmetal angina and no plausible mechanism. Evidence for the other top-ranked predictions is limited to diagnostic use in ocular disease and does not support therapeutic repurposing.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap that prevents safety screening)
- Mechanism of action data from DrugBank
- Any preclinical or clinical evidence of a therapeutic effect in the predicted disease
- Clarification of the original approved indication and the status of the cancelled licence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


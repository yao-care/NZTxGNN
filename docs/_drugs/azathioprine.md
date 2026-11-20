---
layout: default
title: Azathioprine
parent: Model Prediction Only (L5)
nav_order: 54
evidence_level: L5
indication_count: 7
monitor_inn: "AZATHIOPRINE"
predicted_indications: ["colobomatous microphthalmia-rhizomelic dysplasia syndrome", "brachydactyly-syndactyly syndrome", "WHIM syndrome", "granulomatous disease, chronic, autosomal recessive, 5", "osteoarthritis", "granulomatous disease with defect in neutrophil chemotaxis", "acromesomelic dysplasia, Hunter-Thompson type"]
last_modified_at: 2026-11-06
description: "Azathioprine: predicted new indication colobomatous microphthalmia-rhizomelic dysplasia syndrome (evidence level L5). In New Zealand: 2 Pharmac Schedule items"
---

# Azathioprine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

## Azathioprine: purine-analog immunosuppressant → colobomatous microphthalmia-rhizomelic dysplasia syndrome
{: .text-alpha }

## One-Sentence Summary

Azathioprine is a purine-analog immunosuppressant that has been used in immune-mediated conditions.
The TxGNN model predicts it may be relevant to **colobomatous microphthalmia-rhizomelic dysplasia syndrome**, a rare developmental eye and skeletal malformation syndrome.
No clinical trials and no publications in the data compiled for this report support this prediction, so it rests on the model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | colobomatous microphthalmia-rhizomelic dysplasia syndrome |
| TxGNN Prediction Score | 1.0000 (rounded to 4 decimals from 0.99999428) |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

ChEMBL (EMBL-EBI) records the mechanism of action of azathioprine as: Amidophosphoribosyltransferase inhibitor; DNA inhibitor ([ChEMBL CHEMBL1542 (azathioprine): drug mechanism](https://www.ebi.ac.uk/chembl/explore/compound/CHEMBL1542), CC BY-SA 3.0; accessed 2026-11-20.) Based on general knowledge, azathioprine is a purine analog that suppresses lymphocyte proliferation and so dampens immune-mediated inflammation.

For this prediction, no identifiable mechanistic link was found. The syndrome is a rare developmental malformation of the eye and skeleton, and no immune-mediated pathology is described for it. Immunosuppression would not be expected to change a structural developmental defect.

The very high model score is therefore not supported by biology, trials or literature. It should be read as a computational signal only, not as evidence of benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## Safety Considerations

- **Drug Interactions:** The interaction screen completed and returned 100 recorded interacting drugs. The main ones are listed below.
  - **Moderate:** mesalazine, balsalazide, olsalazine, sulfasalazine.
  - **Minor:** zinc acetate, zinc gluconate, zinc sulfate.
  - **Level not classified:** many others, including epinephrine, amphotericin B, acetylsalicylic acid, calcitriol, pantoprazole, omeprazole, metformin, glimepiride, rosiglitazone, doxycycline, clotrimazole, morphine and sucralfate.
- Azathioprine causes myelosuppression and increases infection risk. This is a general class concern, not taken from the data compiled for this report. It matters for any new, unproven use.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no supporting trials or publications, and no plausible mechanistic link to a developmental malformation syndrome. The evidence level is L5 (model prediction only).

**To proceed, the following is needed:**
- Any direct clinical or preclinical study linking azathioprine to this syndrome, or a documented immune-mediated mechanism in it
- Mechanism-of-action data for azathioprine
- Package insert warnings and contraindications from Medsafe for safety screening
- A risk-benefit assessment for immunosuppression in this population, given the infection and myelosuppression risks

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-20 | "Detailed mechanism-of-action data for azathioprine is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [ChEMBL CHEMBL1542 (azathioprine): drug mechanism](https://www.ebi.ac.uk/chembl/explore/compound/CHEMBL1542) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


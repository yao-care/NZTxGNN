---
layout: default
title: Olsalazine
parent: Model Prediction Only (L5)
nav_order: 338
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-30
monitor_inn: "OLSALAZINE"
predicted_indications: ["myelodysplastic syndrome", "hypotrichosis simplex of the scalp", "congenital hypotrichosis milia", "alopecia", "diffuse alopecia areata", "partial deletion of the long arm of chromosome 5", "unclassified myelodysplastic syndrome", "refractory cytopenia of childhood", "aregenerative anemia", "severe congenital hypochromic anemia with ringed sideroblasts"]
description: "Olsalazine: predicted new indication Myelodysplastic syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Olsalazine
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

## Olsalazine: Myelodysplastic syndrome
{: .text-alpha }

## One-Sentence Summary

Olsalazine is a prodrug dimer of 5-aminosalicylic acid (5-ASA) that acts mainly in the colon. The licence data compiled for this report do not record its approved indication.
The TxGNN model predicts it may be effective for **myelodysplastic syndrome**, but there are **0 clinical trials** and **0 publications** supporting this direction, so the prediction is based on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Myelodysplastic syndrome |
| TxGNN Prediction Score | 0.9991 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Olsalazine is a prodrug dimer of 5-ASA that is largely confined to the colon, and systemic exposure is minimal.

Any link to myelodysplastic syndrome (for example, anti-inflammatory or NF-kB modulation by 5-ASA) is speculative. It rests solely on the TxGNN model (deep learning) score, which may reflect similarities the model learned between related diseases, not biological evidence. Olsalazine is not known to affect haematopoiesis.

The other top-ranked predictions are mostly myelodysplastic-syndrome-related terms (unclassified MDS, refractory cytopenia of childhood, partial deletion of 5q) or rare hair and anaemia disorders (hypotrichosis, alopecia, aregenerative anaemia). Their scores come from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence, and the MDS-related terms are largely redundant with one another.

## Literature Evidence

Currently no related literature available.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 426342 | Dipentum (valid) | Tab 500 mg | Not recorded |
| 588679 | Dipentum (valid) | Cap 250 mg | Not recorded |
| 2641704 | Atnahs Olsalazine (valid) | Tab 500 mg | Not recorded |

## Safety Considerations

- **Drug Interactions**: A screening query returned 100 potential interactions. Examples of the **Major**-level interactions are:
  - Human immunoglobulin G (intravenous)
  - Human botulinum neurotoxin A/B immune globulin
  - Cidofovir
  - Iothalamic acid

  **Moderate**-level interactions include:
  - NSAIDs: ketorolac, ibuprofen, celecoxib, diclofenac
  - Nephrotoxic or potentially nephrotoxic agents: acyclovir, amphotericin B, amikacin, bacitracin, capreomycin, cisplatin, colistimethate
  - Others: azathioprine, ibandronate, chlorpropamide, clofarabine

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a TxGNN model (deep learning) score, with no clinical trials, no literature and no mechanistic data. Olsalazine has minimal systemic absorption, and it is marketed in New Zealand, with 3 valid licences out of 3.

**To proceed, the following is needed:**
- Mechanism of action data (for example from DrugBank)
- Package insert warnings and contraindications from Medsafe, to allow safety screening
- Preclinical or mechanistic evidence for a link between 5-ASA and myelodysplastic syndrome
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


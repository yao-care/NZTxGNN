---
layout: default
title: Tramadol
parent: Model Prediction Only (L5)
nav_order: 458
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Tramadol: predicted new indication Acromesomelic dysplasia, Hunter-Thompson type (evidence level L5). Approved use: Osteoarthritis, arthropathy"
monitor_inn: "Tramadol"
predicted_indications: ["acromesomelic dysplasia, Hunter-Thompson type", "brachyolmia-amelogenesis imperfecta syndrome", "myosclerosis", "brachyolmia", "pseudoachondroplasia", "spondyloarthropathy, susceptibility to", "juvenile idiopathic arthritis", "juvenile arthritis due to defect in LACC1", "rheumatoid nodulosis", "rheumatoid factor-positive polyarticular juvenile idiopathic arthritis"]
---

# Tramadol
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

# Tramadol: From Osteoarthritis to Acromesomelic Dysplasia, Hunter-Thompson Type

## One-Sentence Summary

Tramadol is an opioid analgesic originally linked to osteoarthritis and arthropathy in the knowledge graph used for prediction.
The TxGNN model predicts it may be effective for **acromesomelic dysplasia, Hunter-Thompson type**,
but there are **no clinical trials** and **no publications** supporting this prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Osteoarthritis, arthropathy (from the knowledge graph; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Acromesomelic dysplasia, Hunter-Thompson type |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 7 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the compiled data. Based on known pharmacology, tramadol is a mu-opioid receptor agonist that also acts on delta and kappa opioid receptors, and it is described as inhibiting serotonin and norepinephrine reuptake. It is used for symptomatic treatment of moderate to severe pain.

Acromesomelic dysplasia, Hunter-Thompson type is a rare skeletal dysplasia caused by loss-of-function mutations in CDMP1/GDF5. Tramadol has no known action on the BMP/GDF5 pathway, so no mechanistic link to the disease itself has been established. The high score most likely reflects the knowledge graph linking the disease to skeletal and joint conditions similar to the original osteoarthritis and arthropathy indications. At most, tramadol might offer symptomatic pain relief, not disease modification.

Other top-ranked predictions (brachyolmia, pseudoachondroplasia, myosclerosis, juvenile idiopathic arthritis and others) follow the same pattern: all are Evidence Level L5 with no supporting trials. The only linked publication (PMID 20799760, for juvenile idiopathic arthritis) concerns ketoprofen, not tramadol, so it does not support this drug-indication pair.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the top predicted indication.

## New Zealand Market Information

All seven licences are cancelled; none is currently valid. Approved indication text is not available for any of them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2650975 | Tramal SR 200 | Sustained-release tablet 200 mg | Cancelled |
| 2149524 | Tramal SR 200 | Sustained-release tablet 200 mg | Cancelled |
| 2351889 | Arrow-Tramadol | Capsule 50 mg | Cancelled |
| 2650959 | Tramal SR 100 | Sustained-release tablet 100 mg | Cancelled |
| 2650967 | Tramal SR 150 | Sustained-release tablet 150 mg | Cancelled |

## Safety Considerations

- **Disease contraindications from the knowledge graph** (drug-disease contraindication links, not label text): hypertension, liver failure, liver disease, cirrhosis, gallbladder disease, epilepsy, anxiety disorder, neurotic disorder, dysthymic disorder, diabetes mellitus, diabetic ketoacidosis, substance abuse/dependence, substance-related disorder, hyperlipidemia, familial hyperlipidemia, respiratory failure, paracetamol poisoning, serotonin syndrome, pseudotumor cerebri, intracranial hypertension, alcohol withdrawal, Wernicke-Korsakoff syndrome, substance withdrawal syndrome, and central nervous system infectious disorders.
- **Paediatric use**: several predicted indications are juvenile conditions. Opioid use in children carries additional concerns, including CYP2D6 ultra-rapid metabolism and respiratory depression, and regulatory restrictions on tramadol in children exist.
- **Pharmacological targets**: tramadol binds the mu, delta and kappa opioid receptors. No drug-drug interaction data with other medicines are available.

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the graph-based model score. There are no trials or relevant literature, no plausible mechanism for treating the underlying disease, and no currently valid New Zealand licence.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (currently missing and blocking safety screening)
- Mechanism of action data from DrugBank
- Evidence that tramadol affects the disease itself, or a clearly defined symptomatic pain-management objective with supporting studies
- Review of the regulatory status given that all New Zealand licences are cancelled
- For paediatric conditions, a dedicated paediatric safety assessment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


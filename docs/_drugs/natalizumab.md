---
layout: default
title: Natalizumab
parent: Model Prediction Only (L5)
nav_order: 317
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-30
monitor_inn: "NATALIZUMAB"
predicted_indications: ["bronchitis", "parapsoriasis", "psoriasis", "severe nonproliferative diabetic retinopathy", "acute lichenoid pityriasis"]
description: "Natalizumab: predicted new indication Bronchitis (evidence level L5). Approved use: Not recorded in the local data"
---

# Natalizumab
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

## Natalizumab: Bronchitis
{: .text-alpha }

## One-Sentence Summary

Natalizumab is a monoclonal antibody originally used for Crohn's disease, inflammatory bowel disease and multiple sclerosis.
The TxGNN model predicts it may be effective for **bronchitis**, but there are **no clinical trials and no publications** supporting this prediction.
It is a computational prediction only, and the evidence base is very weak.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Bronchitis |
| TxGNN Prediction Score | 0.9946 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, natalizumab blocks alpha-4 integrin and limits leukocyte trafficking into inflamed tissue. This is the basis of its use in multiple sclerosis and Crohn's disease.

Theoretically, reducing leukocyte migration could dampen airway inflammation, which is the link behind the model's prediction. However, natalizumab's immunosuppressive effect also raises infection risk, including respiratory infection. A plausible benefit is therefore not supported by any clinical data, and the prediction remains computational only.

Other predictions from the model were also reviewed and are weaker or unfavourable:
- **Psoriasis (score 0.9919)**: The literature is conflicting. One cohort or case series of 18 MS patients (PMID 33589543, 2021) reported improvement of comorbid psoriasis. Several case reports describe natalizumab inducing or aggravating psoriasis (PMIDs 35646438, 28905124, 30323758). Evidence stays at the case-report level and safety concerns outweigh the signal of benefit.
- **Parapsoriasis (0.9937) and acute lichenoid pityriasis (0.9904)**: The only evidence is a 2020 case report of dermatologic adverse events (PMID 32470781). It suggests natalizumab may cause these conditions rather than treat them.
- **Severe nonproliferative diabetic retinopathy (0.9918)**: No trials or literature. It has only a theoretical rationale, and the PML risk is a major barrier for a non-life-threatening indication.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for bronchitis.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2467062 | Tysabri | Inj 20 mg per ml, 15 ml vial | Not recorded (authorization valid) |

## Safety Considerations

- **Contraindication (knowledge graph)**: Progressive multifocal leukoencephalopathy (PML). This comes from the knowledge graph, not from label text. Multiple reviews (e.g., PMIDs 20298966, 19647202, 30324046) describe natalizumab-associated PML, a rare and often fatal brain infection caused by JC virus.
- **Drug Interactions**: 100 interactions were found. Major interactions include:
  - Corticosteroids: hydrocortisone, prednisolone, prednisone, dexamethasone, betamethasone, budesonide, triamcinolone
  - Immunosuppressants and cytotoxics: mercaptopurine, trimetrexate, levamisole
  - Radiopharmaceuticals: iobenguane (I-131), ibritumomab tiuxetan, tositumomab (with and without I-131)
- Moderate: naltrexone. Minor: zinc salts (sulfate, acetate, gluconate, chloride).
- Case reports describe paradoxical skin reactions (psoriasis-type disease) during treatment.
- Immunosuppression may raise the risk of infections, including respiratory infections, which is directly relevant to a bronchitis indication.

Package insert warnings and contraindications were not available. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The high TxGNN score (0.9946) for bronchitis is not backed by any clinical trial or publication. Natalizumab's infection and PML risks, together with the cancelled New Zealand authorization, argue against pursuing this indication now.

**To proceed, the following is needed:**
- Preclinical or mechanistic evidence for alpha-4 integrin blockade in airway inflammation
- Package insert warnings and contraindications (Medsafe) for safety screening
- Detailed mechanism of action data (DrugBank)
- A risk-benefit assessment addressing PML and respiratory infection risk
- Clarification of the regulatory and supply status in New Zealand, given the cancelled authorization
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


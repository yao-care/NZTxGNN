---
layout: default
title: Ocrelizumab
parent: Model Prediction Only (L5)
nav_order: 332
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Ocrelizumab: predicted new indication HER2 positive breast carcinoma (evidence level L5). Approved use: Progressive multiple sclerosis"
monitor_inn: "OCRELIZUMAB"
predicted_indications: ["HER2 positive breast carcinoma", "normal breast-like subtype of breast carcinoma", "progesterone-receptor positive breast cancer", "breast tumor luminal A or B", "progesterone-receptor negative breast cancer"]
---

# Ocrelizumab
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

# Ocrelizumab: From Progressive Multiple Sclerosis to HER2-Positive Breast Carcinoma

## One-Sentence Summary

Ocrelizumab is an anti-CD20 antibody originally used to treat progressive multiple sclerosis.
The TxGNN model predicts it may be effective for **HER2-positive breast carcinoma**,
but there are currently **0 clinical trials** and **0 publications** supporting this specific prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Progressive multiple sclerosis (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | HER2 positive breast carcinoma |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, ocrelizumab is a humanized anti-CD20 monoclonal antibody that depletes B cells. Its efficacy in multiple sclerosis has been demonstrated, but the data compiled for this report do not show a mechanistic link to HER2-positive breast carcinoma.

The original indication (an autoimmune neurological disease) and the predicted indication (a breast cancer subtype) are biologically quite distant. The compiled data give no evidence that CD20-directed B-cell depletion is relevant to HER2-driven tumours. The high TxGNN score (0.999) is a computational signal only and should be treated as a hypothesis, not as evidence of efficacy.

The same applies to the other breast cancer subtypes predicted for this drug (normal breast-like subtype, progesterone-receptor positive, luminal A or B, and progesterone-receptor negative; scores 99.80–99.81%). None has clinical trials, and the only literature returned (for luminal A/B) concerns multiple sclerosis and other immune conditions, not breast cancer.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for HER2-positive breast carcinoma.

For the lower-ranked prediction "breast tumor luminal A or B", 18 publications were retrieved, but the ones reviewed concern ocrelizumab or B-cell therapy in multiple sclerosis, lupus, or hypogammaglobulinemia. They appear to be matches on the drug name rather than breast cancer evidence, so they are not counted as support for this prediction.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2717441 | Ocrevus SC (cancelled) | Inj 40 mg per ml, 23 ml vial | Not available |
| 2540789 | Ocrevus (cancelled) | Inj 30 mg per ml, 10 ml vial | Not available |

## Safety Considerations

- **Drug Interactions**: 100 interactions were identified in the interaction screening. Examples:
  - Moderate: corticosteroids (hydrocortisone, prednisolone, prednisone, dexamethasone, betamethasone, budesonide, triamcinolone), radiopharmaceuticals (iobenguane I-131, ibritumomab tiuxetan, tositumomab, samarium-153 lexidronam), albendazole, pentamidine, levamisole
  - Minor: zinc salts (sulfate, acetate, gluconate, chloride)

For other safety information (warnings and contraindications), please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the TxGNN model score (L5), with no clinical trials or relevant publications, and no mechanistic link between CD20 B-cell depletion and HER2-positive breast carcinoma is established. In addition, both New Zealand licences for the drug are cancelled.

**To proceed, the following is needed:**
- Mechanism of action data and a biologically plausible rationale linking CD20 B-cell depletion to HER2-positive breast cancer
- Preclinical or other supporting studies in breast cancer models
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Clarification of the New Zealand regulatory status, since both licences are cancelled
- Assessment of route and formulation compatibility for an oncology setting
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


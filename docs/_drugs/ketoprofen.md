---
layout: default
title: Ketoprofen
parent: Model Prediction Only (L5)
nav_order: 252
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-28
monitor_inn: "KETOPROFEN"
predicted_indications: ["acromesomelic dysplasia, Hunter-Thompson type", "brachyolmia-amelogenesis imperfecta syndrome", "myosclerosis", "brachyolmia", "colobomatous microphthalmia-rhizomelic dysplasia syndrome", "brachydactyly-syndactyly syndrome", "pseudoachondroplasia", "WHIM syndrome", "juvenile arthritis due to defect in LACC1"]
description: "Ketoprofen: predicted new indication Acromesomelic dysplasia, Hunter-Thompson type (evidence level L5). Approved use: Not recorded in the local data"
---

# Ketoprofen
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

## Ketoprofen: Acromesomelic dysplasia, Hunter-Thompson type
{: .text-alpha }

## One-Sentence Summary

Ketoprofen is a non-steroidal anti-inflammatory drug (NSAID), originally used for rheumatoid arthritis, osteoarthritis and other joint disorders.
The TxGNN model predicts it may be effective for **acromesomelic dysplasia, Hunter-Thompson type**, but there are currently **0 clinical trials** and **0 publications** supporting this direction. The prediction rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Acromesomelic dysplasia, Hunter-Thompson type |
| TxGNN Prediction Score | 0.9998 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available for ketoprofen. It is a member of the non-selective cyclooxygenase (COX) inhibitor class of NSAIDs. These drugs reduce prostaglandin synthesis, which relieves pain and inflammation in arthritis.

For this predicted indication, **no credible mechanistic link was identified**. Hunter-Thompson type acromesomelic dysplasia is a rare skeletal dysplasia caused by loss-of-function variants in *CDMP1/GDF5*, which affects BMP signalling. Ketoprofen has no known action on the GDF5/BMP pathway. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, such as shared skeletal and joint phenotype terms with ketoprofen's arthritis indications, not biological evidence. Symptomatic relief of joint pain is conceivable, but it would not modify the disease.

### Other Predictions Reviewed

Among the top 10 predictions, only two have a plausible, class-level rationale. Even for these, the evidence is for the NSAID class and not for ketoprofen specifically.

| Rank | Predicted Indication | Score | Assessment |
|---|---|---|---|
| 1 | Acromesomelic dysplasia, Hunter-Thompson type | 0.9998 | No mechanistic link |
| 2 | Brachyolmia-amelogenesis imperfecta syndrome | 0.9998 | No mechanistic link |
| 3 | Myosclerosis | 0.9998 | No direct link; at most symptomatic analgesia |
| 4 | Brachyolmia | 0.9998 | No mechanistic link |
| 5 | Colobomatous microphthalmia-rhizomelic dysplasia syndrome | 0.9998 | No mechanistic link |
| 6 | Brachydactyly-syndactyly syndrome | 0.9997 | No mechanistic link |
| 7 | Pseudoachondroplasia | 0.9995 | Weak and indirect; symptomatic only |
| 9 | WHIM syndrome | 0.9986 | No mechanistic link |
| 10 | Juvenile arthritis due to defect in LACC1 | 0.9974 | Indirect; symptomatic adjunct only; research question (L4) |

**Spondyloarthropathy** is the most credible candidate, but it is essentially an extension of ketoprofen's existing anti-inflammatory use and not novel repurposing.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 397148 (cancelled) | Oruvail SR | Long-acting capsule 200 mg | Not recorded |

## Safety Considerations

- **Contraindications and cautions suggested by the knowledge graph** (not taken from label text): hypertension and hypertensive disorders, liver failure, kidney disease, active peptic ulcer disease, blood coagulation disease, myocardial infarction and coronary thrombosis, cardiovascular and heart disease, hemoglobinopathy, and pyoureter.
- **Drug interactions**: 100 interactions were recorded. Among the 20 reviewed, most are Moderate. They include:
  - corticosteroids (hydrocortisone, dexamethasone, betamethasone, triamcinolone, budesonide)
  - acetylsalicylic acid
  - metformin
  - sulfonylureas (glimepiride, chlorpropamide)
  - mesalazine and balsalazide
  - naltrexone
  - potassium salts (potassium citrate, potassium bicarbonate)
  - bowel-preparation agents (picosulfuric acid, polyethylene glycol with electrolytes)

  Famotidine, ranitidine, cimetidine and linaclotide are Minor.

The package insert warnings and contraindications were not available for this report. Please refer to the package insert for authoritative safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction is supported only by a model score. There are no clinical trials or publications, no plausible mechanistic link to the GDF5/BMP pathway, and the only New Zealand licence has been cancelled. The data compiled for this report do not support further investment in this indication.

**To proceed, the following is needed:**
- Mechanism of action data for ketoprofen
- Package insert warnings and contraindications (a blocking gap for safety screening)
- A mechanistic or preclinical rationale for the predicted indication
- If a more plausible direction is wanted, a separate evaluation of spondyloarthropathy or LACC1-related juvenile arthritis, with a review of ketoprofen-specific evidence and the current New Zealand market situation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


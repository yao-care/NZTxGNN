---
layout: default
title: Lanreotide
parent: Model Prediction Only (L5)
nav_order: 258
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-28
monitor_inn: "LANREOTIDE"
predicted_indications: ["hypertrichosis (disease)", "malformation syndrome with odontal and/or periodontal component", "syndrome with a Dandy-Walker malformation as major feature", "isolated genetic hair shaft abnormality", "Ambras type hypertrichosis universalis congenita"]
description: "Lanreotide: predicted new indication Hypertrichosis (disease) (evidence level L5). Approved use: Not recorded in the local data"
---

# Lanreotide
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

## Lanreotide: Hypertrichosis (disease)
{: .text-alpha }

## One-Sentence Summary

Lanreotide is a somatostatin analogue originally used to treat acromegaly, hyperpituitarism and pituitary gigantism.
The TxGNN model predicts it may be effective for **hypertrichosis**, but **no clinical trials or publications** currently support this prediction, which is purely computational.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Hypertrichosis (disease) |
| TxGNN Prediction Score | 0.9997 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 7 of 7) |
| Number of Authorizations | 7 (valid: 7) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Lanreotide is known to be a somatostatin analogue that suppresses growth hormone (GH) and IGF-1 secretion, which is why it is effective in acromegaly.

The link to hypertrichosis is speculative. Excess IGF-1 signalling might theoretically influence hair follicle growth, but hypertrichosis has many causes (congenital, drug-induced, metabolic), and a somatostatin-based mechanism has not been shown to apply to any of them. The very high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

Other top-ranked predictions show the same pattern of weak mechanistic support:

| Rank | Predicted Indication | Score | Evidence | Comment |
|------|------|------|------|------|
| 2 | Malformation syndrome with odontal and/or periodontal component | 0.9996 | None | No plausible link between SSTR2/SSTR5 agonism and dental malformation |
| 3 | Syndrome with a Dandy-Walker malformation as major feature | 0.9996 | None | Structural congenital defect, unlikely to be reversed by a postnatal somatostatin analogue |
| 4 | Isolated genetic hair shaft abnormality | 0.9996 | None | Usually caused by structural gene defects (e.g. keratin genes), not the GH/IGF-1 axis |
| 5 | Ambras type hypertrichosis universalis congenita | 0.9996 | None | Linked to 8q22 rearrangements affecting TRPS1, not a somatostatin pathway |

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

All seven licences are valid, and approved indication text is not available for any of them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2709597 | Mytolac S29 | Inj 60 mg per 0.5 ml, 0.5 ml syringe | Valid |
| 2720264 | Somatuline Autogel | Inj 60 mg per 0.5 ml, 0.5 ml syringe | Valid |
| 2698153 | Mytolac | Inj 90 mg per 0.5 ml, 0.5 ml syringe | Valid |
| 2698161 | Mytolac | Inj 120 mg per 0.5 ml, 0.5 ml syringe | Valid |
| 2720272 | Somatuline Autogel | Inj 90 mg per 0.5 ml, 0.5 ml syringe | Valid |

Two further cancelled licences (2659131 and 2698145) are also on record.

---

## Safety Considerations

- **Disease-level cautions from the knowledge graph** (these are drug-disease contraindication links, not package insert text): hypertension, liver disease, gallbladder disease and gallstones (cholelithiasis, cholecystolithiasis, choledocholithiasis), hyperglycaemia, hypoglycaemia, hypothyroidism (including myxedema and congenital hypothyroidism), and impaired renal function.
- **Drug Interactions**: 100 interactions were identified in total, and the 20 reviewed are all rated Moderate. They are dominated by glucose-lowering agents, which is consistent with lanreotide's effect on glucose regulation:
  - Insulins (aspart, degludec, detemir, glargine, glulisine, isophane, regular)
  - Metformin, pioglitazone, acarbose, chlorpropamide, glimepiride
  - GLP-1 receptor agonists (albiglutide, dulaglutide, semaglutide)
  - DPP-4 inhibitors (alogliptin, linagliptin)
  - SGLT2 inhibitors (canagliflozin, dapagliflozin, empagliflozin)

Package insert warnings and contraindications were not available, so please refer to the package insert for authoritative safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests solely on a TxGNN model (deep learning) score (L5), with no clinical trials, no literature and no plausible mechanism.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data from DrugBank to allow a proper mechanistic analysis
- Preclinical or observational evidence linking somatostatin or GH/IGF-1 signalling to hypertrichosis
- A review of the current registration and supply situation, since all seven licences are valid
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


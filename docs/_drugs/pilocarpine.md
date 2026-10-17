---
layout: default
title: Pilocarpine
parent: Model Prediction Only (L5)
nav_order: 366
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Pilocarpine: predicted new indication Primary hereditary glaucoma (evidence level L5). Approved use: Open-angle glaucoma, ocular hypertension"
---

# Pilocarpine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

# Pilocarpine: From Open-Angle Glaucoma to Primary Hereditary Glaucoma

## One-Sentence Summary

Pilocarpine was originally used to treat open-angle glaucoma and ocular hypertension.
The TxGNN model predicts it may be effective for **primary hereditary glaucoma**,
but **no clinical trials and no publications** are currently available to support this prediction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Open-angle glaucoma, ocular hypertension (from the knowledge graph; no approved indication text is available in the New Zealand licence records) |
| Predicted New Indication | Primary hereditary glaucoma |
| TxGNN Prediction Score | 99.83% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on general pharmacology, pilocarpine is a muscarinic agonist that acts on M1–M5 receptors, with M3 activity being the most relevant to the eye. It contracts the ciliary muscle and increases trabecular outflow of aqueous humour, which lowers intraocular pressure.

Primary hereditary glaucoma (for example, primary congenital glaucoma, often linked to CYP1B1 or MYOC variants and to trabecular meshwork or angle dysgenesis) is also a disease of impaired aqueous outflow. This makes a mechanistic link plausible. However, it is only a prediction. The very high TxGNN score is probably driven by pilocarpine's existing glaucoma indications, so it should not be treated as independent evidence.

Hereditary glaucomas may also respond differently to miotics than adult open-angle glaucoma. Whether pilocarpine helps at all would depend on the anatomy of the drainage angle.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 252514 | Isopto Carpine | Eye drops 1% | Not stated (cancelled) |
| 252522 | Isopto Carpine | Eye drops 2% | Not stated (cancelled) |
| 252549 | Isopto Carpine | Eye drops 4% | Not stated (cancelled) |
| 307335 | Minims Pilocarpine | Eye drops 2% single dose | Not stated (cancelled) |

## Safety Considerations

- **Contraindications (from knowledge-graph disease associations, not label text)**: The following conditions are linked to pilocarpine as contraindications:
  - Eye: angle-closure (closed-angle) glaucoma, uveitis, iritis, iridocyclitis, keratitis, keratoconjunctivitis, retinal detachment
  - Respiratory: asthma, chronic obstructive pulmonary disease (including severe early-onset COPD), bronchial disease
  - Biliary and liver: cholelithiasis, cholecystolithiasis, choledocholithiasis, cholecystitis, biliary tract disease, obstructive jaundice, low phospholipid-associated cholelithiasis, liver failure
  - Renal: nephrolithiasis (including SLC26A1-related susceptibility), acute urate nephropathy
- **Drug Interactions**: The interaction query returned only pharmacological target data, not drug–drug interactions. Pilocarpine acts on the muscarinic receptors M1, M2, M3, M4 and M5 (CHRM1–CHRM5). Concomitant anticholinergic or other muscarinic drugs could therefore plausibly interact, but this has not been confirmed by the data.

Package insert warnings and contraindications were not available, so please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is based on the model alone (evidence level L5). There are no registered trials or publications, and the high score probably reflects the existing glaucoma indications. In addition, all four New Zealand licences are cancelled and no label safety data are available.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (Medsafe), which are required before safety screening
- Confirmed mechanism of action data (for example, from DrugBank)
- A literature and trial search for pilocarpine in primary congenital and other hereditary glaucomas, including angle anatomy and response to miotics
- Clarification of the regulatory pathway, since there are currently no valid New Zealand licences
- Route compatibility assessment (ophthalmic formulations exist, but compatibility with the new indication has not been assessed)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


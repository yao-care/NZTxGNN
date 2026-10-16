---
layout: default
title: Latanoprost
parent: Model Prediction Only (L5)
nav_order: 261
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Latanoprost: predicted new indication Primary hereditary glaucoma (evidence level L5). Approved use: Open-angle glaucoma, ocular hypertension"
monitor_inn: "LATANOPROST"
predicted_indications: ["primary hereditary glaucoma", "visceral calciphylaxis", "hypotrichosis simplex of the scalp", "arterial thoracic outlet syndrome", "venous thoracic outlet syndrome", "neurogenic thoracic outlet syndrome", "congenital hypotrichosis milia", "angiodysplasia of stomach", "blue toe syndrome", "lymphangiectasis"]
---

# Latanoprost
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

# Latanoprost: From Open-Angle Glaucoma to Primary Hereditary Glaucoma

## One-Sentence Summary

Latanoprost is a prostaglandin F2-alpha analogue originally used to treat open-angle glaucoma and ocular hypertension.
The TxGNN model predicts it may be effective for **primary hereditary glaucoma**, but **no clinical trials and no publications** were found in the data compiled for this report, so the prediction rests on model output and mechanistic plausibility alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Open-angle glaucoma, ocular hypertension (from the knowledge graph; no approved indication text is available for the New Zealand licence) |
| Predicted New Indication | Primary hereditary glaucoma |
| TxGNN Prediction Score | 99.88% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, latanoprost is a prostaglandin F2-alpha analogue that lowers intraocular pressure (IOP) by increasing uveoscleral outflow.

Hereditary glaucoma shares the feature of elevated IOP with open-angle glaucoma, which makes the prediction biologically plausible. The high score is probably driven largely by its similarity to the already established glaucoma indication.

There are important caveats. Primary congenital glaucoma is typically caused by developmental abnormalities of the trabecular meshwork and anterior chamber angle, and it is often managed surgically. Medical efficacy is therefore uncertain, and the prediction is a research hypothesis rather than a supported finding.

### Other Predicted Indications (Lower Priority)

The model also ranked the following conditions. None has any trial or literature support in the compiled data.

| Rank | Predicted Indication | Score | Assessment |
|------|------|------|------|
| 2 | Visceral calciphylaxis | 99.83% | No clear mechanistic link; Hold |
| 3 | Hypotrichosis simplex of the scalp | 99.81% | Plausible: prostaglandin analogues stimulate hair growth (eyelash hypertrichosis is a known effect of latanoprost); Research Question |
| 4 | Arterial thoracic outlet syndrome | 99.78% | Mechanical compression disorder; Hold |
| 5 | Venous thoracic outlet syndrome | 99.78% | Identical score to rank 4, suggesting a graph artefact; Hold |
| 6 | Neurogenic thoracic outlet syndrome | 99.72% | No plausible mechanism; Hold |
| 7 | Congenital hypotrichosis milia | 99.71% | Speculative hair-growth rationale; Research Question |
| 8 | Angiodysplasia of stomach | 99.70% | No established role; minimal systemic exposure from eye drops; Hold |
| 9 | Blue toe syndrome | 99.69% | Minimal systemic exposure; Hold |
| 10 | Lymphangiectasis | 99.67% | No established role; Hold |

Among these, the hair-growth disorders (ranks 3 and 7) have the most mechanistic plausibility, but this comes from background pharmacology rather than evidence in the compiled data.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2553139 | Teva | Eye drops 0.005% | Not stated (licence status: cancelled) |

The only available licence is cancelled, so no latanoprost product is currently authorised in New Zealand according to the data compiled. All available dosage forms are ophthalmic.

## Safety Considerations

- **Disease associations flagged in the knowledge graph (contraindication edges, not label text)**: uveitis, iris disease, ciliary body disease, panuveitis, macular retinal edema, cystoid macular edema, iritis, conjunctivitis, and acute contagious conjunctivitis. These indicate caution in patients with active intraocular inflammation or macular oedema risk and should be verified against the product label.
- **Drug Interactions**: The interaction screen returned 100 potential interacting drugs (the first 20 are listed in the compiled data, for example calcitriol, glimepiride, doxycycline, atropine, metformin, omeprazole, prednisone, simvastatin). All are of unknown severity level, so their clinical significance cannot be judged from these data.

Package insert warnings and contraindications were not available. Please refer to the package insert for complete safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for primary hereditary glaucoma is supported only by model score and mechanistic plausibility (Evidence Level L5), with no trials or literature. The only New Zealand licence is cancelled, and congenital forms of glaucoma are often surgically managed, so medical efficacy is uncertain.

**To proceed, the following is needed:**
- Systematic literature and trial registry search specific to latanoprost in hereditary/congenital glaucoma, including paediatric data
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Clarification of the regulatory pathway, given the cancelled licence
- Paediatric-specific safety and dosing assessment
- If hair-growth indications are of interest, a separate review of the prostaglandin analogue literature on hypotrichosis
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


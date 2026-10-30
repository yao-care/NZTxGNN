---
layout: default
title: Glycerin
parent: Model Prediction Only (L5)
nav_order: 220
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-30
monitor_inn: "glycerin"
predicted_indications: ["cauda equina syndrome", "open-angle glaucoma", "primary hereditary glaucoma", "alopecia", "congenital hypotrichosis milia", "irritable bowel syndrome", "hypotrichosis simplex of the scalp", "diffuse alopecia areata", "migraine disorder", "migraine with brainstem aura"]
description: "Glycerin: predicted new indication Cauda equina syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Glycerin
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

## Glycerin: Cauda equina syndrome
{: .text-alpha }

## One-Sentence Summary

Glycerin is an osmotic agent, originally used to treat constipation.
The TxGNN model gives its highest score to **cauda equina syndrome**, but the data compiled for this report contain **0 clinical trials** and **0 publications** for that condition, so the prediction rests on the model alone.
Among the other top-10 predictions, **open-angle glaucoma** has the most plausible pharmacological rationale, supported only by indirect literature.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Cauda equina syndrome |
| TxGNN Prediction Score | 0.9960 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (4 of 4 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 4 (valid: 4) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, glycerin is an osmotic agent. Its efficacy in constipation is established, since it draws water into the bowel lumen. For the top-ranked prediction, however, no mechanistic link could be identified. Compressive or inflammatory radiculopathy of the cauda equina is managed surgically, and glycerin's osmotic activity has no established relevance to it. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

Other predictions in the top 10 differ in plausibility:

- **Open-angle glaucoma (rank 2, 0.9959)** is the most pharmacologically coherent. Oral glycerin raises plasma osmolality and dehydrates the vitreous, which lowers intraocular pressure. Historically, though, it has been used mainly for acute angle-closure episodes and perioperative pressure control, not chronic open-angle glaucoma.
- **Irritable bowel syndrome (rank 6, 0.9949)** is indirectly related, because glycerin is used as a laxative and this may overlap with constipation-predominant IBS. Symptom relief has not been established.
- **Hair and scalp conditions (alopecia, hypotrichosis, alopecia areata) and migraine disorders** have no credible mechanism. Glycerin appears in this literature only as an excipient or control substance, and oral or intravenous glycerin is known to cause headache.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

No literature was retrieved for cauda equina syndrome. The table below covers the literature retrieved for the next-ranked predictions with literature, mainly open-angle glaucoma. None of it shows a therapeutic effect of glycerin for the predicted indication.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [1614157](https://pubmed.ncbi.nlm.nih.gov/1614157/) | 1992 | Review | Klin Monatsbl Augenheilkd | Pathogenesis and treatment of primary angle-closure glaucoma (indirect; angle-closure, not open-angle) |
| [32890110](https://pubmed.ncbi.nlm.nih.gov/32890110/) | 2020 | Case series | J Glaucoma | Glycerin-preserved donor corneoscleral grafts for glaucoma drainage devices (tissue preservation, not a drug effect) |
| [20872359](https://pubmed.ncbi.nlm.nih.gov/20872359/) | 2011 | Clinical study | Eur J Ophthalmol | Osmoprotective artificial tears on the ocular surface in glaucoma patients on topical therapy (indirect) |
| [14215532](https://pubmed.ncbi.nlm.nih.gov/14215532/) | 1964 | Review/Note | JAMA | Historical note on irritable colon (IBS; does not evaluate glycerin) |
| [3896179](https://pubmed.ncbi.nlm.nih.gov/3896179/) | 1985 | Clinical trial (negative) | Arch Intern Med | Polysorbate 60 was ineffective for male pattern baldness versus a glycerin control (different compound) |
| [14555416](https://pubmed.ncbi.nlm.nih.gov/14555416/) | 2003 | Safety review | Int J Toxicol | Safety assessment of triacetin as a cosmetic ingredient (not glycerin efficacy) |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2377241 | Ora-Sweet | Suspension | Not recorded (valid) |
| 2377268 | Ora-Sweet SF | Suspension | Not recorded (valid) |
| 2377217 | Ora-Blend | Suspension | Not recorded (valid) |
| 2377225 | Ora-Blend SF | Suspension | Not recorded (valid) |

---

## Safety Considerations

- **Drug Interactions**: 220 interactions were found in the interaction database. Most of those listed are of moderate severity: abiraterone, tramadol, acetazolamide, hydrocortisone, salbutamol, alfuzosin, hydrochlorothiazide, amiloride, amiodarone, amisulpride, benazepril, anagrelide, propofol, apalutamide, apomorphine, triamcinolone, arsenic trioxide, lumefantrine and asenapine. Doxycycline is minor.
- **Headache**: Oral or intravenous glycerin is known to cause headache, which is relevant to the migraine predictions.

Please refer to the package insert for warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
All 10 predictions are at evidence level L4 or L5, with no registered clinical trials and no direct clinical literature. The top-ranked prediction, cauda equina syndrome, has no identifiable mechanism. All four New Zealand authorizations are valid, and the drug is marketed.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications, which are currently missing and block safety screening
- Mechanism of action data for glycerin
- If a lead indication is wanted, a focused evidence review of oral glycerin for intraocular pressure lowering, the most plausible direction (open-angle glaucoma)
- A route and formulation compatibility assessment against the target indications
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


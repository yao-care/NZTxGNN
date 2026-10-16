---
layout: default
title: Glycerin
parent: Model Prediction Only (L5)
nav_order: 220
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Glycerin: predicted new indication Cauda equina syndrome (evidence level L5). Approved use: Constipation"
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

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Glycerin: From Constipation to Cauda Equina Syndrome

## One-Sentence Summary

Glycerin is an osmotic agent, originally used to treat constipation.
The TxGNN model gives its highest score to **cauda equina syndrome**, but the data compiled for this report contain **0 clinical trials** and **0 publications** for that condition, so the prediction rests on the model alone.
Among the other top-10 predictions, **open-angle glaucoma** has the most plausible pharmacological rationale, supported only by indirect literature.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Constipation (from the knowledge graph; no approved indication text is recorded in the New Zealand licences) |
| Predicted New Indication | Cauda equina syndrome |
| TxGNN Prediction Score | 99.60% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, glycerin is an osmotic agent. Its efficacy in constipation is established, since it draws water into the bowel lumen. For the top-ranked prediction, however, no mechanistic link could be identified. Compressive or inflammatory radiculopathy of the cauda equina is managed surgically, and glycerin's osmotic activity has no established relevance to it. The high score is a graph-based prediction only.

Other predictions in the top 10 differ in plausibility:

- **Open-angle glaucoma (rank 2, 99.59%)** is the most pharmacologically coherent. Oral glycerin raises plasma osmolality and dehydrates the vitreous, which lowers intraocular pressure. Historically, though, it has been used mainly for acute angle-closure episodes and perioperative pressure control, not chronic open-angle glaucoma.
- **Irritable bowel syndrome (rank 6, 99.49%)** is indirectly related, because glycerin is used as a laxative and this may overlap with constipation-predominant IBS. Symptom relief has not been established.
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
| 2377241 | Ora-Sweet | Suspension | Not recorded (cancelled) |
| 2377268 | Ora-Sweet SF | Suspension | Not recorded (cancelled) |
| 2377217 | Ora-Blend | Suspension | Not recorded (cancelled) |
| 2377225 | Ora-Blend SF | Suspension | Not recorded (cancelled) |

---

## Safety Considerations

- **Drug Interactions**: 220 interactions were found in the interaction database. Most of those listed are of moderate severity: abiraterone, tramadol, acetazolamide, hydrocortisone, salbutamol, alfuzosin, hydrochlorothiazide, amiloride, amiodarone, amisulpride, benazepril, anagrelide, propofol, apalutamide, apomorphine, triamcinolone, arsenic trioxide, lumefantrine and asenapine. Doxycycline is minor.
- **Headache**: Oral or intravenous glycerin is known to cause headache, which is relevant to the migraine predictions.

Please refer to the package insert for warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
All 10 predictions are at evidence level L4 or L5, with no registered clinical trials and no direct clinical literature. The top-ranked prediction, cauda equina syndrome, has no identifiable mechanism. All four New Zealand authorizations are cancelled, so there is no currently marketed product to build on.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications, which are currently missing and block safety screening
- Mechanism of action data for glycerin
- If a lead indication is wanted, a focused evidence review of oral glycerin for intraocular pressure lowering, the most plausible direction (open-angle glaucoma)
- A route and formulation compatibility assessment against the target indications
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


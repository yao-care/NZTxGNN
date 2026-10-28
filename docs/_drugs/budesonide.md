---
layout: default
title: Budesonide
parent: Model Prediction Only (L5)
nav_order: 72
evidence_level: L5
indication_count: 10
monitor_inn: "BUDESONIDE"
predicted_indications: ["atopic eczema", "bronchitis", "dermatitis, atopic", "polyp of vocal cord", "polyp of middle ear", "epulis", "fibroepithelial polyp", "uterine polyp", "polyp of frontal sinus", "polyp of external auditory canal"]
last_modified_at: 2026-10-28
description: "Budesonide: predicted new indication atopic eczema (evidence level L5). Approved use: Not recorded in the local data"
---

# Budesonide
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

## Budesonide: Corticosteroid → atopic eczema
{: .text-alpha }

## One-Sentence Summary

Budesonide is a glucocorticoid with broad anti-inflammatory and immunosuppressive activity, used in inhaled, nasal and modified-release oral forms.
The TxGNN model predicts it may be effective for **atopic eczema**, with a score of 0.9996.
Evidence is thin: **no clinical trials** were found and the **20 retrieved publications** are mostly indirect, with only a preclinical formulation study addressing the disease directly.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | atopic eczema |
| TxGNN Prediction Score | 0.9996 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 6 of 6) |
| Number of Authorizations | 6 (valid: 6) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Budesonide is a glucocorticoid, a class known for broad anti-inflammatory and immunosuppressive effects. These effects plausibly apply to the Th2-driven skin inflammation seen in atopic eczema.

Topical corticosteroids as a class are already established in atopic eczema, so the high score most likely reflects a class effect rather than new evidence specific to budesonide. The only directly relevant item is a preclinical study of budesonide-loaded nanoparticle hydrogels for atopic dermatitis. Another entry in the prediction list, "dermatitis, atopic", is the same disease under a different label and has the same limited evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No randomized or clinical efficacy studies of budesonide in atopic eczema were retrieved. The most relevant items are listed below.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38275852](https://pubmed.ncbi.nlm.nih.gov/38275852/) | 2024 | Preclinical formulation study | Gels | Budesonide-loaded pH-sensitive nanoparticles in hydrogels developed for local therapy of atopic dermatitis |
| [35056927](https://pubmed.ncbi.nlm.nih.gov/35056927/) | 2021 | Ex vivo formulation study | Pharmaceutics | Cyclodextrin and poloxamer hydrogels tested to improve budesonide skin retention |
| [9496795](https://pubmed.ncbi.nlm.nih.gov/9496795/) | 1998 | Open longitudinal study | Pediatric Dermatology | Knemometry in 14 children with atopic dermatitis assessed the effect of topical budesonide on short-term growth |
| [16428071](https://pubmed.ncbi.nlm.nih.gov/16428071/) | 2006 | Laboratory study | International Immunopharmacology | Budesonide, unlike tacrolimus, affected immune functions of normal human keratinocytes |
| [19571596](https://pubmed.ncbi.nlm.nih.gov/19571596/) | 2009 | Review | Neuroimmunomodulation | Adrenal suppression with intranasal corticosteroids in patients with allergic disease, including atopic dermatitis |
| [33931866](https://pubmed.ncbi.nlm.nih.gov/33931866/) | 2021 | Patch test series | Contact Dermatitis | Budesonide is used as a marker of corticosteroid hypersensitivity; allergy has trended down |
| [37927648](https://pubmed.ncbi.nlm.nih.gov/37927648/) | 2023 | Case report | Cureus | Steroid-induced angioedema and urticaria in a patient with a history of atopic dermatitis |
| [14616123](https://pubmed.ncbi.nlm.nih.gov/14616123/) | 2003 | Review | Allergy | Corticosteroid allergy in patients with asthma |

Most of the remaining retrieved papers concern contact sensitization or other conditions such as eosinophilic esophagitis and canine atopic dermatitis, and do not support efficacy in human atopic eczema.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 674273 | Pulmicort Turbuhaler | Powder for inhalation, 200 mcg per dose | Not recorded |
| 2541149 | SteroClear | Metered aqueous nasal spray, 50 mcg per dose | Not recorded |
| 674281 | Pulmicort Turbuhaler | Powder for inhalation, 400 mcg per dose | Not recorded |
| 2661454 | Budesonide Te Arai | Cap modified-release 3 mg | Not recorded |
| 2541068 | SteroClear | Metered aqueous nasal spray, 100 mcg per dose | Not recorded |

## Safety Considerations

Contraindication relations compiled for this report (not label text) include hypertension, epilepsy, liver disease, gallbladder disease, hyperglycemia, congestive heart failure, herpes simplex virus keratitis, measles, and pituitary deficiency conditions. These need confirmation against the product label.

**Drug interactions:** 100 interactions were recorded. Examples:
- **Major:** adalimumab, fosamprenavir
- **Moderate:** zidovudine, isotretinoin, ketorolac, nifedipine, ibuprofen, ethinylestradiol, fluvoxamine
- **Minor:** formoterol, salbutamol

Literature also flags adrenal suppression, growth effects in children, and corticosteroid contact allergy as safety themes for topical or intranasal use. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The score is very high, but no clinical trials exist and the literature contains no clinical efficacy data for budesonide in atopic eczema. Established topical corticosteroids already serve this indication, so the prediction adds little beyond a class effect.

**To proceed, the following is needed:**
- Controlled clinical data on budesonide in atopic eczema, or a clear rationale for a topical formulation that differs from existing corticosteroids
- Mechanism of action data
- Package insert warnings and contraindications from Medsafe
- Merging of the duplicate "atopic eczema" and "dermatitis, atopic" entries for downstream evaluation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


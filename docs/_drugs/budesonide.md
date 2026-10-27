---
layout: default
title: Budesonide
parent: Model Prediction Only (L5)
nav_order: 72
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Budesonide: predicted new indication Atopic eczema (evidence level L5). Approved use: Crohn's disease, inflammatory bowel disease, asthma, allergic rhinitis"
monitor_inn: "BUDESONIDE"
predicted_indications: ["atopic eczema", "bronchitis", "dermatitis, atopic", "polyp of vocal cord", "polyp of middle ear", "epulis", "fibroepithelial polyp", "uterine polyp", "polyp of frontal sinus", "polyp of external auditory canal"]
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

## Budesonide: From Crohn's Disease and Asthma to Atopic Eczema
{: .text-alpha }

## One-Sentence Summary

Budesonide is a glucocorticoid originally used for inflammatory bowel conditions (such as Crohn's disease) and airway allergic conditions (such as asthma and allergic rhinitis).
The TxGNN model predicts it may be effective for **atopic eczema** with a very high score, but **no clinical trials** are registered for this indication and the retrieved literature is mostly indirect, so the evidence is **model prediction only**.
A more strongly supported alternative among the model's predictions is **bronchitis** (**10 clinical trials**, **20 publications**, Phase 2 and 4 studies), although the disease label there is ambiguous.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Crohn's disease, inflammatory bowel disease, asthma, allergic rhinitis (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Atopic eczema |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only (no currently valid authorisation) |
| Number of Authorizations | 6 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available for this drug. Based on known information, budesonide is a glucocorticoid with broad anti-inflammatory and immunosuppressive activity. It is already established in inflammatory conditions of the gut and airways, and mechanistically it may be applicable to Th2-driven inflammation of the skin as seen in atopic eczema.

The link is plausible, but it mostly reflects a **class effect**. Topical corticosteroids are already standard therapy for atopic eczema, so the very high TxGNN score does not represent new evidence specific to budesonide. The only directly relevant therapeutic item is a preclinical study of budesonide nanoparticle hydrogels for atopic dermatitis.

Note that the model lists "atopic eczema" (rank 1) and "dermatitis, atopic" (rank 3) as separate predictions. They are the same disease under different labels and should be treated as one indication.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for atopic eczema.

For reference, the second-ranked prediction (**bronchitis**) has the following trials. Several are only loosely related; trials of ibrutinib and ivermectin were excluded as unrelated to budesonide.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00624754](https://clinicaltrials.gov/study/NCT00624754) | Phase 2 | Completed | 32 | Budesonide/formoterol in bronchiolitis obliterans after stem cell transplant |
| [NCT01560689](https://clinicaltrials.gov/study/NCT01560689) | Phase 2 | Completed | 32 | Open-label inhaled budesonide/formoterol in bronchiolitis obliterans after stem cell transplant |
| [NCT02002715](https://clinicaltrials.gov/study/NCT02002715) | Phase 4 | Completed | 106 | Different inhaled corticosteroid treatment courses in eosinophilic bronchitis |
| [NCT00483743](https://clinicaltrials.gov/study/NCT00483743) | Phase 2 | Completed | 62 | Inhaled TPI 1020 vs inhaled budesonide vs placebo in COPD (6 weeks) |
| [NCT03436225](https://clinicaltrials.gov/study/NCT03436225) | Phase 1 | Unknown | 80 | Steroid therapy in acute bronchiolitis in children |
| [NCT06571942](https://clinicaltrials.gov/study/NCT06571942) | Phase 4 | Unknown | 128 | Inhaled triple therapies in COPD or chronic bronchitis (biomass exposure) |
| [NCT00202189](https://clinicaltrials.gov/study/NCT00202189) | Phase 4 | Completed | 4 | Acute effects of inhaled corticosteroids on airway function in COPD (very small) |
| [NCT01432080](https://clinicaltrials.gov/study/NCT01432080) | Phase 2 | Terminated | 12 | Increased immunosuppression for transplant-associated lung disease (pilot) |

---

## Literature Evidence

**Atopic eczema / atopic dermatitis** (literature is mostly indirect):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38275852](https://pubmed.ncbi.nlm.nih.gov/38275852/) | 2024 | Preclinical formulation study | Gels | Budesonide-loaded pH-sensitive nanoparticle hydrogels for local therapy of atopic dermatitis |
| [21062310](https://pubmed.ncbi.nlm.nih.gov/21062310/) | 2010 | Randomised placebo-controlled trial (dogs) | J Vet Pharmacol Ther | 0.025% budesonide leave-on conditioner reduced skin lesions and itch in canine atopic dermatitis |
| [9496795](https://pubmed.ncbi.nlm.nih.gov/9496795/) | 1998 | Open longitudinal study | Pediatr Dermatol | Knemometry assessing short-term growth effects of topical budesonide in children with atopic dermatitis |
| [16428071](https://pubmed.ncbi.nlm.nih.gov/16428071/) | 2006 | In vitro study | Int Immunopharmacol | Budesonide affected immune functions of normal human keratinocytes |
| [33931866](https://pubmed.ncbi.nlm.nih.gov/33931866/) | 2021 | Patch-test study | Contact Dermatitis | Budesonide as a marker of corticosteroid hypersensitivity; allergy trend decreasing |
| [14616123](https://pubmed.ncbi.nlm.nih.gov/14616123/) | 2003 | Review | Allergy | Corticosteroid allergy in asthma patients |
| [19571596](https://pubmed.ncbi.nlm.nih.gov/19571596/) | 2009 | Review | Neuroimmunomodulation | Adrenal suppression with intranasal corticosteroids |

**Bronchitis** (for reference):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28810628](https://pubmed.ncbi.nlm.nih.gov/28810628/) | 2017 | Clinical study | Exp Ther Med | Budesonide plus terbutaline aerosol vs dexamethasone drip in 60 children with acute asthmatic bronchitis |
| [10780759](https://pubmed.ncbi.nlm.nih.gov/10780759/) | 2000 | Clinical study | Eur Respir J | Airway inflammation and cough before and after inhaled budesonide in eosinophilic bronchitis |
| [11129126](https://pubmed.ncbi.nlm.nih.gov/11129126/) | 2000 | Review | Drugs | Budesonide inhalation suspension in inflammatory respiratory disorders |
| [31015757](https://pubmed.ncbi.nlm.nih.gov/31015757/) | 2019 | Meta-analysis | Int J Chron Obstruct Pulmon Dis | Risks of budesonide/formoterol in stable COPD |
| [37954384](https://pubmed.ncbi.nlm.nih.gov/37954384/) | 2023 | Meta-analysis | Heliyon | Budesonide plus ambroxol inhalation in children with pneumonia |

---

## New Zealand Market Information

All six authorisations are cancelled; there is currently no valid budesonide authorisation in New Zealand. Approved indication text is not available for these licences.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 674273 | Pulmicort Turbuhaler | Powder for inhalation, 200 mcg per dose | Cancelled |
| 674281 | Pulmicort Turbuhaler | Powder for inhalation, 400 mcg per dose | Cancelled |
| 682128 | Pulmicort Turbuhaler | Powder for inhalation, 100 mcg per dose | Cancelled |
| 2541149 | SteroClear | Metered aqueous nasal spray, 50 mcg per dose | Cancelled |
| 2541068 | SteroClear | Metered aqueous nasal spray, 100 mcg per dose | Cancelled |
| 2661454 | Budesonide Te Arai | Modified-release capsule, 3 mg | Cancelled |

No topical dermal formulation is listed among the authorisations, which matters for a skin indication.

---

## Safety Considerations

- **Contraindications recorded in the knowledge graph** (these come from drug–disease links, not from label text): hypertension, epilepsy, liver disease, gallbladder disease, hyperglycaemia, congestive heart failure, herpes simplex keratitis, measles, hypopituitarism and related pituitary hormone deficiencies, and several urea-cycle disorders (e.g. citrullinaemia, hyperargininaemia).
- **Drug Interactions**: 100 interactions were found in total. Major-level examples include **adalimumab** and **fosamprenavir**. Moderate-level examples include zidovudine, isotretinoin, ketorolac, ibuprofen, nifedipine, ethinylestradiol, fluvoxamine and aldesleukin. Minor-level examples include formoterol and salbutamol.
- **Literature-derived concerns**: contact allergy and hypersensitivity to corticosteroids, including budesonide; adrenal suppression with intranasal use; and possible short-term growth effects in children treated with topical budesonide.

Package insert warnings and contraindications could not be retrieved; please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
For atopic eczema, the evidence is at L5 (model prediction only): there are no registered trials, and the literature is mostly about contact allergy and formulation work. The high score largely reflects the corticosteroid class effect, and topical corticosteroids are already established for this condition. In addition, budesonide currently has no valid New Zealand authorisation and no dermal formulation.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data
- Merge the duplicate "atopic eczema" and "dermatitis, atopic" predictions
- Clinical data for a dermal budesonide formulation, including the preclinical nanoparticle hydrogel work
- Consider evaluating better-defined alternatives, such as bronchitis subtypes (e.g. eosinophilic bronchitis), where Phase 2/4 data exist (the disease label needs clarifying), or chronic rhinosinusitis with nasal polyps in place of the vague sinus or ear polyp predictions
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


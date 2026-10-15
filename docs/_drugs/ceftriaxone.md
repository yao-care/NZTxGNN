---
layout: default
title: Ceftriaxone
parent: Model Prediction Only (L5)
nav_order: 89
evidence_level: L5
indication_count: 7
---

# Ceftriaxone
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

# Ceftriaxone: From Bacterial Infections to Hyperamylasemia

## One-Sentence Summary

Ceftriaxone is a broad-spectrum cephalosporin antibiotic, originally used for infections such as bacterial pneumonia, meningitis, gonorrhoea and salmonellosis.
The TxGNN model's top-ranked prediction is **hyperamylasemia**, but **no clinical trials** and only **3 publications** (a case report, a cohort study and a prophylaxis study, none showing a therapeutic effect) are linked to it.
A far better-supported prediction is **infectious otitis media**, with **1 registered trial** (which does not test ceftriaxone) and **19 publications**, including a randomised trial of ceftriaxone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Streptococcal infection, bacterial pneumonia, bacterial meningitis, gonorrhoea, salmonellosis and others (from the knowledge graph; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Hyperamylasemia (top-ranked). The most plausible alternative is infectious otitis media |
| TxGNN Prediction Score | 99.39% (hyperamylasemia); 99.26% (infectious otitis media) |
| Evidence Level | L5 (hyperamylasemia); L3 (infectious otitis media) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold (hyperamylasemia); infectious otitis media is a research question |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the data compiled for this report. Ceftriaxone is a third-generation cephalosporin, a beta-lactam antibiotic that inhibits bacterial cell wall synthesis. Its efficacy in bacterial infections is well established.

**Hyperamylasemia (rank 1):** There is no clear mechanistic link. Hyperamylasemia is a laboratory finding, not an infectious disease. The retrieved literature describes it as a secondary finding in other conditions (Weil syndrome, intracranial bleeding), or concerns ceftriaxone given to prevent infection after endoscopic papillosphincterotomy. The high model score is not supported by clinical evidence.

**Other top-ranked predictions:** Polyclonal hyperviscosity syndrome, congenital analbuminemia and blood group incompatibility also have no identified mechanistic link and no supporting trials. For blood group incompatibility, the only ceftriaxone-related report concerns biliary pseudolithiasis, which is an adverse effect, not a benefit.

**Infectious otitis media (rank 4):** This prediction is biologically plausible. Ceftriaxone is active against the main pathogens of acute otitis media (*Streptococcus pneumoniae*, *Haemophilus influenzae*, *Moraxella catarrhalis*). It is effectively an extension of its existing antibacterial spectrum rather than a novel repurposing mechanism.

**Suppurative and chronic otitis media (ranks 6–7):** Support is weaker. Chronic disease is often caused by *Pseudomonas aeruginosa* and *Staphylococcus aureus*, which ceftriaxone covers poorly, and topical therapy is usually preferred.

---

## Clinical Trial Evidence

No clinical trials are registered for hyperamylasemia. Currently no related clinical trials registered for the top-ranked prediction.

For the better-supported prediction, infectious otitis media:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02567825](https://clinicaltrials.gov/study/NCT02567825) | Not applicable | Completed | 250 | Tympanostomy tubes vs nonsurgical management in children with recurrent acute otitis media. It does not test ceftriaxone and is relevant only as disease context |

---

## Literature Evidence

**Hyperamylasemia (top-ranked prediction):**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10458061](https://pubmed.ncbi.nlm.nih.gov/10458061/) | 1999 | Clinical study | Bratislavske lekarske listy | Ceftriaxone 1 g given to 30 patients after endoscopic papillosphincterotomy, compared with 30 without prophylaxis. The target was infection, not amylase |
| [7522351](https://pubmed.ncbi.nlm.nih.gov/7522351/) | 1994 | Cohort | Southern Medical Journal | Elevated pancreatic enzymes in 38 patients with intracranial bleeding, without pancreatitis. Ceftriaxone not involved |
| [36263834](https://pubmed.ncbi.nlm.nih.gov/36263834/) | 2023 | Case report | Revista espanola de enfermedades digestivas | Weil syndrome with upper gastrointestinal bleeding. Not a ceftriaxone treatment study |

**Infectious otitis media (most plausible alternative):**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8989332](https://pubmed.ncbi.nlm.nih.gov/8989332/) | 1997 | RCT | Pediatrics | Single intramuscular ceftriaxone dose compared with 10 days of oral trimethoprim-sulfamethoxazole for acute otitis media |
| [11099083](https://pubmed.ncbi.nlm.nih.gov/11099083/) | 2000 | Clinical study | Pediatric Infectious Disease Journal | One-day vs three-day intramuscular ceftriaxone in nonresponsive acute otitis media in children |
| [9877360](https://pubmed.ncbi.nlm.nih.gov/9877360/) | 1998 | Clinical study | Pediatric Infectious Disease Journal | Bacteriologic efficacy of a three-day intramuscular ceftriaxone regimen in nonresponsive acute otitis media |
| [35841649](https://pubmed.ncbi.nlm.nih.gov/35841649/) | 2022 | Cohort | Int J Pediatr Otorhinolaryngol | Patterns of ceftriaxone use for acute otitis media in US primary care; increased use may mark antimicrobial resistance |
| [12166789](https://pubmed.ncbi.nlm.nih.gov/12166789/) | 2002 | Consensus recommendations | Clinical Pediatrics | Consensus recommendations for managing acute otitis media |
| [10688388](https://pubmed.ncbi.nlm.nih.gov/10688388/) | 2000 | Review | Clinical Therapeutics | Combined revised recommendations for antimicrobial therapy of acute otitis media |
| [39361280](https://pubmed.ncbi.nlm.nih.gov/39361280/) | 2024 | Review | JAMA Network Open | Appropriateness of paediatric outpatient antibiotic prescribing |
| [20802367](https://pubmed.ncbi.nlm.nih.gov/20802367/) | 2010 | Review | Otology & Neurotology | Prevention and treatment of meningitis and acute otitis media in children with cochlear implants |

For suppurative otitis media, one 1989 comparative study of ceftriaxone against cefotiam is available ([PMID 2523493](https://pubmed.ncbi.nlm.nih.gov/2523493/), Japanese Journal of Antibiotics). Efficacy was 71% with ceftriaxone and 86% with cefotiam in acute suppurative otitis media and acute exacerbation of chronic disease. The remaining literature is mostly microbiological surveys.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2428644 | Ceftriaxone-AFT | Inj 1 g vial | Not available (status: cancelled) |
| 2400820 | Ceftriaxone-AFT | Inj 500 mg vial | Not available (status: cancelled) |

Both licences are cancelled, so there is currently no valid ceftriaxone product in New Zealand according to the data compiled.

---

## Safety Considerations

- **Contraindications flagged in the knowledge graph** (not label text): liver disease, gallbladder disease, neonatal jaundice, *Clostridium difficile* colitis, perinatal necrotizing enterocolitis and haemolytic anaemia.
- **Drug Interactions** (100 interactions recorded; main ones listed):
  - **Major:** calcium chloride, calcium glucoheptonate, calcium gluconate.
  - **Moderate:** kanamycin, neomycin, streptomycin, picosulfuric acid.
  - Others are of unknown severity, e.g. amphotericin B, epinephrine, mannitol and amoxicillin.

Please refer to the package insert for further safety information, including warnings.

---

## Conclusion and Next Steps

**Decision: Hold** (for the top-ranked prediction, hyperamylasemia). Infectious otitis media may be pursued as a research question.

**Rationale:**
- The top-ranked predictions (hyperamylasemia, polyclonal hyperviscosity syndrome, congenital analbuminemia, blood group incompatibility) have no mechanistic link and no supporting trials. They are L5 model outputs only.
- Infectious otitis media (L3) is plausible, but it largely reflects ceftriaxone's existing antibacterial spectrum. Both New Zealand licences are cancelled, and the only registered trial does not test ceftriaxone.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (Medsafe), since safety screening cannot proceed without them
- Detailed mechanism of action data
- A New Zealand-valid product and approved indication text
- For otitis media: current comparative trials against standard first-line therapy, and an assessment of antimicrobial resistance and stewardship concerns, given that guidelines generally favour amoxicillin first
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


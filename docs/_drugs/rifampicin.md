---
layout: default
title: Rifampicin
parent: 僅模型預測 (L5)
nav_order: 304
evidence_level: L5
indication_count: 10
---

# Rifampicin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Rifampicin: From Tuberculosis to Conjunctivitis

## One-Sentence Summary

Rifampicin is a broad-spectrum antimycobacterial antibiotic whose established use — extensively documented across this evidence pack's clinical trial and literature records — is tuberculosis (and related mycobacterial) treatment. The TxGNN model predicts it may also be effective for **Conjunctivitis**, a repurposing direction currently supported by **0 clinical trials** and **20 publications**, most of which are historical, epidemiological, or case-based rather than direct comparative trials.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Tuberculosis / mycobacterial infection (inferred from extensive anti-TB clinical trial and literature content in the evidence pack; no formal indication text was retrievable for this drug) |
| Predicted New Indication | Conjunctivitis |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from DrugBank for this candidate. Based on the information present in this evidence pack, rifampicin is a DNA-dependent RNA polymerase inhibitor with broad-spectrum bactericidal activity, and its efficacy against mycobacterial infection is well established across dozens of clinical trials in this dataset.

The mechanistic link to conjunctivitis rests on rifampicin's known in vitro and historical clinical activity against ocular pathogens — including *Chlamydia trachomatis* (the causative organism of trachoma, a chronic follicular conjunctivitis) and select conjunctival bacteria such as *Staphylococcus aureus* and *Neisseria meningitidis*. Notably, topical rifampicin was investigated as an ophthalmic ointment for endemic trachoma as early as the 1970s, predating its current systemic anti-TB role, so the "new" indication is arguably a re-emergence of an older, narrower historical use rather than a truly novel mechanism.

However, the supporting literature is overwhelmingly epidemiological (pathogen/antibiotic-susceptibility surveys) or single-case reports (e.g., meningococcal conjunctivitis treated with adjunctive systemic rifampin), rather than modern controlled trials establishing rifampicin — oral, systemic, or topical — as an effective conjunctivitis therapy in a defined patient population.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [1096630](https://pubmed.ncbi.nlm.nih.gov/1096630/) | 1975 | Controlled therapy trial | American Journal of Ophthalmology | Controlled trial in Tunisian schoolchildren comparing 1% rifampicin ointment, 1% tetracycline ointment, and 5% boric acid ointment for endemic trachoma; assessed via serial slit-lamp exams and cultures over 39 weeks |
| [5411121](https://pubmed.ncbi.nlm.nih.gov/5411121/) | 1970 | Preclinical/pharmacology | Nature | Early evidence of anti-trachoma activity of rifampicin and rifamycin SV derivatives |
| [5005929](https://pubmed.ncbi.nlm.nih.gov/5005929/) | 1971 | Review | Annals of Ophthalmology | Overview of rifampicin use in ophthalmology (abstract not available) |
| [14686993](https://pubmed.ncbi.nlm.nih.gov/14686993/) | 2003 | Case report | Clinical Microbiology and Infection | 6-year-old with primary meningococcal conjunctivitis treated with topical antibiotics followed by systemic rifampin once diagnosis confirmed; no complications |
| [7806886](https://pubmed.ncbi.nlm.nih.gov/7806886/) | 1994 | Case series | The Journal of Infection | Three cases of primary meningococcal conjunctivitis with systemic sepsis; recommends combined topical/parenteral therapy plus chemoprophylaxis (e.g., rifampin) for close contacts |
| [33457332](https://pubmed.ncbi.nlm.nih.gov/33457332/) | 2020 | Cohort (pathogen/susceptibility survey) | Advanced Biomedical Research | Bacterial etiology and antibiotic susceptibility survey of conjunctivitis isolates in Kashan, Iran |
| [15228931](https://pubmed.ncbi.nlm.nih.gov/15228931/) | 2004 | Cohort (pathogen/susceptibility survey) | Anales de Pediatría | Identifies most prevalent bacterial conjunctivitis pathogens and antibiotic sensitivity patterns in a pediatric population |
| [19941479](https://pubmed.ncbi.nlm.nih.gov/19941479/) | 2010 | Review | Current Medicinal Chemistry | Reviews neglected bacterial diseases including trachoma and Buruli ulcer; notes rifampin/streptomycin as the only effective combination for Buruli ulcer |
| [21484175](https://pubmed.ncbi.nlm.nih.gov/21484175/) | 2011 | Cohort (pathogen/susceptibility survey) | Journal of Ophthalmic Inflammation and Infection | Bacteriologic and plasmid analysis of conjunctivitis-causing organisms in Lagos, Nigeria |
| [10537781](https://pubmed.ncbi.nlm.nih.gov/10537781/) | 1999 | Review | Current Opinion in Ophthalmology | Reviews ocular manifestations of cat-scratch disease (Bartonella henselae), a differential-diagnosis consideration rather than direct rifampicin efficacy evidence |

---

## New Zealand Market Information

Rifampicin is currently **not marketed** in New Zealand under this evidence pack — 0 product authorizations are on file, so no product/dosage-form table can be produced.

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and drug-drug interaction data were not available in this evidence pack — the DDI query returned no results.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence Level L3 reflects mostly epidemiological cohort studies, single-case reports, and one dated (1975) controlled trachoma ointment trial — no modern trial establishes rifampicin as an effective conjunctivitis therapy in a defined route/dose. Combined with the drug's non-marketed status in New Zealand and the complete absence of package-insert safety data (a blocking gap for any initial safety review), the candidate cannot currently advance past a research question.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently blocking, required before any safety pre-evaluation
- Confirmed mechanism of action data from DrugBank
- Route/formulation compatibility analysis — existing evidence is split between oral/systemic use (meningococcal cases) and topical ophthalmic ointment (historical trachoma trials); the currently marketed formulation profile is undetermined
- Modern comparative or controlled trial evidence specific to conjunctivitis treatment, rather than pathogen-susceptibility surveys or case reports
- Clarification of the overlapping "conjunctivitis" / "conjunctivitis (disease)" / "acute contagious conjunctivitis" labels in the underlying disease ontology, which appear to fragment the same signal across multiple ranked candidates
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


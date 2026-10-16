---
layout: default
title: Cefuroxime
parent: Model Prediction Only (L5)
nav_order: 90
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Cefuroxime: predicted new indication Hyperamylasemia (evidence level L5). Approved use: Infectious otitis media, bacterial pneumonia, bacterial meningitis…"
monitor_inn: "cefuroxime"
predicted_indications: ["hyperamylasemia", "polyclonal hyperviscosity syndrome", "congenital analbuminemia", "Peptostreptococcus infectious disease", "blood group incompatibility", "urinary tract infection (disease)", "premalignant hematological system disease", "monoclonal gammopathy", "suppurative otitis media", "hematological disease associated with an acquired peripheral neuropathy"]
---

# Cefuroxime
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

# Cefuroxime: From Bacterial Infections to Urinary Tract Infection

## One-Sentence Summary

Cefuroxime is a second-generation cephalosporin antibiotic, originally used for infections such as otitis media, pneumonia, meningitis, gonorrhoea and Lyme disease.
The TxGNN model's top-ranked predictions are mostly unrelated to infection. The only well-supported one is **urinary tract infection (UTI)**, with **5 clinical trials** and **20 publications** retrieved. UTI is plausibly already within the drug's existing antibacterial use, so this is not a true repurposing finding.
The rank 1 prediction, **hyperamylasemia**, has no supporting evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Infectious otitis media, bacterial pneumonia, bacterial meningitis, gonorrhoea, Lyme disease and other bacterial infections (approved indication text is not available for the New Zealand licence) |
| Predicted New Indication | Hyperamylasemia (rank 1); the best-supported prediction is urinary tract infection (rank 6) |
| TxGNN Prediction Score | 99.76% (hyperamylasemia); 99.62% (urinary tract infection) |
| Evidence Level | L5 (hyperamylasemia); L3 (urinary tract infection) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 (0 currently valid, 1 cancelled) |
| Recommended Decision | Hold (hyperamylasemia); Proceed with Guardrails is not warranted, and UTI is better treated as a research question |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, cefuroxime is a beta-lactam antibiotic that inhibits bacterial cell-wall synthesis. It is active against common respiratory and urinary pathogens.

**Hyperamylasemia (rank 1):** There is no plausible mechanistic link. Hyperamylasemia is a biochemical finding with no bacterial target, and the high score is most likely an artifact of the knowledge graph. Several other top-ranked predictions are in the same position: polyclonal hyperviscosity syndrome, congenital analbuminemia, premalignant haematological disease, and haematological disease with acquired peripheral neuropathy. None has clinical or literature support.

**Urinary tract infection (rank 6):** This is a strong mechanistic fit. Cefuroxime covers common uropathogens such as *E. coli*, and UTI is a natural extension of the antibacterial class. However, no Phase 3 RCT directly evaluating cefuroxime for UTI was identified. The evidence is mostly observational.

**Other infection-related predictions:** Suppurative otitis media is biologically plausible, but it largely overlaps with the existing otitis media indication. *Peptostreptococcus* infection is plausible, but cefuroxime's anaerobic coverage is limited and the only evidence is a 1981 in-vitro study.

---

## Clinical Trial Evidence

Trials below are for the best-supported prediction, urinary tract infection. The rank 1 prediction (hyperamylasemia) has no registered trials.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04616352](https://clinicaltrials.gov/study/NCT04616352) | N/A | Completed | 973 | Effect of inappropriate cefuroxime therapy in adults hospitalised with pyelonephritis; observational data and the most directly relevant trial |
| [NCT01507974](https://clinicaltrials.gov/study/NCT01507974) | N/A | Completed | 220 | Preventive antibiotics during the puerperium in women with recurrent UTI; cefuroxime-specific role not confirmed |
| [NCT05577273](https://clinicaltrials.gov/study/NCT05577273) | N/A | Unknown | 1000 | Antibiotic prophylaxis at urinary catheter removal; cefuroxime not clearly the tested agent |
| [NCT02789579](https://clinicaltrials.gov/study/NCT02789579) | Early Phase 1 | Unknown | 150 | One-week antimicrobial solutions to prevent UTI before minimally invasive upper tract lithotomy; cefuroxime's role unclear |
| [NCT02009098](https://clinicaltrials.gov/study/NCT02009098) | Phase 4 | Withdrawn | 0 | Antibiotic prophylaxis for postpartum infections after caesarean section; withdrawn with no participants |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [2689345](https://pubmed.ncbi.nlm.nih.gov/2689345/) | 1989 | Randomised comparison | Infection | Aztreonam vs cefuroxime in gram-negative upper UTI in hospitalised patients |
| [38215770](https://pubmed.ncbi.nlm.nih.gov/38215770/) | 2024 | Randomised trial | Lancet Infect Dis | SIMPLIFY: de-escalation from antipseudomonal β-lactams in Enterobacterales bloodstream infection |
| [41693075](https://pubmed.ncbi.nlm.nih.gov/41693075/) | 2026 | Systematic review | Pharmacotherapy | Oral β-lactams vs trimethoprim/sulfamethoxazole and fluoroquinolones for complicated UTI |
| [8799689](https://pubmed.ncbi.nlm.nih.gov/8799689/) | 1996 | Review | Drugs | Cefuroxime axetil: antibacterial activity, pharmacokinetics and therapeutic efficacy |
| [6349019](https://pubmed.ncbi.nlm.nih.gov/6349019/) | 1983 | Review | Ther Drug Monit | Cefuroxime antimicrobial activity, pharmacology and clinical efficacy |
| [6344037](https://pubmed.ncbi.nlm.nih.gov/6344037/) | 1983 | Review | Pharmacotherapy | Cefuroxime mechanisms, activity, pharmacokinetics, adverse reactions and indications |
| [18611587](https://pubmed.ncbi.nlm.nih.gov/18611587/) | 1994 | Review | Int J Antimicrob Agents | Cefuroxime axetil; high susceptibility of common respiratory pathogens |
| [39005695](https://pubmed.ncbi.nlm.nih.gov/39005695/) | 2024 | Cohort/observational | Infect Dis Clin Microbiol | Pathogens and ESBL risk factors in community-acquired UTI |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2658445 | Ascend-Cefuroxime | Tab 250 mg | Not available (licence cancelled) |

---

## Safety Considerations

The following are knowledge-graph contraindication associations, not label text. They are listed as flags for review:

- **Contraindication-related conditions:** kidney disease, pyoureter, liver disease, gallbladder disease, blood coagulation disease, *Clostridium difficile* colitis, perinatal necrotizing enterocolitis, congenital prothrombin deficiency.
- **Drug Interactions:** 143 interactions were recorded; the 20 listed are all of Moderate severity. They include:
  - H2 blockers: famotidine, ranitidine, cimetidine, nizatidine
  - Proton pump inhibitors: omeprazole, esomeprazole, lansoprazole, dexlansoprazole, pantoprazole, rabeprazole
  - Antacids: aluminium hydroxide, calcium carbonate, magnesium hydroxide, magnesium carbonate, magnesium oxide, magaldrate
  - Aminoglycosides: kanamycin, neomycin
  - Others: balsalazide

Package insert warnings and contraindications could not be retrieved. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The rank 1 prediction (hyperamylasemia) and most other top-ranked predictions have no mechanistic rationale, no trials and no literature, so they are likely knowledge-graph artifacts (evidence level L5).
- Urinary tract infection has observational support (L3) but is probably already within cefuroxime's existing antibacterial use, so it is a research question rather than a true repurposing opportunity. Suppurative otitis media is similar (L4).
- The only New Zealand licence is cancelled, and the safety review cannot proceed without label data.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- A cefuroxime-specific randomised comparative trial in UTI, if that direction is pursued
- Confirmation of whether UTI is already within the approved or historical labelled indications
- Clarification of the market situation in New Zealand, given the cancelled licence and the absence of an approved indication text
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Cefazolin
parent: 僅模型預測 (L5)
nav_order: 67
evidence_level: L5
indication_count: 8
---

# Cefazolin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **8** 個
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

# Cefazolin: From Bacterial Infections to Infectious Otitis Media

## One-Sentence Summary

Cefazolin is a first-generation cephalosporin antibiotic, internationally recognized for surgical prophylaxis and gram-positive bacterial infection treatment, though it is not currently registered in New Zealand.
The TxGNN model predicts it may be effective for **Infectious Otitis Media**, with **1 clinical trial** and **3 publications** currently supporting this direction.
However, evidence quality is limited — the only registered trial was terminated early, and supporting literature consists primarily of case reports and older reviews.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not registered in New Zealand; internationally used for gram-positive bacterial infections and surgical prophylaxis |
| Predicted New Indication | Infectious Otitis Media |
| TxGNN Prediction Score | 99.44% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data was not retrieved from DrugBank for this evidence pack. Based on established pharmacology, cefazolin is a first-generation cephalosporin that inhibits bacterial cell wall synthesis by binding to penicillin-binding proteins (PBPs), blocking peptidoglycan cross-linking. This bactericidal mechanism is effective against gram-positive cocci — in particular *Staphylococcus aureus* (MSSA) and *Streptococcus pneumoniae* — which are among the pathogens implicated in acute otitis media.

The typical pathogens of acute infectious otitis media (AOM) are *S. pneumoniae* (30–40%), *Haemophilus influenzae* (20–30%), and *Moraxella catarrhalis*. Cefazolin covers *S. pneumoniae* reasonably well, but has limited activity against *H. influenzae* and *M. catarrhalis*, which means it lacks the full antimicrobial spectrum needed for empiric monotherapy in most AOM cases. In practice, its role in otological infections is better defined for surgical prophylaxis (e.g., perioperative coverage for MSSA) or culture-directed therapy after pathogen confirmation.

The TxGNN prediction likely derives from the knowledge graph topology linking cefazolin's antibacterial activity against gram-positive cocci to the polymicrobial pathology of otitis media. While the mechanistic overlap is biologically plausible for a gram-positive-dominant subset of AOM, incomplete pathogen coverage and the absence of oral bioavailability (cefazolin requires IV/IM administration) substantially constrain its clinical applicability as a primary treatment.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|------------|--------------|
| [NCT01511107](https://clinicaltrials.gov/study/NCT01511107) | Phase 2b | Terminated | 520 | Multicenter, double-blind, placebo-controlled RCT comparing 5-day vs. 10-day antibiotic courses for AOM in children aged 6–23 months; terminated before completion in October 2015 — termination reason unknown, constituting a safety/efficacy signal that warrants caution |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|--------------|
| [39567876](https://pubmed.ncbi.nlm.nih.gov/39567876/) | 2025 | Case series | Ann Otol Rhinol Laryngol | Ceftazidime-cefazolin empiric combination used for Gradenigo Syndrome, a rare complication of petrous apicitis secondary to AOM; supports a supporting (not primary) role for cefazolin in gram-positive otological coverage |
| [877649](https://pubmed.ncbi.nlm.nih.gov/877649/) | 1977 | Review | South Med J | Narrative review of cephalosporin use in pediatric infections; notes gram-positive efficacy of first-generation cephalosporins including activity against *S. pneumoniae* relevant to otitis media — limited by age |
| [3742953](https://pubmed.ncbi.nlm.nih.gov/3742953/) | 1986 | Case report | Clin Pharmacy | Stevens-Johnson syndrome case in a child treated for otitis media; cefazolin appears as part of the inpatient antibiotic sequence — incidental rather than indicative of AOM-directed efficacy |

---

## New Zealand Market Information

Cefazolin is not currently registered or marketed in New Zealand. No authorization records are available in the MEDSAFE database.

---

## Safety Considerations

Please refer to the package insert for safety information. TFDA package insert data (warnings and contraindications) was identified as a blocking data gap and was not available in this evidence pack. Drug interaction data was not found in the DDI database query.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The sole registered clinical trial (NCT01511107) was terminated before completion for undisclosed reasons, removing the primary source of controlled efficacy data; the remaining literature is composed of dated reviews and case reports that do not establish cefazolin as a viable treatment for infectious otitis media. Furthermore, cefazolin's antimicrobial spectrum covers only the gram-positive component of typical AOM pathogens, and its IV-only administration route is mismatched with outpatient otitis media management.

**To proceed, the following is needed:**
- Determine the termination reason for NCT01511107 to assess whether it reflects an efficacy failure, a safety signal, or an operational issue
- Retrieve mechanism of action data from DrugBank (data gap DG002) to formalise the pharmacological rationale
- Retrieve TFDA/MEDSAFE safety data including contraindications and key warnings (data gap DG001 — currently blocking S1 safety review)
- Reassess with pathogen-stratified clinical data: cefazolin's role may be viable specifically for *S. pneumoniae*-confirmed AOM or surgical prophylaxis in otological procedures (see Rank 3: middle ear disease, scored "Proceed with Guardrails")
- Evaluate route-of-administration compatibility for the target indication — IV-only delivery significantly limits outpatient AOM use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


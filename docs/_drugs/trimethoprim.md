---
layout: default
title: Trimethoprim
parent: 僅模型預測 (L5)
nav_order: 353
evidence_level: L5
indication_count: 2
---

# Trimethoprim
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
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

# Trimethoprim: From Bacterial Infections to Conjunctivitis

## One-Sentence Summary

Trimethoprim is a dihydrofolate reductase (DHFR) inhibitor classically used against bacterial infections (e.g. urinary tract infections), typically as part of a combination product. The TxGNN model predicts it may be effective for **Conjunctivitis** (bacterial), with **3 clinical trials** and **20 publications** currently identified, including a completed Phase 4 head-to-head RCT against moxifloxacin. This is less a novel discovery than confirmation of an already-established use: trimethoprim/polymyxin B (Polytrim) is a marketed ophthalmic antibiotic elsewhere, though trimethoprim itself is not currently licensed in New Zealand.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not on file — trimethoprim has no New Zealand license, so no approved indication text exists; known pharmacological use is bacterial infections (e.g. UTIs) |
| Predicted New Indication | Conjunctivitis (bacterial) |
| TxGNN Prediction Score | 99.17% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not on file for this drug record (flagged as a High-severity data gap). Based on known pharmacology captured in the evidence pack, trimethoprim inhibits bacterial dihydrofolate reductase (DHFR), blocking folate synthesis and producing bacteriostatic/bactericidal activity against common conjunctivitis pathogens such as *Haemophilus influenzae* and *Streptococcus pneumoniae*.

Critically, this prediction is not really a new hypothesis — trimethoprim/polymyxin B (marketed as Polytrim) is already an approved topical antibiotic for **bacterial** conjunctivitis in multiple jurisdictions. TxGNN's high score reflects a known, mechanistically direct relationship rather than a speculative repositioning. The caveat is specificity: trimethoprim's mechanism only supports **bacterial** conjunctivitis — it has no activity against viral or chlamydial causes (standard therapy for neonatal chlamydial conjunctivitis is macrolide-based, not trimethoprim, per PMID 30007329).

Note: TxGNN also flagged *punctate epithelial keratoconjunctivitis* (score 99.57%) as a top candidate, but this was excluded from further evaluation (Evidence Level L5, decision Hold) — no clinical trials or literature support it, and the condition's predominantly viral/immune-mediated etiology has no clear mechanistic link to trimethoprim's antibacterial action.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00581542](https://clinicaltrials.gov/study/NCT00581542) | Phase 4 | Completed | 124 | Head-to-head comparison of Polytrim (trimethoprim/polymyxin B) vs. moxifloxacin ophthalmic solution for pediatric conjunctivitis ("pink eye") |
| [NCT03187834](https://clinicaltrials.gov/study/NCT03187834) | Phase 4 | Completed | 252 | Antibiotic resistance/microbiome study in children (Burkina Faso); background antibiotic-exposure research, not a conjunctivitis efficacy trial |
| [NCT00168532](https://clinicaltrials.gov/study/NCT00168532) | Phase 3 | Completed | 218 | Community RCT of prophylactic antibiotics in measles infection (Guinea-Bissau); conjunctivitis is only an incidental complication, not the primary treatment target |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [30007329](https://pubmed.ncbi.nlm.nih.gov/30007329/) | 2018 | Systematic Review/Meta-analysis | J Pediatr Infect Dis Soc | Reviewed antibiotic treatments (including trimethoprim) for neonatal chlamydial conjunctivitis; macrolides remain standard, trimethoprim not preferred for this etiology |
| [19043945](https://pubmed.ncbi.nlm.nih.gov/19043945/) | 2008 | RCT | J Pediatr Ophthalmol Strabismus | Multicenter trial comparing speed of clinical efficacy of polymyxin B/trimethoprim vs. 0.5% moxifloxacin in bacterial conjunctivitis |
| [8595639](https://pubmed.ncbi.nlm.nih.gov/8595639/) | 1995 | Cohort/Case series | Clinical Therapeutics | Survey of children with acute bacterial conjunctivitis treated with trimethoprim-polymyxin B ophthalmic solution |
| [6204534](https://pubmed.ncbi.nlm.nih.gov/6204534/) | 1984 | Clinical evaluation | Am J Ophthalmol | Early clinical evaluation of trimethoprim-containing ophthalmic solutions (with sulfacetamide or polymyxin B) for bacterial conjunctivitis/blepharitis |
| [16491721](https://pubmed.ncbi.nlm.nih.gov/16491721/) | 2006 | Review | J Pediatr Ophthalmol Strabismus | Guidance on controlling contagious bacterial conjunctivitis outbreaks with antimicrobial therapy |
| [20084257](https://pubmed.ncbi.nlm.nih.gov/20084257/) | 2001 | Review | Paediatr Child Health | Review of etiology, features, and management of acute infectious conjunctivitis in children |
| [10537781](https://pubmed.ncbi.nlm.nih.gov/10537781/) | 1999 | Review | Curr Opin Ophthalmol | Overview of ocular manifestations of cat-scratch disease (differential diagnosis context) |
| [8924168](https://pubmed.ncbi.nlm.nih.gov/8924168/) | 1996 | Review | Laryngo-Rhino-Otologie | Overview of cat-scratch disease for ENT physicians (differential diagnosis context) |
| [24892274](https://pubmed.ncbi.nlm.nih.gov/24892274/) | 2015 | Case report | Ophthalmic Plast Reconstr Surg | Case of chronic *Nocardia*-related conjunctivitis on a silicone stent, sensitive to trimethoprim/sulfamethoxazole |
| [19913370](https://pubmed.ncbi.nlm.nih.gov/19913370/) | 2010 | Case report | Vet Microbiol | Veterinary case series (equine conjunctivitis pathogens); limited direct relevance to human use |

## Safety Considerations

Please refer to the package insert for safety information. Note that TFDA/NZ package insert warnings and contraindications for this drug are currently a **Blocking** data gap — this must be resolved before a formal S1 safety review can proceed.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Bacterial conjunctivitis as an indication is well supported — a completed Phase 4 head-to-head RCT (NCT00581542) plus several decades of consistent RCT, cohort, and clinical-evaluation literature show trimethoprim/polymyxin B performing comparably to standard antibiotics such as moxifloxacin. However, trimethoprim has no current New Zealand market authorization and no on-file safety/warning data, so guardrails are needed before this evidence can translate into a local decision.

**To proceed, the following is needed:**
- TFDA/NZ package insert warnings and contraindications (Blocking gap — required before S1 safety review)
- Confirmed mechanism-of-action data from DrugBank (High-severity gap)
- A regulatory pathway assessment for New Zealand licensing (currently 0 authorizations)
- Updated resistance-pattern literature, since most supporting evidence dates from 1984–2018
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Vancomycin
parent: 僅模型預測 (L5)
nav_order: 360
evidence_level: L5
indication_count: 10
---

# Vancomycin
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

# Vancomycin: From Gram-Positive Bacterial Infections to Streptococcal Pneumonia

> **Note on indication selection:** This evidence pack (`TW-DB00512-multi`) evaluated 10 TxGNN-predicted indications for vancomycin. Nine of them (ranked #1–#8, #10 by raw TxGNN score) were explicitly flagged in the source data's own `repurposing_rationale` as mechanistically implausible or unsupported by any efficacy evidence — several are outright judged "false positives" by the evidence-collection pipeline itself. Only **streptococcal pneumonia** (rank #9) carries an `L1` evidence level and a `Proceed with Guardrails` recommendation. This report therefore centers on streptococcal pneumonia as the only actionable candidate, and summarizes the other 9 for transparency below.

## One-Sentence Summary

Vancomycin is a glycopeptide antibiotic historically used for serious Gram-positive bacterial infections (original indication text is a data gap in this pack, as the drug is not yet marketed in New Zealand). TxGNN separately assigned it a **99.60% prediction score** for **Streptococcal Pneumonia**, and unlike most of the model's other top hits for this drug, this one is backed by **3 clinical trials** and **20 publications**, including a treatment meta-analysis and randomized comparisons — making it a genuine evidence-reinforcement case rather than a novel biological hypothesis.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from regulatory sources (data gap; not marketed in New Zealand). Based on general drug identity referenced within this evidence pack's own trial data, vancomycin is a glycopeptide antibiotic used for serious Gram-positive infections. |
| Predicted New Indication | Streptococcal Pneumonia |
| TxGNN Prediction Score | 99.60% (rank 3867 among all disease predictions for this drug) |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

### Other TxGNN-Predicted Indications Screened (for transparency)

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Recommendation | Screening Note |
|---|---|---|---|---|---|
| 1 | Diffuse scleroderma | 99.92% | L5 | Hold | No mechanistic link; sole literature hit is an unrelated sepsis case report — likely false positive |
| 2 | Paratyphoid fever | 99.85% | L4 | Hold | Gram-negative pathogen; vancomycin lacks activity; no efficacy evidence |
| 3 | Salmonellosis | 99.81% | L4 | Hold | Gram-negative pathogen; 20 literature hits are epidemiology/surveillance, none evaluate treatment efficacy |
| 4 | Congenital analbuminemia | 99.79% | L5 | Hold | No mechanistic link, zero evidence |
| 5 | Polyclonal hyperviscosity syndrome | 99.79% | L5 | Hold | No mechanistic link, zero evidence |
| 6 | Hyperamylasemia | 99.79% | L5 | Hold | No mechanistic link, zero evidence |
| 7 | Typhoid fever | 99.75% | L4 | Hold | Gram-negative pathogen; only a single unrelated case report supports use |
| 8 | Blood group incompatibility | 99.63% | L5 | Hold | No mechanistic link; immunohematologic condition unrelated to antimicrobial action |
| **9** | **Streptococcal pneumonia** | **99.60%** | **L1** | **Proceed with Guardrails** | **Featured below — real mechanistic and clinical support** |
| 10 | Premalignant hematological system disease | 99.54% | L5 | Hold | No mechanistic link, zero evidence |

This pattern — a cluster of very high but clinically implausible TxGNN scores alongside one biologically coherent hit — suggests the model's raw ranking is not reliable on its own for this drug and should always be triaged against mechanism and evidence before being acted on.

---

## Why is This Prediction Reasonable?

Detailed DrugBank mechanism-of-action text was not available in this evidence pack (flagged as a High-severity data gap, DG002). Based on well-established pharmacology referenced within the evidence pack itself (e.g., trial NCT05395520 describes it as "a glycopeptide antibiotic... prescribed... due to its broad gram-positive coverage"), vancomycin inhibits bacterial cell wall peptidoglycan synthesis, which is bactericidal against Gram-positive organisms.

*Streptococcus pneumoniae* is a Gram-positive coccus, so this mechanism is directly applicable — unlike most of the other TxGNN hits above, which target Gram-negative organisms (*Salmonella* spp.) that vancomycin's large molecular size cannot penetrate, or conditions with no plausible pharmacological connection at all.

Importantly, the underlying evidence pack's own rationale notes this is **not a novel repurposing hypothesis** but a reinforcement of an already-recognized clinical use: vancomycin is a standard or alternative therapy for penicillin-resistant pneumococcus or for severe community-acquired pneumonia where concurrent MRSA is suspected. The TxGNN signal here is best read as the model correctly recovering an existing evidence-backed use rather than surfacing something new.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT05395520](https://clinicaltrials.gov/study/NCT05395520) | N/A | Unknown | 146 | Evaluates whether AUC-based therapeutic monitoring of IV vancomycin is appropriate beyond serious MRSA infections, implicitly covering broader Gram-positive infection populations including pneumonia; direct dose–efficacy evidence for the drug itself (relevance grade A), though status is unverified. |
| [NCT04464291](https://clinicaltrials.gov/study/NCT04464291) | N/A | Completed | 500 | Epidemiological survey of circulating *S. pneumoniae* serotypes in Russia; provides disease background but does not evaluate vancomycin treatment (relevance grade C). |
| [NCT02538211](https://clinicaltrials.gov/study/NCT02538211) | N/A | Completed | 63 | Studies intestinal microbiome effects on rotavirus vaccine response; not directly related to vancomycin treatment of pneumonia (relevance grade C). |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [21211409](https://pubmed.ncbi.nlm.nih.gov/21211409/) | 2010 | Meta-analysis | Chinese Journal of Tuberculosis and Respiratory Diseases | Meta-analysis of RCTs comparing linezolid vs. vancomycin for pneumonia caused by Gram-positive cocci, evaluating efficacy and safety of vancomycin as an active comparator. |
| [10712318](https://pubmed.ncbi.nlm.nih.gov/10712318/) | 2000 | RCT | American Journal of Respiratory and Critical Care Medicine | Prospective randomized, multicenter trial (n=298) comparing quinupristin/dalfopristin vs. vancomycin for Gram-positive nosocomial pneumonia. |
| [26664260](https://pubmed.ncbi.nlm.nih.gov/26664260/) | 2015 | Retrospective Study | International Journal of Medical Sciences | Retrospective analysis found no resistance to penicillin, cefuroxime, cefotaxime, or vancomycin among pneumococcal pneumonia isolates. |
| [3630711](https://pubmed.ncbi.nlm.nih.gov/3630711/) | 1987 | Preclinical (Animal Model) | Acta Pathologica, Microbiologica, et Immunologica Scandinavica | Mouse-protection model shows vancomycin achieves the highest bactericidal rate relative to MIC against *S. pneumoniae* type 3 among tested antibiotics. |
| [36028454](https://pubmed.ncbi.nlm.nih.gov/36028454/) | 2022 | Cohort/Surveillance | Indian Journal of Medical Microbiology | Surveillance of antibiotic resistance rates and penicillin/vancomycin MIC distribution in streptococcal pneumonia patients, 2013–2019. |
| [27161775](https://pubmed.ncbi.nlm.nih.gov/27161775/) | 2016 | Cohort | Clinical Infectious Diseases | Characterizes prevalence and clinical features of *S. aureus* community-acquired pneumonia, informing empiric Gram-positive coverage (incl. vancomycin) decisions. |
| [9404765](https://pubmed.ncbi.nlm.nih.gov/9404765/) | 1997 | Review | Chest | Discusses declining penicillin use for pneumococcal pneumonia amid resistant strains, and concerns about vancomycin overuse as an alternative. |
| [16341681](https://pubmed.ncbi.nlm.nih.gov/16341681/) | 2005 | Review | European Journal of Clinical Microbiology & Infectious Diseases | Reviews antibiotic management of ventilator-associated pneumonia due to resistant Gram-positive bacteria, with vancomycin as a mainstay option. |
| [16735146](https://pubmed.ncbi.nlm.nih.gov/16735146/) | 2006 | Review | American Journal of Medicine | Reviews antimicrobial resistance among Gram-positive bacteria (MRSA, VRE) and vancomycin's central clinical role. |
| [27929242](https://pubmed.ncbi.nlm.nih.gov/27929242/) | 2016 | Review | American Family Physician | General review of community-acquired pneumonia diagnosis and severity-guided antibiotic selection. |

---

## New Zealand Market Information

Vancomycin currently has **no market authorizations on file in New Zealand** (market status: Not Marketed; 0 total licenses). No product-level licensing table is available from this evidence pack.

---

## Safety Considerations

No structured regulatory safety data (key warnings, contraindications, or drug interactions) is currently available for vancomycin in this evidence pack — this is logged as a **Blocking-severity data gap (DG001)** pending retrieval of the official package insert. Formal drug-interaction database lookup also returned no results.

For general context only (not a formal safety assessment): one referenced trial ([NCT05395520](https://clinicaltrials.gov/study/NCT05395520)) notes that intravenous vancomycin carries known risks of **nephrotoxicity, ototoxicity, and hypersensitivity reactions**, and that AUC-guided therapeutic drug monitoring is increasingly used clinically to manage these risks.

Please refer to the official package insert once available for authoritative safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails** *(for streptococcal pneumonia specifically — all other 9 TxGNN-predicted indications for vancomycin in this pack remain at Hold)*

**Rationale:**
Streptococcal pneumonia is the only one of vancomycin's 10 top TxGNN predictions with a coherent mechanism (Gram-positive cell wall synthesis inhibition) and real supporting clinical literature, including a treatment meta-analysis and a randomized comparative trial. However, this represents evidence reinforcement of an already-recognized clinical use (e.g., penicillin-resistant pneumococcus, suspected concurrent MRSA) rather than a novel repurposing signal, and two Blocking/High-severity data gaps (official warnings/contraindications, and confirmed MOA) remain unresolved.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data (warnings, contraindications) — currently Blocking data gap (DG001)
- Confirmed mechanism-of-action detail from DrugBank — currently High-severity data gap (DG002)
- Confirmation of New Zealand regulatory/market-authorization pathway, since the drug is currently unmarketed there
- Formal drug–drug interaction data
- Given that 9 of 10 top TxGNN hits for vancomycin in this pack were assessed as mechanistically implausible, any further TxGNN signals for this drug should be manually triaged for biological plausibility before entering the automated evidence pipeline
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


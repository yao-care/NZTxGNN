---
layout: default
title: Fosfomycin
parent: 僅模型預測 (L5)
nav_order: 160
evidence_level: L5
indication_count: 10
---

# Fosfomycin
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

Using the evidence pack as the sole factual basis, here is the report. I focused the required sections on `predicted_indications[0]` (gonococcal urethritis) per the template's data-extraction rule, and added a screening-overview appendix at the end covering the other 9 candidate indications in this multi-indication evidence pack — since one of them (pyelitis) carries a stronger decision-stage recommendation than the headline prediction and would otherwise be lost.

---

# Fosfomycin: From Urinary Tract Infection to Gonococcal Urethritis

## One-Sentence Summary

Fosfomycin is a broad-spectrum bactericidal antibiotic classically used against Gram-negative uropathogens (its own cited literature in this pack describes it as "well established for oral treatment of uncomplicated lower urinary tract infections"). The TxGNN model predicts it may be effective for **Gonococcal Urethritis**, a repurposing signal supported by **6 publications** (including one randomized controlled trial) but **no currently registered clinical trials**. This same evidence pack also screened 9 additional candidate indications for fosfomycin, several of which are mechanistically implausible (e.g. Ureaplasma urethritis, hyperamylasemia) while one — pyelitis — carries notably stronger evidence than the headline prediction (see appendix).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not on file in Taiwan/NZ regulatory records (0 licenses); per cited literature, fosfomycin is classically indicated for uncomplicated urinary tract infections |
| Predicted New Indication | Gonococcal Urethritis |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Research Question (Decision Stage S2) |

---

## Why is This Prediction Reasonable?

Currently, detailed formal mechanism-of-action (MOA) data from DrugBank is not available for fosfomycin in this evidence pack — it is flagged as a **High-severity data gap (DG002)**. Based on the mechanistic and literature evidence gathered for this specific candidate, fosfomycin exerts bactericidal activity by irreversibly inhibiting MurA (UDP-N-acetylglucosamine enolpyruvyl transferase), an enzyme catalyzing an early, essential step in bacterial peptidoglycan (cell wall) biosynthesis. This broad-spectrum, cell-wall-targeting mechanism underlies its long-standing use against Gram-negative uropathogens.

*Neisseria gonorrhoeae*, the causative organism of gonococcal urethritis, is a Gram-negative diplococcus with a peptidoglycan cell wall structurally susceptible to MurA inhibition. Since the 1970s, intramuscular fosfomycin has been used clinically to treat acute and subacute gonococcal urethritis (PMID 832528, PMID 832523), and in 2016 an open-label randomized controlled trial specifically evaluated oral fosfomycin trometamol for uncomplicated gonococcal urethritis in men (PMID 27064136). This gives the prediction both a plausible mechanistic basis and decades of real-world clinical precedent — it is not a pure embedding artifact.

That said, current international guidelines (WHO/CDC) do **not** list fosfomycin as first-line therapy for gonorrhea, primarily due to concerns over antimicrobial resistance patterns and uncertain clearance at pharyngeal infection sites. The prediction is therefore mechanistically reasonable and partially supported by historical clinical data, but it requires head-to-head comparison against the current standard of care (ceftriaxone) before being considered for renewed clinical development.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [27064136](https://pubmed.ncbi.nlm.nih.gov/27064136/) | 2016 | RCT | Clin Microbiol Infect | Open randomized controlled trial (n=126 evaluable) of oral fosfomycin trometamol 3g on days 1, 3 and 5 for uncomplicated gonococcal urethritis in men |
| [832523](https://pubmed.ncbi.nlm.nih.gov/832523/) | 1977 | Bacteriological/Clinical Study | Chemotherapy | Multicenter Spanish study of 959 patients treated with fosfomycin across several infections, including gonococcal urethritis, confirming broad-spectrum in vitro activity clinically |
| [832528](https://pubmed.ncbi.nlm.nih.gov/832528/) | 1977 | Clinical Treatment Study (non-randomized) | Chemotherapy | 70 patients with acute/subacute gonococcal urethritis treated with IM fosfomycin; single 4g dose achieved 86% cure, split 2g/4h dosing achieved 92% cure |
| [19593988](https://pubmed.ncbi.nlm.nih.gov/19593988/) | 2009 | Review | Zhonghua Nan Ke Xue | Diagnosis and treatment considerations for non-gonococcal *Neisseria* genitourinary infection in men (indirectly relevant background) |
| [35820778](https://pubmed.ncbi.nlm.nih.gov/35820778/) | 2023 | Secondary Cohort Analysis | Sex Transm Infect | Secondary analysis of the NABOGO trial on spontaneous clearance of asymptomatic *N. gonorrhoeae* infection; not fosfomycin-specific |
| [17878816](https://pubmed.ncbi.nlm.nih.gov/17878816/) | 2007 | Case Report | J Fr Ophtalmol | Gonococcal urethritis complicated by perforating corneal abscess, resistant to penicillins/tetracyclines/fluoroquinolones, resolved with 15 days of high-dose parenteral antibiotics |

---

## New Zealand Market Information

Fosfomycin currently holds **no market authorization** in New Zealand — 0 licenses on file.

---

## Safety Considerations

No safety data (key warnings, contraindications, or drug-drug interactions) could be extracted for fosfomycin in this evidence pack — please refer to the official package insert for safety information.

Note: collection of TFDA/package-insert-level warnings and contraindications is flagged as a **Blocking data gap (DG001)** in the source evidence pack. This means the candidate **cannot yet proceed to the S1 safety pre-screening stage** until official label data is obtained.

---

## Conclusion and Next Steps

**Decision: Research Question**

**Rationale:**
Fosfomycin's activity against gonococcal urethritis has decades-old clinical precedent and one supporting RCT, but no currently registered clinical trials exist, current guidelines do not recommend it as first-line therapy, and a Blocking safety data gap (DG001, missing TFDA/package insert) prevents any safety pre-screening. Evidence supports further investigation, not clinical progression.

**To proceed, the following is needed:**
- TFDA/NZ Medsafe package insert (warnings, contraindications) — resolves Blocking gap DG001
- Formal DrugBank mechanism-of-action data — resolves High-severity gap DG002
- Head-to-head comparative data vs. ceftriaxone (current WHO/CDC first-line therapy), including efficacy at pharyngeal/rectal infection sites
- Contemporary antimicrobial susceptibility/resistance surveillance data for *N. gonorrhoeae* against fosfomycin
- Assessment of a New Zealand regulatory pathway, given the drug currently holds zero market authorizations

---

### Appendix: Additional Predicted Indications (Screening Overview)

This evidence pack screened 10 candidate indications for fosfomycin. The table below summarizes the other 9 beyond the headline prediction — one (pyelitis) carries stronger evidence and a more advanced decision stage than gonococcal urethritis itself and may warrant separate evaluation.

| Rank | Disease | TxGNN Score | Evidence Level | Decision Stage | Recommendation | Note |
|------|---------|-------------|-----------------|-----------------|------------------|------|
| 10 | Pyelitis | 99.37% | L2 | S3 | **Proceed with Guardrails** | Strongest candidate in this pack — MurA-mediated activity against E. coli is well matched to pyelonephritis; IV fosfomycin (ZTI-01/Contepo) already has Phase 2/3 RCT support (ZEUS trial, PMID 30861061) and US approval for complicated UTI/acute pyelonephritis |
| 6 | Urogenital tuberculosis | 99.88% | L4 | S0 | Hold | Cited literature discusses bladder TB diagnosis only, not fosfomycin treatment; *M. tuberculosis* cell wall (mycolic acid) is not a plausible MurA target |
| 7 | Laryngitis | 99.68% | L4 | S0 | Hold | Literature is tangential (nebulized use in sinusitis, ototoxicity protection, pediatric respiratory review); laryngitis is predominantly viral |
| 3 | Uterine inflammatory disease | 99.98% | L4 | S0 | Hold | Two clinical trials are generic pediatric PK platforms, not disease-specific; sole literature match concerns a different drug (dienogest) |
| 2 | Ureaplasma urethritis | 99.99% | L5 | S0 | Hold | Mechanistically implausible — *Ureaplasma* lacks a cell wall, so MurA inhibition should have no effect; no supporting trials or literature |
| 4 | Xanthogranulomatous pyelonephritis | 99.98% | L5 | S0 | Hold | No trials or literature; condition typically requires surgical nephrectomy, not antibiotic monotherapy |
| 5 | Epiglottitis | 99.93% | L5 | S0 | Hold | No trials or literature; not an evidence-based indication for fosfomycin |
| 8 | Hyperamylasemia | 99.47% | L5 | S0 | Hold | Metabolic/pancreatic abnormality, not infectious — no plausible mechanistic link to an antibacterial agent |
| 9 | Polyclonal hyperviscosity syndrome | 99.47% | L5 | S0 | Hold | Plasma cell/immunoglobulin disorder, unrelated to antibacterial mechanism — likely a model false positive |

**Note:** No candidates were dropped from this appendix — all 9 non-headline predictions from the evidence pack are listed above.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


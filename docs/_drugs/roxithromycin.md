---
layout: default
title: Roxithromycin
parent: 僅模型預測 (L5)
nav_order: 314
evidence_level: L5
indication_count: 8
---

# Roxithromycin
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

# Roxithromycin: From Bacterial Infections to Leprosy

## One-Sentence Summary

Roxithromycin is a macrolide antibiotic generally used against susceptible bacterial infections. The TxGNN model predicts it may be effective for **Leprosy**, but this direction is currently supported only by **5 preclinical/in vitro publications** and **0 clinical trials** — no human trial data exists for roxithromycin specifically in this indication.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not specified in evidence pack (roxithromycin is a macrolide antibiotic; class-level use is susceptible bacterial infections) |
| Predicted New Indication | Leprosy |
| TxGNN Prediction Score | 99.70% |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (data gap, DrugBank query pending). Based on known information, roxithromycin belongs to the macrolide antibiotic class, which works by reversibly binding the bacterial 50S ribosomal subunit to inhibit protein synthesis, and its efficacy against susceptible bacterial infections is well established.

Leprosy is caused by *Mycobacterium leprae*, an atypical, slow-growing mycobacterium. The literature evidence here does not directly test roxithromycin in humans with leprosy; instead, it reflects a **class-effect hypothesis**: related macrolides (clarithromycin, erythromycin) demonstrated in vitro and in vivo (mouse footpad, macrophage) bactericidal or bacteriostatic activity against M. leprae, and roxithromycin itself showed activity in one mouse-footpad model — though notably inferior to clarithromycin (PMID 1648889).

Mechanistically this is plausible because macrolides as a class can penetrate macrophages and accumulate at high intracellular concentrations, which is relevant to an intracellular pathogen like M. leprae. However, the supporting evidence is exclusively preclinical (in vitro, animal models, and narrative reviews) — no completed or ongoing clinical trial has evaluated roxithromycin against leprosy.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10481449](https://pubmed.ncbi.nlm.nih.gov/10481449/) | 1999 | Review/Clinical practice | Nihon Hansenbyo Gakkai zasshi (Jpn J Leprosy) | Roxithromycin shows anti-inflammatory and immunomodulatory activity alongside anti-*M. leprae* activity; suppresses carrageenin-induced edema in rats |
| [1648889](https://pubmed.ncbi.nlm.nih.gov/1648889/) | 1991 | In vivo (mouse model) | Antimicrobial Agents and Chemotherapy | Roxithromycin and clarithromycin were bactericidal against *M. leprae* in mouse footpad infection; clarithromycin was superior, likely due to higher infection-site drug levels |
| [3072920](https://pubmed.ncbi.nlm.nih.gov/3072920/) | 1988 | In vitro/In vivo (animal) | Antimicrobial Agents and Chemotherapy | Assessed relative in vitro activity of newer macrolides against *M. leprae*, building on erythromycin's known effects on ATP pools and phenolic glycolipid I synthesis |
| [2665640](https://pubmed.ncbi.nlm.nih.gov/2665640/) | 1989 | In vitro (macrophage) | Antimicrobial Agents and Chemotherapy | Screened over 25 antimicrobial agents in mouse peritoneal macrophages for anti-*M. leprae* activity via inhibition of phenolic glycolipid synthesis |
| [12762831](https://pubmed.ncbi.nlm.nih.gov/12762831/) | 2003 | Review | American Journal of Clinical Dermatology | General review of macrolide selection, adverse effects, and drug interactions in cutaneous bacterial infections; not leprosy-specific |

## New Zealand Market Information

Roxithromycin is currently **not marketed** in New Zealand (0 authorizations on record).

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: A TFDA/regulatory label warnings and contraindications review is currently a **Blocking** data gap — DG001 — and prevents a full S1 safety assessment. DDI query also returned no results.)*

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- Evidence for roxithromycin in leprosy is limited to preclinical/in vitro and animal-model data (L4), with no human clinical trials, and one available in vivo comparison shows it is inferior to clarithromycin within its own drug class.
- Safety data (label warnings, contraindications, DDI) is entirely unavailable (Blocking data gap), and the drug is not currently marketed in New Zealand, so a safety and access pathway cannot yet be assessed.

**To proceed, the following is needed:**
- TFDA/manufacturer package insert (warnings, contraindications) — resolve DG001
- Confirmed mechanism of action from DrugBank — resolve DG002
- Any human clinical or case-series data specifically on roxithromycin (not just clarithromycin) in leprosy/mycobacterial infection
- Drug-drug interaction data
- New Zealand market entry/import pathway assessment given current unmarketed status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


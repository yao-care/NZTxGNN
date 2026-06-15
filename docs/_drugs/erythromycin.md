---
layout: default
title: Erythromycin
parent: 僅模型預測 (L5)
nav_order: 138
evidence_level: L5
indication_count: 5
---

# Erythromycin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

# Erythromycin: From Bacterial Infections to Punctate Epithelial Keratoconjunctivitis

## One-Sentence Summary

Erythromycin is a macrolide antibiotic with established activity against gram-positive bacteria and atypical intracellular pathogens, including *Chlamydia* and fusospirochaetal organisms, historically used across a broad spectrum of bacterial infections.
The TxGNN model predicts it may be effective for **Punctate Epithelial Keratoconjunctivitis (PEK)**,
with **0 clinical trials** and **2 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | No approved indication on record in this market |
| Predicted New Indication | Punctate Epithelial Keratoconjunctivitis |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Erythromycin is a macrolide antibiotic whose primary mechanism involves binding to the 50S ribosomal subunit of susceptible bacteria, inhibiting protein synthesis. Beyond direct antibacterial action, macrolides as a class carry well-documented anti-inflammatory properties — including suppression of matrix metalloproteinases (MMPs) and pro-inflammatory interleukins such as IL-8 — which may offer additional benefit in inflammatory ocular surface disease.

Punctate epithelial keratoconjunctivitis (PEK) most commonly arises as a secondary complication of blepharokeratoconjunctivitis (BKC), an eyelid margin infection driven primarily by *Staphylococcus aureus* and *Chlamydia* species. Both pathogens fall within erythromycin's spectrum of activity, and the 0.5% erythromycin ophthalmic ointment is already in clinical use for lid margin and conjunctival bacterial infections. This mechanistic overlap forms a plausible bridge between the drug and PEK.

Currently, detailed mechanism of action data is not available in the supplied dataset. Based on known information, erythromycin belongs to the macrolide antibiotic class; its antibacterial and anti-inflammatory profile provides a coherent rationale for the TxGNN prediction. However, PEK as a stand-alone indication lacks direct RCT evidence, which limits the confidence grade to L4 and warrants further investigation before clinical translation.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [11495307](https://pubmed.ncbi.nlm.nih.gov/11495307/) | 2001 | Review / Case Series | Journal of Pediatric Ophthalmology and Strabismus | Describes clinical features and management of blepharokeratoconjunctivitis in children — the primary upstream aetiology of PEK; supports the pathogen-to-mechanism link for erythromycin |
| [32826651](https://pubmed.ncbi.nlm.nih.gov/32826651/) | 2021 | Case Report | Cornea | Documents *Encephalitozoon hellem* keratoconjunctivitis in an immunocompetent adult diagnosed by metagenomic deep sequencing; illustrates the diagnostic complexity of atypical keratoconjunctivitis and the need for pathogen-directed therapy |

---

## New Zealand Market Information

No regulatory authorizations for erythromycin are on record in the New Zealand Medsafe database for this dataset. The drug is recorded as **not marketed**.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** Key warnings, contraindications, and drug interaction data were not retrievable in this Evidence Pack cycle. The TFDA package insert has been identified as a remediation source (Data Gap DG001, severity: Blocking); this must be resolved before any formal safety screening (S1 stage) can proceed.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence is limited to two indirect publications — a paediatric BKC review and an atypical keratoconjunctivitis case report — with no registered clinical trials evaluating erythromycin specifically in punctate epithelial keratoconjunctivitis. The mechanistic rationale is plausible but unconfirmed at the clinical level, placing this candidate at L4 and precluding a Go recommendation at this stage.

**To proceed, the following is needed:**
- Resolve Data Gap DG001: download and parse the TFDA (or equivalent) package insert to extract key warnings and contraindications, enabling S1 safety screening
- Resolve Data Gap DG002: query DrugBank API for formal MOA data to strengthen the mechanistic argument
- Conduct a targeted literature search specifically for erythromycin ophthalmic ointment in PEK or BKC-associated corneal epithelial disease
- Confirm New Zealand registration status for erythromycin ophthalmic formulations through a direct Medsafe query (the 0.5% ointment may be registered under a brand not captured in the current dataset)
- If ophthalmic registration is confirmed, assess whether PEK falls within the scope of existing label indications or requires a formal new indication filing
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


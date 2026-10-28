---
layout: default
title: Mesalazine
parent: Model Prediction Only (L5)
nav_order: 287
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-15
monitor_inn: "MESALAZINE"
predicted_indications: ["congenital hypotrichosis with juvenile macular dystrophy", "osteoarthritis", "rheumatoid arthritis", "seborrheic keratosis", "vulvar inverted follicular keratosis", "pseudoachondroplasia"]
description: "Mesalazine: predicted new indication Congenital hypotrichosis with juvenile macular dystrophy (evidence level L5). Approved use: Not recorded in the local data"
---

# Mesalazine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Mesalazine: Congenital hypotrichosis with juvenile macular dystrophy
{: .text-alpha }

## One-Sentence Summary

Mesalazine (5-aminosalicylic acid, 5-ASA) was originally used to treat ulcerative colitis and other inflammatory bowel conditions.
The TxGNN model predicts it may be effective for **congenital hypotrichosis with juvenile macular dystrophy**, but **no clinical trials and no publications** currently support this prediction. It is a model output only, with no identifiable mechanistic rationale.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Congenital hypotrichosis with juvenile macular dystrophy |
| TxGNN Prediction Score | 0.9965 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 13 of 13) |
| Number of Authorizations | 13 (valid: 13) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the compiled data. Based on known pharmacology, mesalazine is the active moiety of sulfasalazine and acts as a local anti-inflammatory agent in the gut. Its proposed actions include PPAR-gamma agonism, COX/lipoxygenase inhibition and NF-kB modulation. Its efficacy in ulcerative colitis is well established.

The predicted indication is a rare genetic disorder caused by CDH3 mutation, affecting hair follicle and retinal development. It has no plausible connection to mesalazine's anti-inflammatory action. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

Other predicted indications were also reviewed:

- **Rheumatoid arthritis** (score 0.9957, Evidence Level L4): the only indirect link is through sulfasalazine, an established DMARD. The antirheumatic effect is largely attributed to the sulfapyridine moiety, not 5-ASA, so efficacy cannot be extrapolated to mesalazine alone.
- **Osteoarthritis** (score 0.9963, L5): the link is plausible but weak, since low-grade inflammation is involved but mesalazine has poor systemic absorption.
- **Seborrheic keratosis, osteoarthritis susceptibility, vulvar inverted follicular keratosis and pseudoachondroplasia**: no supporting mechanism, trials or literature.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available for the top-ranked prediction.

For reference, the literature retrieved for the lower-ranked predictions is summarised below. None of it demonstrates clinical benefit of mesalazine in the predicted indications.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38491514](https://pubmed.ncbi.nlm.nih.gov/38491514/) | 2024 | Computational analysis | J Transl Med | Transcriptomic target identification in osteoarthritis; hypothesis-generating only (osteoarthritis) |
| [7588084](https://pubmed.ncbi.nlm.nih.gov/7588084/) | 1995 | Review | Drugs | Sulfasalazine as a DMARD in rheumatoid arthritis; unclear whether the sulfapyridine or the mesalazine moiety is active (rheumatoid arthritis) |
| [7904547](https://pubmed.ncbi.nlm.nih.gov/7904547/) | 1993 | Review | Clin Pharmacokinet | Pharmacokinetics of slow-acting antirheumatic drugs, including sulfasalazine (rheumatoid arthritis) |
| [7781502](https://pubmed.ncbi.nlm.nih.gov/7781502/) | 1995 | Review | Dtsch Med Wochenschr | General review of mesalazine (rheumatoid arthritis) |
| [12235076](https://pubmed.ncbi.nlm.nih.gov/12235076/) | 2002 | Pharmacovigilance | Gut | Serious adverse reactions to sulphasalazine and mesalazine reported to the Committee on Safety of Medicines (rheumatoid arthritis) |
| [41443863](https://pubmed.ncbi.nlm.nih.gov/41443863/) | 2026 | Case report | Intern Med | 5-ASA-induced colitis in a rheumatoid arthritis patient without IBD (rheumatoid arthritis) |
| [31830393](https://pubmed.ncbi.nlm.nih.gov/31830393/) | 2019 | Case report | G Ital Nefrol | DRESS syndrome with acute renal failure after sulfasalazine and mesalazine in a rheumatoid arthritis patient |
| [36855206](https://pubmed.ncbi.nlm.nih.gov/36855206/) | 2023 | Case report | J Med Case Rep | Tofacitinib in IBD-associated arthritis; not about mesalazine (rheumatoid arthritis) |

---

## New Zealand Market Information

All 13 licences are valid. No approved indication text is available. Five representative entries:

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2470616 | Asacol | Tab 800 mg | Valid; indication text not available |
| 2111985 | Pentasa | Suppos 1 g | Valid; indication text not available |
| 2601354 | Pentasa | Modified release granules, 1 g | Valid; indication text not available |
| 2714523 | Octasa | Tab 400 mg | Valid; indication text not available |
| 715867 | Pentasa | Enema 1 g per 100 ml | Valid; indication text not available |

---

## Safety Considerations

- **Drug Interactions**: 100 interactions were identified. Major-level interactions include **cidofovir** and **human botulinum neurotoxin A/B immune globulin**. Moderate-level interactions include NSAIDs (ketorolac, ibuprofen, celecoxib, bromfenac), nephrotoxic agents (acyclovir, amphotericin B and its formulations, amikacin, bacitracin, capreomycin, cisplatin), azathioprine, and sulfonylureas (acetohexamide, chlorpropamide).
- **Disease-level contraindication signals (from the knowledge graph, not label text)**: kidney disease, interstitial nephritis, lipoid nephrosis, pyoureter, liver disease, gallbladder disease, neutropenia, agranulocytosis, anaemia (including macrocytic anaemia), and pyloric stenosis.
- **Reported adverse reactions in the retrieved literature**: DRESS syndrome with acute renal failure and 5-ASA-induced colitis.

Package insert warnings and contraindications were not available in the compiled data. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication has no clinical trials, no literature and no plausible mechanistic link to mesalazine, so the score of 0.9965 appears to be a model artifact. No other predicted indication has clinical evidence either (rheumatoid arthritis reaches only L4 and is confounded by sulfasalazine).

**To proceed, the following is needed:**
- Detailed mechanism of action data for mesalazine, plus any biological evidence connecting CDH3-related pathways to its pharmacology
- Preclinical or mechanistic studies for the predicted indication
- Medsafe package insert warnings and contraindications
- Clarification of the regulatory status given that all 13 licences are valid
- If a repurposing direction is pursued, consideration of better-supported candidates, such as rheumatoid arthritis, which would first require evidence specific to mesalazine rather than sulfasalazine
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


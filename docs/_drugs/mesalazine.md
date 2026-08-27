---
layout: default
title: Mesalazine
parent: 僅模型預測 (L5)
nav_order: 218
evidence_level: L5
indication_count: 7
---

# Mesalazine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **7** 個
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

# Mesalazine: From Ulcerative Colitis to Osteoarthritis

## One-Sentence Summary

> Mesalazine (5-aminosalicylic acid, 5-ASA) is an anti-inflammatory aminosalicylate historically used for ulcerative colitis and other inflammatory bowel conditions.
> The TxGNN model's highest-ranked prediction (congenital hypotrichosis with juvenile macular dystrophy) has **zero supporting evidence** and is judged biologically implausible, so this report focuses instead on **Osteoarthritis**, the top TxGNN-ranked candidate with an actual evidentiary trail —
> currently supported by **0 clinical trials** and **3 publications**, including one direct 2024 mechanistic study on 5-ASA itself.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Ulcerative colitis (inferred from literature evidence in this pack; no New Zealand regulatory record exists, see below) |
| Predicted New Indication | Osteoarthritis |
| TxGNN Prediction Score | 99.63% |
| Evidence Level | L4 (preclinical/mechanistic) |
| New Zealand Market Status | ✗ Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

*Note: the TxGNN model's absolute top-ranked prediction for this drug, "congenital hypotrichosis with juvenile macular dystrophy" (score 99.65%), was excluded from this report — it has 0 clinical trials, 0 literature, and no known mechanistic link to mesalazine's anti-inflammatory action; it is most likely an artifact of the embedding space. A third candidate, rheumatoid arthritis (L3, 6 trials, 20 publications), was also considered but the trial/literature evidence there predominantly concerns **sulfasalazine** (a pro-drug that splits into sulfapyridine + mesalazine), and head-to-head studies (PMID 2860942, PMID 2877851) indicate sulfapyridine — not 5-ASA — is the moiety responsible for anti-rheumatic activity. That signal is therefore judged confounded rather than a genuine mesalazine effect.*

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data for mesalazine is not available in this evidence pack (a High-severity data gap, DG002). Based on the literature evidence collected, mesalazine (5-ASA) is a topically/luminally acting anti-inflammatory agent whose efficacy in ulcerative colitis is established through suppression of intestinal prostaglandin and leukotriene synthesis (COX/lipoxygenase pathway inhibition).

Osteoarthritis (OA) is increasingly understood as an inflammation-driven degenerative joint disease rather than purely mechanical wear, involving cartilage-degrading cytokine and prostaglandin/leukotriene signaling similar in kind to the inflammatory cascade mesalazine suppresses in the gut. This shared inflammatory biology is the plausible basis for the TxGNN association.

Importantly, unlike the rheumatoid arthritis candidate, the strongest piece of evidence here (PMID 38310093, *Nature Communications*, 2024) studies 5-ASA directly — not sulfasalazine — and reports that it suppresses osteoarthritis progression through the OSCAR–PPARγ axis, a cartilage-protective pathway. This makes the OA signal mechanistically more direct and less confounded than the RA signal, even though it remains at the preclinical/mechanistic stage with no clinical trial data yet.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38310093](https://pubmed.ncbi.nlm.nih.gov/38310093/) | 2024 | Preclinical mechanistic study | Nature communications | 5-ASA suppresses osteoarthritis progression via the OSCAR–PPARγ axis, a cartilage-protective signaling pathway; identified as a candidate disease-modifying OA drug |
| [38491514](https://pubmed.ncbi.nlm.nih.gov/38491514/) | 2024 | Bioinformatics/target identification | Journal of translational medicine | Integrative analysis of OA transcriptional datasets and drug-target interaction data to identify novel therapeutic targets/candidates |
| [1673814](https://pubmed.ncbi.nlm.nih.gov/1673814/) | 1991 | In vitro pharmacology | Wiener klinische Wochenschrift | Sulfasalazine and its metabolite 5-ASA modulate prostaglandin/leukotriene release from human synovial tissue in osteoarthritis, chondrocalcinosis, and rheumatoid arthritis samples |

---

## New Zealand Market Information

Mesalazine currently holds **no product authorizations** in New Zealand (market status: 未上市 / Not Marketed, 0 licenses on record). No dosage form or approved-indication data is available for this market.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: TFDA package insert warnings/contraindications are flagged as a Blocking data gap (DG001) in this evidence pack — this must be resolved before any S1 safety review can proceed.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The osteoarthritis signal rests on one strong but purely preclinical mechanistic study (5-ASA-specific, OSCAR–PPARγ axis) with no clinical trial validation, and mesalazine is not currently marketed in New Zealand. Combined with a Blocking gap in TFDA safety/labeling data, the evidence is not yet sufficient to advance past a research hypothesis.

**To proceed, the following is needed:**
- TFDA package insert / warnings and contraindications (Blocking gap, DG001)
- Mechanism of action data from DrugBank (DG002)
- A clinical trial (Phase 2 or later) specifically evaluating mesalazine in osteoarthritis
- Confirmation of original approved indication(s) and NZ regulatory pathway, given the drug is currently unmarketed there
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


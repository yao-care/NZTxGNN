---
layout: default
title: Pimecrolimus
parent: 僅模型預測 (L5)
nav_order: 277
evidence_level: L5
indication_count: 4
---

# Pimecrolimus
{: .fs-9 }

證據等級: **L5** | 預測適應症: **4** 個
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

# Pimecrolimus: From Atopic Dermatitis to Seborrheic Dermatitis

## One-Sentence Summary

Pimecrolimus is a topical calcineurin inhibitor originally developed for mild-to-moderate atopic dermatitis (marketed globally as Elidel/Douglan). The TxGNN model predicts it may also be effective for **Seborrheic Dermatitis**, with **1 clinical trial** and **18 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Atopic dermatitis, mild-to-moderate (per Elidel/Douglan global labeling referenced in the evidence rationale; not independently confirmed via local regulatory filing) |
| Predicted New Indication | Seborrheic Dermatitis |
| TxGNN Prediction Score | 99.73% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed original mechanism-of-action data (`original_moa`) is not available in the regulatory record for this drug. However, the evidence pack's own literature-derived rationale identifies pimecrolimus as a topical **calcineurin inhibitor**: it selectively inhibits T-cell activation and the release of inflammatory cytokines (IL-2, IL-4, IFN-γ, TNF-α), and also inhibits mast cell degranulation — a mechanism well established for its approved use in atopic dermatitis.

Seborrheic dermatitis and atopic dermatitis are both chronic inflammatory skin conditions. Seborrheic dermatitis pathology involves a localized inflammatory response triggered by *Malassezia* yeast, which shares a T-cell/cytokine-driven inflammatory pathway with atopic dermatitis. This overlap provides a plausible mechanistic rationale for extrapolating pimecrolimus's anti-inflammatory effect to seborrheic dermatitis.

An important caveat: pimecrolimus has no intrinsic antifungal activity. Because *Malassezia* overgrowth is a contributing factor in seborrheic dermatitis, use as monotherapy carries a theoretical risk of masking an underlying fungal component, and combination with or exclusion of concurrent antifungal treatment should be considered.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00403559](https://clinicaltrials.gov/study/NCT00403559) | Phase 2 | Completed | 113 | Randomized, double-blind, parallel-group, active-comparator-controlled 4-week study evaluating Elidel (pimecrolimus) for the treatment of seborrheic dermatitis. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34910320](https://pubmed.ncbi.nlm.nih.gov/34910320/) | 2022 | RCT (comparative) | Clinical and Experimental Dermatology | Randomized blinded trial comparing pimecrolimus 1% cream vs. sertaconazole 2% cream for facial seborrheic dermatitis. |
| [22142161](https://pubmed.ncbi.nlm.nih.gov/22142161/) | 2012 | Systematic Review of RCTs | Expert Review of Clinical Pharmacology | Pimecrolimus 1% cream found to be well tolerated and effective vs. corticosteroids, antimycotics, or placebo. |
| [36072203](https://pubmed.ncbi.nlm.nih.gov/36072203/) | 2022 | Systematic Review | Cureus | Reviews RCT evidence for pimecrolimus (a calcineurin inhibitor) among agents used for facial seborrheic dermatitis. |
| [27804089](https://pubmed.ncbi.nlm.nih.gov/27804089/) | 2017 | Systematic Review | American Journal of Clinical Dermatology | Reviews topical treatment options, including calcineurin inhibitors, for facial seborrheic dermatitis. |
| [18677657](https://pubmed.ncbi.nlm.nih.gov/18677657/) | 2009 | Open, randomized, prospective, comparative study | Journal of Dermatological Treatment | Compares topical pimecrolimus 1% cream with ketoconazole 2% cream for seborrheic dermatitis. |
| [23715821](https://pubmed.ncbi.nlm.nih.gov/23715821/) | 2013 | Comparative study | Irish Journal of Medical Science | Compares sertaconazole 2% cream vs. pimecrolimus 1% cream for seborrheic dermatitis. |
| [28589618](https://pubmed.ncbi.nlm.nih.gov/28589618/) | 2018 | Comparative study | Journal of Cosmetic Dermatology | Compares different treatment-period regimens of pimecrolimus 1% cream for facial seborrheic dermatitis. |
| [20000875](https://pubmed.ncbi.nlm.nih.gov/20000875/) | 2010 | Open-label study | American Journal of Clinical Dermatology | Pimecrolimus 1% cream shown effective for resistant facial seborrheic dermatitis. |
| [15700745](https://pubmed.ncbi.nlm.nih.gov/15700745/) | 2004 | Clinical study | Drugs Under Experimental and Clinical Research | Pimecrolimus cream 1% assessed for efficacy, tolerability, and safety in seborrheic dermatitis of the face and trunk. |
| [19255921](https://pubmed.ncbi.nlm.nih.gov/19255921/) | 2009 | Observational follow-up | Journal of Dermatological Treatment | Close follow-up study reporting mean cure/remission times and side-effect profile of pimecrolimus in seborrheic dermatitis. |

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Key warnings, contraindications, and drug interaction data are currently unavailable — flagged as Blocking data gap DG001, pending TFDA/Medsafe package insert retrieval.)*

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
One completed Phase 2 RCT plus multiple systematic reviews and comparative studies consistently support pimecrolimus's efficacy in seborrheic dermatitis, and its mechanism (calcineurin inhibition) plausibly extends from its approved atopic dermatitis indication. However, evidence rests on a single Phase 2 trial without Phase 3 confirmation, and the drug is not currently marketed in New Zealand.

**To proceed, the following is needed:**
- Official package insert / warnings and contraindications (DG001, Blocking — required before any S1 safety assessment)
- Confirmed drug mechanism-of-action documentation (DG002, High priority)
- A Phase 3 confirmatory trial in seborrheic dermatitis, ideally with an antifungal-treatment control arm to address the non-antifungal mechanism caveat
- New Zealand/Medsafe market entry and registration status assessment, since the drug currently has no local authorization
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


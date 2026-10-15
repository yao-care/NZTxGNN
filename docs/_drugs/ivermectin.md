---
layout: default
title: Ivermectin
parent: Model Prediction Only (L5)
nav_order: 249
evidence_level: L5
indication_count: 9
---

# Ivermectin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

# Ivermectin: From Strongyloidiasis and Onchocerciasis to Vulvovaginal Candidiasis

## One-Sentence Summary

Ivermectin is an antiparasitic originally used to treat strongyloidiasis and onchocerciasis.
The TxGNN model predicts it may be effective for **vulvovaginal candidiasis**,
but there are currently **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Strongyloidiasis, onchocerciasis (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Vulvovaginal candidiasis |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, ivermectin is an antiparasitic whose efficacy in strongyloidiasis and onchocerciasis is established. It is generally understood to act on glutamate-gated chloride channels in invertebrates.

Candida is a fungus and lacks these targets, so there is no clear mechanistic reason for ivermectin to work against vulvovaginal candidiasis. The very high score (99.95%) therefore appears to be a graph-based association rather than a prediction backed by biology.

The other top predictions are mostly other candidiasis or vaginitis conditions (esophageal, neonatal, congenital and invasive candidiasis, vulvovaginitis, atrophic vaginitis), plus anogenital HPV infection. None of them has clinical trial evidence, and the few linked papers are false-positive matches. For example, one case report describes disseminated strongyloidiasis after corticosteroid use, and another describes crusted scabies treated with ivermectin in immunocompromised children. Both concern ivermectin's original antiparasitic uses, not candidiasis.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature directly supports the top predicted indication. The only publications linked to lower-ranked predictions are below, and neither supports a candidiasis use.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35835488](https://pubmed.ncbi.nlm.nih.gov/35835488/) | 2022 | Case report | BMJ Case Reports | Disseminated strongyloidiasis after prolonged corticosteroid treatment; matched to esophageal candidiasis only by association |
| [10098288](https://pubmed.ncbi.nlm.nih.gov/10098288/) | 1999 | Case report | Australasian Journal of Dermatology | Oral ivermectin cleared crusted scabies in two immunocompromised children, one with chronic mucocutaneous candidiasis; this is an antiparasitic use, not candidiasis treatment |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2087057 | Stromectol (cancelled) | Tab 3 mg | Not available |
| 2726424 | Stromectol (cancelled) | Tab 3 mg | Not available |

## Safety Considerations

- **Contraindications (knowledge-graph associations, not label text)**: Loiasis and filariasis.
- **Drug Interactions**: 100 interactions were identified in total. The main ones rated Moderate are:
  - Aprepitant
  - Clarithromycin
  - Ethanol
  - Cobicistat
  - Deferasirox
  - Warfarin
  - Dicoumarol
  - Fostamatinib
  - Radioiodine (I-123, I-131)
  - Topical abametapir

  Other interactions, including omeprazole, lansoprazole, corticosteroids (prednisone, prednisolone, dexamethasone), H2 blockers and heparin, have no severity rating.
- Use in neonates and very young or low-weight children is not established and would need separate review.

Please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the TxGNN score. There are no clinical trials or relevant literature, no plausible antifungal mechanism, and no currently valid New Zealand licence (both are cancelled). The few linked papers are false-positive matches.

**To proceed, the following is needed:**
- Mechanism of action data and any evidence of antifungal activity against Candida (for example, in vitro studies)
- Package insert warnings and contraindications from Medsafe, which are required for safety screening
- Preclinical or clinical studies in the proposed indication
- Assessment of route of administration and formulation suitability, since route compatibility has not been evaluated
- A review of the licensing situation, since both New Zealand authorizations are cancelled
- A comparison with existing effective antifungal therapies
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Minocycline
parent: Model Prediction Only (L5)
nav_order: 302
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Minocycline: predicted new indication Punctate epithelial keratoconjunctivitis (evidence level L5). Approved use: Urinary tract infection, bronchitis, acne, gonorrhoea, pharyngitis…"
---

# Minocycline
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

# Minocycline: From Bacterial Infections and Acne to Punctate Epithelial Keratoconjunctivitis

## One-Sentence Summary

Minocycline is a tetracycline-class antibiotic, originally used for infections such as urinary tract infection, bronchitis, gonorrhoea and acne.
The TxGNN model predicts it may be effective for **punctate epithelial keratoconjunctivitis**,
but there are currently **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Urinary tract infection, bronchitis, acne, gonorrhoea, pharyngitis, periodontitis and other bacterial infections (from the knowledge graph; the approved indication text in the New Zealand licences is empty) |
| Predicted New Indication | Punctate epithelial keratoconjunctivitis |
| TxGNN Prediction Score | 99.63% |
| Evidence Level | L5 |
| New Zealand Market Status | ✓ Marketed |
| Number of Authorizations | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, minocycline is part of the tetracycline class of antibiotics, its efficacy in bacterial infections and acne is established, and mechanistically it may be applicable to inflammatory ocular surface conditions.

A plausible but unverified rationale is that minocycline has anti-inflammatory and matrix metalloproteinase (MMP) inhibitory activity. Related tetracyclines such as doxycycline are used for ocular surface inflammation, for example meibomian gland dysfunction and blepharitis.

Whether this extends to punctate epithelial keratoconjunctivitis is not supported by the data compiled for this report. The condition may be viral (for example adenoviral) or inflammatory in origin, and an antibiotic would not be expected to help the viral form. The mechanistic link also cannot be checked against the drug's documented pharmacology, because the original mechanism of action is missing.

A second prediction, **exposure keratitis** (score 99.20%), is also at the model-prediction-only stage. Its management centres on protecting the ocular surface (lubrication, lid closure, treating the underlying cause), and no data show that a tetracycline adds benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 690805 | Minomycin | Cap 100 mg | Not available |
| 472212 | Mino-tabs | Tab 50 mg | Not available |

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (not taken from package insert text): kidney disease, liver disease, gallbladder disease, *Clostridium difficile* colitis, perinatal necrotizing enterocolitis, pseudotumor cerebri, intracranial hypertension, pyoureter and dyskinesia of the oesophagus.
- **Drug Interactions**: 100 interactions were identified in total; the first 20 returned include:
  - **Major**: isotretinoin and acitretin. Combining tetracyclines with retinoids is associated with a risk of raised intracranial pressure, which is consistent with the pseudotumor cerebri contraindication above.
  - **Moderate**: ethinylestradiol, amoxicillin, ampicillin, sodium citrate, calcium acetate, calcium phosphate, bismuth subcitrate potassium, balsalazide, aminolevulinic acid (topical), cannabidiol, brentuximab vedotin, asparaginase (*E. coli* and *Erwinia chrysanthemi*) and calaspargase pegol.
  - **Minor**: acetazolamide, hydrochlorothiazide, amiloride and bumetanide.

Please refer to the package insert for the full warnings and contraindications, which were not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has a high TxGNN score (99.63%) but no supporting clinical trials or publications (Evidence Level L5). The mechanism of action data and package insert safety information are both missing, and the cause of the condition (viral versus inflammatory) may determine whether an antibiotic is relevant at all.

**To proceed, the following is needed:**
- Mechanism of action data for minocycline (for example from DrugBank)
- Medsafe package insert warnings and contraindications, to allow safety screening
- A literature and trial search on tetracyclines (minocycline and doxycycline) in punctate epithelial keratoconjunctivitis and ocular surface inflammation
- Clarification of the intended route (systemic or topical ocular), since route compatibility has not been assessed and the New Zealand products are oral forms only
- Review of the major retinoid interactions and the intracranial hypertension contraindication for any proposed use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Erythromycin
parent: Model Prediction Only (L5)
nav_order: 183
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-30
description: "Erythromycin: predicted new indication Punctate epithelial keratoconjunctivitis (evidence level L4). Approved use: Bacterial infections, including acne, otitis media…"
monitor_inn: "ERYTHROMYCIN"
predicted_indications: ["punctate epithelial keratoconjunctivitis", "acute contagious conjunctivitis", "exposure keratitis", "lymphogranuloma venereum", "necrotizing ulcerative gingivitis"]
---

# Erythromycin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

## Erythromycin: From Bacterial Infections (Including Conjunctivitis) to Punctate Epithelial Keratoconjunctivitis
{: .text-alpha }

## One-Sentence Summary

Erythromycin is a macrolide antibiotic originally used for a range of bacterial infections, including pneumonia, pertussis, gonorrhoea, acne and conjunctivitis.
The TxGNN model predicts it may be effective for **punctate epithelial keratoconjunctivitis**,
but there are **no registered clinical trials** and only **1 publication** (a clinical management review) for this indication, so evidence is limited to model prediction and indirect literature.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Bacterial infections, including acne, otitis media, streptococcal and staphylococcal infections, pneumonia, gonorrhoea, pertussis, urethritis and conjunctivitis (the New Zealand licence record has no indication text) |
| Predicted New Indication | Punctate epithelial keratoconjunctivitis |
| TxGNN Prediction Score | 0.9989 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Erythromycin is a macrolide antibiotic (a class that inhibits bacterial protein synthesis) with activity against Gram-positive organisms such as *Staphylococcus*. It is also reported to have anti-inflammatory and meibomian gland-modulating effects.

Punctate epithelial keratoconjunctivitis is closely related to blepharokeratoconjunctivitis, where staphylococcal blepharitis can drive punctate corneal epithelial disease. Erythromycin's antibacterial and anti-inflammatory properties could plausibly address both components. Erythromycin is also already used for bacterial conjunctivitis, which supports ocular applicability.

The link is nonetheless indirect. The single supporting publication is a general clinical management review in children, not a study of erythromycin in this condition. The very high TxGNN score is a computational prediction only.

Other predicted indications in the data (lower ranked) include acute contagious conjunctivitis (largely overlapping with the existing label), exposure keratitis, lymphogranuloma venereum (biologically strong, as a chlamydial infection, but doxycycline is first-line) and necrotizing ulcerative gingivitis (dated evidence, not a preferred option today).

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [11495307](https://pubmed.ncbi.nlm.nih.gov/11495307/) | 2001 | Review | Journal of Pediatric Ophthalmology and Strabismus | Describes history, symptoms, clinical signs and treatment of chronic blepharokeratoconjunctivitis in children; not specific to erythromycin |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2270234 (cancelled) | Erythrocin IV | Inj 1 g vial | Not recorded |

## Safety Considerations

- **Contraindications / disease associations (from a knowledge graph, not label text)**: long QT syndrome (including familial), torsades de pointes, porphyria, kidney disease, liver disease, gallbladder disease, haematologic disease, *Clostridium difficile* colitis, myasthenia gravis and other neuromuscular junction disease, hearing disorders, exfoliative dermatitis and other dermatitis, folic acid deficiency anaemia, perinatal necrotizing enterocolitis and pyoureter.
- **Drug Interactions**: 100 interactions were identified. Examples among those shown:
  - **Major**: loperamide, clarithromycin
  - **Moderate**: corticosteroids (hydrocortisone, dexamethasone, betamethasone, budesonide, triamcinolone), famotidine, cimetidine, pioglitazone, chlorpropamide, aprepitant, alosetron, balsalazide, bisacodyl, picosulfuric acid, polyethylene glycol with electrolytes
  - **Minor**: amoxicillin

Please refer to the package insert for full warnings and contraindications, as label-based data are not available.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is very high, but there are no registered trials and only one indirect review for this indication. The product's New Zealand licence is cancelled and no label-based safety information is available.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data (e.g. from DrugBank)
- Direct clinical evidence of erythromycin (likely topical ophthalmic) in punctate epithelial keratoconjunctivitis
- Assessment of route compatibility, since the only New Zealand product was an intravenous injection, not an ophthalmic formulation
- Review of QT-prolongation risk and the major drug interactions
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


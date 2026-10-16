---
layout: default
title: Amoxicillin
parent: Model Prediction Only (L5)
nav_order: 37
evidence_level: L5
indication_count: 8
last_modified_at: 2026-10-15
description: "Amoxicillin: predicted new indication Polyclonal hyperviscosity syndrome (evidence level L5). Approved use: Bacterial infections (urinary tract infection…"
---

# Amoxicillin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
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

# Amoxicillin: From Bacterial Infections to Polyclonal Hyperviscosity Syndrome

## One-Sentence Summary

Amoxicillin is a beta-lactam antibiotic originally used for bacterial infections such as urinary tract infection, otitis media, pneumonia and pharyngitis.
The TxGNN model predicts it may be effective for **polyclonal hyperviscosity syndrome**, but **no clinical trials and no publications** currently support this prediction.
This is a model-only prediction with no mechanistic or clinical backing.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Bacterial infections (urinary tract infection, infectious otitis media, streptococcal and staphylococcal infections, bacterial pneumonia, pharyngitis, among others). No approved indication text is available from the New Zealand licence records. |
| Predicted New Indication | Polyclonal hyperviscosity syndrome |
| TxGNN Prediction Score | 99.63% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 8 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. From general pharmacology, amoxicillin is a beta-lactam antibacterial that inhibits bacterial cell wall synthesis, and its efficacy in bacterial infections is well established.

Polyclonal hyperviscosity syndrome is a protein/immunoglobulin-driven rheological disorder. No plausible mechanistic link to an antibacterial was identified. The very high TxGNN score (99.63%) most likely reflects network proximity in the knowledge graph rather than a pharmacological rationale.

For completeness, the other top-ranked predictions (hyperamylasemia, congenital analbuminemia, blood group incompatibility, premalignant haematological disease, monoclonal gammopathy, and haematological disease with acquired peripheral neuropathy) also lack a mechanistic link. The one biologically plausible candidate is **septicemic plague** (rank 8). It is plausible because amoxicillin has in vitro and animal activity against *Yersinia pestis*, but beta-lactams are not standard plague therapy and no human clinical trials exist.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for polyclonal hyperviscosity syndrome.

---

## Literature Evidence

Currently no related literature available for polyclonal hyperviscosity syndrome.

---

## New Zealand Market Information

All eight licences are cancelled; none is currently valid. Approved indication text was not available for any of them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2669617 | Miro-Amoxicillin | Cap 250 mg | Cancelled |
| 2669625 | Miro-Amoxicillin | Cap 500 mg | Cancelled |
| 2272628 | Ibiamox | Inj 250 mg vial | Cancelled |
| 2503565 | Alphamox 250 | Granules for oral liquid 250 mg per 5 ml | Cancelled |
| 2712601 | Amoxycillin Sandoz | Cap 500 mg | Cancelled |

---

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for safety information. The following items are drawn from the knowledge graph and drug-interaction databases.

- **Knowledge-graph contraindication signals (not label text)**: liver disease and liver failure, cholestasis, kidney disease, *Clostridium difficile* colitis, myasthenia gravis, porphyria, long QT syndrome / torsades de pointes / ventricular tachycardia, osteoporosis, bone fracture, infectious mononucleosis, and gallbladder disease. These need verification against the official label.
- **Drug interactions** (100 recorded; main examples):
  - **Major**: methotrexate; live cholera vaccine (*Vibrio cholerae* CVD 103-HgR).
  - **Moderate**: tetracyclines (doxycycline, minocycline, demeclocycline), oestrogens (ethinylestradiol, estradiol), coumarin-type anticoagulants (dicoumarol, anisindione), mycophenolate mofetil, entecavir, balsalazide, and probiotics (*Lactobacillus acidophilus*, *Bifidobacterium longum infantis*).
  - **Minor**: macrolides (azithromycin, clarithromycin, erythromycin), allopurinol, amiloride.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for polyclonal hyperviscosity syndrome rests on model score alone (L5), with no trials, no literature, and no plausible mechanism. In addition, amoxicillin currently has no valid New Zealand licence.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data
- Any mechanistic or clinical rationale linking amoxicillin to polyclonal hyperviscosity syndrome
- If an infection-related direction is wanted, re-evaluate septicemic plague with head-to-head comparison against standard therapy
- Clarification of the regulatory pathway, since all New Zealand licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


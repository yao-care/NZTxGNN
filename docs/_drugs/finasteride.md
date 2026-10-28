---
layout: default
title: Finasteride
parent: Model Prediction Only (L5)
nav_order: 201
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-28
monitor_inn: "FINASTERIDE"
predicted_indications: ["Ambras type hypertrichosis universalis congenita", "hypertrichosis (disease)", "malformation syndrome with odontal and/or periodontal component", "syndrome with a Dandy-Walker malformation as major feature", "isolated genetic hair shaft abnormality", "familial isolated trichomegaly"]
description: "Finasteride: predicted new indication Ambras type hypertrichosis universalis congenita (evidence level L5). Approved use: Not recorded in the local data"
---

# Finasteride
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

## Finasteride: Ambras type hypertrichosis universalis congenita
{: .text-alpha }

## One-Sentence Summary

Finasteride was originally used to treat benign prostatic hyperplasia (BPH).
The TxGNN model predicts it may be effective for **Ambras type hypertrichosis universalis congenita**,
but **no clinical trials and no publications** currently support this prediction, so it rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Ambras type hypertrichosis universalis congenita |
| TxGNN Prediction Score | 0.9999 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, finasteride is a 5-alpha-reductase type II inhibitor that lowers dihydrotestosterone (DHT). This is the basis for its efficacy in BPH and its established use in androgenetic alopecia.

The link to Ambras syndrome is weak. Ambras syndrome is a congenital, genetically driven hypertrichosis (a chromosome 8q22 rearrangement affecting TRPS1 regulation), and its hair overgrowth is not known to depend on DHT. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence, so the mechanistic rationale is speculative.

Five other predictions were also generated, all with scores above 99%: hypertrichosis, a malformation syndrome with dental/periodontal involvement, a Dandy-Walker malformation syndrome, isolated genetic hair shaft abnormality, and familial isolated trichomegaly. None has clinical trial support. Only hypertrichosis has any retrieved literature, and that literature is indirect (see below). The mechanistic links for all of them are weak.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature was retrieved for Ambras syndrome itself. For the second-ranked prediction, **hypertrichosis**, four papers were retrieved. None shows finasteride efficacy in hypertrichosis:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12223963](https://pubmed.ncbi.nlm.nih.gov/12223963/) | 2002 | Review | Annales de Dermatologie et de Vénéréologie | Hypertrichosis is hair growth in non-androgen-dependent areas, unlike hirsutism. Causes in adults include drugs (minoxidil, ciclosporin, diazoxide, corticosteroids), porphyria, anorexia and paraneoplastic disease. |
| [10330884](https://pubmed.ncbi.nlm.nih.gov/10330884/) | 1999 | Review | Therapeutische Umschau | Reviews new treatments for androgenetic alopecia and hirsutism, including oral therapy for male pattern hair loss. |
| [12942187](https://pubmed.ncbi.nlm.nih.gov/12942187/) | 2003 | Review | Der Hautarzt | Finasteride is cited for male pattern hair loss, and laser hair removal for hypertrichosis and hirsutism. |
| [12444520](https://pubmed.ncbi.nlm.nih.gov/12444520/) | 2002 | Methods/Diagnostic tool | Der Hautarzt | Describes TrichoScan, a digital hair analysis tool for monitoring hair loss and treatment response. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2522756 | Ricit (status: valid) | Tab 5 mg | Not recorded |

## Safety Considerations

- **Contraindications (knowledge graph, not label text)**: liver disease and gallbladder disease.
- **Pregnancy**: finasteride is contraindicated in pregnancy because of the risk of abnormal male fetal genital development.
- **Drug Interactions**: 100 potential interactions were identified, but none has a severity level assigned. Examples include calcitriol, doxycycline, clotrimazole, dolasetron, famotidine, acetylsalicylic acid, bupropion, omeprazole, metformin and morphine.

Please refer to the package insert for further safety information. The insert's warnings and contraindications were not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score, with no clinical trials. The only retrieved literature is general, indirect hypertrichosis material. The proposed mechanism is also biologically weak, since the target conditions are not DHT-driven. In addition, the only New Zealand licence is cancelled.

**To proceed, the following is needed:**
- Mechanism of action data (for example from DrugBank) to assess the pathway link
- The Medsafe package insert, to complete warnings and contraindications and allow safety screening
- Preclinical or case-level evidence of a DHT-dependent mechanism in the target condition
- Route and formulation compatibility assessment for the new indication
- Review of the cancelled licence status and its implications for local availability
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Loperamide
parent: Model Prediction Only (L5)
nav_order: 276
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Loperamide: predicted new indication Acute contagious conjunctivitis (evidence level L5). Approved use: Diarrheal disease, dysentery"
---

# Loperamide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Loperamide: From Diarrheal Disease to Acute Contagious Conjunctivitis

## One-Sentence Summary

Loperamide is a peripheral opioid-receptor drug originally used to treat diarrheal disease and dysentery.
The TxGNN model predicts it may be effective for **acute contagious conjunctivitis**, but there are **0 clinical trials** and **0 publications** supporting this prediction.
The prediction rests on a knowledge-graph score alone and should be treated as a computational hypothesis, not as evidence of benefit.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Diarrheal disease, dysentery (from the knowledge graph; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Acute contagious conjunctivitis |
| TxGNN Prediction Score | 99.97% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Pharmacology data show that loperamide acts on the mu (μ) opioid receptor (OPRM1), and it is used to treat diarrhea. It acts mainly on the gut, where it slows motility.

The link to conjunctivitis is weak. Conjunctivitis is an infectious or inflammatory eye condition, and loperamide has no known anti-infective or anti-inflammatory action. A topical eye formulation of a gut-restricted opioid has no plausible rationale. The high score most likely reflects similarity patterns in the knowledge graph, not a real therapeutic effect.

The other top-ranked predictions are mostly other conjunctivitis subtypes (pseudomembranous, serous, parasitic, chronic follicular, conjunctival folliculosis, and Angelucci syndrome), and none has trial or literature support. The two predictions with some literature are:

- **Amebic dysentery (rank 2, score 99.95%, L4):** Loperamide relieves diarrhea symptoms but does not treat the amoebic infection. A case report describes fulminant amoebic colitis after heavy loperamide use, which is a safety concern, not efficacy evidence.
- **Gastroduodenitis (rank 4, score 99.77%, L4):** Loperamide has no known anti-inflammatory action. The only publication is a case report of loperamide-induced respiratory depression in severe gastrointestinal inflammation, which is also a safety signal.

A cationic amphiphilic drug profile has been linked to anti-SARS-CoV-2 activity in drug repurposing screens (a preprint). This is not relevant to conjunctivitis and does not support the prediction.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the top-ranked prediction (acute contagious conjunctivitis).

For context, the literature found for other predicted indications is listed below. These items are not evidence for the primary prediction.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3893119](https://pubmed.ncbi.nlm.nih.gov/3893119/) | 1985 | Review | Am J Med | Reviews non-fluid therapy and chemoprophylaxis of acute diarrhea; oral fluids are the cornerstone of treatment (amebic dysentery prediction) |
| [17241255](https://pubmed.ncbi.nlm.nih.gov/17241255/) | 2007 | Case report | J Travel Med | Fulminant amoebic colitis (toxic megacolon) in a patient with heavy loperamide use; anti-motility agents may worsen the course |
| [41924411](https://pubmed.ncbi.nlm.nih.gov/41924411/) | 2026 | Case report | Cureus | Loperamide-induced respiratory depression in severe GI inflammation after chemotherapy, with increased systemic absorption when the gut barrier is damaged |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2365545 (cancelled) | Diamide Relief | Cap 2 mg | Not available |
| 2184427 (cancelled) | Nodia | Tab 2 mg | Not available |

## Safety Considerations

- **Contraindications (from the knowledge graph, not label text):** liver disease, gallbladder disease, paralytic ileus, intestinal obstruction, familial visceral myopathy, dysentery, diarrheal disease, infectious diarrheal disease, intestinal infectious disease.
- **Safety signals from the literature:**
  - Anti-motility use has been associated with toxic megacolon in infectious colitis, including amoebic colitis.
  - Increased systemic absorption with a damaged gut barrier can cause central opioid effects, including respiratory depression.
- **Drug Interactions:** The only interaction record is the pharmacological target, the mu opioid receptor (OPRM1). No drug-drug interaction data are available.

Please refer to the package insert for full safety information, as label warnings and contraindications were not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction has no clinical trials, no literature, and no plausible mechanism. Loperamide has no New Zealand licence currently valid. The literature found for other predicted indications points to safety concerns, not benefit.

**To proceed, the following is needed:**
- Mechanism of action data, to assess any biological link to conjunctivitis
- Medsafe package insert warnings and contraindications, which are required for safety screening
- Any preclinical or clinical evidence for an ocular effect, which does not currently exist
- Assessment of route compatibility, since no ocular formulation is available
- Current New Zealand regulatory status, since both licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


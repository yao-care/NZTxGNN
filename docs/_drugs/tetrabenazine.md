---
layout: default
title: Tetrabenazine
parent: Model Prediction Only (L5)
nav_order: 446
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Tetrabenazine: predicted new indication Polycystic kidney disease 3 with or without polycystic liver disease (evidence level L5). Approved use: Huntington disease"
monitor_inn: "TETRABENAZINE"
predicted_indications: ["polycystic kidney disease 3 with or without polycystic liver disease", "renal-hepatic-pancreatic dysplasia", "Joubert syndrome with renal defect", "karyomegalic interstitial nephritis", "thoracic malformation", "polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis", "acute intermittent porphyria", "adult familial nephronophthisis-spastic quadriparesia syndrome", "atypical glycine encephalopathy", "congenital disorder of glycosylation with defective fucosylation"]
---

# Tetrabenazine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

## Tetrabenazine: From Huntington Disease to Polycystic Kidney Disease 3 (With or Without Polycystic Liver Disease)
{: .text-alpha }

## One-Sentence Summary

Tetrabenazine was originally used to treat Huntington disease and related movement disorders.
The TxGNN model predicts it may be effective for **polycystic kidney disease 3 with or without polycystic liver disease**,
but **no clinical trials and no relevant publications** currently support this prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Huntington disease (the New Zealand licence record has no approved indication text; the original indication comes from the TxGNN knowledge graph) |
| Predicted New Indication | Polycystic kidney disease 3 with or without polycystic liver disease |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 (0 currently valid, 1 cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Tetrabenazine is generally known as a reversible VMAT2 inhibitor that depletes presynaptic monoamines in the central nervous system. This is why it is used for hyperkinetic movement disorders such as Huntington disease.

Polycystic kidney disease 3 (with or without polycystic liver disease) is a cyst-forming disorder linked to hepatocystin/PRKCSH-related pathways and cyst epithelial proliferation. No plausible mechanistic link between VMAT2 inhibition and this disease has been identified. The high TxGNN score most likely reflects proximity in the knowledge graph rather than a biological rationale.

The other top-ranked predictions show the same pattern. They include renal-hepatic-pancreatic dysplasia, Joubert syndrome with renal defect, karyomegalic interstitial nephritis and acute intermittent porphyria. Most are ciliopathies, metabolic disorders or malformation syndromes with no known connection to monoamine transport. These predictions should be treated as hypotheses only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the top predicted indication.

(For reference, one publication was retrieved for the fifth-ranked prediction, thoracic malformation: [11293103](https://pubmed.ncbi.nlm.nih.gov/11293103/), 2001, case report, Revista de neurologia. It describes spasmodic torticollis coexisting with a vertebral hemangioma. It is only a tangential clinical association and does not show that tetrabenazine treats thoracic malformations.)

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2394383 | Motetis (cancelled) | Tab 25 mg | Not recorded |

## Cytotoxicity

Not applicable. Tetrabenazine is not an antineoplastic drug, so this section is omitted.

## Safety Considerations

- **Drug Interactions**: 100 interactions were identified in total. The Major-level ones among those listed include:
  - Bupropion
  - Dolasetron
  - Metoclopramide

  Many Moderate-level interactions were also listed, including famotidine, loperamide, morphine, clarithromycin, levofloxacin, palonosetron and several laxatives and bowel-preparation agents.
- **Knowledge-graph contraindication signals** (from the knowledge graph, not from label text):
  - Cardiac and blood pressure: long QT syndrome, torsades de pointes, orthostatic hypotension, hypotensive disorders, postural orthostatic tachycardia syndrome, multiple system atrophy
  - Neurological and movement: Parkinson disease and parkinsonian disorders, tardive dyskinesia, drug-induced akathisia
  - Psychiatric: anxiety disorder, neurotic disorder, dysthymic disorder
  - Hepatobiliary: liver disease, gallbladder disease
  - Endocrine: hyperprolactinemia and prolactin-producing pituitary tumours

Please refer to the package insert for official warnings and contraindications, as this information was not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score (L5). There are no clinical trials, no relevant literature and no plausible mechanistic link between VMAT2 inhibition and polycystic kidney disease. The only New Zealand licence is cancelled, and the safety profile is not fully characterized.

**To proceed, the following is needed:**
- Mechanism of action data confirmed from DrugBank
- The Medsafe package insert, to establish official warnings and contraindications (safety screening cannot proceed without it)
- Preclinical or mechanistic evidence linking VMAT2 inhibition to cyst formation or ciliopathy pathways
- Clarification of the cancelled New Zealand licence and available routes of administration
- A review of the cardiac (QT) and other contraindication signals against the intended patient population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


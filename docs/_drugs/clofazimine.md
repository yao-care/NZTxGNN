---
layout: default
title: Clofazimine
parent: Model Prediction Only (L5)
nav_order: 108
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-15
description: "Clofazimine: predicted new indication Pneumocystosis (evidence level L5). Approved use: Leprosy, multibacillary leprosy, pyoderma gangrenosum"
monitor_inn: "CLOFAZIMINE"
predicted_indications: ["pneumocystosis", "malaria", "gastrin secretion abnormality"]
---

# Clofazimine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

## Clofazimine: From Leprosy to Pneumocystosis
{: .text-alpha }

## One-Sentence Summary

Clofazimine is an antimycobacterial drug originally used to treat leprosy (including multibacillary leprosy) and pyoderma gangrenosum.
The TxGNN model predicts it may be effective for **pneumocystosis**, but there are **no registered clinical trials** and only **3 loosely related publications**, none of which show clofazimine acting against Pneumocystis.
This is a model-only prediction with very weak supporting evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Leprosy, multibacillary leprosy, pyoderma gangrenosum (from the knowledge graph; the New Zealand licence record has no indication text) |
| Predicted New Indication | Pneumocystosis |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, clofazimine is a riminophenazine antimycobacterial with immunomodulatory and membrane-disrupting properties. Its efficacy in leprosy is established, but nothing in the data compiled for this report shows activity against *Pneumocystis*.

The retrieved literature does not support the prediction. One randomized trial examined clofazimine as prophylaxis against *Mycobacterium avium* complex (MAC) in AIDS patients, and a case report describes an AIDS patient with *M. kansasii* and *P. carinii* pneumonia. In both, *Pneumocystis* appears as a co-occurring opportunistic infection, not as a target of clofazimine. The very high TxGNN score (0.999) reflects a graph-based association, not demonstrated pharmacological activity.

The two other top-ranked predictions are also weak. Malaria (score 99.60%) has only indirect, preclinical support (an in vitro study of clofazimine analogues, PMID 29026393), and gastrin secretion abnormality (score 99.57%) has no supporting evidence at all.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8501340](https://pubmed.ncbi.nlm.nih.gov/8501340/) | 1993 | Randomized open-label trial | J Infect Dis | Clofazimine as prophylaxis for disseminated MAC in 110 HIV patients, many with prior *P. carinii* pneumonia; the endpoint was MAC, not Pneumocystis |
| [11363899](https://pubmed.ncbi.nlm.nih.gov/11363899/) | 1996 | Review | PI Perspective | General update on opportunistic infections; no abstract available |
| [2714863](https://pubmed.ncbi.nlm.nih.gov/2714863/) | 1989 | Case report | Infection | AIDS patient with *M. kansasii* lung disease complicated by *P. carinii* pneumonia; recovered on a multi-drug regimen, with Pneumocystis treated by trimethoprim-sulfamethoxazole |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 654450 (cancelled) | Lamprene | Cap 50 mg | Not recorded |

---

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: liver disease, gallbladder disease, diarrheal disease, active peptic ulcer disease, digestive system disease, intestinal disease, splenic infarction, splenic disease, hypersplenism.
- **Drug Interactions**: 100 interactions were found in total. The first 20 returned are mostly Moderate. The Major ones are:
  - Cisapride
  - Dolasetron

  Moderate examples include:
  - Clarithromycin
  - Levofloxacin
  - Ondansetron
  - Granisetron
  - Palonosetron
  - Famotidine
  - Loperamide
  - Many laxatives (bisacodyl, lactulose, magnesium hydroxide and others)

  Metronidazole is Minor.

Package insert warnings and contraindications were not available; please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a graph-model score. No trials exist, and the few publications mention Pneumocystis only as a co-infection, not as a treatment target. The product is also no longer licensed in New Zealand.

**To proceed, the following is needed:**
- Mechanism of action data and any evidence of anti-*Pneumocystis* activity (in vitro or in animal models)
- The Medsafe package insert, to assess warnings and contraindications
- A review of the knowledge-graph contraindications (notably gastrointestinal and liver disease) against the likely target population, such as immunocompromised patients
- Clarification of New Zealand supply and licensing, since the only licence is cancelled
- If a direction is pursued, malaria may merit preclinical follow-up as a research question, given the in vitro analogue data
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Docusate
parent: Model Prediction Only (L5)
nav_order: 161
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Docusate: predicted new indication Plummer-Vinson syndrome (evidence level L5). Approved use: Constipation disorder"
monitor_inn: "Docusate"
predicted_indications: ["Plummer-Vinson syndrome", "vitamin B12- and folate-independent constitutional megaloblastic anemia"]
---

# Docusate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

# Docusate: From Constipation and Anemia-Related Labels to Plummer-Vinson Syndrome

## One-Sentence Summary

Docusate is a stool softener (surfactant laxative) whose knowledge-graph indications include constipation and several anemia types, the latter likely because it was included in combination iron/folic acid products.
The TxGNN model predicts it may be effective for **Plummer-Vinson syndrome**, but there are **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Constipation disorder; folic acid deficiency anemia, iron deficiency anemia, hypochromic anemia, deficiency anemia, microcytic anemia (from the knowledge graph; the New Zealand licence records contain no approved indication text) |
| Predicted New Indication | Plummer-Vinson syndrome |
| TxGNN Prediction Score | 99.18% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, docusate is an anionic surfactant stool softener, and its established use is in constipation. It has no known pharmacological action on iron absorption, esophageal web formation, or hematopoiesis.

The high score most likely reflects network proximity in the knowledge graph. Docusate is linked to iron deficiency anemia, hypochromic anemia and microcytic anemia, and Plummer-Vinson syndrome is characterized by iron deficiency anemia with dysphagia and esophageal webs. These anemia links are probably artifacts of combination iron/folic acid products that contained docusate to counter iron-induced constipation. They do not show that docusate itself treats anemia.

The prediction should therefore be seen as a graph artifact rather than a verified therapeutic signal.

The second-ranked prediction, vitamin B12- and folate-independent constitutional megaloblastic anemia (score 99.15%), has the same weakness. It is a rare constitutional condition with no plausible link to a stool softener, and no trials or literature support it.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 252832 | Coloxyl (cancelled) | Tab 50 mg | Not recorded |
| 252859 | Coloxyl (cancelled) | Tab 120 mg | Not recorded |

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (these are graph relationships, not package insert text): peptic ulcer disease, potassium deficiency disease, sickle cell anemia, hereditary persistence of fetal hemoglobin, primary hyperoxaluria, hereditary hemochromatosis, hemosiderosis, iron metabolism disease, Wilson disease, ulcerative colitis, inflammatory bowel disease, hemolytic anemia, appendicitis, chronic atrophic gastritis, Leber hereditary optic neuropathy, gastroenteritis.
- **Drug Interactions** (4 recorded):
  - Lactulose: Moderate
  - Mineral oil: Moderate
  - Phenolphthalein: Moderate
  - Linaclotide: Minor

  Most of these involve other laxatives, so combined use deserves caution.

Please refer to the package insert for other safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no clinical or literature support (Evidence Level L5). No plausible mechanism links a stool softener to Plummer-Vinson syndrome, and both New Zealand licences are cancelled.

**To proceed, the following is needed:**
- Mechanism of action data to assess any biological link
- Package insert warnings and contraindications for safety screening
- Any clinical or preclinical evidence for docusate in Plummer-Vinson syndrome
- Confirmation of whether the anemia-related indications come from combination products rather than docusate itself
- A marketed New Zealand product or formulation, since both licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


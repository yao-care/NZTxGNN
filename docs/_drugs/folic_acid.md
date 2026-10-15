---
layout: default
title: Folic Acid
parent: Model Prediction Only (L5)
nav_order: 213
evidence_level: L5
indication_count: 1
---

# Folic Acid
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

# Folic Acid: From Vitamin Deficiency and Folate-Deficiency Anaemia to Biotin Metabolic Disease

## One-Sentence Summary

Folic acid (vitamin B9) was originally used to treat vitamin deficiency disorders and anaemias, particularly folic acid deficiency anaemia.
The TxGNN model predicts it may be effective for **biotin metabolic disease**, but there are **no registered clinical trials** and only general, indirect literature (**20 publications**, mostly narrative reviews on vitamins) supporting this direction.
This is a model-only prediction without direct clinical support.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Vitamin deficiency disorder; folic acid deficiency anaemia; iron deficiency anaemia; hypochromic anaemia; microcytic anaemia (the New Zealand licence records contain no approved indication text) |
| Predicted New Indication | Biotin metabolic disease |
| TxGNN Prediction Score | 99.49% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, folic acid is a water-soluble B vitamin, its efficacy in folate-deficiency anaemia is well established, and it may be loosely connected to biotin metabolism at the pathway level.

Folate supports one-carbon transfer for nucleotide and amino acid synthesis. Biotin is the cofactor for carboxylases (for example propionyl-CoA carboxylase and pyruvate carboxylase) involved in gluconeogenesis, fatty acid synthesis and amino acid catabolism. Both are cofactor-type vitamins, so they sit close together in a metabolic network.

However, this link is weak. Biotin metabolic diseases (such as biotinidase deficiency and holocarboxylase synthetase deficiency) arise from defects in biotin recycling or attachment, and folate supplementation does not correct these. The very high TxGNN score most likely reflects shared vitamin and cofactor network proximity rather than a validated therapeutic mechanism. The standard of care for primary biotin metabolic disorders is biotin supplementation.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

The compiled literature is almost entirely narrative reviews or indirect studies on vitamins in general. None tests folic acid as a treatment for biotin metabolic disease. A selection of the most relevant publications:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23622402](https://pubmed.ncbi.nlm.nih.gov/23622402/) | 2013 | Review | Handbook of Clinical Neurology | Overview of vitamin-responsive disorders (cobalamin, folate, biotin, B1, E); describes inborn errors of vitamin metabolism and their vitamin-specific treatment |
| [38203763](https://pubmed.ncbi.nlm.nih.gov/38203763/) | 2024 | Review | International Journal of Molecular Sciences | Vitamin B12 deficiency and the nervous system; B12, biotin and folate share linked metabolic reactions |
| [7027768](https://pubmed.ncbi.nlm.nih.gov/7027768/) | 1981 | Review | Acta Vitaminologica et Enzymologica | Vitamins in metabolic diseases: malabsorption, errors of vitamin metabolism and vitamin-dependent syndromes |
| [41692080](https://pubmed.ncbi.nlm.nih.gov/41692080/) | 2026 | Review | Clinics in Dermatology | B vitamins in cellular metabolism and erythrocyte synthesis |
| [14989256](https://pubmed.ncbi.nlm.nih.gov/14989256/) | 2004 | Review | Archives of Biochemistry and Biophysics | Micronutrient deficiencies (including folic acid) and DNA damage; case for optimised supplement intake |
| [37123774](https://pubmed.ncbi.nlm.nih.gov/37123774/) | 2023 | Review | Cureus | Vitamins and type 2 diabetes; thiamine, pyridoxine and biotin lower in diabetes |
| [25388747](https://pubmed.ncbi.nlm.nih.gov/25388747/) | 2015 | Review | Endocrine, Metabolic & Immune Disorders Drug Targets | Individual and combined vitamins in type 2 diabetes |
| [38350491](https://pubmed.ncbi.nlm.nih.gov/38350491/) | 2025 | Cohort | Nutrition Reviews | Micronutrient status in children with severe wasting; implications for rehabilitation formulations |
| [13199008](https://pubmed.ncbi.nlm.nih.gov/13199008/) | 1954 | Animal study | Biologica Latina | Biotin deficiency and combined biotin and folic acid deficiency induced in rats |
| [16183151](https://pubmed.ncbi.nlm.nih.gov/16183151/) | 2006 | Animal study | International Journal of Cardiology | Folic acid reduced VCAM-1 expression in aortas of hyperhomocysteinaemic rats |

---

## New Zealand Market Information

All three licences are cancelled; no folic acid single-ingredient product is currently valid. The records contain no approved indication text.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2610566 | Folic Acid multichem | Tab 0.8 mg | Not recorded |
| 344850 | Biomed | Oral liq 50 mcg per ml | Not recorded |
| 2633450 | Folic Acid Viatris | Tab 5 mg | Not recorded |

---

## Safety Considerations

- **Drug Interactions**: 100 interactions were found in total. Of those listed, pancrelipase is rated Moderate and sulfasalazine Minor. Others are of unknown severity, including calcitriol, pantoprazole, omeprazole, lansoprazole, glimepiride, metformin, acarbose, rosiglitazone, mesalazine, sucralfate, morphine, prednisone, triamcinolone, simvastatin, vancomycin, lactulose, nizatidine and palonosetron.
- **Disease-level contraindication signals (knowledge-graph derived, not label text)**: The knowledge graph links folic acid with a broad set of conditions, for example kidney disease, nephrolithiasis, diabetes mellitus and diabetic ketoacidosis, hyperparathyroidism, hypercalcaemia, sarcoidosis, bipolar disorder, peptic ulcer disease, inflammatory bowel disease and ulcerative colitis, haemolytic anaemia, sickle cell anaemia, hereditary haemochromatosis, Wilson disease and primary hyperoxaluria. These come from graph relationships rather than product labelling, and several look implausible for folic acid, so they need verification against the package insert before any interpretation.

Package insert warnings and contraindications were not available, so please refer to the package insert for authoritative safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a high model score (Evidence Level L5). There are no clinical trials, the literature is indirect, and the mechanism does not support folate correcting biotin pathway defects. Standard care (biotin supplementation) already exists, and folic acid has no currently valid New Zealand licence.

**To proceed, the following is needed:**
- Mechanism of action data for folic acid, and a clear mechanistic argument for benefit in biotinidase or holocarboxylase synthetase deficiency
- Package insert warnings and contraindications from Medsafe, to complete safety screening
- Direct preclinical or clinical evidence of folic acid in biotin metabolic disorders, which is currently absent
- A clinical rationale for use beyond standard biotin therapy, such as an adjunctive role
- Clarification of the regulatory position, since all New Zealand licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


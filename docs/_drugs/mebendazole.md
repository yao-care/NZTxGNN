---
layout: default
title: Mebendazole
parent: Model Prediction Only (L5)
nav_order: 281
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-30
monitor_inn: "MEBENDAZOLE"
predicted_indications: ["acne (disease)"]
description: "Mebendazole: predicted new indication Acne (disease) (evidence level L5). Approved use: Not recorded in the local data"
---

# Mebendazole
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Mebendazole: Acne (disease)
{: .text-alpha }

## One-Sentence Summary

Mebendazole is a benzimidazole anthelmintic originally used to treat intestinal worm infections (enterobiasis, ascaridiasis and trichuriasis).
The TxGNN model predicts it may be effective for **acne**, but there are **0 clinical trials** and only **1 publication** (an unrelated case report) behind this prediction, so the evidence is very weak.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Acne (disease) |
| TxGNN Prediction Score | 0.9920 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Mebendazole is generally understood to be a benzimidazole anthelmintic that inhibits parasite tubulin polymerisation, and its efficacy against intestinal nematodes is established.

Any link to acne is speculative. Antiproliferative or anti-inflammatory effects (for example via tubulin or hedgehog signalling) have been proposed in principle, but nothing in the supplied data supports them. The only basis for this prediction is the high TxGNN TxGNN model (deep learning) score. The link has not been biologically validated, and no similarity analysis between the original and new indications is available.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7072899](https://pubmed.ncbi.nlm.nih.gov/7072899/) | 1982 | Case report | Am J Trop Med Hyg | Report of human proliferative sparganosis in Venezuela. The patient had acne-like skin lesions as a symptom of the parasitic infection. This is not evidence of treating acne. |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 239852 (valid) | Vermox | Oral liquid 100 mg per 5 ml | Not specified in the available data |
| 258040 (cancelled) | Vermox | Tablet 100 mg | Not specified in the available data |

---

## Safety Considerations

- **Contraindication signals from the knowledge graph** (not label text): liver disease and gallbladder disease.
- **Drug Interactions** (10 recorded; mostly moderate):
  - Moderate: metronidazole, iodide I-131, iodide I-123, phenobarbital, carbamazepine, ritonavir, fosphenytoin, phenytoin, primidone
  - Minor: cimetidine

Package insert warnings and contraindications were not available. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score, with no clinical trials, no relevant literature and no documented mechanism linking mebendazole to acne. The one publication retrieved is an unrelated parasitic infection case report.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data, for example from DrugBank
- Mechanistic or preclinical evidence supporting an effect in acne
- A literature and trial search targeted at mebendazole in acne or related skin conditions
- Assessment of route compatibility (oral formulations only are currently marketed; acne treatment would need to be evaluated for oral versus topical use)
- Review of the liver and gallbladder disease contraindication signals and the enzyme-inducer interactions (e.g. phenytoin, carbamazepine) for any long-term dosing scenario
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


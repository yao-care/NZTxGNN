---
layout: default
title: Ribociclib
parent: Model Prediction Only (L5)
nav_order: 401
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-15
description: "Ribociclib: predicted new indication Myeloid leukemia (evidence level L5). Approved use: Not stated in the New Zealand license data compiled for this report (…"
monitor_inn: "RIBOCICLIB"
predicted_indications: ["myeloid leukemia", "thrombocytopenia", "marcothrombocytopenia with mitral valve insufficiency", "hereditary thrombocytopenia with normal platelets"]
---

# Ribociclib
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

## Ribociclib: From Breast Cancer to Myeloid Leukemia
{: .text-alpha }

## One-Sentence Summary

Ribociclib is a CDK4/6 inhibitor, known for use in hormone receptor-positive, HER2-negative breast cancer. The TxGNN model predicts it may be effective for **myeloid leukemia**, but there are **0 registered clinical trials** and only **1 preclinical publication** that directly supports this direction, so the prediction remains a research question.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the New Zealand license data compiled for this report (the literature describes use in HR+/HER2- breast cancer) |
| Predicted New Indication | Myeloid leukemia |
| TxGNN Prediction Score | 99.35% |
| Evidence Level | L4 (preclinical/mechanistic evidence only) |
| New Zealand Market Status | Cancelled only (no currently valid licenses) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, ribociclib inhibits CDK4/6, which regulates the G1-S transition of the cell cycle. Blocking this step stops proliferation of cells that depend on the CDK4/6–retinoblastoma pathway.

Myeloid leukemia cells may depend on CDK4/6 signalling for proliferation, which gives a plausible mechanistic rationale. A 2020 preclinical study (PMID 32560251) suggests CDK4/6 inhibitors can target pharmacokinetic drug resistance in acute myeloid leukemia cells, for example by affecting the ABCB1 and ABCG2 efflux transporters. This points to a possible role in combination with standard induction therapy.

The high TxGNN score is a graph-based prediction only. No clinical trial has tested this drug–disease pair, so the link should be treated as a hypothesis.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Only one publication relates directly to the predicted indication. The second paper returned for this prediction (a vulvar adenocarcinoma case report) is not relevant to myeloid leukemia.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [32560251](https://pubmed.ncbi.nlm.nih.gov/32560251/) | 2020 | Preclinical/In vitro | Cancers | CDK4/6 inhibitors may counter ABCB1/ABCG2-mediated drug resistance in AML cells, which could support combination use |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2701367 | Kisqali | Tab 200 mg | Not available (cancelled) |
| 2701359 | Kisqali | Tab 200 mg | Not available (cancelled) |
| 2701340 | Kisqali | Tab 200 mg | Not available (cancelled) |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (CDK4/6 inhibitor, oral kinase inhibitor) |
| Myelosuppression Risk | High. Neutropenia, leukopenia and thrombocytopenia are well-documented, and a meta-analysis and pharmacovigilance studies describe haematological toxicity as a class effect |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Complete blood count with differential, liver function, ECG/QT interval and electrolytes (per standard practice for this class; confirm against the package insert) |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Haematological toxicity**: Published reports describe myelosuppression (neutropenia, leukopenia, thrombocytopenia) with CDK4/6 inhibitors. This is especially relevant in leukemia, where bone marrow function is already compromised.
- **Other reported adverse events**: Case reports describe pneumonitis and visual hallucination with ribociclib.
- **Drug Interactions**: 100 interactions were identified. Those listed include **Major** interactions with clarithromycin and dolasetron. Moderate interactions include famotidine, hydrocortisone, metformin, pioglitazone, alosetron, loperamide, aprepitant, dexamethasone, cimetidine and saxagliptin, among others.

Please refer to the package insert for warnings and contraindications.

## Other Predicted Indications (for context)

| Predicted Indication | TxGNN Score | Evidence Level | Assessment |
|------|------|------|------|
| Thrombocytopenia | 99.27% | L4 | Hold. The literature describes thrombocytopenia as an adverse effect of ribociclib, not a therapeutic target. Use for this condition is mechanistically implausible and could worsen it |
| Macrothrombocytopenia with mitral valve insufficiency | 99.08% | L5 | Hold. No evidence; likely a graph artefact and a safety concern |
| Hereditary thrombocytopenia with normal platelets | 99.07% | L5 | Hold. No evidence; the ontology entry itself looks inconsistent and should be checked |

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only support for myeloid leukemia is one preclinical study and a plausible mechanism, with no registered clinical trials. In addition, ribociclib's marrow-suppressing profile is a concern in a haematological malignancy, and all New Zealand licenses are cancelled.

**To proceed, the following is needed:**
- Detailed mechanism of action data
- Package insert warnings and contraindications (not yet retrieved)
- Further preclinical work in myeloid leukemia models, including combination with standard AML therapy and assessment of marrow toxicity
- Review of whether a currently valid New Zealand supply route exists, given that all three licenses are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


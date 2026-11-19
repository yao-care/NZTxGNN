---
layout: default
title: Ezetimibe
parent: Model Prediction Only (L5)
nav_order: 192
evidence_level: L5
indication_count: 1
monitor_inn: "EZETIMIBE"
predicted_indications: ["cholesterol-ester transfer protein deficiency"]
last_modified_at: 2026-11-19
description: "Ezetimibe: predicted new indication Cholesterol-ester transfer protein deficiency (evidence level L5). Approved use: Hyperlipidaemia, familial hypercholesterolaemia, sitosterolemia"
---

# Ezetimibe
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

## Ezetimibe: From Hyperlipidaemia to Cholesterol-Ester Transfer Protein Deficiency
{: .text-alpha }

## One-Sentence Summary

Ezetimibe is a cholesterol-absorption inhibitor, recorded in the knowledge graph for hyperlipidaemia and related inherited lipid disorders (including homozygous familial hypercholesterolaemia and sitosterolemia).
The TxGNN model predicts it may be relevant for **cholesterol-ester transfer protein (CETP) deficiency**, but there are **0 clinical trials** and **2 publications** in the data compiled for this report, and neither studies this disease directly.
This is a model prediction only, and the supporting evidence is weak.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hyperlipidaemia, familial hypercholesterolaemia, sitosterolemia (relations recorded in the knowledge graph; the local licence record contains no indication text) |
| Predicted New Indication | Cholesterol-ester transfer protein deficiency |
| TxGNN Prediction Score | 0.9909 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (11 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 11 |
| Number of Approvals | 1 (single-ingredient licence) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for ezetimibe: "Ezetimibe reduces blood cholesterol by inhibiting the absorption of cholesterol by the small intestine. The molecular target of ezetimibe has been shown to be the sterol transporter, Niemann-Pick C1-Like 1 (NPC1L1), which is involved in the intestinal uptake of cholesterol and phytosterols." ([DailyMed: ezetimibe prescribing information, A-S Medication Solutions](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=c37c695b-fb40-423c-9b73-d1aa58a34db9), label version 2026-10-08; accessed 2026-11-19.) From general knowledge, ezetimibe inhibits NPC1L1-mediated cholesterol absorption in the intestine, which lowers LDL cholesterol.

CETP deficiency is a genetic condition marked by markedly raised HDL cholesterol and altered lipoprotein metabolism. The shared involvement of lipid-metabolism pathways probably explains the high model score (0.9909).

However, the link is weak. CETP deficiency is usually a benign, or even potentially cardioprotective, phenotype and is not an established condition needing LDL-lowering treatment. No direct mechanistic or clinical evidence shows that blocking cholesterol absorption meets a clinical need in this condition. The high score should therefore be read as a hypothesis-generating signal, not as support for treatment.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [40911366](https://pubmed.ncbi.nlm.nih.gov/40911366/) | 2025 | Review | European Heart Journal | Overview of lipid-lowering targets (LDL-C, triglycerides, apoB, Lp(a)) and the therapies acting on them. General background, not specific to CETP deficiency. |
| [31779098](https://pubmed.ncbi.nlm.nih.gov/31779098/) | 2019 | Animal study | International Journal of Molecular Sciences | Heterozygous Ldlr-deficient hamster model used to test a PCSK9 antibody in hyperlipidaemia and atherosclerosis. The model has functional CETP, but ezetimibe and CETP deficiency are not the subject. |

---

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2545861 | Ezetimibe Sandoz | Ezetimibe | Tab 10 mg | Yes |
| 2470683 | Zimybe | Ezetimibe with simvastatin | Tab 10 mg with simvastatin 10 mg | Yes |
| 2735539 | Vytorin | Ezetimibe with simvastatin | Tab 10 mg with simvastatin 10 mg | Yes |
| 2470691 | Zimybe | Ezetimibe with simvastatin | Tab 10 mg with simvastatin 20 mg | Yes |
| 2737205 | Ezetimibe Simvastatin Viatris | Ezetimibe with simvastatin | Tab 10 mg with simvastatin 20 mg | Yes |
| 2470705 | Zimybe | Ezetimibe with simvastatin | Tab 10 mg with simvastatin 40 mg | Yes |
| 2727722 | Vytorin | Ezetimibe with simvastatin | Tab 10 mg with simvastatin 40 mg | Yes |
| 2737884 | Ascend | Ezetimibe with simvastatin | Tab 10 mg with simvastatin 40 mg | Yes |
| 2470713 | Zimybe | Ezetimibe with simvastatin | Tab 10 mg with simvastatin 80 mg | Yes |
| 2725622 | Zimybe | Ezetimibe with simvastatin | Tab 10 mg with simvastatin 80 mg | Yes |
| 2727730 | Vytorin | Ezetimibe with simvastatin | Tab 10 mg with simvastatin 80 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

---

## Safety Considerations

- **Contraindications (relations recorded in the knowledge graph, not label text):** pancreatitis, liver disease, gallbladder disease, myopathy, muscular disease, skeletal muscle disease, familial periodic paralysis, substance abuse/dependence, rhabdomyolysis, nodular fasciitis.
- **Drug Interactions:** 100 interactions were found in total. Those rated Moderate include:
  - Chenodeoxycholic acid
  - Eluxadoline
  - Rosuvastatin
  - Simvastatin

  The other listed interactions (for example calcitriol, pantoprazole, glimepiride, metformin, omeprazole) have an unknown severity rating.

Please also refer to the package insert for full warnings, as label text was not available for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on model output alone. No clinical trial or publication directly studies ezetimibe in CETP deficiency, and the condition is generally not a disease needing LDL-lowering therapy. The evidence level is therefore L5.

**To proceed, the following is needed:**
- A clear clinical rationale showing that CETP deficiency has an unmet need that cholesterol-absorption inhibition could address
- Direct mechanistic or clinical studies of ezetimibe in CETP deficiency
- Mechanism-of-action data for ezetimibe
- Medsafe package insert warnings and contraindications for a formal safety review
- Review of the interaction profile, particularly with statins and other lipid-modifying agents

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Detailed mechanism-of-action data for ezetimibe is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: ezetimibe prescribing information, A-S Medication Solutions](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=c37c695b-fb40-423c-9b73-d1aa58a34db9) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


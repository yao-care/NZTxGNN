---
layout: default
title: Anagrelide
parent: Model Prediction Only (L5)
nav_order: 39
evidence_level: L5
indication_count: 2
monitor_inn: "Anagrelide"
predicted_indications: ["reactive thrombocytosis", "inverse Klippel-Trenaunay syndrome"]
last_modified_at: 2026-10-30
description: "Anagrelide: predicted new indication reactive thrombocytosis (evidence level L5). Approved use: Not recorded in the local data"
---

# Anagrelide
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

## Anagrelide: PDE3 inhibitor / platelet-lowering agent → reactive thrombocytosis
{: .text-alpha }

## One-Sentence Summary

Anagrelide is a platelet-lowering medicine that acts on megakaryocyte maturation and is reported to inhibit phosphodiesterase 3A (PDE3A).
The TxGNN model predicts it may be effective for **reactive thrombocytosis**, but there are **0 clinical trials** and only indirect literature (10 publications, mostly reviews and case reports on essential thrombocythaemia), so the evidence is weak.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | reactive thrombocytosis |
| TxGNN Prediction Score | 0.9983 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. From general literature knowledge, anagrelide lowers platelet count by inhibiting megakaryocyte maturation and differentiation. This is reported to involve PDE3 inhibition and suppression of megakaryocyte transcription factors such as GATA-1 and FOG-1. A PDE3A interaction is also recorded in the pharmacology data.

Mechanistically, lowering platelets could work in any raised-platelet state. The model's high score most likely reflects similarity to clonal thrombocytosis conditions such as essential thrombocythaemia.

However, the clinical rationale is weak. Reactive (secondary) thrombocytosis is driven by an underlying condition such as inflammation, iron deficiency, infection, splenectomy or malignancy. It is usually self-limited and benign, with low thrombotic risk. The literature reviewed states that reactive thrombocytosis generally needs no platelet-lowering therapy. The prediction is therefore a computational result, not a clinical signal.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

None of these studies tests anagrelide in reactive thrombocytosis. They concern essential thrombocythaemia or thrombocytosis in general, and mostly mention reactive thrombocytosis as a condition to be distinguished.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15270658](https://pubmed.ncbi.nlm.nih.gov/15270658/) | 2004 | Review | Expert Rev Anticancer Ther | Update on anagrelide mechanisms and therapeutic potential. Reactive thrombocytosis needs no treatment, whereas clonal thrombocytosis may. |
| [16019501](https://pubmed.ncbi.nlm.nih.gov/16019501/) | 2005 | Review | Leuk Lymphoma | Critical review of anagrelide in essential thrombocythaemia. Reactive thrombocytosis is often inconsequential. |
| [10494240](https://pubmed.ncbi.nlm.nih.gov/10494240/) | 1999 | Review | Med J Aust | Essential thrombocythaemia review. Diagnosis requires excluding reactive thrombocytosis. |
| [7783354](https://pubmed.ncbi.nlm.nih.gov/7783354/) | 1995 | Review | Rinsho Ketsueki | Diagnosis and treatment of essential thrombocythaemia, including differentiation from reactive thrombocytosis. Anagrelide is among the drugs listed. |
| [1994734](https://pubmed.ncbi.nlm.nih.gov/1994734/) | 1991 | Review | Am J Med Sci | Clinical spectrum of thrombocytosis and thrombocythaemia. |
| [28380402](https://pubmed.ncbi.nlm.nih.gov/28380402/) | 2017 | Review | Leuk Res | Role of thrombocytapheresis in extreme thrombocytosis in myeloproliferative neoplasms. |
| [17171694](https://pubmed.ncbi.nlm.nih.gov/17171694/) | 2007 | Retrospective case series | Pediatr Blood Cancer | Retrospective analysis of 12 children comparing essential and reactive thrombocythaemia. |
| [38455691](https://pubmed.ncbi.nlm.nih.gov/38455691/) | 2024 | Case report | Eur J Case Rep Intern Med | Acute myocardial infarction in a patient with essential thrombocythaemia treated with anagrelide. |
| [27276864](https://pubmed.ncbi.nlm.nih.gov/27276864/) | 2016 | Case report | Srp Arh Celok Lek | Essential thrombocythaemia with ankylosing spondylitis treated with anagrelide and other agents. |
| [29851840](https://pubmed.ncbi.nlm.nih.gov/29851840/) | 2018 | Case report | Medicine | Digit replantation in a patient with thrombocytosis after splenectomy. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2583054 | Agrylin | Cap 0.5 mg | Not recorded in the local data |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Not a conventional cytotoxic; acts as a platelet-lowering agent. Please refer to the package insert warnings and precautions. |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Platelet count and complete blood count; please also refer to the package insert |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

Package insert warnings and contraindications are not available in the data compiled for this report. Please refer to the package insert for safety information.

- **Drug Interactions**: The only interaction recorded is a pharmacological target relationship with phosphodiesterase 3A (PDE3A). It is not a drug-drug interaction and carries no severity level.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No clinical trial or study directly examines anagrelide in reactive thrombocytosis, so the evidence level is L5. Reactive thrombocytosis is usually benign and self-limited and does not normally call for platelet-lowering therapy. The high model score is not enough to justify moving forward.

**To proceed, the following is needed:**
- Clinical or observational data showing a patient group with reactive thrombocytosis who would benefit from platelet lowering (for example, extreme or thrombotic cases)
- Package insert warnings and contraindications from Medsafe
- Confirmed mechanism of action data from DrugBank
- A risk-benefit assessment, given cardiovascular and other safety concerns in the label
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Pyridostigmine
parent: Model Prediction Only (L5)
nav_order: 394
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-31
monitor_inn: "Pyridostigmine"
predicted_indications: ["autoimmune disease of peripheral nervous system", "disease of receptor activity", "hypersplenism (disease)", "atypical hemolytic-uremic syndrome with B factor anomaly"]
description: "Pyridostigmine: predicted new indication Autoimmune disease of peripheral nervous system (evidence level L5). Approved use: Not recorded in the local data"
---

# Pyridostigmine
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

## Pyridostigmine: Autoimmune disease of peripheral nervous system
{: .text-alpha }

## One-Sentence Summary

Pyridostigmine is an acetylcholinesterase inhibitor originally used to treat myasthenia gravis and related neuromuscular junction disorders.
Currently there are **no registered clinical trials** and **3 publications** (a review, a case report and a cohort study), none of which evaluate pyridostigmine in a controlled way.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Autoimmune disease of peripheral nervous system |
| TxGNN Prediction Score | 0.9975 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, pyridostigmine is a reversible acetylcholinesterase inhibitor. By slowing the breakdown of acetylcholine, it increases acetylcholine at the neuromuscular junction. This is the established symptomatic mechanism in myasthenia gravis.

Thymic hyperplasia is a common pathological feature of myasthenia gravis with acetylcholine receptor antibodies. The predicted indication is therefore a subtype of the disease pyridostigmine already treats, and the mechanistic fit is strong. The high score mostly reflects this close relationship rather than a new therapeutic discovery.

The pharmacology data confirm that pyridostigmine targets acetylcholinesterase (ACHE) and butyrylcholinesterase (BCHE).

Other predictions in the ranked list were considered, but they are weaker:
- The literature consists of reviews and maternal pregnancy cohorts, with no controlled neonatal pyridostigmine studies. Neonatal dosing and safety need dedicated evaluation.
- **Autoimmune disease of peripheral nervous system** (0.9975): a broad category driven mainly by myasthenia gravis. It may include Lambert-Eaton myasthenic syndrome and congenital myasthenic syndromes, where cholinesterase inhibitors can help in some subtypes and worsen others (for example slow-channel syndrome).
- Evidence level L4, Hold.
- **Disease of receptor activity** (0.9973): a non-specific ontology grouping that should be mapped to specific diseases before further assessment. MuSK-positive myasthenia gravis patients may respond poorly or adversely to cholinesterase inhibitors.
- **Hypersplenism** and **atypical haemolytic-uraemic syndrome with factor B anomaly** (0.9930 and 0.9922): no plausible mechanistic link, no trials and no literature. These are computational predictions only (L5).

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25683765](https://pubmed.ncbi.nlm.nih.gov/25683765/) | 2015 | Cohort | Journal of Neurology | Retrospective 2-year outcomes of thymectomy in 39 patients with non-thymomatous, AChR-antibody-positive late-onset generalized MG. It does not evaluate pyridostigmine. |
| [34225443](https://pubmed.ncbi.nlm.nih.gov/34225443/) | 2021 | Review | Molecular Medicine Reports | Review of genomic, phenotypic and epigenetic aspects of MG pathology and autoimmunity, covering neonatal, ocular and generalized forms. |
| [18053719](https://pubmed.ncbi.nlm.nih.gov/18053719/) | 2008 | Case report | Neuromuscular Disorders | MuSK-positive MG with thymus hyperplasia presenting with dropped head syndrome. No controlled evaluation of pyridostigmine. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 251011 (cancelled) | Mestinon | Tab 60 mg | Not recorded |

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report, so please refer to the package insert for label safety information.

- **Contraindications (from knowledge graph, not label text)**: the drug-disease contraindication links in the knowledge graph include kidney disease, pyoureter, asthma (including intrinsic and allergic asthma) and urinary tract obstruction.
- **Drug Interactions**: the interaction query was completed. It returned only pharmacological target relationships, with no drug-drug interactions graded by level: acetylcholinesterase (ACHE) and butyrylcholinesterase (BCHE).
- **Population-specific cautions from the literature**: cholinesterase inhibitors may worsen some congenital myasthenic syndromes and may be poorly effective or harmful in MuSK-positive MG. Increased sensitivity to non-depolarizing neuromuscular blockers has been described in patients taking pyridostigmine.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is mechanistically sound, but it largely restates the existing indication (a subtype of myasthenia gravis). There are no clinical trials, and the supporting literature has no controlled evidence specific to this subtype. These gaps block progression to safety screening.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (currently a blocking gap)
- Detailed mechanism of action data
- Evidence on pyridostigmine response in the thymic hyperplasia subtype, including separation of AChR-positive from MuSK-positive patients
- Dedicated evaluation of neonatal dosing and safety if neonatal MG is pursued
- Mapping of broad predictions (peripheral autoimmune disease, receptor activity disease) to specific diseases before further assessment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


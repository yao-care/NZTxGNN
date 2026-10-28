---
layout: default
title: Diazepam
parent: Model Prediction Only (L5)
nav_order: 146
evidence_level: L5
indication_count: 10
monitor_inn: "DIAZEPAM"
predicted_indications: ["insomnia (disease)", "cauda equina syndrome", "sleep disorder, initiating and maintaining sleep", "attention deficit hyperactivity disorder, inattentive type", "antidepressant type abuse", "hallucinogen abuse", "barbiturate abuse", "specific developmental disorder", "attention deficit-hyperactivity disorder", "obsolete neurogenic bladder (disease)"]
last_modified_at: 2026-10-27
description: "Diazepam: predicted new indication insomnia (disease) (evidence level L5). Approved use: Not recorded in the local data"
---

# Diazepam
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

## Diazepam: GABA-A positive allosteric modulator → insomnia (disease)
{: .text-alpha }

## One-Sentence Summary

Diazepam is a benzodiazepine that enhances GABA-A receptor activity and has long been used for conditions such as seizures and anxiety.
The TxGNN model predicts it may be effective for **insomnia (disease)**, but there are **no registered clinical trials** and the **literature is mostly class-level or indirect**, so the evidence is limited.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | insomnia (disease) |
| TxGNN Prediction Score | 1.0000 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 4 of 4) |
| Number of Authorizations | 4 (valid: 4) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. From general pharmacology, diazepam is a positive allosteric modulator of GABA-A receptors. Enhancing inhibitory GABA signalling produces sedation, which is the basis for the sedative-hypnotic effect of benzodiazepines, so the link to insomnia is mechanistically plausible.

Other benzodiazepines (for example triazolam) have documented hypnotic use. Diazepam, however, has a long half-life and an active metabolite (nordiazepam). This raises concerns about next-day sedation, rebound insomnia after withdrawal, tolerance and dependence.

Note that the model score is very high (the rounded value is 1.0000; the unrounded value is 0.99999750), but a score is a model output and not clinical proof.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

None of the publications below is a diazepam-specific randomized trial in insomnia. Two small, older studies include diazepam directly.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [430730](https://pubmed.ncbi.nlm.nih.gov/430730/) | 1979 | Clinical (sleep laboratory) | JAMA | Five benzodiazepines were tested in 15 sleep-lab studies. Rebound insomnia followed withdrawal of triazolam, nitrazepam and flunitrazepam. Diazepam and flurazepam, which have longer half-lives, behaved differently. |
| [6113175](https://pubmed.ncbi.nlm.nih.gov/6113175/) | 1981 | Double-blind comparison | J Int Med Res | In 100 outpatients with sleep disorders, lormetazepam 1 mg was significantly better than diazepam 5 mg on sleep onset and sleep continuity over 7 days. |
| [6114852](https://pubmed.ncbi.nlm.nih.gov/6114852/) | 1981 | Review | Drugs | Review of triazolam (a different benzodiazepine) for insomnia; a short half-life is considered more suitable for hypnotic use than longer-acting drugs. |
| [39581171](https://pubmed.ncbi.nlm.nih.gov/39581171/) | 2024 | Review | Bioorg Chem | Overview of GABA-A receptor modulators, including diazepam, in epilepsy, anxiety, insomnia and depression. |
| [36692463](https://pubmed.ncbi.nlm.nih.gov/36692463/) | 2023 | Meta-analysis | Acta Pharm | Tranquilizer use in elderly patients with chronic diseases, assessed by dose, outcomes and adverse effects. |
| [23330992](https://pubmed.ncbi.nlm.nih.gov/23330992/) | 2013 | Review | Expert Opin Drug Metab Toxicol | Pharmacokinetics of anxiolytic drugs. |
| [40583063](https://pubmed.ncbi.nlm.nih.gov/40583063/) | 2025 | Clinical and mechanistic | Cell Mol Biol Lett | Long-term use of benzodiazepines and Z-drugs for insomnia and anxiety was linked to increased breast cancer risk. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 425877 | Stesolid | Rectal tubes 5 mg | Not recorded |
| 2328003 | Arrow-Diazepam | Tab 5 mg | Not recorded |
| 2327996 | Arrow-Diazepam | Tab 2 mg | Not recorded |
| 399019 | Hospira | Inj 5 mg per ml, 2 ml ampoule | Not recorded |

## Safety Considerations

- **Disease associations flagged in the data:** liver failure and liver disease, gallbladder disease, glaucoma (open-angle and angle-closure), sleep apnoea syndromes, respiratory failure and other lung disease, myasthenia gravis and related neuromuscular junction disease, and substance-related disorders. These are not taken from label text and should be checked against the package insert.
- **Drug interactions:** 100 interactions were found. Major: morphine. Moderate examples: bupropion, aprepitant, omeprazole, esomeprazole, cimetidine, clarithromycin, cisapride, clotrimazole, miconazole, dronabinol, metoclopramide.
- **Class concerns from the literature:** rebound insomnia on withdrawal, dependence, abuse potential, and next-day sedation, especially with long-acting agents.

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanism is plausible, but no registered trial or cited study directly demonstrates diazepam's efficacy in insomnia, so evidence is at model-prediction level only. Dependence, rebound and sedation concerns also weigh against it, and newer hypnotics are generally preferred.

**To proceed, the following is needed:**
- Diazepam-specific randomized trials in insomnia, compared with current hypnotics
- Mechanism of action data and the full New Zealand package insert warnings and contraindications
- A risk assessment for dependence, next-day sedation and use in older adults
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


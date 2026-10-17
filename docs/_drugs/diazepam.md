---
layout: default
title: Diazepam
parent: Moderate Evidence (L3-L4)
nav_order: 146
evidence_level: L3
indication_count: 10
last_modified_at: 2026-10-15
description: "Diazepam: predicted new indication Insomnia (evidence level L3). Approved use: Epilepsy, status epilepticus, alcohol withdrawal, anxiety, tetanus"
monitor_inn: "DIAZEPAM"
predicted_indications: ["insomnia (disease)", "cauda equina syndrome", "sleep disorder, initiating and maintaining sleep", "attention deficit hyperactivity disorder, inattentive type", "antidepressant type abuse", "hallucinogen abuse", "barbiturate abuse", "specific developmental disorder", "attention deficit-hyperactivity disorder", "obsolete neurogenic bladder (disease)"]
---

# Diazepam
{: .fs-9 }

Evidence Level: **L3** | Predicted Indications: **10** 
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

# Diazepam: From Epilepsy and Anxiety to Insomnia

## One-Sentence Summary

Diazepam is a benzodiazepine originally used for epilepsy, status epilepticus, alcohol withdrawal, anxiety and tetanus.
The TxGNN model predicts it may be effective for **insomnia**, with **0 registered clinical trials** and **18 publications** (mostly class-level reviews or tangential reports) currently supporting this direction.
Evidence is limited to indirect, class-level literature, so this should be treated as a research question rather than an established new use.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Epilepsy, status epilepticus, alcohol withdrawal, anxiety, tetanus (the New Zealand licence records contain no approved indication text) |
| Predicted New Indication | Insomnia |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L3 |
| New Zealand Market Status | Cancelled only (no currently valid authorizations) |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on general pharmacology, diazepam is a benzodiazepine that acts as a positive allosteric modulator of GABA-A receptors. This enhances inhibitory signalling in the brain, which underlies its anticonvulsant, anxiolytic and sedative effects.

Insomnia is closely related to the anxiety and withdrawal states diazepam is already used for. Enhanced GABAergic inhibition promotes sleep initiation and maintenance, and other benzodiazepines such as triazolam are documented hypnotics. This makes the prediction mechanistically plausible.

There are important caveats. Diazepam has a long half-life and active metabolites (such as nordiazepam), raising concerns about next-day sedation, tolerance, dependence and rebound insomnia on withdrawal. The supplied literature contains no diazepam-specific randomized trial for insomnia, and newer agents (orexin antagonists, Z-drugs) are generally preferred.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Most items are class-level, historical or tangential. The most relevant are listed below.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [6113175](https://pubmed.ncbi.nlm.nih.gov/6113175/) | 1981 | Double-blind comparison (100 outpatients) | J Int Med Res | Lormetazepam 1 mg was significantly better than diazepam 5 mg at reducing time to fall asleep in 7-day treatment |
| [430730](https://pubmed.ncbi.nlm.nih.gov/430730/) | 1979 | Sleep laboratory studies | JAMA | Rebound insomnia followed withdrawal of shorter-acting benzodiazepines; diazepam and flurazepam, with longer half-lives, behaved differently |
| [6114852](https://pubmed.ncbi.nlm.nih.gov/6114852/) | 1981 | Review | Drugs | Review of triazolam (a different benzodiazepine) as a hypnotic in insomnia |
| [39581171](https://pubmed.ncbi.nlm.nih.gov/39581171/) | 2024 | Review | Bioorg Chem | GABA-A receptor modulators, including diazepam, used in epilepsy, anxiety and insomnia, with sedation-type side effects |
| [23330992](https://pubmed.ncbi.nlm.nih.gov/23330992/) | 2013 | Review | Expert Opin Drug Metab Toxicol | Pharmacokinetics of anxiolytic drugs |
| [36692463](https://pubmed.ncbi.nlm.nih.gov/36692463/) | 2023 | Meta-analysis | Acta Pharm | Tranquilizer therapy in elderly patients with chronic non-communicable diseases |
| [40583063](https://pubmed.ncbi.nlm.nih.gov/40583063/) | 2025 | Clinical and mechanistic study | Cell Mol Biol Lett | Long-term benzodiazepine and Z-drug use associated with increased breast cancer risk |
| [37984023](https://pubmed.ncbi.nlm.nih.gov/37984023/) | 2024 | Predictive model study | Value Health Reg Issues | Long-term benzodiazepine use linked to memory loss, dependence, falls and traffic accidents |

---

## New Zealand Market Information

All four authorizations are cancelled, and no approved indication text is recorded for them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 425877 | Stesolid | Rectal tubes 5 mg | Not recorded (cancelled) |
| 2328003 | Arrow-Diazepam | Tab 5 mg | Not recorded (cancelled) |
| 2327996 | Arrow-Diazepam | Tab 2 mg | Not recorded (cancelled) |
| 399019 | Hospira | Inj 5 mg per ml, 2 ml ampoule | Not recorded (cancelled) |

---

## Safety Considerations

- **Contraindications (from knowledge-graph disease associations, not label text):** liver failure and liver disease, gallbladder disease, open-angle and angle-closure glaucoma, obstructive sleep apnoea and other sleep apnoea syndromes, congenital central hypoventilation syndrome, respiratory failure and lung disease, myasthenia gravis and neuromuscular junction disease, and substance-related disorders and dependence. Anxiety disorder, neurotic disorder and dysthymic disorder are also listed.
- **Drug Interactions:** 100 interactions were identified; the 20 shown in the data include the following.
  - **Major:** morphine (additive central nervous system and respiratory depression).
  - **Moderate:** bupropion, aprepitant, omeprazole, esomeprazole, cimetidine, clarithromycin, cisapride, clotrimazole, miconazole, dronabinol, metoclopramide.
  - **Minor:** dexamethasone, betamethasone, and antacids (magnesium oxide, aluminium hydroxide, calcium carbonate, magaldrate, magnesium carbonate, magnesium hydroxide).
- **Class-level concerns from the literature:** rebound insomnia on withdrawal, tolerance, dependence, cognitive decline and falls in older adults, and possible associations with suicide-attempt risk and breast cancer with long-term use.

Please refer to the package insert for full safety information, as label warnings were not available.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanism is plausible and the TxGNN score is high, but there are no registered trials and no diazepam-specific randomized evidence for insomnia. The literature is largely class-level and focused on withdrawal, rebound and safety. All New Zealand authorizations are cancelled, and effective alternatives with better-suited profiles already exist.

**To proceed, the following is needed:**
- Diazepam-specific randomized evidence in insomnia compared with current standard hypnotics
- Detailed mechanism of action data and label warnings/contraindications from the package insert
- A risk assessment for dependence, next-day sedation and rebound insomnia, especially in older adults
- Clarification of regulatory status in New Zealand, since no valid authorization currently exists

**Other predicted indications (for reference):** the second-ranked prediction, sleep disorder (initiating and maintaining sleep), is essentially the same entity as insomnia, with similar class-level evidence (L3). The remaining predictions (cauda equina syndrome, ADHD, substance abuse categories, specific developmental disorder and obsolete neurogenic bladder) have little or no direct supporting evidence and are not recommended for further investigation at this stage.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


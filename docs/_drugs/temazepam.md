---
layout: default
title: Temazepam
parent: Model Prediction Only (L5)
nav_order: 439
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Temazepam: predicted new indication Sleep disorder, initiating and maintaining sleep (evidence level L5). Approved use: Insomnia"
---

# Temazepam
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

## Temazepam: From Insomnia to Sleep Disorder (Initiating and Maintaining Sleep)
{: .text-alpha }

## One-Sentence Summary

Temazepam is a benzodiazepine hypnotic originally used to treat insomnia.
The TxGNN model predicts it may be effective for **sleep disorder, initiating and maintaining sleep**, but this is essentially the same condition as its known indication, so it is not a true repurposing candidate.
Currently there are **no registered clinical trials** and **20 publications** (mostly reviews and guidelines) related to this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Insomnia (the New Zealand licence record contains no approved indication text; this comes from the knowledge graph) |
| Predicted New Indication | Sleep disorder, initiating and maintaining sleep |
| TxGNN Prediction Score | 99.82% |
| Evidence Level | L4 (per the scoring in the data compiled for this report; no completed Phase 2/3 or Phase 3 trials) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on general pharmacology, temazepam is a benzodiazepine receptor agonist that positively modulates GABA-A receptors. This enhances inhibitory neurotransmission and produces sedative-hypnotic effects, which is directly relevant to difficulty falling asleep and staying asleep.

The predicted indication is essentially identical to temazepam's original indication of insomnia. The high TxGNN score (99.82%) most likely reflects this overlap with a known use rather than a novel signal. The prediction is therefore pharmacologically sound, but it does not represent a new therapeutic opportunity.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

The literature below was retrieved for this prediction. Relevance to temazepam specifically has not been confirmed for every item; many are class-level reviews of insomnia drugs.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39374004](https://pubmed.ncbi.nlm.nih.gov/39374004/) | 2024 | RCT | JAMA Internal Medicine | Masked taper combined with behavioural intervention (CBT-I) for discontinuing benzodiazepine receptor agonist hypnotics |
| [27998379](https://pubmed.ncbi.nlm.nih.gov/27998379/) | 2017 | Guideline | J Clin Sleep Med | American Academy of Sleep Medicine guideline on pharmacologic treatment of chronic insomnia in adults, assessing individual drugs |
| [33249496](https://pubmed.ncbi.nlm.nih.gov/33249496/) | 2021 | Systematic review / network meta-analysis | Sleep | Comparative efficacy and safety of hypnotics for insomnia in older adults |
| [32544795](https://pubmed.ncbi.nlm.nih.gov/32544795/) | 2020 | Quantitative comparison | Sleep Medicine | Quantitative comparison of drug efficacy for insomnia |
| [31730011](https://pubmed.ncbi.nlm.nih.gov/31730011/) | 2019 | Overview of reviews | Systematic Reviews | Comparative effectiveness and safety of pharmacological and non-pharmacological insomnia interventions |
| [27751669](https://pubmed.ncbi.nlm.nih.gov/27751669/) | 2016 | Review | Clin Ther | Safety and efficacy of sleep medicines in older adults |
| [30058034](https://pubmed.ncbi.nlm.nih.gov/30058034/) | 2018 | Review | Drugs & Aging | Pharmacological management recommendations for insomnia in elderly patients |
| [6132933](https://pubmed.ncbi.nlm.nih.gov/6132933/) | 1983 | Sleep laboratory studies | J Clin Psychopharmacol | Compares flurazepam, temazepam and triazolam; temazepam is slowly absorbed (peak 2-3 hours), limiting its usefulness for sleep onset |
| [2567741](https://pubmed.ncbi.nlm.nih.gov/2567741/) | 1989 | Review | J Clin Psychopharmacol | Critical review of rebound insomnia after discontinuing short half-life benzodiazepines, including temazepam |
| [3332464](https://pubmed.ncbi.nlm.nih.gov/3332464/) | 1987 | Review | Semin Neurol | Temazepam is effective for sleep maintenance, but less so for sleep induction |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2184877 | Normison | Tab 10 mg | Not recorded (status: cancelled) |

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (these come from drug-disease relationship data, not package insert text): anxiety disorder, neurotic disorder, dysthymic disorder, kidney disease, liver disease, gallbladder disease, pyoureter, obstructive sleep apnoea syndrome, sleep apnoea syndrome, congenital central hypoventilation syndrome, Haddad syndrome, amnestic disorder, retrograde amnesia, substance-related disorder, and substance abuse/dependence.
- **Drug Interactions**: 100 interactions were identified in total; the first 20 are shown in the data compiled for this report.
  - **Major**: morphine and liposomal morphine (additive central nervous system and respiratory depression is a known concern with opioids).
  - **Moderate**: bupropion, dronabinol, nabilone, metoclopramide, teduglutide, opium, sibutramine.
  - **Minor**: antacids such as magnesium oxide, aluminium hydroxide, calcium carbonate, magaldrate, magnesium carbonate and magnesium hydroxide.
- Package insert warnings and contraindications could not be obtained. Please refer to the package insert for full safety information.
- Literature in this set highlights rebound insomnia on discontinuation, and tolerance, dependence and abuse with benzodiazepines and Z-drugs, particularly in older adults.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication is effectively the same as temazepam's established use in insomnia, so the high TxGNN score reflects known use rather than a new finding. No clinical trials are registered, the evidence level is L4, and the only New Zealand licence has been cancelled. Package insert safety data is also missing, which blocks safety screening.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (blocking gap)
- Detailed mechanism of action data from DrugBank
- Confirmation of whether this is a genuinely new indication; if not, a decision on whether re-evaluation is warranted
- Clarification of why the New Zealand licence was cancelled and whether any current market route exists
- Benefit-risk assessment for dependence, rebound insomnia and use in older adults
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Lorazepam
parent: Model Prediction Only (L5)
nav_order: 278
evidence_level: L5
indication_count: 10
---

# Lorazepam
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Lorazepam: From Anxiety Disorder and Seizure Conditions to Trigeminal Nerve Neoplasm

## One-Sentence Summary

Lorazepam is a benzodiazepine originally used for anxiety disorder and seizure conditions such as status epilepticus, absence epilepsy and Lennox-Gastaut syndrome.
The TxGNN model's top-ranked prediction is **trigeminal nerve neoplasm**, but **no clinical trials and no publications** support it, so it is a model-only signal.
Among the other top-10 predictions, **insomnia** has the most supporting evidence (older small clinical studies and reviews).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Anxiety disorder, status epilepticus, absence epilepsy, Lennox-Gastaut syndrome (from knowledge-graph indication data; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Trigeminal nerve neoplasm |
| TxGNN Prediction Score | 99.87% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorizations) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Lorazepam is known to be a benzodiazepine that enhances GABA-A receptor-mediated inhibition, producing anxiolytic, sedative and anticonvulsant effects. This is general pharmacology rather than data from this report's sources.

For the top-ranked prediction, trigeminal nerve neoplasm, there is no plausible disease-modifying link. Lorazepam has no known antitumour mechanism. At most it could offer symptomatic relief (anxiolysis, sedation, antiemetic use) in oncology care, which is not treatment of the tumour. This prediction is most likely a knowledge-graph artifact.

Several lower-ranked predictions are mechanistically more coherent, because they relate to the drug's existing sedative and anticonvulsant uses:

| Rank | Predicted Indication | Score | Evidence Level | Assessment |
|------|------|------|------|------|
| 2 | Insomnia | 99.80% | L3 | Plausible; older small studies exist, but current guidance limits benzodiazepines in chronic insomnia |
| 3 | Reading seizures | 99.64% | L4 | Plausible reflex-epilepsy extension; only indirect evidence |
| 7 | Micturition-induced seizures | 99.63% | L4 | Class-level support for acute seizures; no disease-specific evidence |
| 4, 5, 6, 8, 9 | Orgasm-induced seizures, startle epilepsy, thinking seizures, audiogenic seizures, eating seizures | 99.63% | L4–L5 | Prediction-only or animal/indirect evidence |
| 10 | Acute encephalopathy with biphasic seizures and late reduced diffusion | 99.47% | L5 | Prediction-only; supportive seizure control at most |

## Clinical Trial Evidence

Currently no related clinical trials are registered for trigeminal nerve neoplasm. No registered trials were found for any of the other top-10 predicted indications either.

## Literature Evidence

For trigeminal nerve neoplasm, currently no related literature is available. The table below shows the most relevant literature for the next-ranked prediction with direct lorazepam data, **insomnia**. Several of these items are not directly about lorazepam, and the design of the older studies could not be confirmed from titles alone.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10220122](https://pubmed.ncbi.nlm.nih.gov/10220122/) | 1999 | Clinical study | Int Clin Psychopharmacol | Lorazepam 0.5 mg three times daily (24-hour treatment) compared with 1.5 mg at bedtime in primary insomnia |
| [3280615](https://pubmed.ncbi.nlm.nih.gov/3280615/) | 1988 | Double-blind crossover study (8 patients) | J Clin Pharmacol | Lorazepam 2 mg performed better than flurazepam 30 mg on most sleep parameters in chronic insomniacs |
| [30625122](https://pubmed.ncbi.nlm.nih.gov/30625122/) | 2018 | Review | Med Lett Drugs Ther | Overview of drugs for chronic insomnia; benzodiazepines are generally limited by tolerance, dependence and next-day impairment |
| [36692463](https://pubmed.ncbi.nlm.nih.gov/36692463/) | 2023 | Meta-analysis | Acta Pharm | Tranquilizers in elderly patients with chronic non-communicable diseases: dose, outcomes and adverse effects |
| [35087274](https://pubmed.ncbi.nlm.nih.gov/35087274/) | 2022 | Review | J Multidiscip Healthc | Efficacy, safety and drug interactions of insomnia therapy in COVID-19 patients |
| [23330992](https://pubmed.ncbi.nlm.nih.gov/23330992/) | 2013 | Review | Expert Opin Drug Metab Toxicol | Pharmacokinetics of anxiolytic drugs |

For seizure-related predictions, the literature mainly supports benzodiazepines, including lorazepam, for acute seizures, seizure clusters and status epilepticus (for example the RAMPART trial, [22335736](https://pubmed.ncbi.nlm.nih.gov/22335736/), and the review [35971024](https://pubmed.ncbi.nlm.nih.gov/35971024/)). This is indirect for reflex seizure subtypes, and status epilepticus is already an original indication.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 250120 | Ativan | Tab 2.5 mg | Not available (authorization cancelled) |
| 2139022 | Ativan | Tab 1 mg | Not available (authorization cancelled) |

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for safety information.

- **Disease contraindications (from knowledge-graph data, not label text)**: liver failure, liver disease, hepatic encephalopathy and hepatic coma; kidney disease; respiratory failure; obstructive sleep apnea and other sleep apnea syndromes; congenital central hypoventilation syndrome; open-angle and angle-closure glaucoma; inherited porphyria and porphyria; substance-related disorder and substance abuse/dependence; gallbladder disease; and several psychiatric entries (neurotic disorder, dysthymic disorder, anxiety disorder). The appearance of anxiety disorder in this list conflicts with its status as an original indication, so these entries should be verified against the official label.
- **Drug interactions**: 100 interactions were identified. Major ones include **morphine** (including liposomal morphine), where combined central nervous system and respiratory depression is the main concern. Moderate ones include bupropion, dronabinol, nabilone, metoclopramide, opium, sibutramine and teduglutide. The remaining listed interactions are minor (mainly antacids such as magnesium and aluminium salts and calcium carbonate) or of unknown severity.
- **Other safety-relevant literature**: propylene glycol toxicity has been reported with intravenous lorazepam infusion (PMID 22926232), and the retrieved literature highlights tolerance, dependence and withdrawal as class concerns.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction, trigeminal nerve neoplasm, has a very high model score (99.87%) but no trials, no literature and no plausible antitumour mechanism, so it is likely a graph artifact. All New Zealand authorizations are cancelled, which adds a regulatory barrier. Of the other predictions, insomnia (L3) and the seizure-related extensions (L4) are mechanistically reasonable but supported only by dated, indirect or class-level evidence, and are best treated as research questions rather than repurposing candidates.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Confirmation of the design and quality of the older insomnia studies (PMIDs 10220122 and 3280615), and a comparison against current insomnia guidance
- Direct clinical evidence for any seizure subtype extension, such as reading epilepsy
- A regulatory availability assessment, since no valid lorazepam authorization currently exists in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


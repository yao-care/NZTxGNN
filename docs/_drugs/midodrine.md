---
layout: default
title: Midodrine
parent: Model Prediction Only (L5)
nav_order: 301
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Midodrine: predicted new indication Variably protease-sensitive prionopathy (evidence level L5). Approved use: Orthostatic hypotension, POTS, multiple system atrophy"
monitor_inn: "MIDODRINE"
predicted_indications: ["variably protease-sensitive prionopathy", "faciodigitogenital syndrome", "attention deficit-hyperactivity disorder", "attention deficit hyperactivity disorder, inattentive type", "sinoatrial node disease", "monogenic obesity", "specific developmental disorder", "obsolete hypertelorism (disease)", "sinoatrial block"]
---

# Midodrine
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

# Midodrine: From Orthostatic Hypotension to Hypotensive Disorder

## One-Sentence Summary

Midodrine was originally used to treat orthostatic hypotension, postural orthostatic tachycardia syndrome (POTS) and multiple system atrophy-related autonomic failure.
The TxGNN model's top-ranked prediction is variably protease-sensitive prionopathy, which has no supporting evidence. The best-supported prediction is **hypotensive disorder** (rank 4), with **1 clinical trial** and **18 publications**. This largely overlaps with the original indication, so it is closer to on-label use than true repurposing.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Orthostatic hypotension, POTS, multiple system atrophy (from the knowledge graph; no approved indication text in the New Zealand licence records) |
| Predicted New Indication | Variably protease-sensitive prionopathy (TxGNN rank 1); the best-evidenced candidate is hypotensive disorder (rank 4) |
| TxGNN Prediction Score | 99.99% (prionopathy); 99.90% (hypotensive disorder) |
| Evidence Level | L5 (prionopathy); L2 (hypotensive disorder) |
| New Zealand Market Status | ✗ Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold (prionopathy); Proceed with Guardrails (hypotensive disorder) |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, midodrine is a prodrug whose active metabolite, desglymidodrine, is a peripheral alpha-1 adrenergic agonist. It raises vascular tone and blood pressure.

**Top-ranked prediction (variably protease-sensitive prionopathy):** No plausible mechanistic link was identified. Nothing connects peripheral vasoconstriction to prion protein pathology. The very high score (99.99%) is a graph-based result with no supporting trials or literature, so it probably reflects network artefacts rather than biology. The same applies to most other top-10 predictions, such as faciodigitogenital syndrome, monogenic obesity, developmental disorders and hypertelorism.

**Best-supported prediction (hypotensive disorder):** There is a strong mechanistic fit. Raising vascular tone directly addresses low blood pressure, consistent with midodrine's original indications. Because of this overlap, the prediction mainly confirms existing use rather than opening a new therapeutic area.

**Other candidates:**
- **ADHD (rank 3) and ADHD, inattentive type (rank 5):** The link is weak. Midodrine acts peripherally and does not appreciably cross the blood-brain barrier.
- **Sinoatrial node disease (rank 6) and sinoatrial block (rank 10):** Midodrine can cause reflex bradycardia, so this is a safety concern rather than a potential benefit.

## Clinical Trial Evidence

Evidence below is for the best-supported candidate, hypotensive disorder. The top-ranked prediction (prionopathy) has no registered trials.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02893553](https://clinicaltrials.gov/study/NCT02893553) | Phase 2 | Completed | 21 | Effects of normalizing blood pressure on cerebral blood flow in hypotensive individuals with spinal cord injury; small but directly relevant to hypotension |

## Literature Evidence

Evidence below is for hypotensive disorder. The top-ranked prediction (prionopathy) has no related literature.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25644760](https://pubmed.ncbi.nlm.nih.gov/25644760/) | 2015 | Randomized trial | Hepatology | Terlipressin plus albumin vs midodrine and octreotide plus albumin in hepatorenal syndrome |
| [38205630](https://pubmed.ncbi.nlm.nih.gov/38205630/) | 2024 | Scientific statement | Hypertension | AHA statement on orthostatic hypotension in adults with hypertension |
| [38123372](https://pubmed.ncbi.nlm.nih.gov/38123372/) | 2024 | Review | Revue Neurologique | Review and expert position statement on orthostatic hypotension |
| [41941194](https://pubmed.ncbi.nlm.nih.gov/41941194/) | 2026 | Review | JAMA Internal Medicine | Management of orthostatic hypotension, including coexisting supine hypertension |
| [42635998](https://pubmed.ncbi.nlm.nih.gov/42635998/) | 2026 | Review | JAMA | Review of postural orthostatic tachycardia syndrome (POTS) |
| [35029940](https://pubmed.ncbi.nlm.nih.gov/35029940/) | 2022 | Review | American Family Physician | Practical approach to orthostatic hypotension |
| [31996627](https://pubmed.ncbi.nlm.nih.gov/31996627/) | 2020 | Review | Continuum | Management of orthostatic hypotension, with emphasis on the neurogenic form |
| [32979782](https://pubmed.ncbi.nlm.nih.gov/32979782/) | 2020 | Review | Autonomic Neuroscience | Pharmacologic treatment of orthostatic hypotension |
| [28050656](https://pubmed.ncbi.nlm.nih.gov/28050656/) | 2017 | Consensus statement | Journal of Neurology | Consensus recommendations for neurogenic orthostatic hypotension and supine hypertension |
| [31524637](https://pubmed.ncbi.nlm.nih.gov/31524637/) | 2021 | Review | American Journal of Therapeutics | Prevention and management of supine hypertension in orthostatic hypotension |

For the other predictions, only two papers were retrieved: one on orthostatic dysregulation in Japan (PMID 42351284, ADHD; cohort) and one on baroreflex dysfunction in spinal cord injury (PMID 24634993, sinoatrial node disease; cohort). Neither shows midodrine efficacy in those conditions.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2632071 | Midodrine Medsurge | Tab 5 mg | Not recorded (status: cancelled) |
| 2632063 | Midodrine Medsurge | Tab 2.5 mg | Not recorded (status: cancelled) |

Both authorizations are cancelled, so availability in New Zealand may be a practical constraint.

## Safety Considerations

- **Contraindications (knowledge-graph based, not label text):** The knowledge graph links midodrine to contraindications in phaeochromocytoma (including adrenal and sympathetic paraganglioma), thyrotoxicosis, hyperthyroidism, Graves disease, and several cancers (duodenum, small intestine, heart, adrenal gland).
- **Drug Interactions:** 81 interactions were identified. Examples include:
  - Moderate: epinephrine, ephedrine, phentermine, bupropion, diethylpropion, mazindol, sibutramine, lorcaserin, isometheptene, trospium, rolapitant, glycerol phenylbutyrate
  - Minor: ranitidine, cimetidine, metformin
- **Cardiovascular concerns noted in the analysis:** Monitor for supine hypertension. Reflex bradycardia is possible, which matters for sinus node disease. Avoid use in severe heart disease, urinary retention or acute renal disease.

Package insert warnings and contraindications were not available, so please refer to the package insert for complete safety information.

## Conclusion and Next Steps

**Decision: Hold (for the top-ranked prediction, variably protease-sensitive prionopathy); Proceed with Guardrails (for hypotensive disorder)**

**Rationale:**
- The highest TxGNN scores (including 99.99% for prionopathy) are not backed by any trials, literature or plausible mechanism, so they should not drive decisions.
- Hypotensive disorder has a sound mechanism, one completed Phase 2 trial and multiple reviews and guidance documents (L2). It is essentially an on-label use rather than new repurposing.

**To proceed, the following is needed:**
- Obtain the Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Obtain detailed mechanism of action data from DrugBank
- Confirm product availability in New Zealand, since both licences are cancelled
- Define a monitoring plan for supine hypertension, bradycardia and interacting drugs
- Do not pursue the prionopathy, ADHD, sinoatrial or other low-evidence predictions without new supporting data
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


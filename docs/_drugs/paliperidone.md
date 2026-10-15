---
layout: default
title: Paliperidone
parent: Model Prediction Only (L5)
nav_order: 346
evidence_level: L5
indication_count: 10
---

# Paliperidone
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

# Paliperidone: From Schizophrenia to Retinal Dystrophy (Top-Ranked Prediction; Best-Supported Alternative: Treatment-Refractory Schizophrenia)

## One-Sentence Summary

Paliperidone is an antipsychotic originally used for schizophrenia and related psychotic disorders.
The TxGNN model's top-ranked prediction is **retinal dystrophy with or without extraocular anomalies**, but it has **0 clinical trials** and **0 publications** behind it, so it is a computational prediction only.
The only prediction with any supporting activity is **treatment-refractory schizophrenia** (rank 10), with **3 Phase 4 trials** and **2 reviews**. That is largely an extension of the existing indication, not true repurposing.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Psychotic disorder, schizophreniform disorder, mental disorder, schizoaffective disorder, schizophrenia (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Retinal dystrophy with or without extraocular anomalies (rank 1) |
| TxGNN Prediction Score | 99.92% |
| Evidence Level | L5 (rank 1). The best-supported alternative, treatment-refractory schizophrenia, is L3 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 5 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, paliperidone is a dopamine D2 and serotonin 5-HT2A receptor antagonist, and this mechanism underlies its efficacy in schizophrenia.

**Rank 1 (retinal dystrophy):** No mechanistic link is established. The score of 0.999 is a purely computational output, and no known pathway connects D2/5-HT2A antagonism to inherited retinal dystrophy. The other top-ranked predictions are similar: several X-linked or syndromic myopias, hydranencephaly, a congenital glycosylation disorder, a polymicrogyria syndrome, Charcot-Marie-Tooth type 1G and atypical glycine encephalopathy. All have no trials or literature and no plausible pharmacological rationale. The high scores likely reflect network similarity in the knowledge graph rather than biology.

**Rank 10 (treatment-refractory schizophrenia):** This has strong biological plausibility, because the same receptor antagonism is the established mechanism in schizophrenia. However, it extends the approved indication to a refractory subgroup. Clozapine remains the standard for treatment resistance, and paliperidone's evidence in this subgroup is limited to Phase 4 comparative or combination studies, with no demonstrated efficacy signal.

---

## Clinical Trial Evidence

No trials are registered for the rank 1 prediction (retinal dystrophy). The following trials relate to treatment-refractory schizophrenia (rank 10):

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT07047651](https://clinicaltrials.gov/study/NCT07047651) | Phase 4 | Active, not recruiting | 84 | Pharmacotherapy combined with recovery-oriented programs in treatment-resistant schizophrenia and bipolar disorder. Paliperidone's specific contribution is unclear, and no results are available. |
| [NCT06060886](https://clinicaltrials.gov/study/NCT06060886) | Phase 4 | Unknown | 244 | Open-label randomized trial of aripiprazole vs paliperidone/risperidone with multi-omics data in first-episode psychosis. No results reported. |
| [NCT05741502](https://clinicaltrials.gov/study/NCT05741502) | Phase 4 | Terminated | 5 | Exploratory immune and inflammatory marker comparison of clozapine vs non-clozapine antipsychotics. Terminated with n=5, so uninformative for efficacy. |

---

## Literature Evidence

No literature is available for the rank 1 prediction. The following publications relate to treatment-refractory schizophrenia (rank 10):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31648341](https://pubmed.ncbi.nlm.nih.gov/31648341/) | 2019 | Review | Actas Esp Psiquiatr | Psychopharmacology of schizoaffective disorder. Treatment relies mainly on antipsychotics, with few condition-specific studies and no specific guidelines. |
| [23364281](https://pubmed.ncbi.nlm.nih.gov/23364281/) | 2013 | Review | Curr Opin Psychiatry | Efficacy and tolerability of psychosocial and drug treatments in early-onset schizophrenia spectrum disorders, with guidance on dosing, switching and adverse effect monitoring. |

---

## New Zealand Market Information

All five licences are cancelled, and no approved indication text is available for them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2426935 | Invega Sustenna | Inj 75 mg syringe | Not available (cancelled) |
| 2426951 | Invega Sustenna | Inj 150 mg syringe | Not available (cancelled) |
| 2426927 | Invega Sustenna | Inj 50 mg syringe | Not available (cancelled) |
| 2426919 | Invega Sustenna | Inj 25 mg syringe | Not available (cancelled) |
| 2426943 | Invega Sustenna | Inj 100 mg syringe | Not available (cancelled) |

---

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for safety information.

**Contraindication signals from the knowledge graph** (not label text) include:
- **Cardiovascular:** long QT syndrome (including familial), torsades de pointes, orthostatic hypotension, postural orthostatic tachycardia syndrome, hypotensive disorder, myocardial infarction, coronary thrombosis
- **Metabolic:** diabetes mellitus, diabetic ketoacidosis, hyperglycemia, hyperlipidemia (including familial), obesity (including monogenic), potassium deficiency
- **Neurological:** epilepsy, basal ganglia disease, multiple system atrophy, focal hand dystonia, blepharospasm
- **Haematological:** neutropenia, agranulocytosis, hemoglobinopathy
- **Other:** Crohn disease and other inflammatory bowel disease, female breast carcinoma, impaired renal function

**Drug interactions:** 100 interactions were identified, and the first 20 are shown below.
- **Major:** bupropion, morphine
- **Moderate:** antidiabetic agents (acarbose, albiglutide, alogliptin, metformin, pioglitazone, canagliflozin, chlorpropamide), anticholinergic or antimuscarinic agents (atropine, hyoscyamine, glycopyrronium), famotidine, epinephrine, hydrocortisone, amphotericin B (including lipid complex), loperamide, lorcaserin, bisacodyl

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The rank 1 prediction (retinal dystrophy) rests only on a computational score, with no trials, no literature and no plausible mechanism. Treatment-refractory schizophrenia has some Phase 4 activity but is an extension of the existing indication, and the evidence shows no efficacy signal. New Zealand licences are all cancelled, and the safety data has important gaps.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Preclinical or mechanistic evidence linking paliperidone to retinal dystrophy, if that prediction is to be pursued
- Results from the Phase 4 trials, and comparative data against clozapine, for the treatment-refractory schizophrenia question
- Clarification of why all New Zealand licences were cancelled, and of route compatibility for any new indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


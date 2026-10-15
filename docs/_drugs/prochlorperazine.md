---
layout: default
title: Prochlorperazine
parent: Model Prediction Only (L5)
nav_order: 386
evidence_level: L5
indication_count: 10
---

# Prochlorperazine
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

# Prochlorperazine: From Schizophrenia to Retinal Dystrophy (Top TxGNN Prediction)

## One-Sentence Summary

Prochlorperazine is a phenothiazine antipsychotic and antiemetic, originally used for schizophrenia.
The TxGNN model's top-ranked prediction is **retinal dystrophy with or without extraocular anomalies**, but **no clinical trials and no publications** support it. The only candidate among the top 10 with any literature is **manic bipolar affective disorder** (7 publications, mostly historical, no trials), which is a research question rather than an actionable repurposing lead.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Schizophrenia |
| Predicted New Indication | Retinal dystrophy with or without extraocular anomalies |
| TxGNN Prediction Score | 99.998% |
| Evidence Level | L5 |
| New Zealand Market Status | ✓ Marketed |
| Number of Authorizations | 3 (1 valid, 2 cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, prochlorperazine is a phenothiazine dopamine D2 receptor antagonist, and its efficacy in schizophrenia is established.

For the top prediction, retinal dystrophy, no plausible mechanistic link was identified. This is a group of genetic retinal degenerations, and a symptomatic D2 antagonist has no known disease-modifying role. The very high score (99.998%) is most likely an artifact of the knowledge graph rather than evidence of a real pharmacological connection.

The other top-ranked predictions show the same pattern. They are mostly rare congenital or metabolic conditions (hydranencephaly, congenital disorder of glycosylation, polymicrogyria, atypical glycine encephalopathy, Charcot-Marie-Tooth type 1G) and several forms of myopia. For myopia, dopamine signalling is implicated in eye growth, but the literature generally suggests dopamine agonism is protective, so D2 antagonism is of doubtful benefit.

The one biologically plausible candidate is **manic bipolar affective disorder** (rank 10, score 99.979%). Dopamine antagonism is how antipsychotics control mania and agitation, which is consistent with the original schizophrenia indication. However, newer and better-evidenced antipsychotics are available, so the incremental value of prochlorperazine is uncertain.

## Clinical Trial Evidence

Currently no related clinical trials registered, for the top prediction or for any of the other nine predictions.

## Literature Evidence

No literature is available for the top prediction (retinal dystrophy). The table below covers the only candidate with publications, manic bipolar affective disorder.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [19461391](https://pubmed.ncbi.nlm.nih.gov/19461391/) | 2009 | Review | J Psychiatr Pract | Use and safety of antipsychotic drugs during pregnancy, in the context of schizophrenia and related disorders |
| [26819726](https://pubmed.ncbi.nlm.nih.gov/26819726/) | 2015 | Pharmacovigilance/Cohort | J Pharm Health Care Sci | Hyperglycaemic adverse events with antipsychotics in FDA spontaneous adverse event reports |
| [14242542](https://pubmed.ncbi.nlm.nih.gov/14242542/) | 1965 | Review | Obstet Gynecol | Approach to severe psychological disorders of the puerperium |
| [14233737](https://pubmed.ncbi.nlm.nih.gov/14233737/) | 1964 | Survey/Case series | Am J Psychiatry | Electroshock therapy combined with phenothiazines and reserpine |
| [14222730](https://pubmed.ncbi.nlm.nih.gov/14222730/) | 1964 | Case series | L'Encephale | Psychodysleptic manifestations during treatment with psycholeptics |
| [14062306](https://pubmed.ncbi.nlm.nih.gov/14062306/) | 1963 | Review | La Tunisie Medicale | Agitation: emergency treatment approach for the general practitioner |
| [13617778](https://pubmed.ncbi.nlm.nih.gov/13617778/) | 1959 | Case report | Ann Med Psychol | Confusional dream-like attack caused by prochlorperazine in a mild manic-depressive patient |

None of these is a controlled efficacy study. Two are safety-focused, and the rest are historical clinical notes.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2700131 | Prochlorperazine maleate (Brown & Burk) | Tab 3 mg buccal | Valid; indication text not available |
| 255335 | Stemetil | Inj 12.5 mg per ml, 1 ml | Cancelled; indication text not available |
| 2528886 | Nausafix | Tab 5 mg | Cancelled; indication text not available |

## Safety Considerations

- **Contraindications flagged in the knowledge graph** (drug-disease associations, not package insert text): cardiovascular conditions (hypertension, orthostatic hypotension, long QT syndrome, torsades de pointes, coronary artery disease, myocardial infarction); neurological conditions (epilepsy, basal ganglia disease, multiple system atrophy, blepharospasm, focal hand dystonia); blood disorders (neutropenia, agranulocytosis, thrombocytopenia); liver, kidney and gallbladder disease; hyperglycaemia; benign prostatic hyperplasia; gastric ulcer; obesity; substance abuse/dependence; and female breast carcinoma.
- **Drug interactions**: 100 interactions were found. Examples include two **Major** interactions, with **bupropion** and **potassium citrate**. Many **Moderate** interactions were also found, including antidiabetics (metformin, pioglitazone, alogliptin, canagliflozin, chlorpropamide, acarbose, albiglutide), anticholinergics (atropine, hyoscyamine, glycopyrronium), morphine, epinephrine, clarithromycin, loperamide and lorcaserin.
- Safety signals in the literature include hyperglycaemic adverse events with antipsychotics.

Package insert warnings and contraindications were not available, so please refer to the package insert for authoritative safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction (retinal dystrophy) is supported only by a model score, with no trials, no literature and no plausible mechanism, so it is likely a graph artifact. Manic bipolar affective disorder is biologically plausible but has only historical, low-quality evidence, and better-established alternatives exist, so it should be treated as a research question rather than a repurposing candidate.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Detailed mechanism of action data
- For manic bipolar disorder: a systematic literature review and comparison against current antipsychotic standards of care
- For any candidate: preclinical or mechanistic evidence supporting a link before further investment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


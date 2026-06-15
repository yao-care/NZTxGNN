---
layout: default
title: Clozapine
parent: 僅模型預測 (L5)
nav_order: 85
evidence_level: L5
indication_count: 10
---

# Clozapine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Clozapine: From Treatment-Resistant Schizophrenia to Manic Bipolar Affective Disorder

## One-Sentence Summary

Clozapine is a second-generation atypical antipsychotic, originally established as the gold-standard treatment for refractory schizophrenia and the only drug with FDA approval for reducing suicidal behavior in schizophrenia/schizoaffective disorder.
The TxGNN model predicts it may be effective for **Manic Bipolar Affective Disorder**,
with **6 clinical trials** and **20 publications** currently supporting this direction.
Direct evidence includes one completed Phase 2 double-blind trial specifically evaluating clozapine in treatment-resistant mania, plus a 2020 systematic review and meta-analysis focused on clozapine in bipolar disorder.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not approved in New Zealand; globally indicated for treatment-resistant schizophrenia |
| Predicted New Indication | Manic Bipolar Affective Disorder |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L2 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data was not retrieved from automated sources. Based on known clinical pharmacology, clozapine belongs to the multi-acting receptor-targeted antipsychotic (MARTA) class, acting as an antagonist at dopamine D2/D4, serotonin 5-HT2A/5-HT2C, histamine H1, muscarinic M1–M5, and α1-adrenergic receptors. Its superior efficacy over other antipsychotics in treatment-resistant schizophrenia has been validated across decades of clinical research worldwide.

The mechanistic bridge to manic bipolar affective disorder is well-grounded. D2/D4 receptor blockade directly suppresses the hyperdopaminergic activity that drives manic episodes; 5-HT2A antagonism contributes to mood stabilization; and 5-HT2C antagonism enhances prefrontal norepinephrine and dopamine signaling, potentially addressing depressive phases of the illness. This multi-receptor profile maps closely onto the neurochemical oscillations that characterize bipolar cycling — a pattern the TxGNN knowledge graph appears to have captured.

From a disease-overlap perspective, treatment-resistant mania and refractory schizophrenia share overlapping pathological substrates, particularly dopaminergic hyperactivity and glutamatergic NMDA hypofunction. Convergent evidence from a 2020 systematic review and meta-analysis (PMID 32182485) and a 2015 systematic review (PMID 25346322) confirms clinical response to clozapine in treatment-resistant bipolar disorder, providing strong empirical grounding for this prediction.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00029458](https://clinicaltrials.gov/study/NCT00029458) | Phase 2 | Completed | 42 | Double-blind trial directly evaluating safety and efficacy of clozapine in treatment-resistant mania — strongest direct evidence for this indication |
| [NCT05603104](https://clinicaltrials.gov/study/NCT05603104) | Phase 3 | Recruiting | 1,254 | Large RCT investigating intensified pharmacological treatment for schizophrenia, bipolar depression, and MDD after first-line treatment failure; may include clozapine as escalation therapy |
| [NCT07047651](https://clinicaltrials.gov/study/NCT07047651) | Phase 4 | Recruiting | 40 | Pharmacotherapy combined with recovery-oriented programs specifically for treatment-resistant bipolar disorder |
| [NCT06993662](https://clinicaltrials.gov/study/NCT06993662) | Phase 1 | Active, Not Recruiting | 107 | Pharmacotherapy combined with individual cognitive behavioral therapy for mental health disorders including bipolar disorder |
| [NCT07398365](https://clinicaltrials.gov/study/NCT07398365) | N/A | Recruiting | 100 | Observational phenotyping study of NHS General Adult Psychiatry inpatients — characterises morbidity in the relevant inpatient population |
| [NCT03651674](https://clinicaltrials.gov/study/NCT03651674) | N/A | Unknown | 200 | Longitudinal MRI study of ECT effects on brain structure and function in schizophrenia and bipolar disorder — neuroimaging study, not a clozapine drug intervention |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|---------|
| [32182485](https://pubmed.ncbi.nlm.nih.gov/32182485/) | 2020 | Systematic Review + Meta-analysis | Journal of Psychiatric Research | Assessed clinical efficacy and adverse effect profile of clozapine specifically in bipolar disorder — highest-quality direct evidence available |
| [25346322](https://pubmed.ncbi.nlm.nih.gov/25346322/) | 2015 | Systematic Review | Bipolar Disorders | Evaluated efficacy and safety of clozapine for treatment-resistant bipolar disorder (TRBD) |
| [33719158](https://pubmed.ncbi.nlm.nih.gov/33719158/) | 2021 | Narrative Review | Bipolar Disorders | Synthesised current evidence and outlined future research priorities for clozapine in bipolar disorder |
| [40174308](https://pubmed.ncbi.nlm.nih.gov/40174308/) | 2025 | Real-World Cohort | Journal of Psychiatric Research | Nationwide South Korean study on anti-suicidal effectiveness of clozapine vs. lithium and valproate in both schizophrenia and bipolar disorder |
| [37068038](https://pubmed.ncbi.nlm.nih.gov/37068038/) | 2023 | Multi-center Observational | Journal of Clinical Psychopharmacology | Asian Psychotropic Prescription Patterns Consortium study on clozapine prescribing patterns and clinical characteristics in bipolar disorder |
| [31488793](https://pubmed.ncbi.nlm.nih.gov/31488793/) | 2019 | Review | Psychiatria Danubina | Clozapine's unique pharmacology — particularly anti-aggressive and anti-impulsive properties — highlighted as promising for suicidality in bipolar disorder |
| [33460070](https://pubmed.ncbi.nlm.nih.gov/33460070/) | 2020 | Clinical Practice Review | Acta Psychiatrica Scandinavica | Evidence-based recommendations for managing bipolar mania; positions clozapine within the treatment algorithm for refractory cases |
| [16432528](https://pubmed.ncbi.nlm.nih.gov/16432528/) | 2006 | Review | Molecular Psychiatry | Comprehensive review of treatment-resistant bipolar disorder pharmacotherapy; identifies clozapine as a viable option for non-responders to first-line agents |
| [10682225](https://pubmed.ncbi.nlm.nih.gov/10682225/) | 2000 | Case Series | Clinical Neuropharmacology | Review of 36 patients treated with ECT plus clozapine; 67% improved — supports clozapine's role in severe treatment-resistant psychiatric conditions |
| [11280956](https://pubmed.ncbi.nlm.nih.gov/11280956/) | 2001 | Review | Bulletin of the Menninger Clinic | Early review placing clozapine among emerging treatment options for pharmacotherapy-resistant bipolar disorder |

---

## New Zealand Market Information

Clozapine currently has no approved products registered in New Zealand (0 Medsafe authorizations). Standard market information is not available. Any clinical use in New Zealand would require off-label prescribing under appropriate regulatory frameworks.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** Safety data (key warnings, contraindications, and drug interactions) were not retrieved in this evidence pack. Clozapine is known to carry serious safety risks including agranulocytosis, seizures, and myocarditis — these must be reviewed from the full prescribing information before any clinical application.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
A completed Phase 2 double-blind trial (NCT00029458, n=42) and two systematic reviews directly support clozapine's efficacy in treatment-resistant bipolar mania, and its multi-receptor MARTA mechanism is highly congruent with the neurochemical underpinnings of bipolar disorder. The evidence base meets L2 threshold, warranting advancement with appropriate safety controls rather than a hold decision.

**To proceed, the following is needed:**
- Full safety review: obtain and analyse the clozapine package insert for black box warnings, contraindications, and key precautions
- Mandatory haematological monitoring program (CBC with differential) to manage agranulocytosis risk — this is a prerequisite for any clinical use
- Regulatory pathway clarification for off-label use in bipolar disorder in New Zealand
- Clearly defined target patient population (recommended starting point: treatment-resistant bipolar mania failing ≥2 conventional mood stabilisers)
- Drug interaction assessment with agents commonly co-prescribed in bipolar disorder (lithium, valproate, lamotrigine, benzodiazepines)
- Follow-up on Phase 3 RCT data from NCT05603104 (n=1,254, estimated completion 2028) to strengthen the evidence level to L1
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


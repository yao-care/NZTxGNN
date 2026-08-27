---
layout: default
title: Haloperidol
parent: 僅模型預測 (L5)
nav_order: 164
evidence_level: L5
indication_count: 10
---

# Haloperidol
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

# Haloperidol: From Psychotic Disorders to Manic Episodes of Bipolar Disorder

## One-Sentence Summary

> Haloperidol is a first-generation antipsychotic long used to control psychosis and schizophrenia (the evidence pack's structured `original_indications` field is empty, but this is a well-established clinical use). Among the ten TxGNN-predicted indications supplied, only **Manic Bipolar Affective Disorder** is backed by substantial evidence — **9 clinical trials and 20 publications** — while the model's algorithmically top-ranked candidates (rare congenital/ophthalmologic/neurodevelopmental disorders) had **zero** supporting trials or relevant literature and are assessed in the source data itself as likely noise matches. This report therefore focuses on the one indication with real evidentiary support.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the evidence pack's structured field (haloperidol is a long-established first-generation/typical antipsychotic for psychosis and schizophrenia) |
| Predicted New Indication | Manic Bipolar Affective Disorder |
| TxGNN Prediction Score | 99.83% (model rank 2099; this was the 10th of 10 candidates listed, selected over higher-scoring but evidence-free candidates — see note below) |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

**Note on candidate selection:** TxGNN's single highest-scoring prediction for haloperidol was "congenital disorder of glycosylation with defective fucosylation" (99.91%), followed by several rare ophthalmologic and neurodevelopmental disorders (ranks 2–9). All of these returned **no clinical trials and no relevant literature**, and their own mechanistic-link assessments in the evidence pack explicitly flag them as implausible model noise (no known connection between haloperidol's D2/5-HT2/α1 receptor targets and the disease biology). Manic bipolar affective disorder was the only candidate with a coherent mechanism and confirmatory clinical evidence, so it is the subject of this report.

---

## Why is This Prediction Reasonable?

Haloperidol is a potent first-generation D2 dopamine receptor antagonist. Manic and psychotic symptoms are associated with excess mesolimbic dopaminergic transmission, and D2 blockade rapidly reduces agitation and psychotic features — a mechanism that is clinically well established rather than a novel hypothesis.

Strictly speaking, this is less a "repurposing discovery" than a confirmation of existing clinical practice: haloperidol is already used as add-on/adjunct therapy alongside mood stabilizers (lithium, valproate) for acute manic episodes, and is a common active comparator arm in bipolar-mania trials of newer antipsychotics (risperidone, olanzapine, aripiprazole). The gap is administrative — the source registry's `original_indications` field for this drug record is empty — not clinical or mechanistic.

Because haloperidol's antipsychotic effect and its established role in acute mania are pharmacologically continuous, the TxGNN score is well supported by both mechanism and a substantial body of Phase 2/3 trial and meta-analytic literature, unlike the top nine algorithmic candidates.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00253149](https://clinicaltrials.gov/study/NCT00253149) | Phase 3 | Completed | 158 | Risperidone vs. placebo vs. haloperidol as add-on to mood stabilizers for manic episodes; haloperidol used as active comparator. |
| [NCT00253162](https://clinicaltrials.gov/study/NCT00253162) | Phase 3 | Completed | 439 | Flexible-dose risperidone vs. placebo vs. haloperidol in Bipolar I manic episodes; haloperidol maintenance effectiveness assessed at 12 weeks. |
| [NCT00129220](https://clinicaltrials.gov/study/NCT00129220) | Phase 3 | Completed | 224 | Double-blind, placebo- and haloperidol-controlled trial confirming olanzapine efficacy in manic/mixed Bipolar I episodes. |
| [NCT00126009](https://clinicaltrials.gov/study/NCT00126009) | Phase 2 | Completed | 120 | Open, randomized 3-month trial comparing valproate-amisulpride vs. valproate-haloperidol in Bipolar I manic episode. |
| [NCT04327843](https://clinicaltrials.gov/study/NCT04327843) | Phase 3 | Completed | 22 | Long-acting injectable antipsychotic + adherence-focused behavioral program for chronic psychotic disorders in Tanzania. |
| [NCT06049953](https://clinicaltrials.gov/study/NCT06049953) | N/A | Recruiting | 200 | Observational study of antenatal antipsychotic exposure on maternal psychiatric course and infant development. |
| [NCT00097266](https://clinicaltrials.gov/study/NCT00097266) | Phase 3 | Completed | 615 | Aripiprazole monotherapy vs. placebo for acute mania; no explicit haloperidol treatment arm. |
| [NCT00767715](https://clinicaltrials.gov/study/NCT00767715) | Phase 4 | Terminated | 11 | Olanzapine vs. conventional antipsychotics (incl. haloperidol) for acute mania in Sweden; terminated early, small sample. |
| [NCT03541031](https://clinicaltrials.gov/study/NCT03541031) | N/A | Unknown | 120 | Micronutrient/fish-oil supplementation as adjunct to conventional bipolar medication; no direct haloperidol focus. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [22134043](https://pubmed.ncbi.nlm.nih.gov/22134043/) | 2012 | RCT | Journal of Affective Disorders | Randomized, double-blind, placebo- and haloperidol-controlled study confirming olanzapine efficacy in Japanese patients with manic/mixed Bipolar I episodes. |
| [369472](https://pubmed.ncbi.nlm.nih.gov/369472/) | 1979 | RCT | Archives of General Psychiatry | Double-blind controlled trial: lithium plus haloperidol vs. placebo plus haloperidol in excited schizoaffective disorder; modest additional benefit from lithium. |
| [34642461](https://pubmed.ncbi.nlm.nih.gov/34642461/) | 2022 | Systematic Review / Network Meta-analysis | Molecular Psychiatry | Network meta-analysis of double-blind RCTs comparing efficacy, tolerability, and safety of pharmacologic treatments (including haloperidol) for acute bipolar mania. |
| [33460070](https://pubmed.ncbi.nlm.nih.gov/33460070/) | 2020 | Review | Acta Psychiatrica Scandinavica | Evidence-based treatment recommendations for bipolar mania, covering mood stabilizer and antipsychotic (including haloperidol) selection. |
| [18344731](https://pubmed.ncbi.nlm.nih.gov/18344731/) | 2008 | Systematic Review | Journal of Clinical Psychopharmacology | Systematic review of antipsychotic-induced extrapyramidal side effects in bipolar disorder and schizophrenia, relevant to haloperidol's tolerability profile. |
| [27151529](https://pubmed.ncbi.nlm.nih.gov/27151529/) | 2016 | Systematic Review / Meta-analysis | Human Psychopharmacology | Systematic review of pharmacologic treatment for acute agitation in psychotic and bipolar disorder. |
| [36789916](https://pubmed.ncbi.nlm.nih.gov/36789916/) | 2023 | Review | BMJ Mental Health | Comparison of antipsychotic dose equivalents between acute mania and schizophrenia. |
| [22070611](https://pubmed.ncbi.nlm.nih.gov/22070611/) | 2012 | Review | CNS Neuroscience & Therapeutics | Discusses adding haloperidol/other antipsychotics for lithium/valproate/carbamazepine partial responders in refractory bipolar disorder. |
| [19454110](https://pubmed.ncbi.nlm.nih.gov/19454110/) | 2007 | Review | BMJ Clinical Evidence | General overview of bipolar disorder epidemiology, course, and treatment options. |
| [3312180](https://pubmed.ncbi.nlm.nih.gov/3312180/) | 1987 | Controlled Study | The Journal of Clinical Psychiatry | Double-blind controlled comparison of clonazepam vs. lithium vs. haloperidol in acute mania. |

---

## New Zealand Market Information

Haloperidol currently has **no registered authorizations** in this dataset (market status: Not Marketed, 0 licenses on file). No product-level formulation or approved-indication data is available to tabulate.

---

## Safety Considerations

Please refer to the package insert for safety information. No structured warnings, contraindications, or drug-interaction data are currently available in this evidence pack (DDI query returned no results).

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple completed Phase 3 RCTs with direct haloperidol treatment/comparator arms (NCT00253149, NCT00253162, NCT00129220), reinforced by a 2022 network meta-analysis, support haloperidol's efficacy in acute bipolar mania — satisfying L1 evidence criteria. However, this reflects confirmation of an already-established clinical use rather than a novel repurposing discovery, and neither local (New Zealand/TFDA) regulatory status nor safety labeling data are currently available.

**To proceed, the following is needed:**
- Official package insert / warnings and contraindications (currently blocking — DG001)
- Confirmed original-indication registry data for haloperidol to correct the empty `original_indications` field
- Drug-drug interaction dataset (current DDI query returned no results)
- Evaluation of a New Zealand registration pathway, given current "Not Marketed" status
- Note: TxGNN's top nine algorithmically-ranked candidates for haloperidol lack any supporting evidence and should not be pursued without independent mechanistic validation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


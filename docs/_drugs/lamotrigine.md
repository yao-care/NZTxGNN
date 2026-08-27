---
layout: default
title: Lamotrigine
parent: 僅模型預測 (L5)
nav_order: 193
evidence_level: L5
indication_count: 9
---

# Lamotrigine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
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

Using no additional skill — this is a self-contained report-authoring task fully specified by the prompt template; I'll write directly.

Note on indication selection: `predicted_indications[0]` (trigeminal nerve neoplasm) carries score-rank #1 but its own `repurposing_rationale` flags a label/evidence mismatch (Hold, L5, no MOA relevance) — the cited literature discusses tumor-induced neuralgia symptoms, not neoplasm treatment. `predicted_indications[1]` (trigeminal neuralgia) has real comparative trial evidence (L2, "Proceed with Guardrails"). I'm featuring trigeminal neuralgia as the report subject since that's the candidate the evidence actually supports — presenting the neoplasm claim as primary would misrepresent the underlying data.

---

# Lamotrigine: From Epilepsy to Trigeminal Neuralgia

## One-Sentence Summary

> Lamotrigine is a well-established anticonvulsant (sodium-channel blocker) originally developed for epilepsy and later extended to bipolar disorder maintenance treatment.
> The TxGNN model predicts it may be effective for **Trigeminal Neuralgia**,
> with **4 clinical trials** (including a direct head-to-head trial against carbamazepine) and **19 publications** currently supporting this direction.
>
> *Note: TxGNN's single highest-scoring prediction was "trigeminal nerve neoplasm," but the evidence retrieved for that label consists only of tumor-related facial pain and radiosurgery reports with no oncologic relevance — the model has essentially confused the neoplasm label with the neuralgia symptom it can cause. That candidate is scored Hold/L5 and is not carried forward here.*

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Epilepsy (anticonvulsant); NZ regulatory filing not available — see Market Status |
| Predicted New Indication | Trigeminal Neuralgia |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data from DrugBank is not available (data gap DG002). Based on the mechanistic evidence attached to this prediction, lamotrigine acts as a voltage-gated sodium channel blocker that inhibits glutamate release, reducing abnormal neuronal firing in the trigeminal ganglion and brainstem nuclei.

Epilepsy and trigeminal neuralgia share a common pathophysiological substrate: both involve paroxysmal, hyperexcitable neuronal discharge. The first-line drugs for trigeminal neuralgia — carbamazepine and oxcarbazepine — are themselves sodium-channel-blocking anticonvulsants, the same pharmacological class as lamotrigine. This class-level mechanistic overlap is why lamotrigine has been studied as a second-line or add-on agent for trigeminal neuralgia, and why the European Academy of Neurology (EAN) guideline (PMID 30860637) includes it among treatment options when first-line agents fail or are not tolerated.

Two completed trials directly tested lamotrigine against carbamazepine or placebo in trigeminal neuralgia patients (NCT00913107, NCT00203229), lending direct — though small-sample — clinical support to the mechanistic rationale, beyond simple class-effect extrapolation.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00913107](https://clinicaltrials.gov/study/NCT00913107) | Phase 2/3 | Completed | 21 | Direct comparison of lamotrigine efficacy and safety versus carbamazepine in trigeminal neuralgia patients. |
| [NCT00203229](https://clinicaltrials.gov/study/NCT00203229) | NA | Completed | 20 | Double-blind, placebo-controlled add-on study of Lamictal (lamotrigine) evaluating efficacy in reducing TN attack frequency and safety. |
| [NCT00243152](https://clinicaltrials.gov/study/NCT00243152) | NA | Completed | 6 | fMRI-based exploratory study of lamotrigine's effect on neuropathic facial pain/neuralgia; mechanistic imaging endpoint, not a efficacy trial. |
| [NCT04996199](https://clinicaltrials.gov/study/NCT04996199) | Phase 4 | Unknown | 132 | Compares carbamazepine vs. oxcarbazepine in TN; does not include lamotrigine arm — included only as same-class indirect reference. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [30860637](https://pubmed.ncbi.nlm.nih.gov/30860637/) | 2019 | Guideline | European Journal of Neurology | EAN guideline on trigeminal neuralgia management; anticonvulsants including lamotrigine positioned as options beyond first-line therapy. |
| [37892981](https://pubmed.ncbi.nlm.nih.gov/37892981/) | 2023 | Systematic Review | Biomedicines | Umbrella review of drugs used for TN, evaluating efficacy and side effects across anticonvulsants including lamotrigine. |
| [21621166](https://pubmed.ncbi.nlm.nih.gov/21621166/) | 2011 | Comparative Study | Journal of the Chinese Medical Association | Direct comparative study of lamotrigine vs. carbamazepine efficacy and side-effect profile in TN patients (companion publication to NCT00913107). |
| [30081317](https://pubmed.ncbi.nlm.nih.gov/30081317/) | 2018 | Case Report | Multiple Sclerosis and Related Disorders | Refractory TN in a multiple sclerosis patient successfully controlled with pregabalin plus lamotrigine combination therapy. |
| [34108244](https://pubmed.ncbi.nlm.nih.gov/34108244/) | 2021 | Review | Practical Neurology | Practical diagnostic and management guide for TN, covering pharmacological and surgical options. |
| [31908187](https://pubmed.ncbi.nlm.nih.gov/31908187/) | 2020 | Review | Molecular Pain | Overview of TN pathophysiology through pharmacological treatment, including sodium-channel-targeted agents. |
| [38870050](https://pubmed.ncbi.nlm.nih.gov/38870050/) | 2024 | Review | Expert Review of Neurotherapeutics | Update on TN pharmacotherapy, noting limitations of carbamazepine/oxcarbazepine and discussing alternative anticonvulsants. |
| [39365662](https://pubmed.ncbi.nlm.nih.gov/39365662/) | 2025 | Cohort | Pain | Nationwide Danish disease-trajectory study identifying comorbidities temporally associated with TN (7.2M individuals). |
| [38246671](https://pubmed.ncbi.nlm.nih.gov/38246671/) | 2024 | Review | No Shinkei Geka (Neurological Surgery) | Pharmacological treatment review for TN; notes lamotrigine as an off-label alternative when carbamazepine is not tolerated (Japan). |
| [30178160](https://pubmed.ncbi.nlm.nih.gov/30178160/) | 2018 | Review | Drugs | Review of current and innovative pharmacological options for typical and atypical TN. |

## New Zealand Market Information

Lamotrigine currently has **no marketed products or authorizations recorded** in New Zealand (`market_status: 未上市`, 0 licenses on file). Regulatory filing and package-insert data would need to be sourced from Medsafe directly before any local development pathway can be assessed.

## Safety Considerations

Please refer to the package insert for safety information — no structured warnings, contraindications, or drug-interaction data were returned for this candidate (DG001, Blocking: TFDA/Medsafe package insert not yet obtained).

One relevant safety signal did surface independently in the literature search performed for a related seizure-indication candidate: a 2025 target-trial study, [40499085](https://pubmed.ncbi.nlm.nih.gov/40499085/) (*Neurology*), examined a possible association between lamotrigine and ventricular arrhythmias versus levetiracetam. This is not part of the structured safety dataset but should be incorporated once formal safety data collection (DG001) is completed.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Two completed trials directly comparing lamotrigine to carbamazepine/placebo in trigeminal neuralgia, reinforced by an EAN guideline listing it as a treatment option and a matching class-mechanism rationale, support proceeding — but sample sizes (n=20–21) are too small to be conclusive, and no New Zealand regulatory or safety-label data currently exist for this drug.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications, DDI) — currently blocking (DG001)
- Confirmed DrugBank mechanism-of-action record (DG002)
- A larger, adequately powered confirmatory RCT in trigeminal neuralgia
- Assessment of the recent lamotrigine–cardiac arrhythmia signal (PMID 40499085) before advancing to clinical use
- Clarification of New Zealand market/registration pathway given current "not marketed" status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


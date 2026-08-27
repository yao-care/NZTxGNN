---
layout: default
title: Paliperidone
parent: 僅模型預測 (L5)
nav_order: 263
evidence_level: L5
indication_count: 10
---

# Paliperidone
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

# Paliperidone: From Schizophrenia to Treatment-Refractory Schizophrenia

## One-Sentence Summary

> Paliperidone is an atypical antipsychotic (the major active metabolite of risperidone) used to treat schizophrenia and schizoaffective disorder.
> Among 10 TxGNN-predicted indications, 9 (ranks 1–9, including "retinal dystrophy," "myopia," and various congenital syndromes) show **no mechanistic plausibility and no drug-relevant literature** — manual review confirms the retrieved literature for rank 1 is a false-positive keyword match unrelated to paliperidone pharmacology.
> The only prediction with real supporting evidence is **Treatment-Refractory Schizophrenia** (rank 10), backed by **4 clinical trials** and **2 publications**, and this report focuses on that candidate.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Schizophrenia / schizoaffective disorder (based on known antipsychotic classification; not captured in New Zealand registry data as the drug is unmarketed there) |
| Predicted New Indication | Treatment-Refractory Schizophrenia |
| TxGNN Prediction Score | 99.80% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed original MOA data from DrugBank is flagged as a data gap. Based on information available from the evidence pack, paliperidone is the major active metabolite of risperidone, and its core pharmacological mechanism is antagonism at D2 dopamine and 5-HT2A serotonin receptors — the standard target class for antipsychotic treatment of schizophrenia.

The predicted new indication, treatment-refractory schizophrenia, is not a distinct disease but a treatment-response subtype of the original indication. Since paliperidone's mechanism is already the front-line pharmacological approach for schizophrenia broadly, extending its evaluation to the refractory subpopulation is mechanistically direct rather than speculative.

However, current clinical guidelines reserve clozapine as the preferred agent for treatment-refractory schizophrenia due to its established superiority in this population. Existing evidence for paliperidone in the refractory subgroup is limited to Phase 4 post-marketing studies rather than pivotal Phase 3 head-to-head trials against clozapine, which tempers confidence despite the strong mechanistic rationale.

**Note on the other 9 TxGNN predictions:** Ranks 1–9 (retinal dystrophy, X-linked myopia, syndromic myopia, hydranencephaly, congenital glycosylation disorders, CMT1G, glycine encephalopathy, etc.) were screened out. Manual review of the literature retrieved for rank 1 found all 15 papers concern orbital/ophthalmologic anatomy and congenital eye disorders with no relationship to paliperidone or antipsychotic pharmacology — a keyword/disease-name false match. Ranks 2–9 returned zero clinical trials and zero literature. None of these 9 candidates are recommended for further evaluation.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01860781](https://clinicaltrials.gov/study/NCT01860781) | Phase 4 | Completed | 30 | Prospective naturalistic case series evaluating paliperidone palmitate effectiveness across three schizophrenia patient groups |
| [NCT07047651](https://clinicaltrials.gov/study/NCT07047651) | Phase 4 | Recruiting | 40 | Combines pharmacotherapy with recovery-oriented programs for treatment-resistant schizophrenia and treatment-resistant bipolar disorder |
| [NCT05741502](https://clinicaltrials.gov/study/NCT05741502) | Phase 4 | Terminated | 5 | Compared clozapine vs. non-clozapine antipsychotics (paliperidone as a non-clozapine comparator) on inflammatory markers (IL-6) in treatment-refractory schizophrenia; terminated early, small sample |
| [NCT06060886](https://clinicaltrials.gov/study/NCT06060886) | Phase 4 | Unknown | 244 | Open-label, multicenter, randomized trial comparing aripiprazole vs. paliperidone/risperidone using multi-omics data in first-episode psychosis; status needs verification |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31648341](https://pubmed.ncbi.nlm.nih.gov/31648341/) | 2019 | Review | Actas Españolas de Psiquiatría | Reviews psychopharmacology evidence for schizoaffective disorder, noting the lack of disorder-specific treatment guidelines |
| [23364281](https://pubmed.ncbi.nlm.nih.gov/23364281/) | 2013 | Review | Current Opinion in Psychiatry | Reviews pharmacological treatment approaches for early-onset schizophrenia spectrum disorders in adolescents, including dosing and monitoring |

---

## New Zealand Market Information

Currently no New Zealand market authorization exists — paliperidone has "未上市" (not marketed) status with 0 registered licenses.

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and DDI data are currently marked as data gaps and could not be extracted from available sources.)

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanistic link between paliperidone's D2/5-HT2A antagonism and schizophrenia treatment is well established, and Phase 4 clinical evidence (4 trials, including one randomized design) supports feasibility in the refractory subpopulation. However, evidence has not reached the Phase 3 pivotal-trial threshold needed to challenge clozapine's standard-of-care status, and a **Blocking**-severity data gap (missing TFDA/regulatory safety labeling) currently prevents initial safety screening (S1).

**To proceed, the following is needed:**
- Official package insert / regulatory safety labeling (warnings, contraindications) — currently a Blocking data gap
- Confirmed DrugBank-sourced mechanism of action documentation
- Verification of NCT06060886's current recruitment status
- Head-to-head comparative data vs. clozapine in the refractory population, if available

**Note:** Ranks 1–9 of the TxGNN output (retinal dystrophy, myopia subtypes, hydranencephaly, and other congenital/genetic disorders) are not recommended for further action — they lack any mechanistic plausibility or drug-relevant evidence and appear to be false-positive matches.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


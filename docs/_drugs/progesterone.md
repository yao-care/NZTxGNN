---
layout: default
title: Progesterone
parent: 僅模型預測 (L5)
nav_order: 292
evidence_level: L5
indication_count: 10
---

# Progesterone
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

# Progesterone: From Hormonal Therapy to a Predicted Role in Amenorrhea

## One-Sentence Summary

Progesterone is an endogenous steroid hormone used broadly in reproductive endocrinology; detailed original-indication and mechanism-of-action data for this specific evidence pack are not yet available, and the drug is currently **not marketed in New Zealand**. The TxGNN model predicts it may be effective for **amenorrhea**, with **50 clinical trials** and **18 publications** currently associated with this direction, though only a handful directly test progesterone as a treatment for amenorrhea itself.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — progesterone has no Medsafe (New Zealand) license on record, and `original_indications` is empty in this evidence pack |
| Predicted New Indication | Amenorrhea |
| TxGNN Prediction Score | 99.9996% (rank 19 among predicted indications) |
| Evidence Level | L3 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for progesterone in this evidence pack. Based on known clinical pharmacology, progesterone is the principal endogenous progestogen responsible for converting the endometrium from a proliferative to a secretory state, and it regulates gonadotropin (LH/FSH) pulsatility via kisspeptin, neurokinin B, and dynorphin neurons in the hypothalamus.

Amenorrhea and progesterone are already linked in routine clinical practice: the **progesterone challenge test** and **cyclic progesterone withdrawal therapy** are standard tools for diagnosing and managing secondary (anovulatory) amenorrhea. This makes the mechanistic link direct rather than purely associative — unlike many TxGNN predictions that rely on indirect disease-similarity reasoning.

That said, the trial evidence collected for this candidate leans heavily toward amenorrhea **etiology and observational research** (e.g., premature ovarian failure, hypothalamic amenorrhea pathophysiology) rather than interventional trials of progesterone specifically treating amenorrhea. Only a small number of trials (e.g., progesterone-induced withdrawal bleeding studies, MPA-related amenorrhea trials) test progesterone/progestins directly against this indication, which is why the evidence level is capped at L3 rather than L1–L2.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01942668](https://clinicaltrials.gov/study/NCT01942668) | Phase 3 | Completed | 1845 | Estradiol + progesterone combination for vasomotor symptoms in postmenopausal women with intact uterus; supports progesterone safety/PK data but not amenorrhea itself |
| [NCT01185782](https://clinicaltrials.gov/study/NCT01185782) | Phase 3 | Completed | 300 | Comparative study of gonadotropin therapy in subjects with Amenorrhea I or anovulatory cycles |
| [NCT02449161](https://clinicaltrials.gov/study/NCT02449161) | Phase 3 | Terminated | 60 | Effect of postablation medroxyprogesterone acetate on endometrial amenorrhea rates (RCT) |
| [NCT03309176](https://clinicaltrials.gov/study/NCT03309176) | Phase 4 | Completed | 42 | Tests whether progesterone-induced withdrawal bleeding is necessary before ovulation induction in oligo-/amenorrhea |
| [NCT03309709](https://clinicaltrials.gov/study/NCT03309709) | Phase 3 | Unknown | 90 | Subcutaneous progesterone for endometrial polyp regression in premenopausal women |
| [NCT05312190](https://clinicaltrials.gov/study/NCT05312190) | N/A | Unknown | 330 | Progesterone Capsules vs. herbal formula (Zhenqi Buxue) for menstrual disorders, adult women |
| [NCT07224438](https://clinicaltrials.gov/study/NCT07224438) | Phase 2 | Recruiting | 20 | Kisspeptin administration to stimulate reproductive hormones in hypothalamic amenorrhea |
| [NCT00001306](https://clinicaltrials.gov/study/NCT00001306) | N/A | Completed | 33 | Alternate-day prednisone (not progesterone) for autoimmune premature ovarian failure — disease-population overlap only |
| [NCT01927432](https://clinicaltrials.gov/study/NCT01927432) | N/A | Completed | 73 | Observational ultrasound study of ovarian follicle dynamics in women with amenorrhea; no progesterone intervention |
| [NCT01674426](https://clinicaltrials.gov/study/NCT01674426) | N/A | Completed | 17 | Pilot study of cognitive behavior therapy vs. observation for functional hypothalamic amenorrhea; pathophysiology-focused |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38652231](https://pubmed.ncbi.nlm.nih.gov/38652231/) | 2024 | Review | Reviews in Endocrine & Metabolic Disorders | Diagnostic and therapeutic use of oral micronized progesterone across endocrine indications, including menstrual disorders |
| [33716979](https://pubmed.ncbi.nlm.nih.gov/33716979/) | 2021 | Review | Frontiers in Endocrinology | Etiology, symptomatology, and treatment options in premature ovarian insufficiency |
| [35525789](https://pubmed.ncbi.nlm.nih.gov/35525789/) | 2022 | Review | Curr Probl Pediatr Adolesc Health Care | Etiology and management of amenorrhea in adolescents/young adults via the HPO axis |
| [8629565](https://pubmed.ncbi.nlm.nih.gov/8629565/) | 1996 | Review | American Family Physician | Diagnostic evaluation algorithm for amenorrhea in primary care |
| [32233689](https://pubmed.ncbi.nlm.nih.gov/32233689/) | 2020 | Review | Climacteric | Clinical management of postmenopausal vaginal bleeding; discusses estrogen/progesterone-driven amenorrhea physiology |
| [18756412](https://pubmed.ncbi.nlm.nih.gov/18756412/) | 2008 | Review | Seminars in Reproductive Medicine | Intrauterine adhesions (Asherman's syndrome) as a cause of amenorrhea |
| [40474175](https://pubmed.ncbi.nlm.nih.gov/40474175/) | 2025 | Retrospective Cohort | BMC Surgery | High-dose estrogen/progesterone sequential therapy after hysteroscopic adhesiolysis for severe intrauterine adhesions |
| [35463307](https://pubmed.ncbi.nlm.nih.gov/35463307/) | 2022 | Meta-analysis | Frontiers in Oncology | Chemotherapy-induced amenorrhea as a prognostic factor in premenopausal breast cancer |
| [34569009](https://pubmed.ncbi.nlm.nih.gov/34569009/) | 2022 | Review | Clinical Pharmacokinetics | PK/PD overview of selective progesterone receptor modulator vilaprisan (comparator class) |
| [945033](https://pubmed.ncbi.nlm.nih.gov/945033/) | 1976 | Case Series | Annals of Internal Medicine | Galactorrhea-amenorrhea syndromes: hormonal profile and bromocriptine treatment response |

---

## New Zealand Market Information

Progesterone currently has **no Medsafe-registered products** on record in this evidence pack — market status is "Not Marketed" with 0 total authorizations. No authorization table is available.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanistic link between progesterone and amenorrhea is well established in clinical practice (progesterone challenge test, cyclic withdrawal therapy), but the evidence pack itself only reaches L3 (observational/review-level) support, since most collected trials study amenorrhea etiology rather than progesterone treatment outcomes directly. Combined with the drug's unmarketed status in New Zealand, further data collection is warranted before advancing.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently a **Blocking** data gap preventing safety pre-screening
- DrugBank mechanism-of-action data — currently a **High**-severity gap affecting mechanistic-relevance analysis
- Confirmation of any New Zealand or comparable market registration/indication history for progesterone
- Prioritized review of the small subset of trials that directly test progesterone/progestins for amenorrhea (e.g., NCT02449161, NCT03309176, NCT01185782) rather than disease-mechanism studies
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


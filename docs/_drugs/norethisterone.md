---
layout: default
title: Norethisterone
parent: 僅模型預測 (L5)
nav_order: 247
evidence_level: L5
indication_count: 1
---

# Norethisterone
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Norethisterone: From Hormonal Contraception to Amenorrhea

## One-Sentence Summary

Norethisterone is a synthetic progestin (19-nortestosterone derivative) widely used in hormonal contraception, hormone replacement therapy, and menstrual cycle management; a formal original-indication record is not yet on file for this candidate (see Data Gaps DG001/DG002).
The TxGNN model predicts it may be effective for **amenorrhea**, with **8 clinical trials** and **20 publications** currently surfaced, though most of this evidence is indirect (studies of related drugs/combinations rather than norethisterone-specific amenorrhea trials).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not formally recorded in this dataset — commonly used clinically as a progestin for hormonal contraception and menstrual cycle regulation (see Data Gap DG002) |
| Predicted New Indication | Amenorrhea |
| TxGNN Prediction Score | 99.60% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (Data Gap DG002). Based on known pharmacology, norethisterone is a synthetic progestin (19-nortestosterone derivative) and acts as a progesterone receptor agonist. It suppresses the hypothalamic-pituitary-ovarian axis, induces secretory transformation of the endometrium, and produces withdrawal bleeding on discontinuation — the classic mechanism used clinically both to induce menses (e.g., progesterone challenge test) and to manage secondary amenorrhea.

The relationship between norethisterone's established uses (contraception, HRT add-back therapy, menstrual disorder management) and the predicted new indication of amenorrhea is therefore mechanistically direct rather than speculative: progestins of this class are already used off-label to induce or regulate menstruation. However, the evidence surfaced for this specific candidate is largely indirect — most clinical trials involve other agents (elagolix, relugolix) co-administered with norethindrone acetate as "add-back" therapy for heavy menstrual bleeding/uterine fibroids, not norethisterone-driven treatment of amenorrhea itself. One early Phase I trial (PMID 6786825) directly documents amenorrhea as an observed menstrual effect of norethisterone enanthate/acetate, providing the most direct — though old and small-scale — supporting data point.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06953076](https://clinicaltrials.gov/study/NCT06953076) | N/A | Recruiting | 111 | Only trial with a norethisterone-containing regimen (relugolix + estradiol + norethisterone add-back) directly in the mix; evaluates ultrasound changes in uterine fibroids, not amenorrhea as primary endpoint, and has no results yet (Grade B relevance) |
| [NCT01817530](https://clinicaltrials.gov/study/NCT01817530) | Phase 2 | Completed | 571 | Elagolix ± add-back therapy vs placebo for heavy menstrual bleeding with fibroids; GnRH-axis modulation relevant but opposite direction (suppressing rather than inducing bleeding), and drug is not norethisterone |
| [NCT03412890](https://clinicaltrials.gov/study/NCT03412890) | Phase 3 | Completed | 477 | Relugolix + estradiol + norethindrone acetate long-term extension study for heavy menstrual bleeding/fibroids; norethindrone present only as add-back component |
| [NCT03751124](https://clinicaltrials.gov/study/NCT03751124) | Phase 3 | Completed | 229 | Randomized withdrawal study of relugolix + estradiol + norethindrone acetate in uterine fibroids; not focused on norethisterone or amenorrhea endpoints |
| [NCT03049735](https://clinicaltrials.gov/study/NCT03049735) | Phase 3 | Completed | 388 | LIBERTY 1 — relugolix ± estradiol/norethindrone acetate vs placebo for heavy menstrual bleeding; norethindrone as adjunct only |
| [NCT03103087](https://clinicaltrials.gov/study/NCT03103087) | Phase 3 | Completed | 382 | LIBERTY 2 — same design as LIBERTY 1, norethindrone as adjunct only |
| [NCT05620355](https://clinicaltrials.gov/study/NCT05620355) | Phase 3 | Unknown | 312 | BG2109 ± add-back therapy for heavy menstrual bleeding with fibroids; drug not confirmed as norethisterone, status unknown |
| [NCT01441635](https://clinicaltrials.gov/study/NCT01441635) | Phase 2 | Completed | 271 | Elagolix vs placebo for heavy uterine bleeding with fibroids; unrelated drug, included only for adjacent mechanism context |

**Note:** None of the above trials test norethisterone as monotherapy for amenorrhea; all evidence graded C except NCT06953076 (Grade B). This is best characterized as indirect, class-level evidence.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [6786825](https://pubmed.ncbi.nlm.nih.gov/6786825/) | 1981 | Phase I Trial | Contraception | Phase I trial of norethisterone enanthate and acetate in 20 women; documents amenorrhea, spotting, and abolished LH/FSH surges as menstrual effects — the most direct supporting data point |
| [38530848](https://pubmed.ncbi.nlm.nih.gov/38530848/) | 2024 | RCT | PLoS One | WHICH randomized trial comparing DMPA-IM vs norethisterone enanthate (NET-EN) on estradiol levels and menstrual/behavioral outcomes relevant to HIV risk |
| [23641480](https://pubmed.ncbi.nlm.nih.gov/23641480/) | 2013 | Systematic Review | Cochrane Database Syst Rev | Cochrane review of combination injectable contraceptives, including norethisterone-based regimens and their bleeding-pattern effects |
| [37103532](https://pubmed.ncbi.nlm.nih.gov/37103532/) | 2023 | Review | Obstetrics and Gynecology | Review of oral GnRH antagonists co-administered with add-back steroids (incl. norethindrone) for uterine leiomyomas, relevant to HPO-axis suppression mechanism |
| [18843662](https://pubmed.ncbi.nlm.nih.gov/18843662/) | 2008 | Systematic Review | Cochrane Database Syst Rev | Earlier Cochrane review of combination injectable contraceptives (precursor to the 2013 update) |
| [1908716](https://pubmed.ncbi.nlm.nih.gov/1908716/) | 1991 | Review | Curr Opin Obstet Gynecol | Review of sustained-release progestin contraceptive systems and their menstrual effects |
| [8313486](https://pubmed.ncbi.nlm.nih.gov/8313486/) | 1993 | Review | Bull World Health Organ | WHO memorandum reviewing once-a-month injectable contraceptives (Cyclofem, Mesigyna) including effectiveness and bleeding side-effects |
| [2975377](https://pubmed.ncbi.nlm.nih.gov/2975377/) | 1988 | Review | The Practitioner | Review of injectable contraception methods |
| [3071312](https://pubmed.ncbi.nlm.nih.gov/3071312/) | 1988 | Review | Australian Family Physician | Guidance on choosing an oral contraceptive |
| [6508652](https://pubmed.ncbi.nlm.nih.gov/6508652/) | 1984 | Review | Australian Family Physician | Review of oral contraceptives |

## New Zealand Market Information

Norethisterone is currently **not marketed** under this candidate record, with **0 registered authorizations** and no license entries on file.

## Safety Considerations

Please refer to the package insert for safety information. TFDA warnings, contraindications, and drug-drug interaction data are currently a **Blocking** data gap (DG001) that must be resolved before this candidate can complete the S1 safety initial evaluation.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic rationale for norethisterone in amenorrhea is sound and consistent with established progestin pharmacology, but the supporting clinical evidence is indirect (mostly trials of other drugs using norethindrone acetate as an add-back component) and evidence level is only L4. More critically, a Blocking data gap (DG001 — TFDA warnings/contraindications) currently prevents formal S1 safety review.

**To proceed, the following is needed:**
- TFDA/regulatory package insert warnings and contraindications (DG001, Blocking)
- Confirmed mechanism of action documentation from DrugBank (DG002)
- Formal original-indication record for this candidate
- A trial or real-world dataset directly assessing norethisterone efficacy for inducing/regulating menses in amenorrhea patients, rather than relying on adjunct/add-back study data
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


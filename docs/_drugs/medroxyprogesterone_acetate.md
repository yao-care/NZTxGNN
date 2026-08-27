---
layout: default
title: Medroxyprogesterone Acetate
parent: 僅模型預測 (L5)
nav_order: 213
evidence_level: L5
indication_count: 10
---

# Medroxyprogesterone Acetate
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

# Medroxyprogesterone Acetate: From Original Indication (Not Documented) to Amenorrhea

## One-Sentence Summary

> The original approved indication for Medroxyprogesterone Acetate could not be determined from this Evidence Pack — the drug is not currently marketed in this jurisdiction and no license records or mechanism-of-action data are available.
> The TxGNN model predicts it may be effective for **Amenorrhea**, supported by **10 clinical trials** and **20 publications**, though only one trial (terminated) directly targets this indication.
> Because Medroxyprogesterone Acetate (as DMPA) is already well known clinically to induce amenorrhea as a hormonal effect, this is best understood as evidence consolidation of an established pharmacological effect rather than a novel repurposing hypothesis.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in this Evidence Pack |
| Predicted New Indication | Amenorrhea |
| TxGNN Prediction Score | 99.9994% |
| Evidence Level | L1 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism-of-action data is not available from DrugBank in this Evidence Pack (flagged as a High-severity data gap). Based on the repurposing rationale supplied with the prediction, Medroxyprogesterone Acetate is a potent progestin that, with sustained use, induces endometrial atrophy and decidualization — a well-documented pharmacological effect that directly causes amenorrhea. This is precisely the mechanism behind the known side effect of Depot Medroxyprogesterone Acetate (DMPA) injectable contraception, and the compound is also used clinically as a "progestin challenge test" to evaluate the cause of amenorrhea.

Because the original approved indication is not recorded in this pack, the relationship between the (undocumented) original indication and amenorrhea cannot be formally assessed here. However, the mechanistic pathway itself is textbook-level, well-established pharmacology rather than a novel hypothesis — it reflects consolidation of existing clinical knowledge into a structured evidence base rather than discovery of an unexpected new effect.

Since this mechanistic link is already considered established clinical knowledge, the primary value of this prediction is in formalizing existing real-world evidence (e.g., DMPA-induced amenorrhea, progestin withdrawal bleeding physiology) rather than opening a new therapeutic hypothesis requiring exploratory trials.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02449161](https://clinicaltrials.gov/study/NCT02449161) | Phase 3 | Terminated | 60 | Most directly relevant trial: effect of post-endometrial-ablation Medroxyprogesterone Acetate on amenorrhea rates; RCT terminated before completion |
| [NCT01463202](https://clinicaltrials.gov/study/NCT01463202) | Phase 4 | Completed | 184 | Timing of postpartum DMPA administration and its effect on breastfeeding continuation, contraceptive continuation, and postpartum depression |
| [NCT00808132](https://clinicaltrials.gov/study/NCT00808132) | Phase 3 | Completed | 1886 | Large double-blind RCT of bazedoxifene/conjugated estrogens on endometrial hyperplasia and osteoporosis prevention in postmenopausal women |
| [NCT03309176](https://clinicaltrials.gov/study/NCT03309176) | Phase 4 | Completed | 42 | Evaluates whether progestin-induced endometrial withdrawal bleeding is necessary before ovulation induction with clomiphene citrate |
| [NCT01300676](https://clinicaltrials.gov/study/NCT01300676) | Phase 2/3 | Completed | 79 | Tualang honey vs. hormone replacement therapy safety profile in postmenopausal women |
| [NCT03018366](https://clinicaltrials.gov/study/NCT03018366) | Phase 2 | Completed | 29 | Atherosclerosis and inflammation in young women with functional hypothalamic amenorrhea (hypoestrogenemia) |
| [NCT00392093](https://clinicaltrials.gov/study/NCT00392093) | Phase 4 | Completed | 108 | Hormone replacement therapy effect on disease activity, menopausal symptoms, and bone mineral density in peri/postmenopausal women with SLE |
| [NCT06671548](https://clinicaltrials.gov/study/NCT06671548) | Phase 3 | Recruiting | 120 | Relugolix vs. placebo for heavy menstrual bleeding associated with uterine fibroids |
| [NCT02792153](https://clinicaltrials.gov/study/NCT02792153) | Phase 1 | Withdrawn | 0 | Estradiol and fear extinction for calorie-dense foods in women with anorexia nervosa; withdrawn with zero enrollment |
| [NCT07020429](https://clinicaltrials.gov/study/NCT07020429) | N/A | Not Yet Recruiting | 276 | Traditional Chinese herbal formula (Huanjingjian decoction) for premature ovarian insufficiency |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [9554247](https://pubmed.ncbi.nlm.nih.gov/9554247/) | 1998 | RCT | Contraception | 100 women with DMPA-induced amenorrhea randomized to switch to Cyclofem or continue DMPA; 82% of Cyclofem users had vaginal bleeding return within 6 months vs. 10% of DMPA continuers |
| [38530848](https://pubmed.ncbi.nlm.nih.gov/38530848/) | 2024 | RCT | PLoS One | WHICH randomized trial comparing DMPA-IM vs. norethisterone enanthate effects on estradiol levels and menstrual/psychological/behavioral measures relevant to HIV risk |
| [23641480](https://pubmed.ncbi.nlm.nih.gov/23641480/) | 2013 | Systematic Review | Cochrane Database of Systematic Reviews | Cochrane review of combination injectable contraceptives, including bleeding-pattern and acceptability outcomes |
| [18843662](https://pubmed.ncbi.nlm.nih.gov/18843662/) | 2008 | Systematic Review | Cochrane Database of Systematic Reviews | Earlier Cochrane review of combination injectable contraceptives |
| [6141923](https://pubmed.ncbi.nlm.nih.gov/6141923/) | 1984 | Review | Drug Intelligence & Clinical Pharmacy | Review of drug-induced infertility via hypothalamic-pituitary-gonadal axis or direct gonadal toxicity, including progestin effects |
| [8725701](https://pubmed.ncbi.nlm.nih.gov/8725701/) | 1996 | Review | The Journal of Reproductive Medicine | Counseling framework and side-effect management for DMPA users, including amenorrhea counseling |
| [8829701](https://pubmed.ncbi.nlm.nih.gov/8829701/) | 1996 | Review | International Journal of Fertility and Menopausal Studies | Review of long-acting contraceptive options and their bleeding/amenorrhea profiles |
| [7139435](https://pubmed.ncbi.nlm.nih.gov/7139435/) | 1982 | Review | Canadian Medical Association Journal | Discusses whether DMPA should be considered for additional (non-contraceptive) uses |
| [6119259](https://pubmed.ncbi.nlm.nih.gov/6119259/) | 1981 | Review | International Journal of Gynaecology and Obstetrics | Review of postpartum contraception, including postpartum amenorrhea considerations |
| [120837](https://pubmed.ncbi.nlm.nih.gov/120837/) | 1979 | Review | IARC Monographs on the Evaluation of Carcinogenic Risk | IARC monograph on Medroxyprogesterone Acetate |

---

## New Zealand Market Information

Medroxyprogesterone Acetate is currently **not marketed** in this jurisdiction (market status: 未上市), and no license records are available in this Evidence Pack.

---

## Safety Considerations

Please refer to the package insert for safety information. Key warnings, contraindications, and drug interaction data for Medroxyprogesterone Acetate are not currently available in this Evidence Pack (regulatory package insert data collection is flagged as a Blocking data gap).

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanistic link between Medroxyprogesterone Acetate and amenorrhea is well-established pharmacology (progestin-induced endometrial atrophy), and one RCT directly investigates this indication (NCT02449161), though it was terminated. Overall evidence is directionally strong but incomplete for a full safety/efficacy package.

**To proceed, the following is needed:**
- TFDA/regulatory package insert warnings and contraindications (currently a Blocking data gap)
- Confirmed original indication and DrugBank mechanism-of-action data
- Completion or replacement of the terminated NCT02449161 trial, or additional prospective data specifically on Medroxyprogesterone Acetate for amenorrhea (rather than adjacent HRT/contraception trials)
- Drug interaction (DDI) data, currently unavailable ("not_found")
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


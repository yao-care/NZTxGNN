---
layout: default
title: Teriparatide
parent: 僅模型預測 (L5)
nav_order: 336
evidence_level: L5
indication_count: 10
---

# Teriparatide
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

# Teriparatide: From Osteoporosis to Pregnancy and Lactation-Associated Osteoporosis

## One-Sentence Summary

Teriparatide (rhPTH 1-34) is an established bone-anabolic therapy used in osteoporosis. The TxGNN model, supported by real-world literature, predicts it may also be effective for **Pregnancy and Lactation-Associated Osteoporosis (PLO)**, with **2 clinical trials** (indirectly relevant) and **20 publications** — including two cohort studies and a systematic review specifically on teriparatide/PLO — currently supporting this direction. Note: among the 10 TxGNN-ranked candidates for this drug, ranks 1–7, 9 and 10 have little or no supporting evidence and were screened out (see Evidence Level Determination); PLO (rank 8) is the only candidate reaching L3/S2.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Osteoporosis (bone-anabolic therapy) — no New Zealand label text available (drug not marketed); see Data Gaps |
| Predicted New Indication | Pregnancy and Lactation-Associated Osteoporosis (PLO) |
| TxGNN Prediction Score | 99.55% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (Blocking data gap, see below). Based on the information in the evidence pack, teriparatide is a recombinant human parathyroid hormone fragment, rhPTH(1-34), that stimulates coupled osteoclast/osteoblast bone remodeling; when given intermittently it favors net bone formation, which is the basis of its approved use as a bone-anabolic osteoporosis therapy.

PLO is a rare condition in which accelerated bone turnover during late pregnancy and lactation causes rapid bone density loss and fragility (vertebral) fractures. This pathophysiology — rapid bone loss requiring an anabolic response — maps closely onto teriparatide's known mechanism, unlike most of the other TxGNN-ranked candidates for this drug (e.g., duodenal ulcer, esophageal disease, Worth syndrome), which the evidence pack itself flags as having no plausible mechanistic link and no supporting data.

Because PLO is rare and lacks dedicated RCTs, treatment practice has already moved toward off-label teriparatide use, and this is reflected in the literature: multiple retrospective cohorts and a systematic review directly evaluate teriparatide (or compare it to other agents) in PLO patients, giving this prediction a level of real-world grounding the other candidates lack.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02440581](https://clinicaltrials.gov/study/NCT02440581) | N/A | Completed | 141 | Renal osteodystrophy (CKD-related bone loss) study — relevance graded "C" (indirect); shares only the general theme of bone-metabolism regulation, not PLO-specific. |
| [NCT00277706](https://clinicaltrials.gov/study/NCT00277706) | Phase 1 | Completed | 40 | PTH(1-34)/Forteo effect on oral bone regeneration after periodontal surgery — relevance graded "C" (indirect); supports PTH's general bone-forming mechanism but not the PLO population. |

Neither trial directly studied teriparatide in PLO patients; both provide only mechanism-level support.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37708365](https://pubmed.ncbi.nlm.nih.gov/37708365/) | 2024 | Systematic Review/Meta-analysis | J Clin Endocrinol Metab | Comparative effectiveness of PLO therapies; treatment-response data inconclusive due to limited studies. |
| [34132853](https://pubmed.ncbi.nlm.nih.gov/34132853/) | 2021 | Cohort | Calcified Tissue International | Multicenter retrospective cohort: 19 PLO patients treated with teriparatide (20 μg/day) vs. conventional management, assessing BMD and trabecular bone score. |
| [35903718](https://pubmed.ncbi.nlm.nih.gov/35903718/) | 2022 | Cohort/Case series | Geburtshilfe und Frauenheilkunde | 47 PLO women with vertebral fractures treated with teriparatide; evaluated subsequent fracture risk and BMD. |
| [34037833](https://pubmed.ncbi.nlm.nih.gov/34037833/) | 2021 | Cohort/Case series | Calcified Tissue International | BMD outcomes after teriparatide discontinuation, with or without sequential antiresorptive therapy, in PLO. |
| [36764958](https://pubmed.ncbi.nlm.nih.gov/36764958/) | 2023 | Case Report | Calcified Tissue International | Bone microarchitecture/strength changes during teriparatide + zoledronic acid treatment in a PLO patient with multiple vertebral fractures. |
| [39008200](https://pubmed.ncbi.nlm.nih.gov/39008200/) | 2024 | Review | Endocrine | Review of PLO treatment strategies with a specific focus on teriparatide use. |
| [40205203](https://pubmed.ncbi.nlm.nih.gov/40205203/) | 2025 | Systematic Review | Osteoporosis International | Meta-analysis of 35 studies/943 patients on PAO presentation and risk factors; treatment-response analysis inconclusive from limited data. |
| [28084543](https://pubmed.ncbi.nlm.nih.gov/28084543/) | 2017 | Review | Zeitschrift für Rheumatologie | PLO review concluding teriparatide and bisphosphonates appear to be the best treatment options. |
| [39156353](https://pubmed.ncbi.nlm.nih.gov/39156353/) | 2024 | Case Report | Cureus | 29-year-old PLO patient treated with teriparatide who later had a successful second pregnancy. |
| [36676643](https://pubmed.ncbi.nlm.nih.gov/36676643/) | 2022 | Case Report | Medicina (Kaunas) | PLO successfully treated with romosozumab (alternative to teriparatide); highlights the lack of comparative drug-therapy data in PLO. |

## New Zealand Market Information

Teriparatide is currently **not marketed** in New Zealand (0 authorizations on record), so no product/authorization table is available.

## Safety Considerations

Formal safety data (TFDA/label warnings, contraindications, DDI) are unavailable for this candidate (see Blocking data gap DG001 below). Separately, general literature on osteoporosis treatments — not specific to the PLO indication — flags known safety concerns worth carrying into any development plan:

- **Adverse event literature (general osteoporosis use)**: Case report of worsening calcinosis cutis with teriparatide in patients with underlying systemic autoimmune disease (dermatomyositis, CREST syndrome) (PMID 26992073); broader reviews of osteoporosis-drug adverse events cover atrial fibrillation, bone pain, osteonecrosis of the jaw, and atypical fractures (PMID 19412101, 25118550).

Please refer to the package insert for complete safety information once available.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Teriparatide's PLO prediction is backed by real cohort and systematic-review evidence (L3) with a plausible, literature-supported mechanistic rationale, unlike the other 9 TxGNN-ranked candidates for this drug, which have no supporting clinical or literature evidence. However, the drug is not currently marketed in New Zealand and lacks RCT-level evidence in PLO specifically, so guardrails are warranted before any commercial or clinical development step.

**To proceed, the following is needed:**
- Resolve Blocking data gap DG001: obtain TFDA/NZ package-insert warnings and contraindications (currently no S1 safety screening possible)
- Resolve High-priority data gap DG002: obtain detailed mechanism-of-action data from DrugBank to strengthen the mechanistic-link analysis
- Clarify New Zealand market/supply pathway, since teriparatide has 0 current authorizations
- Seek prospective or comparative (vs. bisphosphonate/romosozumab) studies in PLO to move evidence beyond retrospective cohorts toward L1/L2
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


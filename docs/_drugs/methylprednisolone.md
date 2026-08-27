---
layout: default
title: Methylprednisolone
parent: 僅模型預測 (L5)
nav_order: 221
evidence_level: L5
indication_count: 10
---

# Methylprednisolone
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

# Methylprednisolone: From Corticosteroid Therapy to Alopecia Areata

## One-Sentence Summary

Methylprednisolone is a systemic glucocorticoid; no specific original indication is recorded in this evidence pack, though it is broadly used for inflammatory and autoimmune conditions. The TxGNN model predicts it may be effective for **Alopecia Areata**, with **18 registered clinical trials retrieved** (3 directly on methylprednisolone/pulse steroid therapy for alopecia areata, the rest are background noise from unrelated SLE/oncology trials) and **20 publications** specifically on methylprednisolone pulse therapy in alopecia areata.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not specified in evidence pack (glucocorticoid, class-wide use for inflammatory/autoimmune/allergic disease) |
| Predicted New Indication | Alopecia Areata |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L2 |
| Taiwan Market Status | 未上市 (Not Marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (Data Gap DG002), and no original indication is recorded for this candidate. Based on known pharmacology, methylprednisolone is a potent synthetic glucocorticoid used broadly for anti-inflammatory and immunosuppressive therapy across specialties.

Mechanistically, alopecia areata is an autoimmune disease driven by collapse of the hair follicle's immune-privileged status and infiltration of CD8+NKG2D+ T cells attacking the follicle. Methylprednisolone's immunosuppressive and anti-inflammatory action directly targets this pathway, which is consistent with its established dermatologic use: oral/IV pulse methylprednisolone is already a recognized clinical option for severe, treatment-resistant alopecia areata, supported by decades of case series and cohort data (see Literature Evidence below).

The prediction is therefore not a purely theoretical extrapolation — pulse corticosteroid therapy for alopecia areata is existing, real-world clinical practice, and the TxGNN score is corroborated by a directly relevant Phase 4 trial and a substantial, disease-specific literature base.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01167946](https://clinicaltrials.gov/study/NCT01167946) | Phase 4 | Completed | 42 | Oral mega-pulse methylprednisolone in severe, therapy-resistant alopecia areata; evaluates higher-dose, more frequent pulses than standard regimens |
| [NCT07101471](https://clinicaltrials.gov/study/NCT07101471) | N/A | Completed | 296 | Observational safety/effectiveness study of tofacitinib in alopecia, with participants receiving treatment with or without adjuvant prednisolone |
| [NCT01017510](https://clinicaltrials.gov/study/NCT01017510) | N/A | Unknown | 20 | Comparison of Dermojet vs. conventional syringe for intralesional steroid injection in alopecia areata |

Note: The evidence pack's raw clinical trial search returned 18 records, but the majority (SLE trials of baricitinib/sirolimus/anifrolumab, a prostate cancer trial, a headache nerve-block trial) are keyword-matching noise unrelated to methylprednisolone in alopecia areata and have been excluded from this table.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [32270396](https://pubmed.ncbi.nlm.nih.gov/32270396/) | 2020 | Systematic Review | Dermatology and Therapy | Cyclosporine with/without systemic corticosteroids in alopecia areata treatment |
| [37992355](https://pubmed.ncbi.nlm.nih.gov/37992355/) | 2023 | Review | Dermatology Practical & Conceptual | Efficacy and adverse effects of corticosteroid pulse therapy across AA severity |
| [28378336](https://pubmed.ncbi.nlm.nih.gov/28378336/) | 2017 | Review | International Journal of Dermatology | Review of treatment options for alopecia totalis and universalis, including steroids |
| [35986630](https://pubmed.ncbi.nlm.nih.gov/35986630/) | 2022 | Retrospective Cohort | Dermatologic Therapy | Methylprednisolone alone vs. methylprednisolone+methotrexate in extensive AA (n=26) |
| [18608727](https://pubmed.ncbi.nlm.nih.gov/18608727/) | 2008 | Cohort | J Dermatological Treatment | Combination cyclosporine + methylprednisolone in severe AA |
| [30745958](https://pubmed.ncbi.nlm.nih.gov/30745958/) | 2019 | Cohort | Open Access Maced J Med Sci | Methotrexate + mini-pulse methylprednisolone in severe AA (Vietnamese cohort) |
| [25566921](https://pubmed.ncbi.nlm.nih.gov/25566921/) | 2015 | Cohort/Case Series | Indian J Dermatol Venereol Leprol | IV methylprednisolone pulse therapy in severe AA |
| [36865845](https://pubmed.ncbi.nlm.nih.gov/36865845/) | 2022 | Retrospective/Review | Indian Journal of Dermatology | Sex differences in AA response to steroid pulse therapy |
| [9777767](https://pubmed.ncbi.nlm.nih.gov/9777767/) | 1998 | Open Prospective Study | J American Academy of Dermatology | Pulse methylprednisolone in severe AA, open prospective study of 45 patients |
| [21592197](https://pubmed.ncbi.nlm.nih.gov/21592197/) | 2011 | Cohort | The Journal of Dermatology | Prognostic factors for response to methylprednisolone pulse therapy (n=70) |

## New Zealand Market Information

Not applicable — the evidence pack reports 台灣 (Taiwan) regulatory status as **未上市 (not marketed)**, with 0 authorizations and no license records available. There is currently no TFDA-approved product listing for methylprednisolone to summarize in a market table.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
A Phase 4 trial and a consistent body of cohort/case-series literature (spanning 1993–2025) support oral/IV pulse methylprednisolone as an established off-label option for severe alopecia areata, corroborating the L2 evidence level. However, the drug is not currently marketed in Taiwan and two blocking/high-severity data gaps (TFDA package insert warnings/contraindications, and MOA data) remain unresolved.

**To proceed, the following is needed:**
- TFDA package insert (warnings, contraindications) — currently a Blocking data gap (DG001) preventing S1 safety pre-assessment
- Confirmed mechanism of action documentation from DrugBank (DG002)
- Clarification of regulatory pathway, since methylprednisolone has no active Taiwan marketing authorization (0 licenses)
- Drug-drug interaction data (current DDI query returned no results)
- A dedicated dose/regimen protocol for pulse therapy in AA, since no completed randomized controlled trial specifically validates this use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Tocilizumab
parent: 僅模型預測 (L5)
nav_order: 342
evidence_level: L5
indication_count: 10
---

# Tocilizumab
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

# Tocilizumab: From Rheumatoid Arthritis to Ankylosing Spondylitis

## One-Sentence Summary

Tocilizumab is a humanized anti-IL-6 receptor monoclonal antibody whose established use is rheumatoid arthritis (per literature within this evidence pack, e.g. PMID 19368420, 22315615).
The TxGNN model predicts a **99.99%** score for **Ankylosing Spondylitis** as a new indication, but the evidence behind this top-ranked prediction is actually **negative**: two dedicated Phase 3 RCTs (n=113 and n=306) were terminated early for lack of efficacy, so this candidate should not be read as a positive repurposing signal despite its high evidence-level classification.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Rheumatoid Arthritis *(not present in `taiwan_regulatory.licenses`, which is empty; derived from literature evidence in this pack, e.g. PMID 19368420)* |
| Predicted New Indication | Ankylosing Spondylitis |
| TxGNN Prediction Score | 99.99% (rank 245) |
| Evidence Level | L1 (quantity-based; see caveat below) |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in this evidence pack (`original_moa`: Data Gap). Based on literature captured in the evidence itself, tocilizumab is a recombinant humanized monoclonal antibody that blocks the IL-6 receptor (IL-6R), used as a biologic DMARD primarily in rheumatoid arthritis and related IL-6-driven inflammatory conditions.

Ankylosing spondylitis (AS) is superficially similar to RA as a chronic inflammatory rheumatic disease, which is why the TxGNN model scores this pairing so highly. However, the actual pathophysiology of AS is dominated by the IL-17/TNF/IL-23 axis, with IL-6 playing a comparatively minor role — this is a textbook case of a "mechanistically plausible but clinically disproven" prediction.

This is confirmed by the trial evidence itself: two purpose-built Phase 3 RCTs (NCT01209689 and NCT01209702, the BUILDER-1/2 program) tested tocilizumab against placebo in AS and were **terminated early**, consistent with a lack of demonstrated efficacy. The high TxGNN score and L1 evidence-level label therefore reflect the *volume* of Phase 3 investigation, not a positive outcome — this distinction is critical for interpreting the recommendation below.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01209689](https://clinicaltrials.gov/study/NCT01209689) | Phase 3 | Terminated | 113 | RCT of tocilizumab vs placebo in AS patients with inadequate response to prior TNF antagonist therapy — terminated early (negative/insufficient efficacy signal) |
| [NCT01209702](https://clinicaltrials.gov/study/NCT01209702) | Phase 2/3 | Terminated | 306 | Seamless Ph II/III RCT of tocilizumab vs placebo in NSAID-failure, TNF-naïve AS patients — terminated early, same negative program as above |
| [NCT05670301](https://clinicaltrials.gov/study/NCT05670301) | N/A | Recruiting | 2500 | Observational biomarker/cytokine profiling study across systemic inflammatory diseases (not AS/tocilizumab-specific) |
| [NCT01965132](https://clinicaltrials.gov/study/NCT01965132) | N/A | Recruiting | 10000 | Korean nationwide registry of biologics/targeted DMARDs safety in RA, AS, and PsA (real-world safety, not efficacy) |
| [NCT02569736](https://clinicaltrials.gov/study/NCT02569736) | N/A | Completed | 60 | Mechanistic study of tocilizumab's effect on T follicular helper cells and B-cell maturation in RA patients (not AS) |
| [NCT07138898](https://clinicaltrials.gov/study/NCT07138898) | Phase 2 | Not yet recruiting | 80 | Perioperative immunosuppressant management around shoulder arthroplasty in rheumatology patients (not disease-specific to AS) |
| [NCT07477795](https://clinicaltrials.gov/study/NCT07477795) | Phase 2 | Not yet recruiting | 52 | Trial of secukinumab (not tocilizumab) in Takayasu arteritis — low relevance, likely broad-category match |
| [NCT02925338](https://clinicaltrials.gov/study/NCT02925338) | N/A | Completed | 1431 | Real-world observational registry for Inflectra (infliximab), not tocilizumab — low relevance |
| [NCT05696106](https://clinicaltrials.gov/study/NCT05696106) | N/A | Unknown | 750000 | Risk of incident immune-mediated inflammatory diseases in patients on biologics/immunosuppressants generally, not tocilizumab/AS-specific |

**Note:** only the top two trials are graded "A" (directly relevant) by the pack's own relevance review; both are negative results. The remaining trials are broader disease-category matches with limited direct bearing on tocilizumab-in-AS efficacy.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23765873](https://pubmed.ncbi.nlm.nih.gov/23765873/) | 2014 | RCT | Annals of the Rheumatic Diseases | BUILDER-1/BUILDER-2 RCTs assessing short-term symptomatic efficacy and safety of tocilizumab in AS — corresponds to the terminated Phase 3 program above |
| [26986130](https://pubmed.ncbi.nlm.nih.gov/26986130/) | 2016 | Systematic Review / Network Meta-analysis | Medicine | Bayesian network meta-analysis comparing effectiveness of all available biologic regimens for AS |
| [22452603](https://pubmed.ncbi.nlm.nih.gov/22452603/) | 2012 | Review | Inflammation & Allergy Drug Targets | Short review specifically on antagonizing IL-6 in AS |
| [21803631](https://pubmed.ncbi.nlm.nih.gov/21803631/) | 2011 | Review | Joint Bone Spine | Biologic agents for AS beyond TNFα antagonists |
| [22450391](https://pubmed.ncbi.nlm.nih.gov/22450391/) | 2012 | Review | Current Opinion in Rheumatology | Treatment options for AS refractory to TNF inhibition |
| [29278210](https://pubmed.ncbi.nlm.nih.gov/29278210/) | 2017 | Review | Current Pharmaceutical Biotechnology | Biologics in inflammatory and immune-mediated arthritis, including AS |
| [19822066](https://pubmed.ncbi.nlm.nih.gov/19822066/) | 2009 | Review | Clinical and Experimental Rheumatology | Biologics in RA and AS, contrasting pathogenesis and treatment response |
| [27789989](https://pubmed.ncbi.nlm.nih.gov/27789989/) | 2009 | Review | Open Access Rheumatology | Comprehensive review of biologics in RA, AS, and PsA |
| [29290076](https://pubmed.ncbi.nlm.nih.gov/29290076/) | 2018 | Meta-analysis | Clinical Rheumatology | Risk of serious infections with biologics in AS/non-radiographic axSpA |
| [20959960](https://pubmed.ncbi.nlm.nih.gov/20959960/) | 2011 | Review | Osteoporosis International | Systemic bone effects of biologic therapies in RA and AS |

## New Zealand Market Information

Tocilizumab currently holds **no marketing authorization in New Zealand** (0 licenses on record; market status: Not Marketed). No product-level dosage form or approved-indication data is available for this market.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Despite an L1 evidence-level classification (driven by two Phase 3 RCTs) and a top TxGNN score, both dedicated Phase 3 trials of tocilizumab in AS were terminated early for lack of efficacy — this is confirmatory negative evidence, not a repurposing opportunity. AS pathology is IL-17/TNF/IL-23-driven, and IL-6 blockade has not shown a viable clinical benefit in this population.

**To proceed, the following is needed:**
- This candidate should not advance further based on current evidence; no additional AS-specific trials are warranted.
- TFDA/NZ package insert data (DG001, Blocking) and mechanism-of-action confirmation (DG002, High) are still needed to complete baseline safety profiling for tocilizumab generally, independent of this specific indication.
- If pursuing repurposing opportunities from this same evidence pack, consider the higher-confidence candidates instead: polyarticular JIA and RF-positive polyarticular JIA (both L1, "Proceed with Guardrails," backed by a completed 188-patient placebo-controlled Phase 3 RCT), noting these already represent internationally approved indications rather than novel repurposing.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


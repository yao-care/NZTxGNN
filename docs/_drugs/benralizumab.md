---
layout: default
title: Benralizumab
parent: 僅模型預測 (L5)
nav_order: 46
evidence_level: L5
indication_count: 5
---

# Benralizumab
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

# Benralizumab: From Severe Eosinophilic Asthma to Dermatitis

## One-Sentence Summary

Benralizumab (Fasenra) is an anti-IL-5 receptor alpha (IL-5Rα) monoclonal antibody approved internationally as add-on maintenance treatment for severe eosinophilic asthma. The TxGNN model predicts it may be effective for **Dermatitis**, with **6 clinical trials** and **20 publications** currently supporting this direction. Critically, the pivotal Phase 2 HILLIER trial reported **no clinical benefit** for general atopic dermatitis; the eosinophil-predominant **DRESS** subtype (Drug Reaction with Eosinophilia and Systemic Symptoms) remains a mechanistically plausible and actively investigated niche.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Severe eosinophilic asthma — add-on maintenance treatment (approved internationally; not registered in New Zealand) |
| Predicted New Indication | Dermatitis |
| TxGNN Prediction Score | 99.16% |
| Evidence Level | L2 (1 Phase 2 RCT — terminated early, negative primary endpoint for general AD) |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails (DRESS subtype only) |

---

## Why is This Prediction Reasonable?

Benralizumab is an afucosylated monoclonal antibody that targets the alpha subunit of the IL-5 receptor (IL-5Rα), expressed on eosinophils and basophils. Upon binding, it recruits NK cells via its Fc region to trigger antibody-dependent cell-mediated cytotoxicity (ADCC), achieving near-complete depletion of circulating and tissue eosinophils within days. This rapid and profound eosinophil-depleting mechanism is the basis of its proven efficacy in severe eosinophilic asthma — and provides the theoretical rationale for exploring eosinophil-driven skin conditions.

Atopic dermatitis (AD) is a Th2-predominant inflammatory skin disease characterized by tissue eosinophilia, mast cell activation, and IgE-mediated immune dysregulation. Eosinophils infiltrate AD skin lesions and release toxic granule proteins (MBP, ECP) that contribute to barrier damage and itch. Basophils participate in IgE-mediated acute-phase reactions. IL-5Rα depletion theoretically could attenuate local Th2 inflammation in skin — and a 2025 translational study (PMID 40781582) confirmed that benralizumab does deplete IL-5Rα-bearing cells in AD skin lesions, establishing proof of cellular mechanism.

Despite this biological plausibility, clinical reality has diverged from the mechanistic rationale. The multinational, double-blind, placebo-controlled HILLIER Phase 2 RCT (NCT04605094, n=194) was terminated early and reported no significant improvement in AD signs or symptoms (PMID 37178404, 2023). This indicates that eosinophil depletion alone is insufficient to achieve clinical benefit in most AD patients — likely because dupilumab-targetable IL-4/IL-13 pathways are the dominant drivers. The more mechanistically justified niche is **DRESS**, a severe eosinophil-predominant drug hypersensitivity reaction where eosinophils play a central, not supporting, pathogenic role. An ongoing Phase 2 study (NCT06734884, n=96) is testing this hypothesis directly.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04605094](https://clinicaltrials.gov/study/NCT04605094) | Phase 2 | Terminated | 194 | **HILLIER study** — multinational double-blind RCT vs placebo in moderate-to-severe AD. Terminated early; primary endpoint not met. Benralizumab showed no significant clinical benefit over placebo in general atopic dermatitis |
| [NCT06734884](https://clinicaltrials.gov/study/NCT06734884) | Phase 2 | Not Yet Recruiting | 96 | Anti-IL-5Rα (benralizumab) vs standard care in **DRESS** — a severe eosinophil-driven drug hypersensitivity reaction. High mechanistic alignment; results expected 2029 |
| [NCT03563066](https://clinicaltrials.gov/study/NCT03563066) | Phase 2 | Completed | 20 | Mechanistic study in AD: confirmed benralizumab depletes eosinophils, basophils, and ILC2s in skin lesions. Small sample (n=20) limits clinical efficacy conclusions; primarily provides biomarker data |
| [NCT06477653](https://clinicaltrials.gov/study/NCT06477653) | Phase 2 | Recruiting | 30 | Dupilumab add-on therapy for hypereosinophilic syndrome (HES) in patients with partial response to eosinophil-depleting biologics (including benralizumab); indirect relevance to eosinophilic skin disease management |
| [NCT04126499](https://clinicaltrials.gov/study/NCT04126499) | N/A | Completed | 28 | Retrospective observational study of benralizumab in severe eosinophilic asthma patients (Spain individualized access programme); skin findings incidental, evidence grade low |
| [NCT04763447](https://clinicaltrials.gov/study/NCT04763447) | Phase 4 | Recruiting | 234 | Omalizumab withdrawal in well-controlled severe allergic asthma; benralizumab is not the primary agent — indirect relevance to atopic comorbidity phenotyping only |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37178404](https://pubmed.ncbi.nlm.nih.gov/37178404/) | 2023 | Phase 2 RCT | JEADV | **HILLIER trial primary result**: benralizumab shows no significant effect on signs and symptoms of moderate-to-severe atopic dermatitis — the pivotal negative finding for general AD |
| [38695680](https://pubmed.ncbi.nlm.nih.gov/38695680/) | 2024 | Phase 2 RCT | Immunotherapy | HILLIER plain-language summary confirming negative primary endpoints; provides accessible interpretation of the trial's null result |
| [40781582](https://pubmed.ncbi.nlm.nih.gov/40781582/) | 2025 | Translational | Clin Transl Allergy | Benralizumab confirmed to deplete IL-5Rα-bearing cells in AD skin lesions — **proof of cellular mechanism**, but depletion alone did not translate to clinical improvement |
| [39600395](https://pubmed.ncbi.nlm.nih.gov/39600395/) | 2024 | Review | Allergologie select | Comprehensive update on biologics for atopic diseases; benralizumab reviewed alongside dupilumab and IL-13 inhibitors — current evidence does not support an AD indication |
| [36355314](https://pubmed.ncbi.nlm.nih.gov/36355314/) | 2023 | Review | Dermatol Ther | Combination of dupilumab with other monoclonal antibodies for AD comorbidities; discusses safety considerations for benralizumab co-administration |
| [36411004](https://pubmed.ncbi.nlm.nih.gov/36411004/) | 2023 | Review | Immunol Allergy Clin North Am | Biologics for atopic diseases (including AD) during pregnancy and lactation; safety data for benralizumab in pregnancy remains limited |
| [35987486](https://pubmed.ncbi.nlm.nih.gov/35987486/) | 2022 | Review | J Allergy Clin Immunol Pract | Safety review of 7 FDA-approved biologics including benralizumab during pregnancy; maternal and fetal outcome data summarized for 7 agents |
| [38074921](https://pubmed.ncbi.nlm.nih.gov/38074921/) | 2024 | Case Report | Respirology Case Rep | Dual biologic therapy (benralizumab + dupilumab) in two patients with severe asthma and AD comorbidity; combined phenotype showed improvement suggesting complementary pathways |
| [33236428](https://pubmed.ncbi.nlm.nih.gov/33236428/) | 2020 | Review | Pediatr Allergy Immunol | Anti-IL-5 biologics in pediatric allergic diseases including AD; IL-5 pathway reviewed as candidate target, but clinical evidence for paediatric AD remains absent |
| [31690400](https://pubmed.ncbi.nlm.nih.gov/31690400/) | 2019 | Review | Allergy Asthma Proc | Overview of immunobiologics for severe asthma, AD, and urticaria; benralizumab positioned for asthma, not AD — reflects pre-HILLIER landscape |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The pivotal HILLIER Phase 2 RCT clearly demonstrates that benralizumab is ineffective for **general** moderate-to-severe atopic dermatitis, ruling out broad AD as a repurposing target. However, the eosinophil-predominant **DRESS** subtype represents a mechanistically distinct and justified niche — NCT06734884 (Phase 2, n=96) is currently testing this hypothesis directly, and benralizumab's proven ability to deplete IL-5Rα-bearing cells in skin tissue provides biological support for monitoring this trial's outcome.

**To proceed, the following is needed:**
- Await results of NCT06734884 (DRESS Phase 2; estimated completion September 2029)
- Obtain benralizumab package insert to complete contraindications and key warning data (currently unavailable in this dataset)
- Query DrugBank API to retrieve formal MOA documentation for mechanistic-link analysis
- Define eosinophil-threshold criteria for patient subgroup selection (DRESS vs general AD vs HES-associated skin disease) before any clinical protocol development
- Reassess New Zealand market entry pathway if NCT06734884 reports positive efficacy in DRESS
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


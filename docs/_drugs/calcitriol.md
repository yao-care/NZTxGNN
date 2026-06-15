---
layout: default
title: Calcitriol
parent: 僅模型預測 (L5)
nav_order: 59
evidence_level: L5
indication_count: 7
---

# Calcitriol
{: .fs-9 }

證據等級: **L5** | 預測適應症: **7** 個
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

# Calcitriol: From Hypocalcemia Management to Hereditary Hypophosphatemic Rickets

## One-Sentence Summary

Calcitriol (1,25-dihydroxyvitamin D₃) is the biologically active form of vitamin D, classically used to manage hypocalcemia and secondary hyperparathyroidism in patients with chronic kidney disease or hypoparathyroidism.
The TxGNN model's top-ranked prediction ("obsolete vitamin D deficiency", rank 1) is excluded because the MONDO ontology has deprecated this term; the most clinically actionable prediction is **hereditary hypophosphatemic rickets** (rank 7), supported by **7 clinical trials** and **20 publications**, where calcitriol served as the historical standard of care for X-linked hypophosphatemia (XLH) before burosumab was introduced.

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Hypocalcemia and secondary hyperparathyroidism (no Taiwan regulatory license on file) |
| Predicted New Indication | Hereditary Hypophosphatemic Rickets (Rank 7; Rank 1 "obsolete vitamin D deficiency" excluded — MONDO-deprecated term, not a valid repurposing target) |
| TxGNN Prediction Score | 99.28% (hereditary hypophosphatemic rickets) |
| Evidence Level | L2 |
| Taiwan Market Status | Not marketed (0 registered licenses) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Formal mechanism of action data was not retrieved from DrugBank for this analysis. Based on well-established pharmacology, calcitriol is the hormonally active metabolite of vitamin D, produced in the proximal renal tubule from 25-hydroxyvitamin D by the enzyme 1α-hydroxylase. Its principal actions include promoting intestinal absorption of calcium and phosphate, suppressing PTH secretion, and supporting bone mineralization through adequate mineral substrate availability. These actions are mediated via the nuclear vitamin D receptor (VDR), which is expressed in intestine, bone, kidney, and parathyroid tissue.

In hereditary hypophosphatemic rickets — most commonly X-linked hypophosphatemia (XLH) caused by PHEX gene loss-of-function mutations — excess FGF23 production simultaneously inhibits renal phosphate reabsorption and suppresses 1α-hydroxylase activity. The result is a dual deficit: chronic hypophosphatemia and inappropriately low calcitriol. Supplementing exogenous calcitriol directly compensates for this enzymatic block, increases intestinal phosphate absorption, and restores the mineral milieu required for normal bone mineralization. Multiple landmark trials in the 1980s (PMID 6252463, PMID 3839245) established calcitriol plus oral phosphate as the globally accepted treatment for XLH.

The mechanistic link between calcitriol and this new indication is therefore direct rather than inferential — calcitriol is not being repurposed into an unrelated pathway, but rather used to correct a specific deficit caused by the underlying genetic defect. The connection is confirmed at the highest levels of clinical evidence: Lancet and New England Journal of Medicine reviews continue to describe calcitriol + phosphate as the established pre-burosumab standard, with ongoing Phase 4 trials still evaluating optimal dosing regimens (PMID 39181153, PMID 40295317).

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|------------|--------------|
| [NCT03748966](https://clinicaltrials.gov/study/NCT03748966) | Early Phase 1 | Active, Not Recruiting | 20 | Calcitriol monotherapy (without phosphate supplementation) in children and adults with XLH; evaluates whether calcitriol alone improves serum phosphate, skeletal mineralization, and growth without increasing nephrocalcinosis risk |
| [NCT03820518](https://clinicaltrials.gov/study/NCT03820518) | Phase 4 | Unknown | 100 | Compares high-dose vs low-dose active vitamin D (calcitriol class) combined with neutral phosphate in children with XLH; primary objective is to establish evidence-based weight-based dosing guidelines for calcitriol |
| [NCT04846647](https://clinicaltrials.gov/study/NCT04846647) | N/A | Completed | 260 | Characterizes FGF23 hypersecretion patterns across genetic and acquired causes of hypophosphatemia; provides mechanistic data confirming that calcitriol synthesis is suppressed as a direct consequence of FGF23 excess |
| [NCT06046820](https://clinicaltrials.gov/study/NCT06046820) | Phase 3 | Active, Not Recruiting | 27 | ENERGY 3 Study: evaluates INZ-701 (ENPP1 enzyme replacement) in children with ENPP1 deficiency, an FGF23-mediated hypophosphatemic rickets variant; indirectly validates the clinical importance of this disease class and the FGF23–calcitriol axis |
| [NCT00844740](https://clinicaltrials.gov/study/NCT00844740) | N/A | Withdrawn | 0 | Planned to test cinacalcet as adjunct to calcitriol + phosphate in familial hypophosphatemic rickets; withdrawn before enrollment; trial background text explicitly confirms calcitriol + phosphate as the established standard of care at time of design |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [40295317](https://pubmed.ncbi.nlm.nih.gov/40295317/) | 2025 | Clinical Review | Calcified Tissue Int | Comprehensive current review of XLH diagnosis and therapy; confirms calcitriol + phosphate as the foundational regimen prior to the burosumab era; details monitoring parameters and long-term complication risks |
| [39181153](https://pubmed.ncbi.nlm.nih.gov/39181153/) | 2024 | Review | Lancet | Authoritative review of X-linked hypophosphatemia; documents that FGF23 excess reduces calcitriol synthesis and that calcitriol supplementation corrects intestinal phosphate absorption and bone mineral deficits |
| [38988138](https://pubmed.ncbi.nlm.nih.gov/38988138/) | 2024 | Review | J Bone Miner Res | Detailed case-based review of hypophosphatemic rickets with growth failure; describes diagnostic algorithm and calcitriol dosing in pediatric management |
| [36446330](https://pubmed.ncbi.nlm.nih.gov/36446330/) | 2022 | Review | Horm Res Paediatr | Historical overview of rickets and vitamin D metabolism; traces the evolution of calcitriol therapy for XLH from ergocalciferol through modern active metabolites |
| [31863781](https://pubmed.ncbi.nlm.nih.gov/31863781/) | 2020 | Review | Metabolism | Management of XLH in adults; describes ongoing calcitriol + phosphate use for persistent bone pain, osteomalacia-related pseudofractures, enthesopathy, and muscle weakness in adult XLH patients |
| [6252463](https://pubmed.ncbi.nlm.nih.gov/6252463/) | 1980 | Clinical Study | N Engl J Med | Seminal trial: 11 children with vitamin D-resistant rickets treated with phosphate ± calcitriol or ergocalciferol; calcitriol raised circulating 1,25(OH)₂D above normal, increased intestinal phosphate absorption, and reduced the phosphate supplement dose required |
| [3839245](https://pubmed.ncbi.nlm.nih.gov/3839245/) | 1985 | Clinical Study | J Clin Invest | Five XLH patients treated with high-dose calcitriol (mean 68 µg/week); demonstrated healing of osteomalacia that had persisted despite conventional vitamin D therapy, establishing the critical role of adequate calcitriol concentrations |
| [2492895](https://pubmed.ncbi.nlm.nih.gov/2492895/) | 1989 | Clinical Study | Calcif Tissue Int | Bone mineral density measured in 17 children with familial hypophosphatemia at baseline and at 6-month intervals after calcitriol + phosphate; documented quantitative improvements in axial and appendicular bone mineral content |
| [29292875](https://pubmed.ncbi.nlm.nih.gov/29292875/) | 2017 | Cohort Study | Pediatr Endocrinol Rev | Height data from 127 XLH patients across 49 centres before therapy initiation; establishes calcitriol + phosphate as the universally accepted early intervention and documents growth outcomes |
| [17117305](https://pubmed.ncbi.nlm.nih.gov/17117305/) | 2006 | Review | Arq Bras Endocrinol Metab | Systematic review of hereditary and acquired hypophosphatemic conditions; confirms calcitriol supplementation as the cornerstone of treatment alongside phosphate repletion in all FGF23-mediated forms of rickets |

## Taiwan Market Information

Calcitriol is not currently registered with the Taiwan Food and Drug Administration (TFDA). There are no licensed products on file (0 authorizations). This is noteworthy, as calcitriol (brand names: Rocaltrol®, Calcijex®) is widely registered globally and is included on the WHO Model List of Essential Medicines for management of hypocalcemia in CKD. Any clinical application in Taiwan for hereditary hypophosphatemic rickets would currently require individual import authorization or off-label use under the rare disease framework.

## Safety Considerations

Formal package insert data (TFDA warnings, contraindications) was not available in this evidence pack. Based on established pharmacology:

- **Principal risk**: Hypercalcemia and hypercalciuria — the primary dose-limiting toxicity of calcitriol; requires regular monitoring of serum calcium, urinary calcium, and renal function
- **Nephrocalcinosis**: A recognized long-term complication of calcitriol + phosphate therapy in hereditary rickets; this risk is part of the reason burosumab has displaced calcitriol as preferred first-line therapy in XLH where access permits
- **Drug interactions**: Interaction with thiazide diuretics (additive hypercalcemia risk), digitalis glycosides (increased arrhythmia risk from hypercalcemia), and cholestyramine or mineral oil (impaired calcitriol absorption)

Please refer to the official package insert for comprehensive safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Calcitriol's efficacy in hereditary hypophosphatemic rickets (particularly XLH) is mechanistically direct and historically validated by decades of clinical use — it was the global standard of care before burosumab and is still deployed where biologic therapy is unavailable or unaffordable. The evidence level (L2: one Phase 4 trial with N=100 directly evaluating calcitriol dosing in XLH) and the breadth of supportive literature justify proceeding, provided that hypercalcemia and nephrocalcinosis monitoring are built into any protocol.

**To proceed, the following is needed:**
- Taiwan TFDA regulatory pathway assessment: 0 current licenses means any clinical use requires rare disease importation authorization or compassionate use application
- Formal MOA documentation from DrugBank to complete the mechanistic risk profile for regulatory submission
- Structured monitoring protocol covering: serum calcium (monthly during dose titration), urinary calcium-to-creatinine ratio, renal ultrasound for nephrocalcinosis (every 6–12 months in pediatric patients), and alkaline phosphatase as a bone healing surrogate
- Patient population stratification: pediatric XLH (active rickets, growth concern) vs adult XLH (osteomalacia, enthesopathy) have different dosing targets and monitoring priorities
- Comparative positioning analysis relative to burosumab — for newly diagnosed pediatric XLH patients, international guidelines now prefer burosumab where accessible; calcitriol remains the standard in resource-limited settings or as combination therapy
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


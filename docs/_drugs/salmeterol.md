---
layout: default
title: Salmeterol
parent: 僅模型預測 (L5)
nav_order: 318
evidence_level: L5
indication_count: 7
---

# Salmeterol
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

# Salmeterol: From Asthma/COPD Maintenance Therapy to Bronchitis

## One-Sentence Summary

Salmeterol is a long-acting β2-adrenergic bronchodilator historically used for maintenance treatment of asthma and chronic obstructive pulmonary disease (COPD); it is currently **not marketed in New Zealand**. The TxGNN model predicts it may be effective for **Bronchitis** (the chronic-bronchitis phenotype of COPD), with **16 clinical trials** and **20 publications** already supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Asthma / COPD maintenance bronchodilator (class-level; no New Zealand license on file) |
| Predicted New Indication | Bronchitis |
| TxGNN Prediction Score | 99.92% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known pharmacology, salmeterol belongs to the long-acting β2-adrenergic receptor agonist (LABA) class. It activates β2-receptors on airway smooth muscle, raising intracellular cAMP and producing sustained bronchodilation (~12 hours per dose). Its efficacy in asthma and COPD maintenance therapy has been proven through decades of international clinical use, even though no New Zealand marketing authorization currently exists for it.

Chronic bronchitis is clinically classified as a phenotype of COPD ("COPD associated with chronic bronchitis"), rather than a distinct disease entity. Salmeterol-containing combination products (e.g., fluticasone/salmeterol Diskus 250/50mcg) are already approved in other markets specifically for "COPD associated with chronic bronchitis," which directly supports the TxGNN prediction as a label-adjacent extension rather than a novel mechanistic hypothesis.

Mechanistically, beyond bronchodilation, salmeterol has also been shown to improve mucociliary and cough clearance in patients with chronic bronchitis (PMID 15970448), providing a plausible symptom-modifying rationale beyond simple airflow relief. Given the substantial overlap between the trial population used to establish salmeterol's COPD efficacy and patients with chronic bronchitis specifically, this prediction should be viewed as validating an already well-established use rather than a speculative new indication.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00857766](https://clinicaltrials.gov/study/NCT00857766) | Phase 4 | Completed | 249 | 16-week RCT of fluticasone/salmeterol DISKUS 250/50mcg BID vs placebo evaluating arterial stiffness in COPD |
| [NCT00633217](https://clinicaltrials.gov/study/NCT00633217) | Phase 4 | Completed | 247 | 12-week double-blind, double-dummy comparison of FSC HFA MDI vs FSC DISKUS, dose corresponds to the indication for COPD associated with chronic bronchitis |
| [NCT02173691](https://clinicaltrials.gov/study/NCT02173691) | Phase 3 | Completed | 584 | 6-month double-blind comparison of tiotropium, salmeterol monotherapy, and placebo in COPD |
| [NCT01332409](https://clinicaltrials.gov/study/NCT01332409) | N/A | Completed | 2000 | Post-marketing drug use investigation of salmeterol/fluticasone in COPD (bronchitis chronic/emphysema), pneumonia as priority safety endpoint |
| [NCT01110200](https://clinicaltrials.gov/study/NCT01110200) | Phase 4 | Completed | 639 | FSC 250/50mcg BID vs salmeterol 50mcg BID on COPD exacerbation rate post-hospitalization |
| [NCT00064415](https://clinicaltrials.gov/study/NCT00064415) | Phase 3 | Completed | 799 | 12-month chronic safety study of arformoterol (LABA class) in COPD |
| [NCT00064402](https://clinicaltrials.gov/study/NCT00064402) | Phase 3 | Completed | 741 | 12-week bronchodilator effect and safety of (R,R)-formoterol as maintenance treatment in COPD |
| [NCT03333018](https://clinicaltrials.gov/study/NCT03333018) | N/A | Completed | 22155 | Large-scale European drug utilisation study characterizing real-world use patterns in COPD |
| [NCT00403286](https://clinicaltrials.gov/study/NCT00403286) | Phase 2 | Completed | 457 | Dose-finding trial of fluticasone/formoterol vs Advair Diskus (fluticasone/salmeterol) in COPD |
| [NCT00268177](https://clinicaltrials.gov/study/NCT00268177) | Phase 3 | Completed | 130 | 13-week study of bronchial anti-inflammatory activity of salmeterol/fluticasone vs placebo in COPD |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15970448](https://pubmed.ncbi.nlm.nih.gov/15970448/) | 2006 | RCT | Pulm Pharmacol Ther | Salmeterol improves mucociliary and cough clearance in mild-moderate chronic bronchitis vs placebo |
| [9916607](https://pubmed.ncbi.nlm.nih.gov/9916607/) | 1998 | RCT | Clin Ther | Inhaled salmeterol vs oral theophylline: efficacy, tolerability, quality of life in mild-to-moderate COPD |
| [12970006](https://pubmed.ncbi.nlm.nih.gov/12970006/) | 2003 | RCT | Chest | Fluticasone/salmeterol Diskus combination efficacy and safety vs placebo and monotherapy in COPD |
| [19124357](https://pubmed.ncbi.nlm.nih.gov/19124357/) | 2008 | Cohort/Comparative | Ther Adv Respir Dis | One-year safety and tolerance evaluation of arformoterol and salmeterol in COPD |
| [25515181](https://pubmed.ncbi.nlm.nih.gov/25515181/) | 2015 | Guideline/Review | Basic Clin Pharmacol Toxicol | Finnish national COPD guideline covering diagnosis and pharmacotherapy of stable disease |
| [17196106](https://pubmed.ncbi.nlm.nih.gov/17196106/) | 2006 | Meta-analysis | Respir Res | Meta-analysis showing improved outcomes with salmeterol vs placebo/usual therapy in COPD |
| [15329047](https://pubmed.ncbi.nlm.nih.gov/15329047/) | 2004 | Review | Drugs | Review of salmeterol/fluticasone propionate use in COPD, including chronic bronchitis indication |
| [16915216](https://pubmed.ncbi.nlm.nih.gov/16915216/) | 2006 | Patient Experience Trial | MedGenMed | Management of COPD associated with chronic bronchitis using inhaled fluticasone propionate/salmeterol |
| [19210134](https://pubmed.ncbi.nlm.nih.gov/19210134/) | 2009 | Observational | Curr Med Res Opin | Healthcare utilization and costs in chronic bronchitis patients initiating fluticasone/salmeterol vs other maintenance therapies |
| [10832348](https://pubmed.ncbi.nlm.nih.gov/10832348/) | 2000 | Review | MMW Fortschr Med | Review of pharmacological options, including LABAs, for smokers with chronic bronchitis and emphysema |

---

## New Zealand Market Information

Salmeterol is currently **not marketed in New Zealand** — no authorizations, brand products, or dosage forms are on file (0 licenses recorded).

---

## Safety Considerations

- **Key Consideration (from evidence pack repurposing rationale):** LABA monotherapy carries a known class-level black box warning for increased risk of severe asthma-related exacerbation and death; regulatory guidance requires salmeterol to be used in combination with an inhaled corticosteroid (ICS) rather than alone, particularly relevant when evaluating asthma-adjacent uses of this drug.

Detailed New Zealand-specific package insert warnings, contraindications, and drug-drug interaction data are not currently available (DDI query returned no results). Please refer to the package insert for full safety information once available.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The bronchitis prediction is backed by L1-level evidence — multiple completed Phase 3/4 RCTs and large real-world studies directly support salmeterol's efficacy in COPD associated with chronic bronchitis, effectively validating an already well-established use rather than a speculative new indication. However, the drug is not currently marketed in New Zealand and critical safety documentation is entirely missing, so guardrails are needed before any regulatory or clinical next step.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert warnings, precautions, and contraindications (DG001 — Blocking; required before any S1 safety pre-assessment can proceed)
- Verified DrugBank mechanism-of-action data (DG002 — High priority; needed to formally support the mechanistic rationale)
- Assessment of the New Zealand registration/import pathway, since salmeterol currently has zero authorizations in-market
- Formal drug-drug interaction (DDI) profile, as the current query returned no results
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Levothyroxine
parent: Model Prediction Only (L5)
nav_order: 272
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Levothyroxine: predicted new indication Nodular goiter (disease) (evidence level L5). Approved use: Hypothyroidism"
monitor_inn: "LEVOTHYROXINE"
predicted_indications: ["endemic goiter", "renal hypodysplasia/aplasia", "nodular goiter (disease)", "bilateral renal agenesis", "nontoxic goiter", "lingual goiter", "substernal goiter", "dyshormonogenic goiter", "Potter sequence", "Carney complex"]
---

# Levothyroxine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

# Levothyroxine: From Hypothyroidism to Nodular Goiter

## One-Sentence Summary

Levothyroxine is a synthetic thyroid hormone, originally used to treat hypothyroidism and other thyroid conditions (including thyroid cancer as TSH-suppressive therapy).
The TxGNN model predicts it may be effective for **nodular goiter**,
with **2 clinical trials** (including one completed Phase 4 randomized, placebo-controlled trial with 1,024 patients) and **20 publications** currently supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypothyroidism (the licence records do not contain indication text, so this comes from the knowledge-graph indication list) |
| Predicted New Indication | Nodular goiter (disease) |
| TxGNN Prediction Score | 99.64% |
| Evidence Level | L1 (based on RCT-level evidence; the largest trial is Phase 4 and the other has no phase designation, so this is not a strict "two Phase 3" L1) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 9 (all cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Based on known pharmacology, levothyroxine is a synthetic form of thyroxine (T4). Its efficacy in replacing thyroid hormone in hypothyroidism is well established, and mechanistically it may be applicable to nodular goiter.

Goiter growth is driven largely by TSH stimulation, often in the setting of iodine deficiency. Levothyroxine lowers TSH, which reduces the growth stimulus on thyroid tissue. This is the same TSH-suppression principle used in thyroid-related conditions already on the original indication list, such as goiter and differentiated thyroid carcinoma. The link between the original and new indication is therefore a shared thyroid-tissue, TSH-dependent mechanism.

The benefit in nodular disease appears modest and mixed. Suppressive therapy carries risks, including bone loss and atrial fibrillation, and is not routinely recommended. Radioiodine and surgery are generally favoured alternatives, so use would be limited to selected patients.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00277589](https://clinicaltrials.gov/study/NCT00277589) | Phase 4 | Completed | 1024 | Randomized, observer-blind, placebo-controlled trial of TSH-adapted levothyroxine therapy in nodular goiter; primary outcome was change in total nodule volume |
| [NCT00941551](https://clinicaltrials.gov/study/NCT00941551) | N/A | Completed | 150 | Five-year follow-up of a randomized trial of unilateral lobectomy with or without postoperative levothyroxine; compared recurrence of nodular goiter in the contralateral lobe |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10426591](https://pubmed.ncbi.nlm.nih.gov/10426591/) | 1999 | Clinical study | Endocrine Journal | 70 patients with diffuse or nodular euthyroid goiter treated with levothyroxine for 6 months; effects on thyroid and nodule volume and thyroglobulin |
| [10382684](https://pubmed.ncbi.nlm.nih.gov/10382684/) | 1999 | Retrospective study | Endocrine Research | Suppressive versus replacement levothyroxine in solitary nodule and multinodular goiter |
| [20683172](https://pubmed.ncbi.nlm.nih.gov/20683172/) | 2010 | Clinical study | Endocrine Journal | Short-term levothyroxine effects in pre-menopausal women with multinodular disease |
| [19636201](https://pubmed.ncbi.nlm.nih.gov/19636201/) | 2009 | Retrospective study | J Endocrinol Invest | Levothyroxine for preventing nodular recurrence after hemithyroidectomy |
| [11238476](https://pubmed.ncbi.nlm.nih.gov/11238476/) | 2001 | RCT | J Clin Endocrinol Metab | Related RCT in sporadic nontoxic nodular goiter comparing suppressive levothyroxine with radioiodine (listed under the nontoxic goiter prediction) |
| [8844728](https://pubmed.ncbi.nlm.nih.gov/8844728/) | 1996 | Review | Otolaryngol Clin North Am | Evaluation and management of multinodular goiter |
| [25189867](https://pubmed.ncbi.nlm.nih.gov/25189867/) | 2015 | Review | Eur J Endocrinol | Recombinant human TSH and radioiodine in benign multinodular goiter |
| [12397534](https://pubmed.ncbi.nlm.nih.gov/12397534/) | 2002 | Observational | Exp Clin Endocrinol Diabetes | Long-term follow-up of benign thyroid nodule growth in 109 patients |

## New Zealand Market Information

All nine licences are cancelled, and no licence records include approved indication text.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2534800 | Synthroid | Tab 100 mcg | Not recorded (cancelled) |
| 250287 | Eltroxin | Tab 50 mcg | Not recorded (cancelled) |
| 2534797 | Synthroid | Tab 50 mcg | Not recorded (cancelled) |
| 2534789 | Synthroid | Tab 25 mcg | Not recorded (cancelled) |
| 250295 | Eltroxin | Tab 100 mcg | Not recorded (cancelled) |

## Safety Considerations

- **Contraindications suggested by the knowledge graph (not label text)**: hypertensive disorders, pituitary hormone deficiency conditions (e.g., hypopituitarism, Sheehan syndrome), osteoporosis (including postmenopausal), diabetes mellitus and diabetic ketoacidosis, coronary artery disease, and myocardial infarction.
- **Drug Interactions**: 100 interactions were found. The ones shown are mostly moderate, including:
  - Antidiabetic drugs: metformin, pioglitazone, acarbose, alogliptin, canagliflozin, dapagliflozin, empagliflozin, chlorpropamide, albiglutide, dulaglutide
  - Acid-suppressing drugs: omeprazole, rabeprazole, dexlansoprazole (cimetidine is minor)
  - Minerals and antacids: calcium acetate, calcium phosphate, magnesium oxide
  - Sympathomimetics: epinephrine, isometheptene, diethylpropion
- **Therapy-specific risks from the literature**: suppressive dosing is associated with bone loss and atrial fibrillation, and benefit is limited in older patients.

Please also refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Two randomized trials, including a large completed placebo-controlled Phase 4 trial, address levothyroxine in nodular goiter. However, the benefit is modest and mixed, suppressive therapy carries cardiac and bone risks, and radioiodine or surgery are generally preferred. The evidence is not Phase 3 grade.

**To proceed, the following is needed:**
- Mechanism of action data
- Package insert warnings and contraindications (not available for this report; also needed to confirm approved indications)
- Confirmation of the regulatory situation, since all New Zealand licences are cancelled and a new or re-instated licence would be needed
- Patient selection criteria and a monitoring plan (TSH targets, bone density, cardiac assessment, iodine status)
- Review of the trial results for NCT00277589 and NCT00941551 to quantify the effect on nodule volume and recurrence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Theophylline
parent: High Evidence (L1-L2)
nav_order: 449
evidence_level: L1
indication_count: 7
last_modified_at: 2026-10-15
description: "Theophylline: predicted new indication Thrombotic disease (evidence level L1). Approved use: Asthma"
monitor_inn: "THEOPHYLLINE"
predicted_indications: ["thrombotic disease", "nasal cavity disease", "laryngotracheitis", "tracheal disease", "obstructive lung disease", "pharyngitis", "acute laryngopharyngitis"]
---

# Theophylline
{: .fs-9 }

Evidence Level: **L1** | Predicted Indications: **7** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Theophylline: From Asthma to Thrombotic Disease

## One-Sentence Summary

Theophylline is a methylxanthine bronchodilator, originally used for asthma, bronchitis, emphysema and other chronic airway diseases.
The TxGNN model predicts it may be effective for **thrombotic disease** (top-ranked prediction), but **no clinical trials** are registered for this indication and the retrieved literature is largely unrelated, so the prediction currently rests on model output and mechanistic plausibility alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Asthma (the data compiled for this report do not include approved-indication text from New Zealand licences; the original indications come from the knowledge graph: asthma, bronchitis, emphysema and related lung diseases) |
| Predicted New Indication | Thrombotic disease |
| TxGNN Prediction Score | 99.62% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. From general pharmacology, theophylline is a non-selective phosphodiesterase (PDE) inhibitor and adenosine receptor antagonist. In platelets, PDE inhibition raises cAMP, which could reduce platelet aggregation, similar in principle to dipyridamole and cilostazol.

The link to its original respiratory indications is indirect. In the airways, the same PDE inhibition produces bronchodilation. Whether this cAMP effect translates into an antithrombotic benefit is unproven. Adenosine antagonism may oppose it, because adenosine itself inhibits platelet aggregation. The retrieved literature is mostly unrelated (ticlopidine pharmacokinetics, blood-sample processing methods, a theophylline-detection sensor), so the high score is a prediction without clinical support.

## Clinical Trial Evidence

Currently no related clinical trials registered for thrombotic disease.

## Literature Evidence

No publication directly tests theophylline as an antithrombotic. The items below are the closest in topic, and none provides efficacy evidence.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8055680](https://pubmed.ncbi.nlm.nih.gov/8055680/) | 1994 | Pharmacokinetic review | Clin Pharmacokinet | Pharmacokinetics of ticlopidine, an antiplatelet drug (not theophylline) |
| [6771102](https://pubmed.ncbi.nlm.nih.gov/6771102/) | 1980 | Review | CRC Crit Rev Biochem | Prostaglandins, platelets and atherosclerosis; indirect mechanistic context (cAMP and platelet aggregation) |
| [15475744](https://pubmed.ncbi.nlm.nih.gov/15475744/) | 2004 | Observational study | Inflamm Bowel Dis | Platelet-leukocyte aggregates in inflammatory bowel disease; not drug-specific |
| [8981060](https://pubmed.ncbi.nlm.nih.gov/8981060/) | 1996 | Laboratory study | Gen Pharmacol | Milrinone and adenosine inhibit human platelet response via cAMP; related mechanism, not theophylline |
| [29254574](https://pubmed.ncbi.nlm.nih.gov/29254574/) | 2018 | Analytical method | Anal Chim Acta | Aptasensor for detecting theophylline; not clinical |
| [749930](https://pubmed.ncbi.nlm.nih.gov/749930/) | 1978 | Assay method | Br J Haematol | Platelet factor 4 radioimmunoassay; theophylline is used only as an anticoagulant additive in sample preparation |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 255092 | Nuelin | Oral liquid 80 mg per 15 ml | Not available (cancelled) |
| 255149 | Nuelin-SR | Long-acting tablet 250 mg | Not available (cancelled) |

Both licences are cancelled, so no valid theophylline product currently appears on the New Zealand register in the data compiled.

## Safety Considerations

- **Contraindications / cautions (from knowledge-graph drug-disease edges, not label text):** hypertension, liver failure and liver disease, epilepsy, anxiety and other neurotic disorders, psychotic disorders (including schizophreniform and schizoaffective disorders), inherited porphyria, kidney disease, gallbladder disease, thrombocytopenia, neutropenia and agranulocytosis, hypothyroidism, peptic ulcer disease, open-angle glaucoma, diabetes mellitus and diabetic ketoacidosis, benign prostatic hyperplasia, and dermatitis herpetiformis. Notably, thrombocytopenia is listed, which is relevant to any antithrombotic use.
- **Drug Interactions:** 100 interactions were found. Examples from the interactions retrieved:
  - **Major:** bupropion.
  - **Moderate:** clarithromycin, cimetidine, famotidine, ranitidine, omeprazole, rabeprazole, doxycycline, tetracycline, minocycline, corticosteroids (hydrocortisone, dexamethasone, betamethasone, triamcinolone, budesonide), activated charcoal, and bowel-preparation products (picosulfuric acid, polyethylene glycol).
  - **Minor:** ephedrine.
- Package insert warnings and contraindications were not available in the data compiled. Please refer to the package insert for label safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score for thrombotic disease is very high (99.62%), but there are no clinical trials and no relevant literature. The mechanism is only plausible and may be opposed by adenosine antagonism. In addition, theophylline has a narrow therapeutic window, a heavy interaction burden, and no currently valid New Zealand licence.

**To proceed, the following is needed:**
- Mechanism of action data and in vitro platelet-aggregation studies with theophylline at therapeutic concentrations
- Any clinical or observational signal of antithrombotic effect (for example, thrombotic outcomes in existing theophylline users)
- Package insert warnings and contraindications, to allow a safety screening
- A plan for serum level monitoring and interaction management, and clarification of product availability in New Zealand

**Other predicted indications (for context):**

| Predicted Indication | Evidence Level | Note |
|------|------|------|
| Obstructive lung disease | L1 | Overlaps with the original indications, so not true repurposing. Includes Phase 3 trials, such as NCT01599871 (52-week placebo-controlled add-on trial, n=70) and NCT02340520 (n=13). Proceed with Guardrails: narrow therapeutic index, serum level monitoring, CYP1A2 variability, extensive interactions, and cancelled-only market status |
| Nasal cavity disease (allergic rhinitis) | L3 | A small nasal allergen challenge study (n=14) and a Phase 2 trial, NCT01132781 (n=28); no Phase 3 data. Research question |
| Tracheal disease | L3 | Mostly preclinical, veterinary or indirect evidence. Research question |
| Pharyngitis | L4 | No credible direct evidence. Hold |
| Laryngotracheitis | L5 | Prediction only. Hold |
| Acute laryngopharyngitis | L5 | Prediction only. Hold |
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


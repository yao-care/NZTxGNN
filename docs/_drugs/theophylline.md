---
layout: default
title: Theophylline
parent: Model Prediction Only (L5)
nav_order: 449
evidence_level: L5
indication_count: 6
monitor_inn: "THEOPHYLLINE"
predicted_indications: ["thrombotic disease", "nasal cavity disease", "laryngotracheitis", "tracheal disease", "pharyngitis", "acute laryngopharyngitis"]
last_modified_at: 2026-10-30
description: "Theophylline: predicted new indication Thrombotic disease (evidence level L5). Approved use: Asthma"
---

# Theophylline
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Theophylline: From Asthma to Thrombotic Disease
{: .text-alpha }

## One-Sentence Summary

Theophylline is a bronchodilator whose knowledge-graph-recorded uses are asthma and other chronic airway diseases.
The TxGNN model predicts it may be effective for **thrombotic disease**, but there are **0 clinical trials** and **no cited publication that directly studies theophylline in this disease**, so the prediction currently rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Asthma (relation recorded in the knowledge graph; the local licence records contain no indication text) |
| Predicted New Indication | Thrombotic disease |
| TxGNN Prediction Score | 0.9962 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Number of Approvals | 2 (both single-ingredient licences; approval type not specified in the records) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on general pharmacology, theophylline is a non-selective phosphodiesterase (PDE) inhibitor and adenosine receptor antagonist. Its relation to asthma, bronchitis, emphysema and related airway diseases is recorded in the knowledge graph.

Mechanistically, PDE inhibition raises cAMP in platelets, which could plausibly reduce platelet aggregation and hence thrombosis. This is a theoretical link only. The retrieved literature is mostly off-target: ticlopidine pharmacokinetics, blood-sample processing and biomarker assay methods, platelet activation measurements, and a theophylline sensor paper. None of it demonstrates an antithrombotic effect of theophylline in patients. The high TxGNN score is therefore not supported by clinical data.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

None of the retrieved publications directly studies theophylline for thrombotic disease. The items below are the closest in topic and are shown for context only.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8055680](https://pubmed.ncbi.nlm.nih.gov/8055680/) | 1994 | Review | Clinical Pharmacokinetics | Pharmacokinetics of ticlopidine, an antiplatelet drug; not about theophylline |
| [6771102](https://pubmed.ncbi.nlm.nih.gov/6771102/) | 1980 | Review | CRC Critical Reviews in Biochemistry | Prostaglandins, platelets and atherosclerosis; cAMP-mediated platelet inhibition as background |
| [8981060](https://pubmed.ncbi.nlm.nih.gov/8981060/) | 1996 | Laboratory study | General Pharmacology | Milrinone (a PDE inhibitor) and adenosine inhibit human platelet response via cAMP; indirect mechanistic support only |
| [25856065](https://pubmed.ncbi.nlm.nih.gov/25856065/) | 2015 | Methods/Laboratory | Platelets | Measurement of soluble CLEC-2 as a platelet activation marker |
| [29220362](https://pubmed.ncbi.nlm.nih.gov/29220362/) | 2017 | Methods | PLoS One | Plasma preparation for monitoring platelet-stored molecules |
| [749930](https://pubmed.ncbi.nlm.nih.gov/749930/) | 1978 | Methods/Laboratory | British Journal of Haematology | Platelet factor 4 radioimmunoassay (theophylline used as a sample additive) |
| [4776567](https://pubmed.ncbi.nlm.nih.gov/4776567/) | 1973 | Clinical (no abstract) | Minerva Cardioangiologica | A theophylline nicotinate derivative in chronic obliterating peripheral arteriopathy; a different compound |
| [29254574](https://pubmed.ncbi.nlm.nih.gov/29254574/) | 2018 | Analytical method | Analytica Chimica Acta | Electrochemical aptasensor for detecting theophylline; not a therapeutic study |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 255092 | Nuelin | Oral liquid 80 mg per 15 ml | Not stated in the licence records |
| 255149 | Nuelin-SR | Long-acting tablet 250 mg | Not stated in the licence records |

## Safety Considerations

- **Contraindication-type relations recorded in the knowledge graph** (not label text): hypertension, liver failure and liver disease, epilepsy, anxiety and other mental disorders (including psychotic disorders), porphyria, kidney disease, hypothyroidism, peptic ulcer disease, open-angle glaucoma, diabetes mellitus and diabetic ketoacidosis, benign prostatic hyperplasia, thrombocytopenia, neutropenia and agranulocytosis, and others.
- **Drug Interactions**: 100 interactions were found in the interaction database; the 20 shown include one Major interaction, **bupropion**. Moderate interactions include H2 blockers and PPIs (famotidine, ranitidine, cimetidine, omeprazole, rabeprazole), corticosteroids (hydrocortisone, dexamethasone, betamethasone, budesonide, triamcinolone), tetracyclines (doxycycline, minocycline, tetracycline), clarithromycin, activated charcoal and some bowel-preparation products. Ephedrine is a Minor interaction.
- Theophylline has a narrow therapeutic index, and interactions that change its blood levels are clinically important. Please refer to the package insert for full warnings.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a high model score and a theoretical PDE/cAMP platelet mechanism. No clinical trial or publication directly studies theophylline in thrombotic disease. Combined with a narrow therapeutic index and many interactions (especially with antithrombotic-relevant care settings), the evidence does not justify progressing.

**To proceed, the following is needed:**
- Direct preclinical or clinical data showing an antithrombotic effect of theophylline at tolerable doses
- Mechanism of action data and Medsafe package insert warnings and contraindications
- A benefit-risk comparison with established antiplatelet and anticoagulant therapy
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


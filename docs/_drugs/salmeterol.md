---
layout: default
title: Salmeterol
parent: Model Prediction Only (L5)
nav_order: 420
evidence_level: L5
indication_count: 3
last_modified_at: 2026-11-06
monitor_inn: "SALMETEROL"
predicted_indications: ["respiratory malformation", "Rienhoff syndrome", "atopic eczema"]
description: "Salmeterol: predicted new indication Respiratory malformation (evidence level L5). Approved use: Not recorded in the local data"
---

# Salmeterol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

## Salmeterol: Respiratory malformation
{: .text-alpha }

## One-Sentence Summary

Salmeterol is an inhaled long-acting beta2-adrenergic agonist used as a bronchodilator. The TxGNN model predicts it may be relevant to **respiratory malformation**, but no trial or publication studies this condition directly. The two registered trials address cystic fibrosis and asthma in pregnancy, so this is a **model-only prediction**.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Respiratory malformation |
| TxGNN Prediction Score | 0.9991 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (6 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 6 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the data compiled for this report. Salmeterol belongs to the long-acting beta2-agonist class, which relaxes airway smooth muscle. Salmeterol may also influence ciliary beat frequency, which is relevant to airway clearance.

The link to respiratory malformation is weak. The high score appears to come from the condition's closeness to airway and ciliary disorders in the model's ontology. There is no evidence that salmeterol treats structural malformations of the respiratory tract, which are developmental defects that a bronchodilator would not be expected to correct. The prediction should be read as a hypothesis only.

## Clinical Trial Evidence

Neither registered trial studies respiratory malformation as a treatment target.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00005110](https://clinicaltrials.gov/study/NCT00005110) | NA | Suspended | Not reported | Six-month inhaled salmeterol vs albuterol or placebo in cystic fibrosis. This is a different disease. |
| [NCT01681979](https://clinicaltrials.gov/study/NCT01681979) | Not applicable | Completed | 1 | Asthma treatment in pregnancy and adverse pregnancy outcomes. It does not evaluate malformation as a treatment target. |

## Literature Evidence

The publications retrieved concern related airway conditions, mostly asthma and COPD, and none studies respiratory malformation.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34902004](https://pubmed.ncbi.nlm.nih.gov/34902004/) | 2022 | Randomized clinical trial | JAMA Pediatrics | Inhaled corticosteroids alone or with long-acting beta2-agonists for reduced lung function in preterm-born children. |
| [15293609](https://pubmed.ncbi.nlm.nih.gov/15293609/) | 2004 | Randomized crossover study | Eur Respir J | Salmeterol and exercise ventilation in COPD (23 patients). |
| [20528601](https://pubmed.ncbi.nlm.nih.gov/20528601/) | 2010 | Review | J Asthma | Budesonide/formoterol inhaler in persistent asthma. |
| [26229457](https://pubmed.ncbi.nlm.nih.gov/26229457/) | 2015 | Review | Int J COPD | Dual bronchodilation in COPD. |
| [22608352](https://pubmed.ncbi.nlm.nih.gov/22608352/) | 2012 | Basic/ex vivo study | Respir Med | Depressed ciliary beating in nasal cilia from COPD patients. |
| [20703071](https://pubmed.ncbi.nlm.nih.gov/20703071/) | 2010 | Case report | Arerugi | Primary ciliary dyskinesia in a patient previously treated as asthma. |

## New Zealand Market Information

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2243903 | Serevent | Salmeterol | Aerosol inhaler CFC-free, 25 mcg per dose | Yes |
| 479489 | Serevent Accuhaler | Salmeterol | Powder for inhalation, 50 mcg per dose, breath ac… | Yes |
| 2120208 | Seretide | Fluticasone with salmeterol | Aerosol inhaler 125 mcg with salmeterol 25 mcg | Yes |
| 2120194 | Seretide | Fluticasone with salmeterol | Aerosol inhaler 50 mcg with salmeterol 25 mcg | Yes |
| 2120216 | Seretide Accuhaler | Fluticasone with salmeterol | Powder for inhalation 100 mcg with salmeterol 50… | Yes |
| 2120224 | Seretide Accuhaler | Fluticasone with salmeterol | Powder for inhalation 250 mcg with salmeterol 50… | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

## Safety Considerations

Please refer to the package insert for safety information.

Drug interaction screening returned 100 records. Examples:
- **Major:** clarithromycin.
- **Moderate:** epinephrine, famotidine, acarbose, and several antidiabetic agents (e.g., alogliptin, canagliflozin, dapagliflozin, empagliflozin).
- **Minor:** corticosteroids such as hydrocortisone, dexamethasone, budesonide and beclomethasone.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high (0.9991), but no trial or publication studies respiratory malformation, and the mechanistic link is weak. The evidence is limited to the model prediction.

**To proceed, the following is needed:**
- Mechanism-of-action data and a defensible biological rationale for a bronchodilator in structural airway malformation
- Studies that directly address this condition
- Package insert warnings and contraindications from Medsafe, for a safety review

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


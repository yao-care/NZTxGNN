---
layout: default
title: Terbutaline
parent: Model Prediction Only (L5)
nav_order: 443
evidence_level: L5
indication_count: 2
last_modified_at: 2026-11-06
monitor_inn: "terbutaline"
predicted_indications: ["respiratory malformation", "Rienhoff syndrome"]
description: "Terbutaline: predicted new indication Respiratory malformation (evidence level L5). Approved use: Not recorded in the local data"
---

# Terbutaline
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

## Terbutaline: Respiratory malformation
{: .text-alpha }

## One-Sentence Summary

Terbutaline is an inhaled β2-adrenergic agonist bronchodilator. The TxGNN model predicts it may be relevant to **respiratory malformation**, but the evidence is weak: only **1 clinical trial** (a general paediatric pharmacokinetics study) and **13 publications** were retrieved, and none directly studies treatment of this condition.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Respiratory malformation |
| TxGNN Prediction Score | 0.9952 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on general pharmacology, terbutaline is a β2-adrenergic agonist that relaxes airway smooth muscle and produces bronchodilation. It may also enhance mucociliary clearance, as suggested by a small older study of terbutaline aerosol in chronic sinusitis.

In principle, this bronchodilator action could ease secondary airway hyperreactivity or obstructive symptoms in patients with structural airway abnormalities. However, there is no evidence that terbutaline affects the malformation itself. The high score (0.9952) most likely reflects network proximity to respiratory disease terms rather than a true therapeutic link. The retrieved literature concerns mostly asthma, bronchitis, or unrelated topics, such as fetal heart block, feline lung disease and an autism hypothesis.

A second prediction, Rienhoff syndrome (score 0.9938), has no trials or literature at all, and no plausible mechanistic link to β2-agonism can be established from the available data.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04278404](https://clinicaltrials.gov/study/NCT04278404) | N/A | Recruiting | 5000 | General pharmacokinetics and safety study of understudied drugs given to children. Not specific to respiratory malformation and provides no efficacy evidence for it. |

## Literature Evidence

None of the publications below studies terbutaline in respiratory malformation. The more relevant ones are listed first.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3281307](https://pubmed.ncbi.nlm.nih.gov/3281307/) | 1988 | RCT | Thorax | Double-blind comparison of subcutaneous adrenaline and terbutaline in 20 patients with acute severe asthma. |
| [1629507](https://pubmed.ncbi.nlm.nih.gov/1629507/) | 1992 | RCT | J Allergy Clin Immunol | Inhaled budesonide vs terbutaline on airway inflammation and lung function in 14 adults with newly diagnosed asthma. |
| [12061354](https://pubmed.ncbi.nlm.nih.gov/12061354/) | 2002 | Cohort | Acta Paediatr | Central airway stenosis (vascular rings, subglottic and tracheal stenosis) in 14 school-aged children, with differential diagnosis from asthma. |
| [1620986](https://pubmed.ncbi.nlm.nih.gov/1620986/) | 1992 | Cohort | Respiration | Abnormal respiratory impedance in 33 chronic bronchitis patients, acutely reversed by inhaled terbutaline. |
| [42303000](https://pubmed.ncbi.nlm.nih.gov/42303000/) | 2026 | Cohort | Respir Med | Long-term treatment of infants with post-infectious bronchiolitis obliterans: symptoms, lung function and imaging. |
| [6314736](https://pubmed.ncbi.nlm.nih.gov/6314736/) | 1983 | Experimental / clinical | Acta Otolaryngol Suppl | β-adrenergic stimulant enhanced ciliary beating; terbutaline aerosol evaluated in chronic sinusitis. |
| [8327281](https://pubmed.ncbi.nlm.nih.gov/8327281/) | 1993 | Methodology | Pediatr Pulmonol | Nonlinear analysis of flow-volume plots in intubated premature infants after terbutaline. |
| [11813927](https://pubmed.ncbi.nlm.nih.gov/11813927/) | 2001 | Case series | Cardiol Young | Oral terbutaline in fetal complete heart block (not a respiratory condition). |

## New Zealand Market Information

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2578417 | Bricanyl Turbuhaler | Terbutaline sulphate | Powder for inhalation, 200 mcg per dose (equivale… | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

## Safety Considerations

- **Conditions flagged as cautions or contraindications in reference data** (not label text): hypertension and hypertensive disorders, epilepsy, hyperglycaemia, diabetes mellitus and diabetic ketoacidosis, potassium deficiency, hyperthyroidism (Graves disease, thyrotoxicosis), coronary artery disease and myocardial ischaemia, heart conduction disease, sudden cardiac arrest, pulmonary oedema, respiratory failure, and placental abruption.
- **Drug Interactions**: 407 interactions were recorded. Those listed include:
  - Moderate: epinephrine, clarithromycin, and several antidiabetic agents (acarbose, alogliptin, pioglitazone, canagliflozin, chlorpropamide, albiglutide).
  - Moderate: laxatives and bowel-preparation agents (bisacodyl, picosulfuric acid, polyethylene glycol with electrolytes), plus loperamide, famotidine and isometheptene.
  - Minor: corticosteroids (hydrocortisone, dexamethasone, budesonide, betamethasone, triamcinolone, beclomethasone).

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a computational score alone. No retrieved study directly addresses respiratory malformation, and the only registered trial is a general paediatric pharmacokinetics study. Pharmacologically, any benefit would be limited to symptom relief, not correction of the malformation.

**To proceed, the following is needed:**
- Direct clinical or preclinical evidence of terbutaline benefit in respiratory malformation, for example symptomatic airway obstruction in affected patients
- Mechanism of action data and a clearer rationale for the prediction
- Package insert warnings and contraindications for the New Zealand product
- A safety assessment for the paediatric and cardiovascular populations most likely to be involved

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


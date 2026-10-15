---
layout: default
title: Epinephrine
parent: High Evidence (L1-L2)
nav_order: 180
evidence_level: L1
indication_count: 4
---

# Epinephrine
{: .fs-9 }

Evidence Level: **L1** | Predicted Indications: **4** 
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

# Epinephrine: From Asthma and Anaphylaxis to Obstructive Lung Disease

## One-Sentence Summary

Epinephrine is an adrenergic agonist originally used for asthma, bronchitis, emphysema, bronchiectasis, anaphylaxis, hypotensive disorders and open-angle glaucoma.
The TxGNN model predicts it may be effective for **obstructive lung disease**, though this is largely an extension of its existing respiratory uses rather than a true new indication.
Supporting this are **15 registered clinical trials** (4 completed Phase 3 trials among them) and **20 publications**.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Asthma, bronchitis, pulmonary emphysema, bronchiectasis, anaphylaxis, hypotensive disorder, open-angle glaucoma (from the knowledge graph; the New Zealand licence records contain no indication text) |
| Predicted New Indication | Obstructive lung disease |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L1 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 6 (all cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the source data. Based on general pharmacology, epinephrine is a non-selective adrenergic agonist. Beta-2 receptor activation relaxes bronchial smooth muscle and produces bronchodilation, while alpha-1 activity reduces mucosal oedema through vasoconstriction.

Obstructive lung disease covers asthma and chronic obstructive pulmonary disease. Asthma, bronchitis, emphysema and bronchiectasis are already among epinephrine's known indications, so this prediction mostly reflects the existing label rather than a new therapeutic direction. The mechanism fits well with the airway narrowing and oedema seen in these conditions.

Other predictions in this analysis are weaker:
- **Food-dependent exercise-induced anaphylaxis** (score 99.57%, evidence level L4): a subtype of anaphylaxis, which is already an original indication. There are no clinical trials for this subtype; evidence is limited to reviews and case reports.
- **Respiratory malformation** (score 99.56%, evidence level L3): epinephrine would not correct structural defects. The one related trial concerns transient tachypnoea of the newborn, a functional condition. This is best treated as a research question.
- **Rienhoff syndrome** (score 99.57%, evidence level L5): no trials, no literature and no mechanistic rationale. Recommendation: Hold.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01357642](https://clinicaltrials.gov/study/NCT01357642) | Phase 3 | Completed | 373 | 12-week efficacy and safety of epinephrine HFA inhalation aerosol vs placebo and Primatene Mist in adolescents and adults with asthma |
| [NCT01476904](https://clinicaltrials.gov/study/NCT01476904) | Phase 3 | Completed | 208 | 3-month double-blind safety extension of epinephrine inhalation vs placebo in asthma |
| [NCT01460511](https://clinicaltrials.gov/study/NCT01460511) | Phase 3 | Completed | 70 | 4-week placebo-controlled study of epinephrine inhalation aerosol (E004) in children aged 4-11 with asthma |
| [NCT01737905](https://clinicaltrials.gov/study/NCT01737905) | Phase 3 | Completed | 28 | Single-dose crossover study of E004 in children with asthma |
| [NCT01705964](https://clinicaltrials.gov/study/NCT01705964) | Phase 4 | Completed | 49 | Intramuscular epinephrine as an add-on to inhaled beta-2 agonists in severe paediatric asthma exacerbation |
| [NCT01834820](https://clinicaltrials.gov/study/NCT01834820) | Phase 4 | Completed | 120 | Epinephrine, dexamethasone and hypertonic saline in bronchiolitis (combination therapy) |
| [NCT00817466](https://clinicaltrials.gov/study/NCT00817466) | Phase 4 | Unknown | 500 | Comparison of inhalation treatments in infants with acute bronchiolitis |
| [NCT00114478](https://clinicaltrials.gov/study/NCT00114478) | Not applicable | Unknown | 600 | Epinephrine vs albuterol in bronchiolitis |
| [NCT04207840](https://clinicaltrials.gov/study/NCT04207840) | Phase 4 | Completed | 28 | Systemic exposure of Primatene Mist vs intramuscular epinephrine and ProAir in healthy adults (pharmacokinetics) |
| [NCT01255709](https://clinicaltrials.gov/study/NCT01255709) | Phase 2 | Completed | 24 | Pharmacokinetics of epinephrine HFA inhaler in healthy volunteers |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35093318](https://pubmed.ncbi.nlm.nih.gov/35093318/) | 2022 | RCT | The Journal of Pediatrics | Corticosteroids plus nebulised epinephrine vs standard care for duration of positive pressure support in children with bronchiolitis in intensive care |
| [33893229](https://pubmed.ncbi.nlm.nih.gov/33893229/) | 2021 | Network meta-analysis | Pediatrics | Comparative efficacy of bronchiolitis interventions in acute care |
| [21678340](https://pubmed.ncbi.nlm.nih.gov/21678340/) | 2011 | Systematic review | Cochrane Database Syst Rev | Epinephrine for bronchiolitis |
| [14974006](https://pubmed.ncbi.nlm.nih.gov/14974006/) | 2004 | Systematic review | Cochrane Database Syst Rev | Earlier Cochrane review of epinephrine for bronchiolitis |
| [35953216](https://pubmed.ncbi.nlm.nih.gov/35953216/) | 2022 | Review | Emerg Med Clin North Am | Evaluation and management of asthma and COPD exacerbations in the emergency department |
| [30488718](https://pubmed.ncbi.nlm.nih.gov/30488718/) | 2019 | Review | Expert Rev Respir Med | Role of racemic epinephrine, corticosteroids, hypertonic saline and high-flow oxygen in paediatric bronchiolitis |
| [23393929](https://pubmed.ncbi.nlm.nih.gov/23393929/) | 2012 | Review | Duodecim | Bronchiolitis overview; racemic epinephrine inhalation used when required |
| [20876171](https://pubmed.ncbi.nlm.nih.gov/20876171/) | 2010 | Cost-effectiveness analysis | Pediatrics | Cost-effectiveness of epinephrine and dexamethasone in bronchiolitis |
| [11940264](https://pubmed.ncbi.nlm.nih.gov/11940264/) | 2002 | Review | Critical Care | Clinical review of severe asthma |
| [30856157](https://pubmed.ncbi.nlm.nih.gov/30856157/) | 2019 | News item | Med Lett Drugs Ther | Return of over-the-counter Primatene Mist |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2309823 | Aspen Adrenaline | Inj 1 in 1,000, 1 ml ampoule | Not recorded (cancelled) |
| 603341 | DBL Adrenaline | Inj 1 in 1,000, 1 ml ampoule | Not recorded (cancelled) |
| 534196 | Hospira | Inj 1 in 10,000, 10 ml ampoule | Not recorded (cancelled) |
| 208191 | Epipen Jr | Inj 0.15 mg per 0.3 ml auto-injector | Not recorded (cancelled) |
| 2390949 | Aspen Adrenaline | Inj 1 in 10,000, 10 ml ampoule | Not recorded (cancelled) |

A sixth licence (223972, Epipen, Inj 0.3 mg per 0.3 ml auto-injector) is also cancelled. All six licences are injectable forms; no inhaled epinephrine product is licensed in New Zealand.

## Safety Considerations

**Contraindications flagged in the knowledge graph** (drug-disease associations, not package insert text): hypertension, cardiovascular and heart disease, coronary artery disease, myocardial infarction, atrial fibrillation, heart conduction disease, sudden cardiac arrest, hyperthyroidism, diabetes mellitus and diabetic ketoacidosis, liver and kidney disease or failure, epilepsy, angle-closure glaucoma, benign prostatic hyperplasia, respiratory failure, myasthenia gravis and other neuromuscular junction disease, gallbladder disease, potassium deficiency, hemoglobinopathy, butyrylcholinesterase deficiency, and pyoureter.

**Drug interactions:** 100 interactions were identified. Those shown include:
- **Major:** amitriptyline, amoxapine
- **Moderate:** beta-blockers (acebutolol, atenolol, betaxolol), other beta-agonists (formoterol, salbutamol, arformoterol), stimulants (methylphenidate, amphetamine, atomoxetine, benzphetamine), antidiabetic drugs (acarbose, acetohexamide, albiglutide, alogliptin), and antipsychotics (amisulpride, aripiprazole, asenapine)

Please refer to the package insert for official warnings and contraindications.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
- Several completed Phase 3 trials of inhaled epinephrine in asthma support the obstructive lung disease prediction, but this largely mirrors existing indications.
- No valid New Zealand licences remain, none cover an inhaled form, and the safety profile includes extensive cardiovascular and metabolic cautions.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (currently missing; a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Clarification of the regulatory route for an inhaled epinephrine product in New Zealand
- A cardiovascular and drug-interaction risk management plan, particularly for patients on beta-blockers or tricyclic antidepressants
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


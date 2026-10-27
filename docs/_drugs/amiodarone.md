---
layout: default
title: Amiodarone
parent: Model Prediction Only (L5)
nav_order: 34
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Amiodarone: predicted new indication Catecholaminergic polymorphic ventricular tachycardia (evidence level L5). Approved use: Ventricular fibrillation"
monitor_inn: "Amiodarone"
predicted_indications: ["catecholaminergic polymorphic ventricular tachycardia", "incessant infant ventricular tachycardia", "ventricular tachycardia", "obsolete bundle branch block", "rheumatoid arthritis", "brachydactyly-syndactyly syndrome", "heparin cofactor 2 deficiency", "trichotillomania", "colobomatous microphthalmia-rhizomelic dysplasia syndrome", "gout"]
---

# Amiodarone
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

## Amiodarone: From Ventricular Fibrillation to Catecholaminergic Polymorphic Ventricular Tachycardia
{: .text-alpha }

## One-Sentence Summary

Amiodarone is an antiarrhythmic drug, originally used to treat ventricular fibrillation.
The TxGNN model's top-ranked prediction is **catecholaminergic polymorphic ventricular tachycardia (CPVT)**, but only **9 publications** (mostly case reports and cohort descriptions, none showing amiodarone efficacy) and **no clinical trials** support it.
Among the other predictions, **ventricular tachycardia** is far better supported, with **18 clinical trials** and **20 publications**, although this is largely an established use rather than true repurposing.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Ventricular fibrillation (the approved indication text is not recorded in the New Zealand licence data) |
| Predicted New Indication | Catecholaminergic polymorphic ventricular tachycardia (top-ranked); ventricular tachycardia and incessant infant ventricular tachycardia are the better-supported predictions |
| TxGNN Prediction Score | 99.78% (CPVT) |
| Evidence Level | L4 for CPVT (L1 for ventricular tachycardia; L2 for incessant infant ventricular tachycardia) |
| New Zealand Market Status | Cancelled only (no currently valid authorisation) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold for CPVT; Proceed with Guardrails for ventricular tachycardia and incessant infant ventricular tachycardia |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data are not available for this report. Based on known pharmacology, amiodarone is a class III antiarrhythmic with multichannel effects (potassium, sodium and calcium channel block plus beta-adrenergic antagonism). It prolongs action potential duration and refractoriness. Pharmacology data compiled for this report also list interactions with Nav1.5, Kv1.7 and the M5 receptor.

**CPVT (rank 1):** CPVT is caused by RYR2 or CASQ2 dysfunction, which leaks calcium during diastole and triggers delayed afterdepolarisations. Amiodarone's calcium-channel and beta-blocking actions offer a plausible but weak link. Guideline-preferred therapy is beta-blockers, with flecainide as an add-on, and amiodarone is generally considered ineffective for CPVT. The retrieved literature contains no direct evidence of amiodarone efficacy in CPVT, so this prediction is best treated as a research question.

**Ventricular tachycardia (rank 3):** This is mechanistically adjacent to the original indication, since ventricular tachycardia can degenerate into ventricular fibrillation. Amiodarone directly suppresses re-entrant ventricular arrhythmias.

**Incessant infant ventricular tachycardia (rank 2):** Paediatric data support amiodarone in refractory incessant tachycardias, but most evidence comes from mixed tachyarrhythmia populations rather than ventricular tachycardia alone.

**Lower-ranked predictions (ranks 4–10):** Obsolete bundle branch block, rheumatoid arthritis, brachydactyly-syndactyly syndrome, heparin cofactor 2 deficiency, trichotillomania, colobomatous microphthalmia-rhizomelic dysplasia syndrome and gout have no plausible therapeutic link and appear to be knowledge-graph artefacts. For bundle branch block, amiodarone slows conduction and could worsen block. For rheumatoid arthritis, the retrieved literature describes amiodarone toxicity, not benefit. All are held at L5.

---

## Clinical Trial Evidence

There are currently no clinical trials registered for CPVT. The table below lists the most relevant trials for the better-supported prediction, **ventricular tachycardia**.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01401647](https://clinicaltrials.gov/study/NCT01401647) | Phase 3 | Completed | 3024 | ALPS: amiodarone vs lidocaine vs placebo in out-of-hospital cardiac arrest from VF/pulseless VT |
| [NCT00000531](https://clinicaltrials.gov/study/NCT00000531) | Phase 3 | Completed | Not reported | AVID: antiarrhythmic drugs (mainly amiodarone) vs implantable defibrillators in life-threatening ventricular arrhythmias |
| [NCT00383799](https://clinicaltrials.gov/study/NCT00383799) | Phase 4 | Terminated | 302 | PROCAMIO: IV amiodarone vs IV procainamide in well-tolerated wide-QRS tachycardia; early termination limits conclusions |
| [NCT00257959](https://clinicaltrials.gov/study/NCT00257959) | Phase 4 | Completed | 400 | OPTIC: amiodarone plus beta-blocker vs sotalol vs beta-blocker alone for ICD shock prevention |
| [NCT03299517](https://clinicaltrials.gov/study/NCT03299517) | Phase 4 | Unknown | 80 | Amiodarone vs lidocaine in stable VT in the emergency room |
| [NCT03855826](https://clinicaltrials.gov/study/NCT03855826) | Phase 4 | Unknown | 756 | Nifekalant vs amiodarone for VT and VF |
| [NCT02830360](https://clinicaltrials.gov/study/NCT02830360) | Phase 4 | Completed | 416 | VANISH2: catheter ablation vs antiarrhythmic drug therapy after myocardial infarction |
| [NCT01722942](https://clinicaltrials.gov/study/NCT01722942) | N/A | Unknown | 1100 | CHAGASICS: amiodarone vs ICD for primary prevention in Chagas cardiomyopathy |
| [NCT01097330](https://clinicaltrials.gov/study/NCT01097330) | Phase 3 | Terminated | 12 | Catheter ablation vs amiodarone for shock prophylaxis in ICD patients |
| [NCT03734562](https://clinicaltrials.gov/study/NCT03734562) | N/A | Completed | 180 | Substrate ablation vs antiarrhythmic drugs in scar-related monomorphic VT |

---

## Literature Evidence

Literature for the top prediction (CPVT) consists of cohort studies, a general review and case reports, with no amiodarone efficacy data. The table below lists the most relevant publications across the better-supported predictions.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|---------|---------|
| [27354046](https://pubmed.ncbi.nlm.nih.gov/27354046/) | 2017 | RCT | European Heart Journal | PROCAMIO: IV procainamide vs IV amiodarone for tolerated wide-QRS (probably ventricular) tachycardia |
| [16316969](https://pubmed.ncbi.nlm.nih.gov/16316969/) | 2005 | RCT | Circulation | Double-blind trial of IV amiodarone for incessant tachyarrhythmias in children |
| [14508682](https://pubmed.ncbi.nlm.nih.gov/14508682/) | 2003 | Cohort/Case series | European Journal of Pediatrics | Safety and efficacy of IV amiodarone in 23 infants with life-threatening incessant tachycardias |
| [30084131](https://pubmed.ncbi.nlm.nih.gov/30084131/) | 2018 | Cohort | Emergency Medicine Australasia | Paramedic-administered amiodarone for stable VT in the prehospital setting |
| [20730062](https://pubmed.ncbi.nlm.nih.gov/20730062/) | 2010 | Review | Vascular Health and Risk Management | Amiodarone for treatment and prevention of VF and VT |
| [36878200](https://pubmed.ncbi.nlm.nih.gov/36878200/) | 2023 | Review | Cardiology | Pharmacotherapy in ventricular arrhythmias |
| [9137212](https://pubmed.ncbi.nlm.nih.gov/9137212/) | 1997 | Review | J Am Coll Cardiol | Pharmacology and efficacy of IV amiodarone in malignant ventricular arrhythmias |
| [41217320](https://pubmed.ncbi.nlm.nih.gov/41217320/) | 2026 | Trial substudy | J Am Coll Cardiol | VANISH2 substudy: catheter ablation vs sotalol or amiodarone for VT |
| [2114081](https://pubmed.ncbi.nlm.nih.gov/2114081/) | 1990 | Case series | Arch Mal Coeur Vaiss | Incessant idiopathic VT in infants; arrhythmia reduced after oral amiodarone in 5 cases |
| [26513538](https://pubmed.ncbi.nlm.nih.gov/26513538/) | 2015 | Review | Expert Opin Pharmacother | Advances in pharmacologic treatment of ventricular arrhythmias (retrieved for CPVT) |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2565129 | Max Health | Inj 50 mg per ml, 3 ml ampoule | Not recorded (cancelled) |
| 770639 | Aratac | Tab 200 mg | Not recorded (cancelled) |
| 770620 | Aratac | Tab 100 mg | Not recorded (cancelled) |

All three authorisations are cancelled, so no valid amiodarone product is currently registered in the compiled data.

---

## Safety Considerations

- **Disease-level contraindication signals** (from knowledge-graph drug-disease edges, not label text):
  - Cardiac conduction and rhythm: long QT syndrome (including familial), torsades de pointes, atrioventricular block, sick sinus syndrome
  - Thyroid: hypothyroidism, myxedema, congenital hypothyroidism, hyperthyroidism, Graves disease, thyrotoxicosis
  - Liver and biliary: liver disease, drug-induced liver injury, gallbladder disease
  - Lung: interstitial lung disease, eosinophilic pneumonia, pulmonary eosinophilia, extrinsic allergic alveolitis, hypersensitivity pneumonitis, farmer's lung disease
  - Eye and nerve: optic neuritis, optic nerve disease, visual pathway disease, corneal deposit, peripheral nervous system disease, peripheral neuropathy
  - Metabolic: potassium deficiency disease
- **Pharmacological interactions:** The compiled pharmacology data show interactions with the Nav1.5 sodium channel, the Kv1.7 potassium channel and the M5 muscarinic receptor. These are target-level interactions, not drug-drug interaction severity ratings.
- **Literature-reported concerns:**
  - Thyroid dysfunction (hypothyroidism and hyperthyroidism)
  - Pulmonary toxicity, including pulmonary fibrosis and diffuse alveolar hemorrhage
  - Proarrhythmia
  - Iodine load in neonates
  - Drug-induced ventricular dyssynchrony in a newborn
  - Rhabdomyolysis when combined with atorvastatin and fluconazole
  - Interaction with cyclosporine

Package insert warnings and contraindications are not available in the compiled data. Please refer to the package insert for authoritative safety information.

---

## Conclusion and Next Steps

**Decision: Hold for CPVT (top-ranked prediction); Proceed with Guardrails for ventricular tachycardia and incessant infant ventricular tachycardia**

**Rationale:**
- For CPVT, the highest-scoring prediction, there are no clinical trials and no literature showing amiodarone efficacy. Guidelines favour beta-blockers and flecainide, and amiodarone is generally considered ineffective, so the finding is a research question (L4).
- Ventricular tachycardia has strong support (L1) from multiple completed Phase 3/4 trials, but this is mostly an established use. Incessant infant ventricular tachycardia has moderate support (L2) from paediatric studies, including a double-blind RCT in mixed incessant tachyarrhythmias. The remaining predictions (ranks 4–10) have no plausible link and are held.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (currently missing and blocking for safety screening)
- Detailed mechanism of action data
- Clarification of the New Zealand regulatory position, since all three authorisations are cancelled and no approved indication text is recorded
- For incessant infant ventricular tachycardia: studies restricted to ventricular tachycardia, plus a monitoring plan for thyroid, hepatic, pulmonary and proarrhythmic toxicity and for neonatal iodine load
- For CPVT: direct comparative evidence against beta-blockers and flecainide before any further development
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


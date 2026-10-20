---
layout: default
title: Salbutamol
parent: Model Prediction Only (L5)
nav_order: 418
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Salbutamol: predicted new indication Papillary conjunctivitis (evidence level L5). Approved use: Asthma, bronchial disease"
monitor_inn: "SALBUTAMOL"
predicted_indications: ["papillary conjunctivitis", "nasal cavity disease", "pharyngitis", "bronchitis", "acute laryngopharyngitis", "anaphylaxis", "common cold", "atopic conjunctivitis", "anorectal stricture"]
---

# Salbutamol
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

# Salbutamol: From Asthma to Papillary Conjunctivitis

## One-Sentence Summary

Salbutamol is a short-acting beta-2 agonist bronchodilator, originally used to treat asthma and bronchial disease.
The TxGNN model's top-ranked prediction is **papillary conjunctivitis**, but no clinical trials or publications currently support it (evidence level L5, model prediction only).
Other predicted indications have more evidence: **obstructive lung disease** (a 1,118-patient completed Phase 3 trial, though this is close to the existing use), **bronchitis** (9 trials, 20 publications) and **anaphylaxis** (1 Phase 3 trial, terminated early).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Asthma, bronchial disease (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Papillary conjunctivitis |
| TxGNN Prediction Score | 99.996% |
| Evidence Level | L5 |
| New Zealand Market Status | ✓ Marketed |
| Number of Authorizations | 9 (1 valid, 8 cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Salbutamol is known as a beta-2 adrenergic agonist. Its efficacy in asthma and bronchial disease is established, and it works by relaxing airway smooth muscle.

For papillary conjunctivitis, the link is speculative. Beta-2 agonism could plausibly reduce mast-cell mediator release in ocular allergy. Preclinical work supports this idea for related conditions: topical salbutamol strongly suppressed immediate allergic conjunctivitis in guinea pigs, and beta-2 agonists showed topical anti-inflammatory activity in the conjunctiva. However, papillary conjunctivitis is often driven by contact lenses or mechanical irritation, so an allergy-based mechanism may not apply. No human data were retrieved.

The prediction with the strongest support is **obstructive lung disease**, which overlaps directly with the original indications. This is better described as established bronchodilator use than as true repurposing.

---

## Clinical Trial Evidence

No clinical trials are registered for the top-ranked prediction, papillary conjunctivitis. Currently no related clinical trials registered.

For context, the table below shows the most relevant trials for the best-supported predicted indication, **obstructive lung disease** (rank 10).

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02177253](https://clinicaltrials.gov/study/NCT02177253) | Phase 3 | Completed | 1118 | 12-week trial of ipratropium/salbutamol via Respimat vs COMBIVENT, ipratropium alone and placebo in COPD |
| [NCT01721291](https://clinicaltrials.gov/study/NCT01721291) | Phase 4 | Completed | 64 | Inhaler treatment and small-airways assessment in COPD |
| [NCT05987371](https://clinicaltrials.gov/study/NCT05987371) | Phase 2 | Completed | 75 | Placebo-controlled dose exploration of TQC3721 inhalation suspension in COPD (link to salbutamol uncertain) |
| [NCT04446637](https://clinicaltrials.gov/study/NCT04446637) | Phase 3 | Withdrawn | 0 | Ipratropium/levosalbutamol vs salbutamol plus ipratropium in COPD; no participants enrolled |
| [NCT02235428](https://clinicaltrials.gov/study/NCT02235428) | Phase 4 | Terminated | 3 | Ipratropium added to high-dose salbutamol in childhood asthma attacks; uninformative |
| [NCT02686086](https://clinicaltrials.gov/study/NCT02686086) | N/A | Completed | 60 | Bronchodilator delivery by vibrating mesh vs jet nebuliser in COPD |
| [NCT01521572](https://clinicaltrials.gov/study/NCT01521572) | N/A | Unknown | 200 | Effect of salbutamol on respiratory mechanics in healthy people, smokers and COPD patients |

Other predictions with trials:
- **Bronchitis**: [NCT00114478](https://clinicaltrials.gov/study/NCT00114478) (n=600, epinephrine vs albuterol in bronchiolitis) and [NCT00696540](https://clinicaltrials.gov/study/NCT00696540) (Phase 2, hypertonic vs normal saline as salbutamol diluent) are the most relevant. The disease label is ambiguous between bronchitis and bronchiolitis.
- **Anaphylaxis**: [NCT05653024](https://clinicaltrials.gov/study/NCT05653024) is a Phase 3 double-blind trial of inhaled salbutamol vs placebo for food-allergy abdominal pain. It was terminated with only 24 participants, so it is underpowered.

---

## Literature Evidence

No literature was retrieved for papillary conjunctivitis. Currently no related literature available.

The most relevant publications for the other predictions are:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [24937099](https://pubmed.ncbi.nlm.nih.gov/24937099/) | 2014 | Cochrane review | Cochrane Database Syst Rev | Bronchodilators for bronchiolitis in infants |
| [33893229](https://pubmed.ncbi.nlm.nih.gov/33893229/) | 2021 | Network meta-analysis | Pediatrics | Comparative efficacy of bronchiolitis interventions in acute care |
| [36765418](https://pubmed.ncbi.nlm.nih.gov/36765418/) | 2023 | Guideline | Ital J Pediatr | 2022 Italian guidelines on bronchiolitis in infants; management is largely supportive |
| [35383741](https://pubmed.ncbi.nlm.nih.gov/35383741/) | 2022 | RCT | Mymensingh Med J | Nebulized 3% hypertonic saline vs normal saline and salbutamol in acute bronchiolitis |
| [34366098](https://pubmed.ncbi.nlm.nih.gov/34366098/) | 2021 | Case report/Review | J Allergy Clin Immunol Pract | Use of salbutamol for gastrointestinal anaphylaxis |
| [33831623](https://pubmed.ncbi.nlm.nih.gov/33831623/) | 2021 | Clinical study | J Allergy Clin Immunol Pract | Potential efficacy of high-dose inhaled salbutamol for abdominal pain during oral food challenge |
| [26859020](https://pubmed.ncbi.nlm.nih.gov/26859020/) | 2014 | RCT | East Afr Med J | Honey vs salbutamol vs placebo for cough in children with common cold |
| [3666475](https://pubmed.ncbi.nlm.nih.gov/3666475/) | 1987 | Preclinical | Graefes Arch Clin Exp Ophthalmol | Topical salbutamol suppressed immediate allergic conjunctivitis in guinea pigs |
| [1729064](https://pubmed.ncbi.nlm.nih.gov/1729064/) | 1992 | RCT (crossover) | Chest | Theophylline and salbutamol improved pulmonary function in irreversible COPD |
| [18447148](https://pubmed.ncbi.nlm.nih.gov/18447148/) | 2008 | Case report | J Investig Allergol Clin Immunol | Anaphylaxis to salbutamol (safety) |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2026414 (valid) | Ventolin | Aerosol inhaler, 100 mcg per dose CFC free | Not available |
| 2140543 (cancelled) | Ventolin | Oral liquid 400 mcg per ml | Not available |
| 2710811 (cancelled) | UK Cipla | Nebuliser solution, 1 mg per ml, 2.5 ml ampoule | Not available |
| 2154315 (cancelled) | Asthalin | Nebuliser solution, 1 mg per ml, 2.5 ml ampoule | Not available |
| 2404516 (cancelled) | SalAir | Aerosol inhaler, 100 mcg per dose CFC free | Not available |

Only the Ventolin CFC-free inhaler is currently valid. The other eight licences (oral liquid, nebuliser solutions, infusion and injection forms) are cancelled.

---

## Safety Considerations

- **Conditions flagged in the knowledge graph as contraindications or cautions** (from drug-disease edges, not label text): hypertension, long QT syndrome, epilepsy, potassium deficiency, diabetes mellitus and diabetic ketoacidosis, metabolic acidosis, benign prostatic hyperplasia and bladder neck obstruction, ocular hypertension, angle-closure glaucoma, hyperthyroidism and thyrotoxicosis, sudden cardiac arrest, heart conduction disease, myocardial ischemia and coronary artery disease.
- **Drug Interactions**: 100 interactions are recorded. Moderate-level examples include acarbose, famotidine, epinephrine, clarithromycin, cisapride, and several antidiabetic agents (alogliptin, canagliflozin, dapagliflozin, chlorpropamide). Minor-level examples include corticosteroids such as hydrocortisone, budesonide and dexamethasone.
- **Hypersensitivity**: anaphylaxis to salbutamol itself has been reported in the literature.

For full warnings and contraindications, please refer to the package insert.

---

## Conclusion and Next Steps

**Decision: Hold** (for the top-ranked prediction, papillary conjunctivitis)

**Rationale:**
- Papillary conjunctivitis has no supporting trials or literature, only a high model score and a speculative mast-cell mechanism. Preclinical evidence exists only for allergic conjunctivitis in animals.
- Among the other predictions, obstructive lung disease reaches L1 (a completed Phase 3 trial, n=1118) and could proceed with guardrails, but it is essentially established bronchodilator use rather than new repurposing. Bronchitis (L2) and anaphylaxis (L3) remain research questions.

**To proceed, the following is needed:**
- Mechanism of action data and package insert warnings and contraindications (both currently missing)
- For papillary conjunctivitis: human studies, a decision on whether a topical ocular formulation is feasible (none is marketed in New Zealand), and clarification of whether the cause is allergic or mechanical
- For obstructive lung disease: confirmation of label coverage, plus monitoring of cardiovascular effects and hypokalaemia, and review of drug interactions
- For bronchitis: clarification of whether bronchitis or bronchiolitis is intended, since evidence in infants shows inconsistent benefit
- For anaphylaxis: an adequately powered trial to replace the terminated Phase 3 study
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


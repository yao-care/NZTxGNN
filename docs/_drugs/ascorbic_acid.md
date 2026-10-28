---
layout: default
title: Ascorbic Acid
parent: Model Prediction Only (L5)
nav_order: 46
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Ascorbic Acid: predicted new indication Non-syndromic esophageal malformation (evidence level L5). Approved use: Ascorbic acid deficiency, scurvy"
monitor_inn: "ASCORBIC ACID"
predicted_indications: ["non-syndromic esophageal malformation", "esophageal disease", "congenital prothrombin deficiency", "injury", "biotin metabolic disease", "florid cemento-osseous dysplasia", "segmental odontomaxillary dysplasia", "perinatal disease", "disease by subcellular system affected", "vitamin deficiency disorder"]
---

# Ascorbic Acid
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

## Ascorbic Acid: From Ascorbic Acid Deficiency (Scurvy) to Non-Syndromic Esophageal Malformation
{: .text-alpha }

## One-Sentence Summary

Ascorbic acid (vitamin C) was originally used to treat ascorbic acid deficiency and scurvy.
The TxGNN model's top-ranked prediction is **non-syndromic esophageal malformation**, but there are **0 clinical trials** and **0 publications** supporting it, so it is a computational prediction only.
Among the other predicted indications, **injury** has the most support (16 clinical trials, 20 publications), and it is the more credible direction to pursue.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Ascorbic acid deficiency, scurvy (the New Zealand licence record has no approved indication text) |
| Predicted New Indication | Non-syndromic esophageal malformation |
| TxGNN Prediction Score | 0.9996 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

The top-ranked prediction has no supporting evidence. The best-supported predicted indication is "injury" (L2), covered below.

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, ascorbic acid is an essential vitamin whose efficacy in scurvy is well established. It acts as an antioxidant and as a cofactor for collagen synthesis.

**Top-ranked prediction (non-syndromic esophageal malformation):** This is a structural congenital malformation. Ascorbic acid has no plausible mechanism for correcting it, and there are no trials or publications. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence, so the prediction is not considered credible.

**Better-supported predictions:**
- **Injury (L2):** Antioxidant and collagen-supporting actions are plausible for oxidative-stress-driven tissue damage (sepsis-induced lung injury, burns, trauma, surgical injury, wound healing). Evidence is mixed, and the sepsis trial literature includes notable failures.
- **Esophageal disease (L4):** Ascorbic acid inhibits N-nitrosamine formation, which is relevant to esophageal cancer epidemiology. However, a rat acid-reflux model showed *enhanced* esophageal carcinogenesis when ascorbic acid was combined with sodium nitrite, which is a safety signal.
- **Perinatal disease (L4):** There is a biological rationale in neonatal hypoxic-ischaemic injury, but the evidence is one small trial of a combination therapy plus preclinical work.
- **Vitamin deficiency disorder (L3):** This is essentially the drug's original use, so it is not true repurposing.

---

## Clinical Trial Evidence

There are no clinical trials for the top-ranked indication (non-syndromic esophageal malformation). Currently no related clinical trials registered.

For reference, the most relevant trials for the best-supported indication, **injury**:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02106975](https://clinicaltrials.gov/study/NCT02106975) | Phase 2 | Completed | 170 | IV vitamin C in sepsis-induced acute lung injury; organ failure score and inflammation biomarkers |
| [NCT03509662](https://clinicaltrials.gov/study/NCT03509662) | Phase 2 | Completed | 270 | Early high-dose vitamin C in post-cardiac arrest syndrome |
| [NCT04344184](https://clinicaltrials.gov/study/NCT04344184) | Phase 2 | Completed | 47 | Safety of 96-hour IV vitamin C in COVID-19 acute lung injury |
| [NCT04153487](https://clinicaltrials.gov/study/NCT04153487) | Phase 2 | Completed | 40 | Ascorbic acid infusion in transfusion-related acute lung injury |
| [NCT03965637](https://clinicaltrials.gov/study/NCT03965637) | Phase 3 | Completed | 80 | Ascorbic acid on blood loss and wound healing in abdominal hysterectomy |
| [NCT01167569](https://clinicaltrials.gov/study/NCT01167569) | Phase 4 | Completed | 57 | High-dose vitamin C after cardiac surgery (double-blind RCT) |
| [NCT05327348](https://clinicaltrials.gov/study/NCT05327348) | Phase 3 | Unknown | 34 | Perioperative parenteral ascorbic acid for oxidative stress in free flap surgery |
| [NCT06384833](https://clinicaltrials.gov/study/NCT06384833) | Phase 4 | Unknown | 110 | Vitamin C plus N-acetylcysteine after rotator cuff repair |
| [NCT02455180](https://clinicaltrials.gov/study/NCT02455180) | Phase 4 | Completed | 20 | Pharmacokinetics of high-dose IV vitamin C in critically ill patients |
| [NCT01434121](https://clinicaltrials.gov/study/NCT01434121) | Phase 1/2 | Completed | 24 | Safety of IV ascorbic acid in human sepsis |

---

## Literature Evidence

There is no literature for the top-ranked indication. Currently no related literature available.

For reference, selected publications for **injury**:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38051821](https://pubmed.ncbi.nlm.nih.gov/38051821/) | 2024 | Guideline | J Burn Care Res | American Burn Association guideline on burn shock resuscitation |
| [23994411](https://pubmed.ncbi.nlm.nih.gov/23994411/) | 2013 | Meta-analysis | J Am Coll Cardiol | Ascorbic acid for prevention of contrast-induced acute kidney injury |
| [31440996](https://pubmed.ncbi.nlm.nih.gov/31440996/) | 2020 | Review | Neurocrit Care | High-dose IV ascorbic acid in traumatic brain injury |
| [27600125](https://pubmed.ncbi.nlm.nih.gov/27600125/) | 2016 | Review | Crit Care Clin | Vitamin C in burn resuscitation |
| [33675075](https://pubmed.ncbi.nlm.nih.gov/33675075/) | 2021 | Review | JPEN | Ascorbic acid in the acute care setting |
| [35881034](https://pubmed.ncbi.nlm.nih.gov/35881034/) | 2022 | Modeling study | J Trauma Acute Care Surg | Explains failures of hydrocortisone, ascorbic acid and thiamine trials in sepsis |
| [37199082](https://pubmed.ncbi.nlm.nih.gov/37199082/) | 2023 | Animal study | Clin Exp Pharmacol Physiol | Melatonin and ascorbic acid protect against sepsis-induced lung injury in rats |
| [19111610](https://pubmed.ncbi.nlm.nih.gov/19111610/) | 2009 | RCT | Free Radic Biol Med | Vitamin E and C supplementation and inflammation before ACL surgery |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2642255 | Cvite | Tab 100 mg | Not recorded (authorisation valid) |

---

## Safety Considerations

- **Drug Interactions:** 25 interactions were identified. Moderate-level interactions include amphetamine, dextroamphetamine, lisdexamfetamine, metamfetamine, benzphetamine, aluminium hydroxide, bortezomib and deferoxamine. The remainder are minor, including paclitaxel, cisplatin, doxorubicin, ethinylestradiol, pseudoephedrine, ephedrine, flecainide, fluphenazine, indinavir and mexiletine.
- **Knowledge-graph contraindication signals** (not label text): hypertension, epilepsy, kidney disease, liver disease, peptic ulcer disease, diabetes mellitus, diabetic ketoacidosis, sarcoidosis, hypercalcaemia, nephrocalcinosis, primary hyperoxaluria, hereditary hemochromatosis, sickle cell anaemia, blood coagulation disease, rheumatoid arthritis, systemic lupus erythematosus, multiple sclerosis, among others. These should be verified against the product label.
- **Indication-specific signal:** In a rat acid-reflux model, combined ascorbic acid and sodium nitrite enhanced esophageal carcinogenesis. A human case report also describes localised proximal esophagitis after ascorbic acid ingestion.

Package insert warnings and contraindications were not available, so please refer to the package insert for authoritative safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (non-syndromic esophageal malformation) has no trials, no literature and no plausible mechanism. Its score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. Of the other predictions, "injury" (L2) is worth following as a research question, but it is a heterogeneous category with mixed evidence. The drug is marketed in New Zealand, where its only authorisation (1 of 1) is valid, but no label safety data were available, which blocks safety screening.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data (MOA)
- Definition of specific injury subtypes (for example sepsis-induced lung injury, burns, perioperative injury) and a focused review of the Phase 2 RCTs
- Confirmation of whether any valid ascorbic acid product is available in New Zealand
- Review of the esophageal carcinogenesis safety signal before considering any esophageal indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


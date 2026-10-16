---
layout: default
title: Moxifloxacin
parent: Model Prediction Only (L5)
nav_order: 312
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Moxifloxacin: predicted new indication Hyperamylasemia (evidence level L5). Approved use: Bacterial infections (e.g.…"
---

# Moxifloxacin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Moxifloxacin: From Bacterial Infections to Hyperamylasemia

## One-Sentence Summary

Moxifloxacin is a fluoroquinolone antibiotic originally used for bacterial infections such as pneumonia, peritonitis, conjunctivitis and plague.
The TxGNN model's top-ranked prediction is **Hyperamylasemia**, but there are **0 clinical trials** and **0 publications** supporting it, so it rests on model prediction alone.
The only prediction with meaningful supporting evidence is **bubonic plague**, which is closely related to an existing indication and is not a true new use.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Bacterial infections (e.g., streptococcal and staphylococcal infections, bacterial pneumonia, peritonitis, conjunctivitis, plague). Based on the knowledge graph, because the approved indication text in the New Zealand licence record is empty. |
| Predicted New Indication | Hyperamylasemia |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 (0 valid, 1 cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Moxifloxacin is an antibacterial fluoroquinolone that inhibits bacterial DNA gyrase and topoisomerase IV. Detailed mechanism-of-action data is not available in the data compiled for this report, but this class mechanism is well established.

For the top-ranked prediction, **hyperamylasemia**, no plausible mechanism is evident. Moxifloxacin has no known effect on amylase metabolism, and the high score (about 99.98%) is a model output without trial or literature support. The same is true of the other high-scoring predictions: polyclonal hyperviscosity syndrome, congenital analbuminemia, blood group incompatibility, premalignant haematological disease and haematopoietic and lymphoid neoplasm. None has a mechanistic link, and none has supporting trials.

Some predictions point to safety concerns rather than benefits. Peripheral neuropathy is a known fluoroquinolone adverse effect, which makes the prediction of a haematological disease associated with acquired peripheral neuropathy a safety signal. Likewise, fluoroquinolone-associated dysglycaemia is relevant to the congenital analbuminemia case report on recurrent hypoglycaemia.

The one biologically plausible prediction is **bubonic plague**. Moxifloxacin is active against *Yersinia pestis*, and pneumonic plague and plague are already among its original indications. This is an extension within existing uses rather than a new indication. The evidence is preclinical only (animal and in vitro models), with no human trials.

---

## Clinical Trial Evidence

No clinical trials are registered for the top-ranked prediction, hyperamylasemia. Trials below relate to other lower-ranked predictions and are only indirectly relevant. All were graded C for relevance.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00324324](https://clinicaltrials.gov/study/NCT00324324) | Phase 3 | Terminated | 240 | Placebo-controlled trial of moxifloxacin antibacterial prophylaxis after allogeneic stem cell transplantation (listed under congenital haematological disorder). The population is not shown to be a congenital disorder, and any benefit would be infection prevention, not disease modification. |
| [NCT00062231](https://clinicaltrials.gov/study/NCT00062231) | NA | Terminated | 351 | Moxifloxacin alone vs ciprofloxacin plus amoxicillin/clavulanic acid for fever in low-risk neutropenic cancer patients (listed under monoclonal gammopathy and congenital haematological disorder). Addresses febrile neutropenia, not those conditions. |
| [NCT07023029](https://clinicaltrials.gov/study/NCT07023029) | Phase 1 | Completed | 33 | Etavopivat effect on cardiac repolarisation in healthy participants. Moxifloxacin is likely a QT-prolongation positive control, so there is no therapeutic relevance. |

---

## Literature Evidence

No literature is available for hyperamylasemia. The publications below relate to other predictions.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [20052916](https://pubmed.ncbi.nlm.nih.gov/20052916/) | 2009 | Preclinical (mouse) | Antibiotiki i khimioterapiia | Levofloxacin, lomefloxacin and moxifloxacin were highly active against *Y. pestis* strains; ED50 of moxifloxacin and levofloxacin was 5.5-14.0 mg/kg in infected mice. |
| [15555886](https://pubmed.ncbi.nlm.nih.gov/15555886/) | 2004 | Preclinical (mouse) | Int J Antimicrob Agents | Gatifloxacin and moxifloxacin were assessed against systemic and pneumonic plague in mice, compared with ciprofloxacin. All gave full protection for up to 6 h after systemic challenge and up to 30 h after aerosol challenge. |
| [21115791](https://pubmed.ncbi.nlm.nih.gov/21115791/) | 2011 | In vitro model | Antimicrob Agents Chemother | In vitro pharmacodynamic model used to derive a moxifloxacin regimen that optimises kill of *Y. pestis* and prevents resistance. |
| [21486959](https://pubmed.ncbi.nlm.nih.gov/21486959/) | 2011 | In vitro model | Antimicrob Agents Chemother | Comparative efficacy of candidate antibiotics against *Y. pestis* in an in vitro pharmacodynamic model. |
| [35619805](https://pubmed.ncbi.nlm.nih.gov/35619805/) | 2022 | Case report | Front Public Health | Disseminated nocardiosis in a patient with macroglobulinemia. The isolate was susceptible to moxifloxacin, but treatment was meropenem and doxycycline. Infection management, not treatment of the gammopathy. |
| [39901105](https://pubmed.ncbi.nlm.nih.gov/39901105/) | 2025 | Case report | BMC Infect Dis | Co-infection with Nocardia, Candida and Aspergillus in a patient with multiple myeloma. Not evidence for moxifloxacin efficacy. |
| [32181025](https://pubmed.ncbi.nlm.nih.gov/32181025/) | 2020 | Case report | Case Rep Endocrinol | Recurrent hypoglycaemia in congenital analbuminemia. Does not evaluate moxifloxacin as a treatment. |
| [29623187](https://pubmed.ncbi.nlm.nih.gov/29623187/) | 2018 | Case report | Ther Adv Drug Saf | Moxifloxacin-induced tinnitus in an older adult. An adverse-event report unrelated to plague efficacy. |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2022362 | Avelox | Tab 400 mg | Not recorded (status: cancelled) |

---

## Safety Considerations

**Contraindication-related conditions (from the knowledge graph, not label text):**
- **Cardiac:** long QT syndrome, familial long QT syndrome, torsades de pointes, sudden cardiac arrest, heart conduction disease, myocardial ischaemia, coronary artery disease
- **Neurological and neuromuscular:** epilepsy, peripheral neuropathy, peripheral nervous system disease, myasthenia gravis (including adult-onset), neuromuscular junction disease
- **Musculoskeletal:** tendinitis, periarthritis, exostosis
- **Hepatic:** cirrhosis of liver, drug-induced liver injury
- **Gastrointestinal:** *Clostridium difficile* colitis, perinatal necrotizing enterocolitis
- **Metabolic:** potassium deficiency disease

**Drug Interactions:** The interaction screen returned 100 interactions in total, and 20 are shown in the data compiled for this report.
- **Major:** hydrocortisone, dexamethasone, betamethasone, triamcinolone (corticosteroids, relevant to tendon risk), bupropion, chlorpropamide
- **Moderate:** antidiabetic agents (acarbose, alogliptin, albiglutide, canagliflozin, metformin, pioglitazone), famotidine, loperamide, acetylsalicylic acid, balsalazide, bisacodyl, calcium phosphate, calcium acetate, potassium citrate

Please refer to the package insert for label warnings and contraindications, which were not available in the data compiled for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction, hyperamylasemia, has no mechanistic rationale, no trials, and no literature, so it rests on a model score alone. Moxifloxacin also has no valid New Zealand licence, since its only authorisation is cancelled. The sole plausible prediction, bubonic plague, is an extension of existing plague indications and is supported only by preclinical data (graded L4, research question).

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, which are required before any safety screening
- Detailed mechanism-of-action data from DrugBank
- For bubonic plague: human clinical data, and confirmation of whether it is already covered by existing plague indications
- For the other predictions: a plausible mechanism and at least preliminary clinical evidence before any further evaluation
- A review of the New Zealand regulatory pathway, given the cancelled licence, and a safety assessment of the QT, tendon, neuropathy and dysglycaemia risks
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


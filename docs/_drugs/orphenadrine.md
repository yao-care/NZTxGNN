---
layout: default
title: Orphenadrine
parent: Model Prediction Only (L5)
nav_order: 341
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-30
description: "Orphenadrine: predicted new indication Schizophrenia (evidence level L5). Approved use: Parkinsonian disorder / Parkinson disease"
monitor_inn: "Orphenadrine"
predicted_indications: ["retinal dystrophy with or without extraocular anomalies", "congenital disorder of glycosylation with defective fucosylation", "polymicrogyria, perisylvian, with cerebellar hypoplasia and arthrogryposis", "Charcot-Marie-Tooth disease, demyelinating, type 1G", "schizophrenia", "myopia X-linked", "myopia 26, X-linked, female-limited"]
---

# Orphenadrine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

## Orphenadrine: From Parkinson Disease to Schizophrenia
{: .text-alpha }

## One-Sentence Summary

Orphenadrine is an anticholinergic agent originally used for parkinsonian disorders, including Parkinson disease.
The TxGNN model's top-ranked prediction is a retinal dystrophy with no supporting evidence. The best-supported candidate is **schizophrenia** (TxGNN rank 5), which has **0 registered clinical trials** and **17 publications**. These are mostly older studies of orphenadrine as an adjunct for antipsychotic-induced movement side effects, not as an antipsychotic.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Parkinsonian disorder / Parkinson disease (the New Zealand licence has no approved indication text) |
| Predicted New Indication | Schizophrenia (the top-ranked prediction, retinal dystrophy with or without extraocular anomalies, has no evidence and is not pursued here) |
| TxGNN Prediction Score | 0.9913 (schizophrenia); the rank 1 prediction scores 0.9929 |
| Evidence Level | L5 (schizophrenia); L5 for rank 1 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 (valid: 1) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Based on known information, orphenadrine is an anticholinergic/antihistaminic agent with NMDA-channel blocking activity. It is used for parkinsonism and muscle spasm.

The top-ranked predictions (retinal dystrophy, a glycosylation disorder, polymicrogyria with cerebellar hypoplasia, Charcot-Marie-Tooth type 1G, and two X-linked myopias) are inherited disorders. None has a plausible mechanistic link, clinical trial or publication. Their scores are purely computational. For the myopias, muscarinic blockade is relevant to myopia control (as with atropine), but systemic orphenadrine is not a validated approach and these are monogenic forms.

Schizophrenia is the only prediction with a literature base. Orphenadrine has mainly been studied there for controlling extrapyramidal symptoms caused by antipsychotics such as haloperidol, not for treating psychosis itself. A preclinical study (PMID 12459931) suggests NMDA-channel blockers can disrupt sensorimotor gating, a model relevant to schizophrenia. This is only a loose link, and it concerns channel blockers as a class rather than showing a therapeutic effect. Anticholinergics may also worsen cognition and tardive dyskinesia in this population.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

The table lists the most relevant publications for schizophrenia. Study types are as classified in the data compiled for this report.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [29341071](https://pubmed.ncbi.nlm.nih.gov/29341071/) | 2018 | Review | Cochrane Database Syst Rev | Anticholinergics for antipsychotic-induced tardive dyskinesia; anticholinergics are widely co-prescribed for movement side effects, and animal data suggest chronic use could itself cause tardive dyskinesia |
| [10796321](https://pubmed.ncbi.nlm.nih.gov/10796321/) | 2000 | Review | Cochrane Database Syst Rev | Earlier Cochrane review of anticholinergics for neuroleptic-induced tardive dyskinesia |
| [4571143](https://pubmed.ncbi.nlm.nih.gov/4571143/) | 1972 | RCT | Psychol Med | Amantadine vs orphenadrine vs placebo for phenothiazine-induced parkinsonism |
| [8369232](https://pubmed.ncbi.nlm.nih.gov/8369232/) | 1993 | RCT | Schizophr Res | Placebo, orphenadrine and rising chlorpromazine doses on an attention task in chronic schizophrenia (two-year study) |
| [3698889](https://pubmed.ncbi.nlm.nih.gov/3698889/) | 1986 | Clinical study | L'Encephale | 22 patients on haloperidol; adding orphenadrine significantly reduced extrapyramidal side effects, and withdrawal led to marked deterioration |
| [2616635](https://pubmed.ncbi.nlm.nih.gov/2616635/) | 1989 | Naturalistic study | Pharmacopsychiatry | 44 outpatients on haloperidol plus orphenadrine assessed for residual parkinsonian symptoms |
| [324238](https://pubmed.ncbi.nlm.nih.gov/324238/) | 1977 | Clinical study | Acta Psychiatr Scand | 118 patients on long-term neuroleptics; need for anticholinergics depended on neuroleptic potency (none needed among 18 on low-potency agents) |
| [12459931](https://pubmed.ncbi.nlm.nih.gov/12459931/) | 2003 | Preclinical | Psychopharmacology | NMDA channel blockers and disruption of prepulse inhibition in rats, an animal model relevant to schizophrenia |
| [13685260](https://pubmed.ncbi.nlm.nih.gov/13685260/) | 1961 | Clinical study | Psychiatr Neurol | Very high doses of reserpine plus orphenadrine in asymptomatic schizophrenia (no abstract available) |
| [19524386](https://pubmed.ncbi.nlm.nih.gov/19524386/) | 2009 | Analytical method | J Pharm Biomed Anal | HPLC method for measuring orphenadrine plasma levels in schizophrenic patients |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2645564 (cancelled) | Norflex | Tab 100 mg | Not specified |

---

## Safety Considerations

- **Contraindications flagged in the knowledge graph** (not taken from label text): hypertension, liver failure and liver disease, kidney disease and impaired renal function, epilepsy, insomnia, glaucoma, benign prostatic hyperplasia and bladder neck obstruction, peptic ulcer disease, pyloric stenosis, achalasia and megaesophagus, myasthenia gravis and other neuromuscular junction disease, heart conduction disease, sudden cardiac arrest, thrombocytopenia, anaemia, hemophilia, gout, gallbladder disease, pyoureter, substance abuse/dependence, and paracetamol poisoning.
- **Drug Interactions**: The interaction query returned 100 interactions; 20 are listed in the data compiled for this report.
  - **Major**: morphine (including liposomal), potassium chloride and potassium citrate.
  - **Moderate** (mostly other anticholinergics or antispasmodics, which add to the anticholinergic burden): hyoscyamine, atropine, glycopyrronium, dicyclomine, clidinium, mepenzolate, methscopolamine and propantheline. Others are loperamide, bupropion, dronabinol, nabilone, eluxadoline, metoclopramide and opium.

Please refer to the package insert for warnings, as label data were not available.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked predictions rest on computational scores alone, with no trials, no literature and no plausible mechanism. For schizophrenia, the literature supports orphenadrine only for managing antipsychotic side effects, and anticholinergics may worsen cognition and tardive dyskinesia. Orphenadrine's only New Zealand licence has been cancelled.

**To proceed, the following is needed:**
- Mechanism of action data (from DrugBank)
- Medsafe package insert warnings and contraindications, which is a blocking gap for safety screening
- For schizophrenia, a well-designed study of any antipsychotic or cognitive benefit separate from the control of extrapyramidal side effects, with attention to the cognitive and tardive dyskinesia risks of anticholinergics
- A decision on whether the cancelled New Zealand licence matters for the intended use, since no current product is marketed
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


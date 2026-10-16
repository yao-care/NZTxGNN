---
layout: default
title: Arginine
parent: Moderate Evidence (L3-L4)
nav_order: 43
evidence_level: L4
indication_count: 1
last_modified_at: 2026-10-15
description: "Arginine: predicted new indication Gastroparesis (evidence level L4). Approved use: Not available"
---

# Arginine
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **1** 
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

# Arginine: From No Documented Original Indication to Gastroparesis

## One-Sentence Summary

Arginine is an amino acid that has been sold in New Zealand as a supplement-type product, but no approved indication text is available for it.
The TxGNN model predicts it may be useful for **gastroparesis**,
and the current support is **0 clinical trials** and **8 publications**, all preclinical or case-level.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available (no approved indication text in the licence records) |
| Predicted New Indication | Gastroparesis |
| TxGNN Prediction Score | 99.42% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. The following reasoning is based on known biology rather than on a recorded mechanism for the drug.

L-arginine is the substrate of neuronal nitric oxide synthase (nNOS), which produces nitric oxide (NO). NO is the main inhibitory neurotransmitter driving nitrergic relaxation of the gastric fundus and pyloric sphincter. Loss of nNOS and impaired nitrergic signalling in myenteric neurons are recognised features of diabetic and other forms of gastroparesis. Replenishing the substrate is therefore a plausible approach.

Two preclinical studies are consistent with this. In mice, glucocorticoids induced gastroparesis through depletion of L-arginine (PMID 25057793). In a rat Parkinson's disease model, nitrergic relaxation of the pyloric sphincter was impaired (PMID 35380456). However, this evidence is indirect and preclinical. No human trial has tested arginine supplementation for gastroparesis, and the high TxGNN score is a computational prediction that does not replace clinical evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25057793](https://pubmed.ncbi.nlm.nih.gov/25057793/) | 2014 | Animal study | Endocrinology | Glucocorticoids induced gastroparesis in mice through depletion of L-arginine |
| [35380456](https://pubmed.ncbi.nlm.nih.gov/35380456/) | 2022 | Animal study | Am J Physiol Gastrointest Liver Physiol | Impaired nitrergic relaxation of the pyloric sphincter in a Parkinson's disease rat model with gastroparesis |
| [18312542](https://pubmed.ncbi.nlm.nih.gov/18312542/) | 2008 | Animal study | Neurogastroenterol Motil | Jejunal myenteric neuropathy and nNOS expression in diabetic BB-rats |
| [19023028](https://pubmed.ncbi.nlm.nih.gov/19023028/) | 2009 | Animal study | Am J Physiol Gastrointest Liver Physiol | Gastric electrical stimulation improved vagotomy-induced impaired accommodation via the nitrergic pathway in dogs |
| [31984783](https://pubmed.ncbi.nlm.nih.gov/31984783/) | 2020 | Animal study | Am J Physiol Gastrointest Liver Physiol | Sacral nerve stimulation increased gastric accommodation in rats |
| [21193530](https://pubmed.ncbi.nlm.nih.gov/21193530/) | 2011 | Animal study | Am J Physiol Gastrointest Liver Physiol | Hyperglycaemia inhibited gastric motility via nodose ganglia KATP channels |
| [18322959](https://pubmed.ncbi.nlm.nih.gov/18322959/) | 2008 | Animal study | World J Gastroenterol | Ghrelin and GHRP-6 effects on gastric motility in diabetic mice with gastroparesis |
| [8194696](https://pubmed.ncbi.nlm.nih.gov/8194696/) | 1994 | Animal study | Gastroenterology | Delayed gastric emptying in rats after food-protein-induced anaphylaxis |
| [33867519](https://pubmed.ncbi.nlm.nih.gov/33867519/) | 2021 | Case report | Am J Case Rep | Lifestyle changes normalised serum lactate in an m.3243A>G carrier (limited relevance) |

## New Zealand Market Information

All four authorisations are cancelled, and no approved indication text is recorded for any of them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2393786 | Biomed | Powder | Not recorded |
| 2609282 | Solgar | Cap 500 mg | Not recorded |
| 2668750 | Arginine2000 | Powder, 4 g sachets | Not recorded |
| 2400588 | Clinicians | Tab 1,000 mg | Not recorded |

## Safety Considerations

- **Contraindication signals from the knowledge graph** (drug-disease links in the TxGNN knowledge graph, not label text): 
  - Kidney: kidney disease, chronic kidney disease, chronic renal failure, end-stage renal failure, nephrolithiasis, nephrocalcinosis, acute urate nephropathy
  - Calcium and bone: hypercalcaemia, humoral hypercalcaemia of malignancy, hyperparathyroidism, osteitis fibrosa, lytic metastatic bone lesion, sarcoidosis, phosphorus metabolism disease
  - Acid-base: metabolic acidosis, renal tubular acidosis, acidosis disorder
  - Cardiovascular: heart disease, cardiovascular disease, digitalis poisoning, ventricular fibrillation
  - Other: sickle cell anaemia, hereditary persistence of fetal haemoglobin, phenylketonuria and other phenylalanine metabolism disorders, constipation disorder, pyoureter

These are model-derived signals and need confirmation against authoritative labelling. Notably, many gastroparesis patients have diabetes with associated kidney or cardiovascular disease, so this overlap deserves careful attention.

Please refer to the package insert for further safety information, as no label warnings or drug-interaction data are available in the data compiled for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a plausible mechanism (arginine as the substrate for nitrergic gastric relaxation) and a high model score, but the evidence is only preclinical (L4). There are no registered clinical trials, no human data, and no currently valid New Zealand authorisation. Safety information is incomplete, and the knowledge-graph contraindication signals overlap with common comorbidities in gastroparesis.

**To proceed, the following is needed:**
- Safety data from the Medsafe package insert (warnings and contraindications); this is a blocking gap
- Mechanism of action data from DrugBank
- Human evidence, such as a pilot or Phase 2 study of arginine supplementation in gastroparesis
- Assessment of the relationship to the original indication and of route and formulation compatibility
- A regulatory pathway given that all New Zealand licences are cancelled
- A renal, cardiovascular and metabolic risk assessment for the target population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


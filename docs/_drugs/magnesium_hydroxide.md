---
layout: default
title: Magnesium Hydroxide
parent: Model Prediction Only (L5)
nav_order: 280
evidence_level: L5
indication_count: 1
monitor_inn: "MAGNESIUM HYDROXIDE"
predicted_indications: ["stomach disease"]
last_modified_at: 2026-10-28
description: "Magnesium Hydroxide: predicted new indication Stomach disease (evidence level L5). Approved use: Peptic ulcer disease, gastroesophageal reflux disease, esophagitis…"
---

# Magnesium Hydroxide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Magnesium Hydroxide: From Acid-Related Gastrointestinal Disorders to Stomach Disease
{: .text-alpha }

## One-Sentence Summary

Magnesium hydroxide is an antacid and laxative. Listed uses include peptic ulcer disease, gastroesophageal reflux disease, esophagitis, constipation and dyspepsia.
The TxGNN model predicts it may be effective for **stomach disease**, but **no clinical trials** were found for this indication and the literature is mostly older reviews, animal studies and studies of combination antacids.
This prediction largely overlaps with its known acid-related uses and is not a distinct new repurposing signal.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Peptic ulcer disease, gastroesophageal reflux disease, esophagitis, constipation, dyspepsia (relations recorded in the knowledge graph; the New Zealand licence text contains no approved indication) |
| Predicted New Indication | Stomach disease |
| TxGNN Prediction Score | 0.9923 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Number of Approvals | 1 licence (single-ingredient product) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, magnesium hydroxide is an antacid that neutralizes gastric acid and raises intragastric pH. This is directly relevant to acid-related stomach conditions such as peptic ulcer, dyspepsia and reflux.

"Stomach disease" is a very broad category that mostly duplicates the uses already recorded in the knowledge graph. The high score (0.9923) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence or a novel finding.

One animal study (PMID 2595273) showed that an aluminium/magnesium hydroxide antacid protected rat stomachs against induced lesions, possibly through endogenous prostaglandins. This hints at a protective effect beyond acid neutralization, but it is preclinical data from a combination product only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No cited study evaluates magnesium hydroxide alone against "stomach disease" as a defined condition. Most studies involve combination aluminium/magnesium hydroxide antacids, and several are old or off-target. The most relevant items are listed below.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7267678](https://pubmed.ncbi.nlm.nih.gov/7267678/) | 1981 | Double-blind randomized trial | Pharmatherapeutica | In 36 patients with symptomatic gastritis, magnesium/aluminium hydroxide suspension improved symptoms significantly more often than hydrotalcite/dimethicone |
| [8520743](https://pubmed.ncbi.nlm.nih.gov/8520743/) | 1995 | Randomized double-blind trial | Am J Respir Crit Care Med | Sucralfate vs antacids for stress ulcer prophylaxis in ventilated ICU patients, looking at gastric acidity and pneumonia |
| [3018068](https://pubmed.ncbi.nlm.nih.gov/3018068/) | 1986 | Clinical study | J Clin Gastroenterol | Sodium bicarbonate vs aluminium-magnesium hydroxide on postprandial gastric acid in duodenal ulcer patients |
| [236651](https://pubmed.ncbi.nlm.nih.gov/236651/) | 1975 | Review | Am J Hosp Pharm | Pharmacology and clinical use of antacids in acid-peptic disease |
| [6755653](https://pubmed.ncbi.nlm.nih.gov/6755653/) | 1982 | Review | Scand J Gastroenterol Suppl | Antacids hasten ulcer healing and prevent stress-ulcer bleeding; gastrin release and acid rebound discussed |
| [1500660](https://pubmed.ncbi.nlm.nih.gov/1500660/) | 1992 | Review | J Clin Gastroenterol | Reappraisal of calcium-induced acid rebound in peptic ulcer therapy |
| [2595273](https://pubmed.ncbi.nlm.nih.gov/2595273/) | 1989 | Preclinical (rat) | Scand J Gastroenterol | Aluminium/magnesium hydroxide antacid dose-dependently prevented gastric lesions; prostanoids may be involved |
| [8641951](https://pubmed.ncbi.nlm.nih.gov/8641951/) | 1996 | Clinical pharmacology (horses) | J Am Vet Med Assoc | Effect of aluminium/magnesium hydroxide antacid on gastric pH in horses; not human data |
| [6613216](https://pubmed.ncbi.nlm.nih.gov/6613216/) | 1983 | Clinical pharmacokinetic study | Z Gastroenterol | No interaction found between an aluminium/magnesium hydroxide antacid and cimetidine in peptic ulcer patients |
| [20278299](https://pubmed.ncbi.nlm.nih.gov/20278299/) | 1947 | Historical report | Am J Dig Dis | Colloidal aluminium hydroxide gel and magnesium hydroxide in peptic ulcer management |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2599953 | Phillips Milk of Magnesia | Suspension 8% | No indication text recorded |

## Safety Considerations

- **Relations recorded in the knowledge graph as contraindications (not label text):** kidney disease and impaired renal function, nephrolithiasis, nephrocalcinosis, liver disease, gallbladder disease, hypercalcaemia, hyperparathyroidism, sarcoidosis, hypophosphataemia and other phosphorus metabolism disorders, diarrhoeal disease (including chronic), appendicitis, intestinal impaction, haemorrhoids, and gastric cancer or neoplasm. These need verification against the product label.
- **Drug Interactions:** 100 interactions were found in the interaction databases. Examples:
  - Major: dolutegravir.
  - Moderate: doxycycline, alendronic acid, acetazolamide, hydrochlorothiazide, amiodarone, tramadol, hydrocortisone, salbutamol, abiraterone, acalabrutinib, alfuzosin, amiloride, amisulpride, dextroamphetamine.
  - Minor: famotidine, ranitidine, ibuprofen, acebutolol, alprazolam.

Please refer to the package insert for label warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No clinical trial and no study directly addressing "stomach disease" with magnesium hydroxide alone were found, so the evidence level is L5. The high prediction score mainly reflects overlap with existing acid-related uses, and "stomach disease" is too broad to serve as a distinct repurposing target.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, which are currently missing and block safety screening
- Mechanism of action data (for example from DrugBank)
- A narrower, clinically defined target (such as functional dyspepsia or gastritis) with studies of magnesium hydroxide alone
- Review of the major and moderate interactions (such as dolutegravir) and of renal-function precautions
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Acarbose
parent: Model Prediction Only (L5)
nav_order: 13
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-15
description: "Acarbose: predicted new indication Classic stiff person syndrome (evidence level L5). Approved use: Type 2 diabetes mellitus"
monitor_inn: "ACARBOSE"
predicted_indications: ["classic stiff person syndrome", "focal stiff limb syndrome", "thiamine-responsive dysfunction syndrome", "opsismodysplasia", "drug-induced localized lipodystrophy", "centrifugal lipodystrophy", "pressure-induced localized lipoatrophy", "idiopathic localized lipodystrophy", "pancreatic agenesis"]
---

# Acarbose
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

# Acarbose: From Type 2 Diabetes Mellitus to Classic Stiff Person Syndrome

## One-Sentence Summary

Acarbose is an intestinal alpha-glucosidase inhibitor, originally used to manage type 2 diabetes mellitus.
The TxGNN model predicts it may be effective for **classic stiff person syndrome**, but there are **0 clinical trials** and **0 publications** supporting this specific prediction.
The high score appears to reflect knowledge-graph associations rather than a therapeutic rationale.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Type 2 diabetes mellitus (from the knowledge graph; no approved indication text is available in the New Zealand licence records) |
| Predicted New Indication | Classic stiff person syndrome |
| TxGNN Prediction Score | 99.65% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on general pharmacology, acarbose inhibits alpha-glucosidase in the gut, which slows carbohydrate digestion and reduces postprandial glucose rises. It acts locally and is poorly absorbed.

Stiff person syndrome is an autoimmune neurological disorder, typically associated with anti-GAD antibodies. There is no clear mechanistic link between reduced intestinal carbohydrate absorption and this pathology. The high score most likely arises from knowledge-graph associations, such as the shared GAD/diabetes comorbidity, rather than a true therapeutic rationale.

The same pattern applies to the other top predictions, which are not supported by any mechanism or study:
- Focal stiff limb syndrome (identical score, likely a graph-propagation artefact)
- Thiamine-responsive dysfunction syndrome
- Opsismodysplasia (a theoretical link to insulin signalling via INPPL1/SHIP2, but acarbose does not act on this pathway)
- Several localized lipodystrophies (drug-induced, centrifugal, pressure-induced, idiopathic)
- Pancreatic agenesis (acarbose cannot replace insulin; at most an adjunct for postprandial glucose)

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature was retrieved for the top-ranked prediction (classic stiff person syndrome). The only literature in the compiled data relates to the rank 9 prediction, pancreatic agenesis, and it consists of diabetes keyword matches rather than direct evidence. A selection:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10094112](https://pubmed.ncbi.nlm.nih.gov/10094112/) | 1999 | Preclinical (animal) | Metabolism | Acarbose prevented and reversed metabolic abnormalities in the diabetic OLETF rat; relevant to diabetes, not pancreatic agenesis |
| [12877088](https://pubmed.ncbi.nlm.nih.gov/12877088/) | 2003 | Review | Nihon Rinsho | Alpha-glucosidase inhibitors for postprandial hyperglycaemia in hypertensive type 2 diabetes |
| [21697254](https://pubmed.ncbi.nlm.nih.gov/21697254/) | 2011 | Review | J Clin Endocrinol Metab | Preservation of β-cell function for diabetes prevention |
| [9506190](https://pubmed.ncbi.nlm.nih.gov/9506190/) | 1998 | Review | Advances in Internal Medicine | Overview of type 2 diabetes pathophysiology |
| [15449970](https://pubmed.ncbi.nlm.nih.gov/15449970/) | 2004 | Review | Am J Cardiovasc Drugs | Interrelationship between heart failure and impaired glucose metabolism |
| [34562871](https://pubmed.ncbi.nlm.nih.gov/34562871/) | 2021 | Case report | Diabetes Metab Syndr | Continuous glucose monitoring and rituximab in insulin autoimmune syndrome |
| [30572448](https://pubmed.ncbi.nlm.nih.gov/30572448/) | 2018 | Case report | Medicine | Three cases of insulin autoimmune syndrome |

None of these papers addresses pancreatic agenesis or stiff person syndrome directly.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2402416 | Accarb | Tab 100 mg | Not available (cancelled) |
| 2402408 | Accarb | Tab 50 mg | Not available (cancelled) |

## Safety Considerations

- **Disease contraindications from the knowledge graph** (not label text): kidney disease, pyoureter, liver disease, gallbladder disease, cirrhosis of liver, inflammatory bowel disease, hypoglycaemia, injury.
- **Drug interactions**: 100 interactions were identified, of which the first 20 reviewed were Moderate or Minor. Examples of Moderate interactions include acetazolamide, salbutamol, aloe vera leaf, alpelisib, amprenavir, aripiprazole, asenapine, atazanavir, bedaquiline, bendroflumethiazide, benzthiazide and betamethasone. Minor interactions include albiglutide, alclometasone (topical), lipoic acid, amcinonide and anisindione.

Please also refer to the package insert for label warnings and contraindications, which were not available in the data compiled for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score, with no clinical trials, no relevant literature and no plausible mechanistic link. Acarbose has no currently valid New Zealand licence, which further limits near-term feasibility.

**To proceed, the following is needed:**
- Mechanism of action data and a credible mechanistic hypothesis connecting alpha-glucosidase inhibition to stiff person syndrome
- Targeted literature and trial searches for stiff person syndrome and acarbose
- Package insert warnings and contraindications from Medsafe
- Clarification of why the licences were cancelled and whether any product is currently available
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Zuclopenthixol
parent: Model Prediction Only (L5)
nav_order: 487
evidence_level: L5
indication_count: 9
---

# Zuclopenthixol
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

# Zuclopenthixol: From Schizophrenia to Retinal Dystrophy

## One-Sentence Summary

Zuclopenthixol is a thioxanthene antipsychotic, originally used to treat schizophrenia.
The TxGNN model predicts it may be effective for **retinal dystrophy with or without extraocular anomalies**,
but there are currently **0 clinical trials** and **0 publications** supporting this prediction, so it rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Schizophrenia (from the knowledge graph; no approved indication text is available in the New Zealand licence record) |
| Predicted New Indication | Retinal dystrophy with or without extraocular anomalies |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, zuclopenthixol is a thioxanthene antipsychotic that acts mainly as a dopamine D1/D2 and 5-HT2A receptor antagonist. Its efficacy in schizophrenia comes from this receptor blockade.

For the predicted indication, **no mechanistic link has been established**. Inherited retinal dystrophies are genetically diverse, and nothing in the available data connects dopamine receptor antagonism to their pathophysiology. The TxGNN score (0.99994) is a graph-based prediction only. In addition, drugs in the related phenothiazine class are associated with retinal toxicity, which argues against benefit rather than for it.

Other top-ranked predictions show the same pattern: none has clinical or literature support.

| Rank | Predicted Disease | Score | Assessment |
|------|------|------|------|
| 2 | Hydranencephaly | 99.99% | Structural congenital brain malformation; no plausible link to dopamine antagonism |
| 3 | Congenital disorder of glycosylation with defective fucosylation | 99.99% | Glycan biosynthesis disorder; no known drug action on this pathway |
| 4 | Charcot-Marie-Tooth disease, demyelinating, type 1G | 99.99% | Hereditary neuropathy; no link to myelin maintenance or repair |
| 5 | X-linked myopia | 99.99% | Retinal dopamine agonism, not antagonism, is considered protective against myopia, so the direction of effect is unfavourable |
| 6 | Perisylvian polymicrogyria with cerebellar hypoplasia and arthrogryposis | 99.99% | Congenital cortical malformation; no expected benefit |
| 7 | Syndromic myopia | 99.99% | Same unfavourable direction of effect as above |
| 8 | Myopia 26, X-linked, female-limited | 99.99% | Monogenic; no link to the underlying gene; antagonism unfavourable |
| 9 | Atypical glycine encephalopathy | 99.99% | Inborn error of glycine metabolism; no action on this pathway |

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 390372 (cancelled) | Clopixol | Tab 10 mg | Not available |

## Cytotoxicity

Not applicable. Zuclopenthixol is an antipsychotic, not an antineoplastic drug.

## Safety Considerations

- **Retinal toxicity concern**: Phenothiazine-class antipsychotics are associated with retinal toxicity, which is particularly relevant when considering use in retinal disease.
- **Drug Interactions**: No interaction records were found in the data compiled for this report.

Please refer to the package insert for further safety information, including warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a graph-model score. There are no clinical trials or publications, no plausible mechanism, and a theoretical safety concern (retinal toxicity). The only New Zealand licence is cancelled.

**To proceed, the following is needed:**
- Mechanistic evidence linking dopamine/5-HT receptor antagonism to inherited retinal dystrophy pathophysiology
- Preclinical or literature evidence in retinal disease models
- Package insert warnings and contraindications from Medsafe, to allow safety screening
- Confirmation of the original mechanism of action data from DrugBank
- Assessment of route and formulation suitability for retinal indications
- Clarification of current New Zealand availability, since the only licence is cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


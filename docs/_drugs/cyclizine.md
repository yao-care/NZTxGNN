---
layout: default
title: Cyclizine
parent: Model Prediction Only (L5)
nav_order: 120
evidence_level: L5
indication_count: 9
---

# Cyclizine
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

# Cyclizine: From Allergic Rhinitis and Motion Sickness to Allergic Urticaria

## One-Sentence Summary

Cyclizine is a first-generation H1 antihistamine, originally used for allergic and vasomotor rhinitis, conjunctivitis, the common cold and motion sickness.
The TxGNN model predicts it may be effective for **allergic urticaria**, but **no clinical trials and no publications** currently support this prediction, so it rests on the model prediction alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Allergic rhinitis, vasomotor rhinitis, pharyngitis, common cold, conjunctivitis (atopic, papillary, vernal), nasopharyngitis, motion sickness (from the knowledge graph; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Allergic urticaria |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (0 valid, 2 cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Based on known information, cyclizine is a first-generation H1 antihistamine. The pharmacology data compiled for this report lists the H1 receptor (HRH1) as its target. Its efficacy in allergic rhinitis, conjunctivitis and motion sickness is reflected in its original indications. Mechanistically, it may be applicable to allergic urticaria.

Urticaria is largely driven by histamine released from mast cells, so blocking the H1 receptor is biologically plausible. Allergic urticaria is closely related to cyclizine's existing allergic indications, such as allergic rhinitis and conjunctivitis.

This reasoning is plausible, but it is not evidence. No trials or literature specific to cyclizine in urticaria were found. Newer second-generation antihistamines are generally preferred because they cause less sedation.

### Other Predictions

All other predictions also have no clinical trial support (L5, Hold):

| Rank | Predicted Indication | Score | Comment |
|---|---|---|---|
| 2 | Nasal cavity disease | 99.96% | Largely overlaps with existing rhinitis indications |
| 3 | Acute laryngopharyngitis | 99.95% | Overlaps with existing pharyngitis and cold indications |
| 4 | Cold urticaria | 99.84% | H1 antihistamines are a standard class for this condition, but there is no cyclizine-specific evidence |
| 5 | Rosacea conjunctivitis | 99.75% | Weak mechanistic link, as the disease is driven mainly by gland dysfunction and inflammation |
| 6 | Headache disorder | 99.49% | Only indirect literature (see below) |
| 7 | Trigeminal autonomic cephalalgia | 99.43% | No established mechanistic link |
| 8 | Rhinitis | 99.35% | Already an original indication, so not a new opportunity |
| 9 | Recalcitrant atopic dermatitis | 99.28% | Sedating antihistamines have a limited role, and targeted therapies are preferred |

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for allergic urticaria, the top predicted indication.

For the sixth-ranked prediction (headache disorder), two papers were retrieved. Judging from titles and abstracts, they appear to be indirect or incidental:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3926322](https://pubmed.ncbi.nlm.nih.gov/3926322/) | 1985 | RCT | Cephalalgia | Randomized trial of 114 patients comparing naproxen sodium with an ergotamine combination (ergotamine, caffeine and 50 mg cyclizine) for acute migraine. Cyclizine was only a component of the comparator, so this does not test cyclizine alone. |
| [20420788](https://pubmed.ncbi.nlm.nih.gov/20420788/) | 2010 | Cohort | Int J Clin Pharmacol Ther | Pharmacy database analysis of migraine prescribing patterns and cost, focused on triptans. It does not appear to evaluate cyclizine efficacy. |

## New Zealand Market Information

Both licences are cancelled, and no approved indication text is available.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2231220 | Nausicalm | Tab 50 mg | Not available (cancelled) |
| 2595877 | Hameln | Inj 50 mg per ml, 1 ml ampoule | Not available (cancelled) |

## Safety Considerations

- **Disease associations flagged in the knowledge graph (not label text):** The TxGNN knowledge graph lists the following conditions as contraindication-type associations. These should be checked against the official package insert:
  - Cardiovascular: hypertension, hypotensive disorder, coronary artery disease, myocardial infarction, coronary thrombosis, coronary and other atherosclerosis, arteriosclerosis
  - Neurological and ocular: epilepsy, ocular hypertension
  - Renal, hepatic and biliary: kidney disease, pyoureter, liver disease, gallbladder disease, pancreatitis
  - Endocrine and metabolic: adrenocortical insufficiency, Addison disease, hypothyroidism (including myxedema and congenital hypothyroidism), diabetes mellitus, diabetic ketoacidosis
  - Gastrointestinal: peptic ulcer disease, inflammatory bowel disease, *Clostridium difficile* colitis, perinatal necrotizing enterocolitis
  - Other: benign prostatic hyperplasia, hemoglobinopathy
- **Drug Interactions:** The interaction query completed with one pharmacology entry, the H1 receptor as the primary target. No specific drug-drug interactions were identified.

Please refer to the package insert for official warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high (99.98%), and H1 blockade is a plausible mechanism for allergic urticaria. However, there are no clinical trials or publications for this use (L5, model prediction only). The New Zealand licences are all cancelled, and the official safety information is not available.

**To proceed, the following is needed:**
- Official New Zealand package insert, including warnings, contraindications and approved indications
- Detailed mechanism of action data
- A systematic search for cyclizine-specific clinical studies in urticaria
- A comparison with second-generation antihistamines, which are better tolerated and are the usual first-line choice
- Confirmation of route and formulation availability, given that both New Zealand licences are cancelled
- A review of the knowledge-graph contraindication associations against label text
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


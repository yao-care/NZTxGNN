---
layout: default
title: Dantrolene
parent: Model Prediction Only (L5)
nav_order: 133
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-15
description: "Dantrolene: predicted new indication Malignant hyperthermia, susceptibility to (evidence level L5). Approved use: Malignant hyperthermia of anaesthesia"
monitor_inn: "DANTROLENE"
predicted_indications: ["moderate multiminicore disease with hand involvement", "King-Denborough syndrome", "congenital multicore myopathy with external ophthalmoplegia", "central core myopathy", "hypokalemic periodic paralysis", "thyrotoxic periodic paralysis, susceptibility to", "thyrotoxic periodic paralysis", "myopathy, centronuclear"]
---

# Dantrolene
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

# Dantrolene: From Malignant Hyperthermia of Anaesthesia to Malignant Hyperthermia, Susceptibility To

## One-Sentence Summary

Dantrolene is a skeletal muscle relaxant originally used to treat malignant hyperthermia during anaesthesia and exercise-induced malignant hyperthermia.
The TxGNN model predicts it may be effective for **malignant hyperthermia, susceptibility to**, a closely related condition, supported by **0 clinical trials** and **18 publications** (mostly reviews and guidelines).
Several other RYR1-related muscle disorders were also predicted, but with much weaker evidence.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Malignant hyperthermia of anaesthesia; exercise-induced malignant hyperthermia (from the knowledge graph; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Malignant hyperthermia, susceptibility to |
| TxGNN Prediction Score | 99.93% |
| Evidence Level | L3 (reviews and guidelines, no RCTs) |
| New Zealand Market Status | ✗ Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the data compiled for this report. Based on the literature, dantrolene inhibits RYR1-mediated calcium release from the sarcoplasmic reticulum in skeletal muscle. In malignant hyperthermia, gain-of-function RYR1 variants cause uncontrolled calcium release when susceptible people are exposed to volatile anaesthetics or succinylcholine. One mechanistic study (PMID 28373535) reports that dantrolene requires Mg²⁺ to arrest this process.

The predicted indication is essentially the same disease as the original one, viewed from the genetic-susceptibility side. The match is therefore direct, and the high TxGNN score is consistent with established clinical practice. The L3 grade reflects the absence of RCTs in the supplied data rather than weak support. Clinical standing rests on guidelines and long-standing practice, which an RCT-based scale undercounts.

Other predictions were also generated. They fall into two groups:
- **RYR1-related myopathies** (King-Denborough syndrome, central core myopathy, centronuclear myopathy, multiminicore disease): the mechanism is plausible, but the evidence is indirect. Dantrolene's relevance is mainly perioperative management of malignant hyperthermia risk, not treatment of the underlying myopathy.
- **Periodic paralyses** (hypokalaemic and thyrotoxic): these are channelopathies with only a speculative link to RYR1 inhibition. There are no trials or literature, and the model score is the only support.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Selected publications for the top prediction (malignant hyperthermia, susceptibility to):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [33399225](https://pubmed.ncbi.nlm.nih.gov/33399225/) | 2021 | Guideline | Anaesthesia | Association of Anaesthetists guideline on malignant hyperthermia in genetically susceptible individuals |
| [33131754](https://pubmed.ncbi.nlm.nih.gov/33131754/) | 2021 | Guideline | Br J Anaesth | European Malignant Hyperthermia Group consensus on perioperative management; notes no interventional trial evidence exists because of the rarity of the condition |
| [26238698](https://pubmed.ncbi.nlm.nih.gov/26238698/) | 2015 | Review | Orphanet J Rare Dis | Pharmacogenetic skeletal muscle disorder; hypermetabolic response to volatile anaesthetics and succinylcholine |
| [39171998](https://pubmed.ncbi.nlm.nih.gov/39171998/) | 2024 | Review | Crit Care Med | Narrative review of epidemiology and management in critically ill patients |
| [32008650](https://pubmed.ncbi.nlm.nih.gov/32008650/) | 2020 | Review | Anesthesiol Clin | Update on a rare, potentially lethal calcium release channel disorder; mortality has improved |
| [28373535](https://pubmed.ncbi.nlm.nih.gov/28373535/) | 2017 | Mechanistic | PNAS | Dantrolene requires Mg²⁺ to arrest malignant hyperthermia |
| [9538480](https://pubmed.ncbi.nlm.nih.gov/9538480/) | 1998 | Review | Postgrad Med J | Dantrolene sodium is described as the ultimate treatment |

Selected evidence for related predictions:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [9539627](https://pubmed.ncbi.nlm.nih.gov/9539627/) | 1998 | Case report | Anesth Analg | Prophylactic dantrolene in a patient with central core disease |
| [1508238](https://pubmed.ncbi.nlm.nih.gov/1508238/) | 1992 | Case report | Muscle Nerve | King-Denborough syndrome with a hyperthermic episode that responded to dantrolene |
| [27918309](https://pubmed.ncbi.nlm.nih.gov/27918309/) | 2017 | Case report | A A Case Rep | Intraoperative MH in a child with an RYR1 variant, leading to a King-Denborough syndrome diagnosis |
| [15299003](https://pubmed.ncbi.nlm.nih.gov/15299003/) | 2004 | Preclinical | J Biol Chem | In myotubes from patients with RYR1 mutations, IL-6 release was blocked by dantrolene |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 252417 | Dantrium | Cap 50 mg | Not available (cancelled) |
| 2648946 | Dantrium S29 | Cap 25 mg | Not available (cancelled) |

Both authorizations are cancelled, so no currently valid licence exists in the data compiled for this report.

## Safety Considerations

- **Contraindications (knowledge-graph associations, not label text)**: liver disease, gallbladder disease, myopathy, muscular disease, skeletal muscle disease, familial periodic paralysis, lung disease, and an obsolete rare pulmonary disease category. Note that several of these overlap with the predicted new indications (myopathies and periodic paralyses), which needs careful review.
- **Drug Interactions**: 72 interactions were identified. Major interactions: morphine and liposomal morphine. Moderate interactions include dronabinol, metoclopramide, nabilone, naltrexone, opium, sibutramine, ethanol and promethazine. Several others (for example proton pump inhibitors, metformin, simvastatin, clopidogrel) have an unclassified severity.

For warnings and full contraindications, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
For malignant hyperthermia susceptibility, the indication is directly continuous with dantrolene's established use and is supported by international guidelines and extensive review literature, despite the lack of RCTs. Predictions for other myopathies rest on indirect evidence (Research Question or Hold), and the periodic paralysis predictions have no supporting evidence. The cancelled New Zealand licences are a supply and regulatory limitation.

**To proceed, the following is needed:**
- Confirm the current supply and regulatory pathway in New Zealand, since both licences are cancelled (the S29 product name suggests supply under Section 29 of the Medicines Act, which should be verified)
- Obtain and review the Medsafe package insert for warnings and contraindications
- Obtain mechanism-of-action data from DrugBank
- Review the knowledge-graph contraindication signals (liver disease, myopathy, periodic paralysis) against the label
- Do not advance the myopathy or periodic paralysis predictions without direct clinical evidence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Dihydrocodeine
parent: Model Prediction Only (L5)
nav_order: 151
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-15
description: "Dihydrocodeine: predicted new indication Nasal cavity disease (evidence level L5). Approved use: Allergic rhinitis, vasomotor rhinitis, pharyngitis, common cold…"
---

# Dihydrocodeine
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

# Dihydrocodeine: From Allergic Rhinitis and Common Cold to Nasal Cavity Disease

## One-Sentence Summary

Dihydrocodeine is an opioid analgesic and antitussive. In the knowledge graph used for this report it is linked to upper respiratory conditions such as allergic rhinitis, pharyngitis and the common cold.
The TxGNN model predicts it may be effective for **nasal cavity disease**, but there are **no clinical trials** and **no publications** supporting this specific prediction.
The prediction rests on model output alone and should be treated as a hypothesis only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Allergic rhinitis, vasomotor rhinitis, pharyngitis, common cold, rhinitis, nasopharyngitis (from the knowledge graph; no approved indication text is available for the New Zealand licence) |
| Predicted New Indication | Nasal cavity disease |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, dihydrocodeine is an opioid with antitussive and analgesic effects. It offers symptomatic relief rather than disease modification.

The predicted "nasal cavity disease" is very close to the drug's existing graph-based indications (rhinitis, nasopharyngitis, common cold), which are themselves nasal cavity conditions. The high score most likely reflects this overlap and does not indicate a genuinely new use. No mechanism has been established by which dihydrocodeine would treat nasal disease itself.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the predicted indication of nasal cavity disease.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 772887 | DHC Continus (cancelled) | Long-acting tablet 60 mg | Not stated |

## Cytotoxicity

Dihydrocodeine is not an antineoplastic drug, so this section does not apply.

## Safety Considerations

- **Contraindication signals from the knowledge graph (not label text)**: the graph links dihydrocodeine to a long list of conditions where use is flagged as a concern. Examples include:
  - respiratory: chronic obstructive pulmonary disease;
  - liver and kidney: liver failure, liver disease, kidney disease;
  - neurological: epilepsy;
  - gastrointestinal: pancreatitis, gallbladder disease, peptic ulcer disease, inflammatory bowel disease;
  - cardiovascular: hypertension, hypotensive disorder, coronary artery disease, myocardial infarction;
  - endocrine: hypothyroidism, diabetes mellitus, diabetic ketoacidosis;
  - other: substance abuse/dependence, thrombocytopenia, benign prostatic hyperplasia, gout.
- **Drug Interactions**: 100 interactions were found in the interaction check. The first 20 returned include two rated **Major**:
  - alvimopan;
  - bupropion.

  The others were rated Moderate, including:
  - anticholinergics (atropine, glycopyrronium, hyoscyamine, dicyclomine);
  - antidiarrhoeals (loperamide, kaolin, attapulgite);
  - 5-HT3 antagonists (granisetron, dolasetron);
  - cimetidine, dexamethasone, aprepitant, dronabinol and eluxadoline.
- **Misuse and dependence**: a Japanese clinical study of cough-suppressant syrup abuse describes dependence associated with dihydrocodeine-containing products.

Please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has a very high model score but no trial or literature support. It largely overlaps with the drug's existing indications. The only New Zealand licence has been cancelled, and the safety profile (dependence, many interactions and flagged conditions) is substantial.

**To proceed, the following is needed:**
- Mechanism of action data and the Medsafe package insert (warnings and contraindications), which are currently missing and block safety screening
- Evidence that the predicted indication is distinct from the existing symptomatic uses
- Any clinical or preclinical studies in nasal cavity disease
- Confirmation of whether any dihydrocodeine product is currently marketed in New Zealand

## Other Predictions Reviewed

All eight further predictions also have no clinical trials, and only headache disorder has any literature (two indirectly relevant papers). All are on Hold.

| Rank | Predicted Indication | Score | Evidence Level | Comment |
|---|---|---|---|---|
| 2 | Acute laryngopharyngitis | 99.99% | L5 | Close to existing indications; not clearly distinct |
| 3 | Allergic urticaria | 99.92% | L5 | Opioids can trigger histamine release, so benefit is doubtful |
| 4 | Papillary conjunctivitis | 99.87% | L5 | Likely an allergy-network association; no direct mechanism |
| 5 | Faucial diphtheria | 99.39% | L5 | No antimicrobial or antitoxin activity |
| 6 | Cervical disc degenerative disorder | 99.32% | L5 | Symptomatic pain relief only |
| 7 | Headache disorder | 99.26% | L4 | Papers concern abuse and a general review; opioids carry medication-overuse risk |
| 8 | Cold urticaria | 99.21% | L5 | Histamine-related; benefit doubtful |
| 9 | Trigeminal autonomic cephalalgia | 99.16% | L5 | Opioids are not an established treatment |
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


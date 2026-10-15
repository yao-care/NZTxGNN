---
layout: default
title: Rivastigmine
parent: Moderate Evidence (L3-L4)
nav_order: 410
evidence_level: L4
indication_count: 1
---

# Rivastigmine
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

# Rivastigmine: From Alzheimer Disease to Glaucoma

## One-Sentence Summary

Rivastigmine is a cholinesterase inhibitor originally used to treat Alzheimer disease and related neurodegenerative conditions.
The TxGNN model predicts it may be effective for **glaucoma**, but there are currently **no registered clinical trials** and only **3 publications** (one rabbit study, two reviews), so the prediction rests mainly on computational and preclinical support.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Alzheimer disease (the approved indication text is not available in the New Zealand licence records; this comes from the knowledge graph, which also lists related conditions such as Pick disease and cerebellar degeneration) |
| Predicted New Indication | Glaucoma |
| TxGNN Prediction Score | 99.27% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this drug. Based on its drug class, rivastigmine is a carbamate-type inhibitor of acetylcholinesterase and butyrylcholinesterase. By slowing the breakdown of acetylcholine, it raises cholinergic activity. In Alzheimer disease this supports neurotransmission in the brain.

The same mechanism may apply to the eye. Increased cholinergic tone at the ciliary muscle and trabecular meshwork may improve aqueous humour outflow and lower intraocular pressure (IOP), the main modifiable risk factor in glaucoma. This is analogous to miotic agents such as pilocarpine and physostigmine. A rabbit study found that topical rivastigmine lowered IOP in normotensive animals, and a 2024 systems genetics and modelling review of cholinergic agents for IOP reduction offers indirect support.

The very high TxGNN score is a computational prediction, not clinical evidence. The link between Alzheimer disease and glaucoma is mechanistic (cholinesterase inhibition), not a shared disease pathway, and the mechanism here is inferred from the drug class.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10673128](https://pubmed.ncbi.nlm.nih.gov/10673128/) | 2000 | Preclinical (rabbit study) | J Ocul Pharmacol Ther | Topical rivastigmine, a selective acetylcholinesterase inhibitor, lowered intraocular pressure in normotensive rabbits |
| [39130374](https://pubmed.ncbi.nlm.nih.gov/39130374/) | 2024 | Review / computational systems genetics | Front Mol Biosci | Examines cholinergic agents for IOP reduction via muscarinic receptors in the trabecular meshwork; notes that systemic cholinergic side effects limit existing agents |
| [27967267](https://pubmed.ncbi.nlm.nih.gov/27967267/) | 2017 | Review | Expert Opin Ther Pat | Patent review of acetylcholinesterase inhibitors and reactivators; mentions that mild AChE inhibition has therapeutic relevance in Alzheimer disease, myasthenia gravis and glaucoma |

## New Zealand Market Information

Both licences are cancelled, and no approved indication text is recorded for them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2611570 | Rivastigmine Patch BNM 5 | Patch 4.6 mg per 24 hour | Not recorded (cancelled) |
| 2611589 | Rivastigmine Patch BNM 10 | Patch 9.5 mg per 24 hour | Not recorded (cancelled) |

## Safety Considerations

- **Contraindication signals (knowledge graph, not label text)**: The knowledge graph links rivastigmine to the following conditions as contraindications:
  - Cardiac: sick sinus syndrome (including familial), atrioventricular block, heart conduction disease, sudden cardiac arrest
  - Respiratory: asthma (including intrinsic and allergic forms), chronic obstructive pulmonary disease
  - Gastrointestinal and urinary: active peptic ulcer disease, diarrhoeal disease, urinary tract obstruction
  - Neurological: epilepsy, Parkinson disease and parkinsonian disorders, basal ganglia disease, focal hand dystonia, blepharospasm
  - Other: anorexia nervosa
- **Drug Interactions**: 100 interactions were identified. Examples:
  - **Major**: bupropion
  - **Moderate**: anticholinergics (atropine, hyoscyamine, glycopyrronium, dicyclomine, trospium, clidinium, mepenzolate), H2 blockers (famotidine, ranitidine, cimetidine), loperamide, metronidazole, clarithromycin, levofloxacin, and the antiemetics dolasetron and palonosetron
- **Ocular use considerations (from the mechanistic analysis)**: Miosis, ocular surface irritation, and systemic cholinergic effects such as bradycardia and gastrointestinal effects if the drug is absorbed.

Package insert warnings and contraindications were not available for this report; please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The glaucoma prediction is supported only by a high model score, one rabbit study and two reviews, with no clinical trials. Rivastigmine also has no currently valid New Zealand licence, and package insert safety information is missing.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe
- Detailed mechanism of action data from DrugBank
- Further preclinical work on ocular formulations, including tolerability and systemic absorption, followed by early-phase human studies
- Assessment of the cardiac, respiratory and other contraindication signals for any ocular route
- A regulatory pathway for a new ocular formulation, since the existing licences are for patches and are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


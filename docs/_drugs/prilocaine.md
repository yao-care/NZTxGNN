---
layout: default
title: Prilocaine
parent: Model Prediction Only (L5)
nav_order: 381
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-28
monitor_inn: "prilocaine"
predicted_indications: ["papillary conjunctivitis", "manic bipolar affective disorder", "bronchitis", "migraine disorder", "neuralgia", "migraine with brainstem aura", "atopic eczema", "allergic asthma", "rhinitis", "rosacea conjunctivitis"]
description: "Prilocaine: predicted new indication Papillary conjunctivitis (evidence level L5). Approved use: Not recorded in the local data"
---

# Prilocaine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

## Prilocaine: Papillary conjunctivitis
{: .text-alpha }

## One-Sentence Summary

Prilocaine is an amide local anaesthetic, sold as part of the lidocaine-prilocaine cream EMLA.
The TxGNN model's top-ranked prediction is papillary conjunctivitis, but that prediction has no supporting evidence. The best-supported prediction in this dataset is **neuralgia** (mainly postherpetic neuralgia), with **2 clinical trials** (1 directly relevant) and **14 publications**. The evidence is small, mostly old, and based on the combination product.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Papillary conjunctivitis |
| TxGNN Prediction Score | 0.9978 |
| Evidence Level | L5 for papillary conjunctivitis; L3 for neuralgia |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold for papillary conjunctivitis; Proceed with Guardrails (as a research question) for neuralgia |

## Why is This Prediction Reasonable?

Detailed mechanism of action data are not available in the data compiled for this report. Prilocaine is an amide local anaesthetic that blocks voltage-gated sodium channels. In EMLA it is combined with lidocaine in a eutectic mixture (2.5% each) for topical use.

The original indications (premature ejaculation, dyspareunia) are conditions where topical numbing of sensitive tissue helps. Neuralgia is a pain condition driven by hyperactive nociceptive nerve fibres, so sodium channel blockade gives a direct biological rationale. This is the most plausible of the ten predictions.

For the other predictions the link is weak or speculative:
- **Papillary conjunctivitis, rosacea conjunctivitis:** no evidence; ocular use of prilocaine is not supported.
- **Manic bipolar disorder, bronchitis, migraine with brainstem aura:** no evidence; the rationale is theoretical.
- **Migraine disorder:** a nerve-block rationale exists, but the retrieved trial and paper do not test prilocaine.
- **Atopic eczema, allergic asthma:** the literature mostly documents safety problems (absorption, methaemoglobinaemia, allergy), not benefit.
- **Rhinitis:** one 1987 skin-model study only, which is indirect.

## Clinical Trial Evidence

Neuralgia (rank 5):

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00916942](https://clinicaltrials.gov/study/NCT00916942) | Phase 2 | Completed | 20 | Open-label study of lidocaine 2.5%/prilocaine 2.5% cream as pre-treatment before the capsaicin 8% patch (NGX-4010) in postherpetic neuralgia. It assessed tolerability, not efficacy. |
| [NCT06899438](https://clinicaltrials.gov/study/NCT06899438) | N/A | Completed | 38 | Botulinum toxin A versus prilocaine in myofascial pain syndrome. Not relevant to prilocaine efficacy. |

Migraine disorder (rank 4): [NCT02403687](https://clinicaltrials.gov/study/NCT02403687) (N/A, completed, n=300) is a 24-week observational study of topical NSAIDs for pain. It has no demonstrated link to prilocaine or migraine.

No clinical trials were found for the other predicted indications.

## Literature Evidence

Neuralgia (most relevant publications):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [2493878](https://pubmed.ncbi.nlm.nih.gov/2493878/) | 1989 | Clinical study | BMJ | Lignocaine-prilocaine cream in postherpetic neuralgia |
| [2616182](https://pubmed.ncbi.nlm.nih.gov/2616182/) | 1989 | Clinical study | Pain | 12 patients with refractory PHN; pain scores improved significantly 6 h after EMLA 5 g or 10 g, with pharmacokinetic data |
| [8695081](https://pubmed.ncbi.nlm.nih.gov/8695081/) | 1996 | Case report | J Clin Anesth | Successful EMLA treatment of PHN resistant to other therapies |
| [1430539](https://pubmed.ncbi.nlm.nih.gov/1430539/) | 1992 | Review | J Dermatol Surg Oncol | EMLA gives effective, safe analgesia on normal and diseased skin; PHN is listed among other uses |
| [22182397](https://pubmed.ncbi.nlm.nih.gov/22182397/) | 2011 | Clinical study | BMC Anesthesiol | Tolerability of the capsaicin 8% patch after lidocaine/prilocaine pretreatment in PHN |
| [10342425](https://pubmed.ncbi.nlm.nih.gov/10342425/) | 1999 | Clinical study | Pain | EMLA pre-treatment failed to reduce pain from 1% topical capsaicin in 9 healthy subjects |
| [41777672](https://pubmed.ncbi.nlm.nih.gov/41777672/) | 2026 | Retrospective cohort | Front Aging Neurosci | EMLA alone versus paravertebral block plus EMLA for capsaicin patch tolerability in thoracic PHN |
| [7897165](https://pubmed.ncbi.nlm.nih.gov/7897165/) | 1995 | Case report | J Allergy Clin Immunol | EMLA-induced allergic contact dermatitis, with prilocaine as an immunogen (safety signal) |

Safety-relevant literature for other predictions:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [27397059](https://pubmed.ncbi.nlm.nih.gov/27397059/) | 2016 | Case report | Pediatr Dermatol | Seizures and methaemoglobinaemia after EMLA in a 3.5-year-old child with atopic dermatitis |
| [40519459](https://pubmed.ncbi.nlm.nih.gov/40519459/) | 2025 | Review | Cureus | Higher systemic absorption and toxicity risk of topical anaesthetics in barrier-compromised skin |
| [3621549](https://pubmed.ncbi.nlm.nih.gov/3621549/) | 1987 | Experimental study | Clin Allergy | EMLA inhibited the flare but not the wheal response to allergen and histamine in skin prick tests (rhinitis patients) |

No literature was found for papillary conjunctivitis, manic bipolar disorder, bronchitis, migraine with brainstem aura or rosacea conjunctivitis. The single migraine paper (PMID 37400308) concerns neuropeptides and microvascular function in women with PCOS, not prilocaine.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 719757 | EMLA | Cream 2.5% with prilocaine 2.5% | Not available (valid) |
| 657697 | EMLA | Cream 2.5% with prilocaine 2.5% (5 g tubes) | Not available (valid) |

## Safety Considerations

- **Contraindications flagged in the knowledge graph** (drug-disease edges, not label text): hypertension and other cardiovascular disease (including coronary artery disease, myocardial infarction, cardiac arrest, heart conduction disease, atrioventricular block); epilepsy; kidney, liver and gallbladder disease; diabetes mellitus and diabetic ketoacidosis; hemoglobinopathy; respiratory failure; angle-closure glaucoma; myasthenia gravis and other neuromuscular junction disease; butyrylcholinesterase deficiency; hyperthyroidism, Graves disease and thyrotoxicosis.
- **Drug interactions:** 22 interactions were recorded. Most are Major, and they cluster around methaemoglobinaemia risk or additive local anaesthetic effects. Major interactions include other local anaesthetics (lidocaine, topical lidocaine, topical benzocaine, cinchocaine), oxidising or methaemoglobin-inducing agents (amyl nitrite, nitrous acid, nitric oxide, silver nitrate, silver sulfadiazine), dapsone (oral and topical), antimalarials (chloroquine, primaquine, quinine, tafenoquine), rasburicase, sulfasalazine and metoclopramide. Moderate: liposomal morphine. Minor: hyaluronidase.
- **Literature-reported risks:** methaemoglobinaemia, seizures, systemic absorption through broken or inflamed skin, allergic contact dermatitis, purpura, and rare nerve injury after nerve blocks.

Package insert warnings and contraindications could not be retrieved. Please refer to the package insert for authoritative safety information.

## Conclusion and Next Steps

**Decision: Hold (papillary conjunctivitis and the other unsupported predictions); Proceed with Guardrails as a research question for neuralgia**

**Rationale:**
- The top-ranked prediction, papillary conjunctivitis, has no trials or publications, so the score is a computational result only.
- Neuralgia has a direct mechanistic rationale and small, mostly older studies in postherpetic neuralgia. These are mostly uncontrolled or based on the lidocaine-prilocaine combination, so prilocaine's own contribution cannot be separated. Both New Zealand licences are cancelled, so no product is currently marketed.

**To proceed, the following is needed:**
- The package insert from Medsafe, to complete the safety screening (warnings and contraindications). This is currently a blocking gap.
- Mechanism of action data from DrugBank.
- Controlled trials of lidocaine-prilocaine cream in postherpetic neuralgia, with attention to whether prilocaine adds benefit over lidocaine alone.
- A safety plan covering methaemoglobinaemia, the interactions listed above, and use on compromised skin or in children.
- A route and formulation compatibility assessment, and a decision on how to supply a topical product given the cancelled licences.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


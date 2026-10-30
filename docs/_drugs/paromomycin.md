---
layout: default
title: Paromomycin
parent: Model Prediction Only (L5)
nav_order: 350
evidence_level: L5
indication_count: 8
last_modified_at: 2026-10-30
monitor_inn: "PAROMOMYCIN"
predicted_indications: ["hepatoportal sclerosis", "early-onset familial noncirrhotic portal hypertension", "hepatopulmonary syndrome", "idiopathic copper-associated cirrhosis", "primitive portal vein thrombosis", "hepatic porphyria", "acute urate nephropathy", "peritonitis"]
description: "Paromomycin: predicted new indication Hepatoportal sclerosis (evidence level L5). Approved use: Not recorded in the local data"
---

# Paromomycin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
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

## Paromomycin: Hepatoportal sclerosis
{: .text-alpha }

## One-Sentence Summary

Paromomycin is a poorly absorbed aminoglycoside antibiotic, originally used for hepatic coma and hepatic encephalopathy.
The TxGNN model (deep learning) predicts it may be effective for **hepatoportal sclerosis**, but **no clinical trials** and **no publications** currently support this specific prediction. It is a model-based prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Hepatoportal sclerosis |
| TxGNN Prediction Score | 0.9990 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, paromomycin is an oral aminoglycoside that is barely absorbed from the gut. It is used to reduce intestinal bacteria and the ammonia they produce, which is the basis for its use in hepatic encephalopathy.

Hepatoportal sclerosis is a form of non-cirrhotic portal hypertension. A link to paromomycin is speculative and could only run through the gut-liver axis (for example, gut-derived bacteria or endotoxin affecting the liver). No data compiled for this report supports that idea.

The same score (0.9990) was given to several unrelated liver and portal conditions, such as early-onset familial noncirrhotic portal hypertension, hepatopulmonary syndrome, idiopathic copper-associated cirrhosis and primitive portal vein thrombosis. The score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. It should be treated as a hypothesis only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for hepatoportal sclerosis.

For context, the second-ranked-by-literature prediction, peritonitis (score 0.9903), has 16 associated publications, but they are not about peritonitis treatment. The ten provided are mainly reviews, in vitro studies and formulation work on *Leishmania* and amoebiasis, plus one case report of amoebic colitis treated with metronidazole and paromomycin. They are indirect evidence at best.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2434652 (cancelled) | Humatin | Cap 250 mg | Not available |

## Safety Considerations

- **Contraindication signals from the knowledge graph (not label text)**: kidney disease, pyoureter, hearing loss disorder, and X-linked mixed deafness with perilymphatic gusher. These are consistent with the known nephrotoxicity and ototoxicity concerns of aminoglycosides.
- **Drug Interactions**: 36 interactions were found in total. Major interactions include:
  - Neuromuscular blockers: cisatracurium, atracurium, doxacurium, metocurine, mivacurium
  - Live cholera vaccine (*Vibrio cholerae* CVD 103-HgR strain)
- Moderate interactions include proton pump inhibitors (rabeprazole, omeprazole, esomeprazole, lansoprazole, dexlansoprazole), estrogens (ethinylestradiol, estradiol, mestranol), methotrexate, mycophenolate mofetil, picosulfuric acid, enflurane and indium In-111 oxyquinoline. Digoxin is a minor interaction.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on model score alone (Evidence Level L5). There are no clinical trials or relevant publications, and the proposed mechanism is speculative. The only New Zealand authorization has been cancelled, and label safety information is unavailable.

**To proceed, the following is needed:**
- Mechanism of action data (for example, from DrugBank)
- Package insert warnings and contraindications from Medsafe
- Preclinical or clinical evidence linking gut decontamination to hepatoportal sclerosis or non-cirrhotic portal hypertension
- A renal and hearing safety assessment given the knowledge-graph contraindication signals
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


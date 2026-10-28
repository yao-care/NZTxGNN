---
layout: default
title: Phenoxybenzamine
parent: Model Prediction Only (L5)
nav_order: 361
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Phenoxybenzamine: predicted new indication Primary hereditary glaucoma (evidence level L5). Approved use: Adrenal gland pheochromocytoma / phaeochromocytoma"
monitor_inn: "Phenoxybenzamine"
predicted_indications: ["primary hereditary glaucoma", "open-angle glaucoma"]
---

# Phenoxybenzamine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

## Phenoxybenzamine: From Pheochromocytoma to Primary Hereditary Glaucoma
{: .text-alpha }

## One-Sentence Summary

Phenoxybenzamine is an irreversible, non-selective alpha-adrenergic blocker, originally used to control hypertension and sweating in pheochromocytoma.
The TxGNN model predicts it may be effective for **primary hereditary glaucoma** (and also open-angle glaucoma as a second prediction),
but there are currently **0 clinical trials** and **0 publications** supporting this direction, so the evidence is model prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Adrenal gland pheochromocytoma / phaeochromocytoma (the New Zealand licence has no approved indication text on record) |
| Predicted New Indication | Primary hereditary glaucoma |
| TxGNN Prediction Score | 0.9955 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, phenoxybenzamine is an irreversible, non-selective alpha-adrenergic antagonist. It blocks alpha-1 and alpha-2 adrenoceptors, including the α1D and α2B subtypes. Its efficacy in pheochromocytoma, where it controls catecholamine-driven hypertension and sweating, is established.

The link to glaucoma is only theoretical. Alpha-blockade could alter ocular vascular tone, aqueous humour production or uveoscleral outflow, and adrenergic pathways are an established target class in glaucoma therapy (for example, alpha-2 agonists). However, primary hereditary (congenital/juvenile) glaucoma is mainly caused by developmental anomalies of the trabecular meshwork and anterior chamber angle (for example, CYP1B1 and MYOC variants). Pharmacological alpha-blockade is not expected to correct this structural pathology.

The second prediction, open-angle glaucoma (score 0.9948), faces similar problems. Phenoxybenzamine's effect on intraocular pressure is not established, and systemic hypotension could worsen optic nerve head perfusion. Well-established, safer topical alternatives already exist. The very high TxGNN scores are therefore computational signals only and should not be read as evidence of efficacy.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2457636 | BNM | Cap 10 mg | Not recorded (status: valid) |

## Cytotoxicity

Phenoxybenzamine is not a cytotoxic anticancer agent. It is an alpha-blocker used for symptom control in pheochromocytoma, so no cytotoxicity assessment applies.

## Safety Considerations

- **Contraindications (associations in the data, not label text)**: kidney disease, pyoureter, hypotensive disorder, coronary artery disease.
- **Receptor interactions**: Phenoxybenzamine antagonises the α1D-adrenoceptor (ADRA1D) and α2B-adrenoceptor (ADRA2B). It is not a selective antagonist and acts at other alpha-adrenoceptors too. No drug-drug interaction severity levels were available.
- **Expected systemic effects (from the mechanistic assessment)**: orthostatic hypotension, reflex tachycardia and nasal congestion. Hypotension is of particular concern in glaucoma because of the risk of reduced optic nerve perfusion.

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predictions rest on model scores alone, with no clinical trials or literature. The mechanism does not address the structural cause of hereditary glaucoma, and systemic adverse effects are a concern. The only New Zealand licence is cancelled, and safer topical alternatives exist.

**To proceed, the following is needed:**
- A targeted literature search on alpha-blockers and intraocular pressure, beyond the data compiled here
- Preclinical ocular pharmacology data (IOP effect, ocular perfusion, local tolerability)
- Mechanism of action data from DrugBank
- Package insert warnings and contraindications from Medsafe
- An assessment of whether a topical ocular formulation is feasible, given that only a 10 mg capsule has been licensed in New Zealand
- A review of the regulatory pathway, given the cancelled market status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


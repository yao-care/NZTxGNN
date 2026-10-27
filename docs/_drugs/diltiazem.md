---
layout: default
title: Diltiazem
parent: Model Prediction Only (L5)
nav_order: 153
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Diltiazem: predicted new indication Obsolete susceptibility to ischemic stroke (evidence level L5). Approved use: Hypertension"
monitor_inn: "Diltiazem"
predicted_indications: ["obsolete susceptibility to ischemic stroke"]
---

# Diltiazem
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

## Diltiazem: From Hypertension to Obsolete Susceptibility to Ischemic Stroke
{: .text-alpha }

## One-Sentence Summary

Diltiazem is a calcium channel blocker originally used for hypertension, atrial fibrillation, Prinzmetal angina and ventricular tachycardia.
The TxGNN model predicts it may be effective for **obsolete susceptibility to ischemic stroke**,
but **no clinical trials and no publications** were found in the data compiled for this report, so the prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension (the New Zealand licence records contain no approved indication text; this is taken from the knowledge-graph indication list) |
| Predicted New Indication | Obsolete susceptibility to ischemic stroke |
| TxGNN Prediction Score | 99.08% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Based on known information, diltiazem is a non-dihydropyridine calcium channel blocker. It lowers blood pressure and controls ventricular rate in atrial fibrillation, and its efficacy in these conditions is established.

Hypertension and atrial fibrillation are both major risk factors for ischemic stroke. If diltiazem has any benefit here, it would most likely come from reducing these risk factors rather than from a direct neuroprotective or antithrombotic effect. This link is a hypothesis only and is not supported by evidence in the data compiled for this report.

There is also a concern about the disease term itself. It is labelled "obsolete", which suggests a deprecated ontology entry. The high score may be an artifact of a retired or merged concept. The term should be mapped to a current one (for example, ischemic stroke or cerebral infarction) before any further evaluation.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 254584 | Cardizem CD | Cap long-acting 180 mg | Not recorded (cancelled) |
| 2648172 | Diltiazem CD Clinect | Cap long-acting 120 mg | Not recorded (cancelled) |
| 254592 | Cardizem CD | Cap long-acting 240 mg | Not recorded (cancelled) |

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not from label text)**: the knowledge graph links diltiazem to the following conditions as contraindications:
  - Cardiac conduction and rhythm: atrioventricular block, sick sinus syndrome (including the familial form), Wolff-Parkinson-White syndrome, Lown-Ganong-Levine syndrome, and ventricular tachycardia (including catecholaminergic and incessant infant forms)
  - Haemodynamic and structural heart conditions: hypotensive disorder, aortic stenosis and related aortic valve disease, hypertrophic cardiomyopathy, and renal artery obstruction
  - Angioedema (acquired, hereditary, and RAAS-blocker-induced forms)
  - Blood disorders: neutropenia and agranulocytosis
  - Organ disease: kidney disease, liver disease, gallbladder disease, pyoureter
  - Connective tissue, bone and cartilage disease
- **Drug Interactions**: the interaction query completed, but the 10 returned records are pharmacological target-binding entries, not clinical drug-drug interactions. They cover 5-HT3A, Cav1.1–1.4 calcium channels, and Kv1.1, 1.2, 1.5, 1.7 and 3.1 potassium channels, across several species. No clinically graded interaction data are available.

For label-based warnings and contraindications, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The model score is very high, but there are no clinical trials or publications behind it. The predicted disease term is obsolete, and the drug has no currently valid New Zealand licence. The only mechanistic argument is indirect, through blood pressure and atrial fibrillation control.

**To proceed, the following is needed:**
- Map the obsolete disease term to a current one (for example, ischemic stroke or cerebral infarction) and re-run the prediction
- Search for clinical and observational evidence of diltiazem in stroke prevention or outcomes
- Obtain detailed mechanism of action data
- Obtain the Medsafe package insert warnings and contraindications
- Clarify why all New Zealand licences were cancelled, and whether any product is still available
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


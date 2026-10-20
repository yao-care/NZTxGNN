---
layout: default
title: Bisoprolol
parent: Model Prediction Only (L5)
nav_order: 67
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Bisoprolol: predicted new indication Malignant renovascular hypertension (evidence level L5). Approved use: Hypertension (hypertensive disorder)"
monitor_inn: "Bisoprolol"
predicted_indications: ["malignant hypertensive renal disease", "pulmonary hypertension with unclear multifactorial mechanism", "pulmonary hypertension owing to lung disease and/or hypoxia", "Braddock syndrome"]
---

# Bisoprolol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

# Bisoprolol: From Hypertension to Malignant Renovascular Hypertension

## One-Sentence Summary

Bisoprolol is a beta-1 selective blocker originally used to treat hypertension.
The TxGNN model predicts it may be effective for **malignant renovascular hypertension**,
but there are currently **0 clinical trials** and **0 publications** directly supporting this prediction (evidence is model-based only).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension (hypertensive disorder) |
| Predicted New Indication | Malignant renovascular hypertension |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorizations) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known information, bisoprolol is a beta-1 selective adrenergic blocker. Its efficacy in hypertension is established, and it targets the beta-1 adrenoceptor (ADRB1). Mechanistically, it may be applicable to renovascular hypertension.

By reducing heart rate, cardiac output and renin release, bisoprolol could plausibly lower blood pressure in renovascular hypertension, where the renin-angiotensin system plays a central role. This link rests on the TxGNN prediction and general pharmacology only.

There are important caveats. Malignant (accelerated) hypertension is an emergency usually managed with parenteral agents, and beta-blockers are not first-line. The other predicted indications show similar limits:

- **Malignant hypertensive renal disease** (score 99.94%): a close extension of the existing hypertension indication, but with no trials or literature, and the added value of bisoprolol is unestablished.
- **Pulmonary hypertension, unclear multifactorial mechanism** (score 99.93%): beta-blockers are generally used cautiously because they can reduce cardiac output and exercise capacity when the right heart is under strain.
- **Pulmonary hypertension owing to lung disease and/or hypoxia** (score 99.93%): eight publications were retrieved, but none is a clinical study of bisoprolol in this condition. They offer only indirect support (evidence level L4).
- **Braddock syndrome** (score 99.91%): no trials or literature, and no rationale for bisoprolol can be established from the available data.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature is available for the top predicted indication (malignant renovascular hypertension). The only literature retrieved relates to the fourth-ranked prediction, pulmonary hypertension owing to lung disease and/or hypoxia, and is indirect:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23506636](https://pubmed.ncbi.nlm.nih.gov/23506636/) | 2013 | Clinical comparative study | Int J Cardiol | CARNEBI: carvedilol vs nebivolol vs bisoprolol in moderate heart failure |
| [19789971](https://pubmed.ncbi.nlm.nih.gov/19789971/) | 2009 | Review | Cardiovasc Drugs Ther | Respiratory effects of beta-blockers in heart failure; the view that they are contraindicated with impaired lung function has been challenged |
| [18845967](https://pubmed.ncbi.nlm.nih.gov/18845967/) | 2008 | Physiological study | Med Sci Sports Exerc | Beta-adrenergic blockade and metabo-chemoreflex contributions to exercise capacity |
| [23706341](https://pubmed.ncbi.nlm.nih.gov/23706341/) | 2013 | Physiological study | J Am Soc Echocardiogr | Left ventricular adaptation to acute hypoxia, with and without beta-1 blockade |
| [32363697](https://pubmed.ncbi.nlm.nih.gov/32363697/) | 2020 | Preclinical | Fundam Clin Pharmacol | Bisoprolol protects myocardial cells from ischemia-reperfusion injury via the PI3K/AKT/GSK3β pathway |
| [39088923](https://pubmed.ncbi.nlm.nih.gov/39088923/) | 2024 | Preclinical (animal) | Int Immunopharmacol | Diosmin plus bisoprolol attenuated cobalt chloride-induced cardiotoxicity in rats |
| [27687650](https://pubmed.ncbi.nlm.nih.gov/27687650/) | 2016 | Preclinical (cell) | Nan Fang Yi Ke Da Xue Xue Bao | Bisoprolol pretreatment and hypoxia/reoxygenation injury in H9c2 cardiomyocytes |
| [12887136](https://pubmed.ncbi.nlm.nih.gov/12887136/) | 2003 | Case report | Hypertens Res | Two cases of pheochromocytoma with tetralogy of Fallot |

## New Zealand Market Information

All three authorizations are cancelled. No approved indication text is recorded.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2660350 | Ipca-Bisoprolol | Tab 2.5 mg | Not recorded (cancelled) |
| 2660369 | Ipca-Bisoprolol | Tab 5 mg | Not recorded (cancelled) |
| 2660377 | Ipca-Bisoprolol | Tab 10 mg | Not recorded (cancelled) |

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for safety information.

Additional information from the knowledge graph (not label text):

- **Disease associations flagged as contraindications in the knowledge graph**: asthma (including intrinsic and allergic), diabetes mellitus and diabetic ketoacidosis, liver disease, hepatic coma/encephalopathy, kidney disease, anuria, pancreatitis, gallbladder disease, systemic lupus erythematosus, psoriasis, gout, hyperparathyroidism, hypercalcemia, myasthenia gravis, anxiety and related disorders, and several metabolic conditions (e.g. potassium deficiency, phosphorus metabolism disease). These come from a computational knowledge graph and should be verified against the official label.
- **Pharmacology**: bisoprolol acts on the beta-1 adrenoceptor (ADRB1). No drug-drug interaction details were available.
- **Indication-specific concern**: beta-blockers can reduce cardiac output and exercise capacity in pulmonary hypertension.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predictions rest on a high TxGNN score and general pharmacology, with no clinical trials and no direct literature for any predicted indication (evidence level L5, or L4 for pulmonary hypertension owing to lung disease and/or hypoxia, based on indirect literature). Bisoprolol currently has no valid New Zealand authorization, and malignant hypertension is typically managed with other agents.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data (e.g. from DrugBank)
- Clinical evidence of bisoprolol in renovascular or malignant hypertension
- A safety assessment for the pulmonary hypertension predictions, given the risk of reduced cardiac output
- Clarification of regulatory status, since all New Zealand authorizations are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


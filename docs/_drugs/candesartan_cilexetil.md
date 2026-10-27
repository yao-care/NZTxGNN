---
layout: default
title: Candesartan Cilexetil
parent: Model Prediction Only (L5)
nav_order: 81
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Candesartan Cilexetil: predicted new indication Malignant hypertensive renal disease (evidence level L5). Approved use: Hypertensive disorder / hypertension"
monitor_inn: "CANDESARTAN CILEXETIL"
predicted_indications: ["malignant hypertensive renal disease", "pulmonary hypertension with unclear multifactorial mechanism", "pulmonary hypertension owing to lung disease and/or hypoxia", "Braddock syndrome"]
---

# Candesartan Cilexetil
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

## Candesartan Cilexetil: From Hypertension to Malignant Hypertensive Renal Disease
{: .text-alpha }

## One-Sentence Summary

Candesartan cilexetil is an angiotensin II receptor blocker originally used to treat hypertension.
The TxGNN model predicts it may be effective for **malignant hypertensive renal disease**,
but **no clinical trials and no directly relevant publications** currently support this prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertensive disorder / hypertension (from the knowledge graph; no approved indication text is available from the New Zealand licences) |
| Predicted New Indication | Malignant hypertensive renal disease |
| TxGNN Prediction Score | 99.68% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorisations) |
| Number of Authorizations | 8 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, candesartan is an angiotensin II type 1 (AT1) receptor blocker. Blocking AT1 receptors lowers blood pressure and can reduce renal injury driven by the renin-angiotensin system.

Malignant hypertensive renal disease is a severe, hypertension-driven form of kidney damage. It is closely related to the original hypertension indication, so a mechanistic rationale exists. However, it is largely an extension of the approved use rather than a truly new one.

The other top predictions are weaker or less certain:
- **Malignant renovascular hypertension** (score 99.68%): biologically plausible because it depends on renin-angiotensin activation. ARBs can reduce glomerular filtration in bilateral renal artery stenosis or stenosis of a solitary kidney, so caution is needed.
- **Pulmonary hypertension, unclear multifactorial mechanism** (99.67%): evidence for ARB benefit is weak and the subgroup is heterogeneous.
- **Pulmonary hypertension owing to lung disease and/or hypoxia** (99.67%): the two retrieved publications do not support this indication (see below), and systemic vasodilation might worsen ventilation-perfusion matching.
- **Braddock syndrome** (99.56%): no mechanistic link could be established; this needs expert review of the disease biology.

## Clinical Trial Evidence

Currently no related clinical trials registered for the top predicted indication or for any of the other four predictions.

## Literature Evidence

No literature is available for the top predicted indication. The only publications retrieved relate to the fourth-ranked prediction (pulmonary hypertension owing to lung disease and/or hypoxia), and neither is clinical evidence for it:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31396238](https://pubmed.ncbi.nlm.nih.gov/31396238/) | 2019 | Preclinical (mouse) | Frontiers in Immunology | Candesartan cilexetil's anti-inflammatory effect improved neurological outcomes in a mouse model of neonatal hypoxia; not a study of pulmonary hypertension |
| [21423692](https://pubmed.ncbi.nlm.nih.gov/21423692/) | 2011 | Preclinical (rat) | Evidence-based Complementary and Alternative Medicine | Hachimijiogan, a herbal formula, was compared with an angiotensin receptor blocker in remnant kidney rats, with involvement of HIF-1α; not a study of candesartan in pulmonary hypertension |

## New Zealand Market Information

All 8 authorisations are cancelled, and no approved indication text is available for them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2380420 | Candestar | Tab 16 mg | Not available (cancelled) |
| 2725371 | Candesartan Viatris Sante | Tab 16 mg | Not available (cancelled) |
| 2725355 | Candesartan Viatris Sante | Tab 4 mg | Not available (cancelled) |
| 2380404 | Candestar | Tab 4 mg | Not available (cancelled) |
| 2380439 | Candestar | Tab 32 mg | Not available (cancelled) |

Three further cancelled authorisations (Candesartan Viatris Sante 8 mg and 32 mg, and Candestar 8 mg) are not listed here.

## Safety Considerations

Product-label warnings and contraindications were not available for this report; please refer to the package insert for safety information.

The knowledge graph links candesartan to the following conditions as contraindications (these are graph relationships, not label text):
- **Kidney and renal vascular:** impaired renal function, anuria, renal artery obstruction, nephrocalcinosis
- **Metabolic and endocrine:** diabetes mellitus, diabetic ketoacidosis, potassium deficiency, hypercalcemia, phosphorus metabolism disease, hyperparathyroidism, osteitis fibrosa, gout, hyperuricemia
- **Liver and biliary:** liver disease, hepatic coma, hepatic encephalopathy, gallbladder disease, neonatal jaundice
- **Other:** pancreatitis, systemic lupus erythematosus (including paediatric), hypotensive disorder

The renal artery obstruction and impaired renal function entries are especially relevant to the renal and renovascular predictions above. No drug interaction data were found.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
All five predictions are supported only by the TxGNN model, with no clinical trials and no directly relevant publications (Evidence Level L5). The top prediction is mainly an extension of the original hypertension use, and the drug currently has no valid New Zealand authorisation.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- A literature and trial search specific to candesartan in malignant hypertensive nephropathy and renovascular hypertension
- A safety assessment for renal artery stenosis and impaired renal function, given the knowledge-graph contraindications
- Clarification of the regulatory pathway, since all current New Zealand authorisations are cancelled
- Expert review of the disease biology for Braddock syndrome and the pulmonary hypertension subtypes before any further consideration
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


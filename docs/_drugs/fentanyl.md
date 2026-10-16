---
layout: default
title: Fentanyl
parent: Model Prediction Only (L5)
nav_order: 196
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Fentanyl: predicted new indication Nephrogenic syndrome of inappropriate antidiuresis (evidence level L5). Approved use: Not available in the New Zealand license records"
monitor_inn: "FENTANYL"
predicted_indications: ["nephrogenic syndrome of inappropriate antidiuresis", "Tourette syndrome"]
---

# Fentanyl
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

# Fentanyl: From Opioid Analgesia to Nephrogenic Syndrome of Inappropriate Antidiuresis

## One-Sentence Summary

Fentanyl is a potent opioid (mu-opioid receptor agonist) that has been marketed in New Zealand as transdermal patches and an injectable, but all of its New Zealand authorizations are now cancelled.
The TxGNN model predicts it may be effective for **Nephrogenic Syndrome of Inappropriate Antidiuresis (NSIAD)**, but there are **0 clinical trials** and **0 publications** supporting this direction.
This is a model-only prediction (evidence level L5) and the recommendation is **Hold**.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in the New Zealand license records |
| Predicted New Indication | Nephrogenic syndrome of inappropriate antidiuresis |
| TxGNN Prediction Score | 99.46% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorizations) |
| Number of Authorizations | 7 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Fentanyl is known as a potent mu-opioid receptor agonist used for analgesia, but this comes from general pharmacological knowledge rather than the compiled data.

The review of the prediction found **no credible mechanistic link** to the new indication. NSIAD is caused by gain-of-function mutations in the vasopressin V2 receptor (AVPR2), which cause water retention independent of vasopressin. Opioids can indirectly promote ADH release and cause SIADH-like effects, which would worsen rather than treat antidiuresis.

The high TxGNN score (0.995) is a computational output only and is not backed by pharmacological rationale, trials, or literature. It should not be treated as evidence of efficacy.

**Second-ranked prediction (for reference):** Tourette syndrome (TxGNN score 99.05%, also L5, Hold). The link is weak and speculative: the opioid system modulates dopaminergic and striatal circuits involved in tic disorders. However, fentanyl's high abuse liability, respiratory depression risk, and tolerance/dependence make the risk-benefit profile unfavorable for a chronic, often pediatric-onset condition that already has safer options (alpha-2 agonists, antipsychotics, VMAT2 inhibitors, CBIT behavioral therapy). No trials or literature support it either.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

All authorizations below are cancelled. The license records contain no approved indication text.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2472899 | Fentanyl Sandoz | Patch 50 mcg per hour | Cancelled |
| 2472902 | Fentanyl Sandoz | Patch 75 mcg per hour | Cancelled |
| 2472880 | Fentanyl Sandoz | Patch 25 mcg per hour | Cancelled |
| 2472910 | Fentanyl Sandoz | Patch 100 mcg per hour | Cancelled |
| 2688824 | Fentanyl Sandoz | Patch 12 mcg per hour | Cancelled |

Two further cancelled authorizations (Boucher and Muir injection, 50 mcg per ml, 2 ml and 10 ml ampoules; 2381176 and 2381168) are not shown in the table above.

## Safety Considerations

- **Disease associations flagged by the knowledge graph (not label text):** The TxGNN knowledge graph lists contraindication links with conditions including hypertension, orthostatic hypotension and other hypotensive disorders, long QT syndrome and torsades de pointes, liver failure and liver disease, kidney disease, chronic obstructive pulmonary disease, asthma, pancreatitis, gallbladder disease, inflammatory bowel disease, benign prostatic hyperplasia, anxiety and neurotic disorders, potassium deficiency, and substance abuse/dependence.
- **Drug Interactions:** 100 interactions were found. Examples of **Major** interactions: alvimopan, aprepitant, bupropion, clarithromycin, clotrimazole, dexamethasone, dexfenfluramine, fenfluramine, dolasetron, granisetron, lorcaserin. Examples of **Moderate** interactions: atropine, cimetidine, clidinium, dicyclomine, dronabinol, eluxadoline, glycerol phenylbutyrate, glycopyrronium, hyoscyamine.

Package insert warnings and contraindications were not available; please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score, with no clinical trials, no literature, and no plausible mechanism. Pharmacologically, opioids may worsen antidiuresis. Fentanyl also has a significant safety burden and no currently valid New Zealand authorization.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (from Medsafe) to complete safety screening
- Mechanism of action data (from DrugBank)
- A credible mechanistic hypothesis linking mu-opioid agonism to AVPR2-driven antidiuresis, followed by preclinical support
- Any clinical or preclinical literature for the predicted indications (NSIAD or Tourette syndrome)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


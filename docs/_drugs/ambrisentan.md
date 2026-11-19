---
layout: default
title: Ambrisentan
parent: Model Prediction Only (L5)
nav_order: 31
evidence_level: L5
indication_count: 5
monitor_inn: "AMBRISENTAN"
predicted_indications: ["pulmonary arteriovenous malformation (disease)", "malformation syndrome with odontal and/or periodontal component", "hypotrichosis simplex of the scalp", "hypertrichosis (disease)", "syndrome with a Dandy-Walker malformation as major feature"]
last_modified_at: 2026-11-19
description: "Ambrisentan: predicted new indication pulmonary arteriovenous malformation (disease) (evidence level L5). Approved use: Not recorded in the local data"
---

# Ambrisentan
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

## Ambrisentan: Endothelin receptor A antagonist → pulmonary arteriovenous malformation (pulmonary arteriovenous malformation (disease))
{: .text-alpha }

## One-Sentence Summary

Ambrisentan is an endothelin receptor type A antagonist. The TxGNN model predicts it may be relevant to **pulmonary arteriovenous malformation (disease)**, but support is thin: **0 clinical trials** and **1 publication** (a case report that concerns pulmonary arterial hypertension in a patient with hereditary hemorrhagic telangiectasia, not treatment of pulmonary arteriovenous malformation).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | pulmonary arteriovenous malformation (disease) |
| TxGNN Prediction Score | 0.9941 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for ambrisentan: "Endothelin-1 (ET-1) is a potent autocrine and paracrine peptide. Two receptor subtypes, ET A and ET B, mediate the effects of ET-1 in the vascular smooth muscle and endothelium. […] Ambrisentan is a high-affinity (K i =0.011 nM) ET A receptor antagonist with a high selectivity for the ET A versus ET B receptor (>4000-fold)." ([DailyMed: Letairis (ambrisentan) prescribing information, Gilead Sciences, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=725d4e73-6c83-477a-adc6-0ae4a133a844), label version 2026-06-22; accessed 2026-11-19.) From its drug class, ambrisentan is an endothelin receptor type A antagonist. Endothelin-mediated vasoconstriction and vascular remodelling could plausibly be relevant to pulmonary vascular disease in hereditary hemorrhagic telangiectasia (HHT), the condition in which pulmonary arteriovenous malformations commonly occur.

However, the link is weak. The only publication is a case report of pulmonary arterial hypertension in an HHT patient. It addresses pulmonary arterial hypertension, not treatment of pulmonary arteriovenous malformations. No evidence shows a benefit on the malformations themselves, and vasodilation could theoretically worsen right-to-left shunting. The prediction should therefore be treated as a hypothesis from the model, not as an established finding.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [33969094](https://pubmed.ncbi.nlm.nih.gov/33969094/) | 2021 | Case report | World Journal of Clinical Cases | Pulmonary arterial hypertension in a patient with HHT, with family gene analysis; raises awareness of this co-occurrence. It does not test treatment of pulmonary arteriovenous malformation. |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2626454 | Ambrisentan Viatris | Ambrisentan | Tab 10 mg | Yes |
| 2626446 | Ambrisentan Viatris | Ambrisentan | Tab 5 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Drug Interactions**: 100 interactions were found in the interaction database. Those listed as moderate include canagliflozin, dapagliflozin, empagliflozin, ertugliflozin, eliglustat and naltrexone. Minor ones include clarithromycin, miconazole and omeprazole.
- **Teratogenicity**: Ambrisentan is teratogenic, which argues against use in developmental conditions and requires care in people who can become pregnant.
- **Vascular shunting**: Vasodilation could theoretically worsen right-to-left shunting in pulmonary arteriovenous malformation.

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No clinical trial or publication directly studies ambrisentan for pulmonary arteriovenous malformation, so the evidence level is L5 (model prediction only). The single case report concerns pulmonary arterial hypertension, and a theoretical risk of worsening shunting exists.

**To proceed, the following is needed:**
- Direct clinical or preclinical studies of endothelin receptor A blockade in pulmonary arteriovenous malformation
- Assessment of shunt and oxygenation risk in patients with HHT
- Package insert warnings and contraindications from the regulator's website
- Detailed mechanism of action data

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Letairis (ambrisentan) prescribing information, Gilead Sciences, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=725d4e73-6c83-477a-adc6-0ae4a133a844) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


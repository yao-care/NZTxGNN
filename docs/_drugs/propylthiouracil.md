---
layout: default
title: Propylthiouracil
parent: Moderate Evidence (L3-L4)
nav_order: 392
evidence_level: L4
indication_count: 3
last_modified_at: 2026-10-15
description: "Propylthiouracil: predicted new indication Resistance to thyroid hormone due to a mutation in thyroid hormone re… (evidence level L4). Approved use: Hyperthyroidism, Graves disease, thyrotoxicosis, t…"
monitor_inn: "PROPYLTHIOURACIL"
predicted_indications: ["resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta", "hyperthyroxinemia"]
---

# Propylthiouracil
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **3** 
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

# Propylthiouracil: From Hyperthyroidism to Resistance to Thyroid Hormone (THRB Mutation)

## One-Sentence Summary

Propylthiouracil (PTU) is an antithyroid drug originally used to treat hyperthyroidism, Graves disease, thyrotoxicosis and thyroid crisis.
The TxGNN model predicts it may be effective for **resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta**, but **no clinical trials** and only **4 publications** (mouse models and case reports, none testing PTU as a therapy) are available, so this is a model prediction without clinical support.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hyperthyroidism, Graves disease, thyrotoxicosis, thyroid crisis (from the knowledge graph; no approved indication text on the New Zealand licence) |
| Predicted New Indication | Resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta |
| TxGNN Prediction Score | 99.66% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 (0 valid, 1 cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the data compiled for this report. From general pharmacology, PTU inhibits thyroid peroxidase, which reduces thyroid hormone synthesis, and it also reduces peripheral conversion of T4 to T3. Its efficacy in hyperthyroid states is well established.

In resistance to thyroid hormone beta (RTHβ), tissues respond poorly to circulating hormone, and T4/T3 are elevated with a TSH that is not suppressed. In theory, lowering hormone synthesis would reduce the elevated hormone levels, which is probably why the model links the two conditions.

The mechanism is, however, a poor fit in practice. Lowering hormone output removes negative feedback and raises TSH, which can enlarge a goitre and stimulate thyroid growth. The retrieved literature supports this concern: mouse models carrying a TRβ mutation develop thyroid carcinoma in a TSH-dependent manner (PMID 22919057, 21909131), and one case report describes a patient previously treated with PTU for presumed thyrotoxicosis whose goitre became larger (PMID 10724359). Antithyroid drugs are generally not recommended in RTHβ. The very high TxGNN score is a model output only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [22919057](https://pubmed.ncbi.nlm.nih.gov/22919057/) | 2012 | Preclinical (mouse) | Endocrinology | In TRβ-mutant mice, TSH contributes to spontaneous development of thyroid carcinoma |
| [21909131](https://pubmed.ncbi.nlm.nih.gov/21909131/) | 2012 | Preclinical (mouse) | Oncogene | Thyroid hormone activates tumour cell proliferation in a mouse model of follicular thyroid carcinoma with a TRβ mutation |
| [12201835](https://pubmed.ncbi.nlm.nih.gov/12201835/) | 2002 | Case report | Clin Endocrinol | Neonatal thyrotoxicosis and maternal infertility in a family with a TRβ (M313T) mutation |
| [10724359](https://pubmed.ncbi.nlm.nih.gov/10724359/) | 1999 | Case report | Endocr J | De novo L330S mutation in a young woman previously treated with PTU; her goitre enlarged on treatment |

None of these studies tests PTU as a treatment for RTHβ.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2010526 (cancelled) | PTU | Tab 50 mg | Not stated |

## Cytotoxicity

Omitted: PTU is not an antineoplastic drug.

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: thrombocytopenia, hypothyroidism, myxedema, congenital hypothyroidism, hyperbilirubinemia (including hereditary forms and Gilbert syndrome), interstitial lung disease, drug-induced liver injury, and blood-cell disorders such as leukopenia, pancytopenia, agranulocytosis and neutropenia (including congenital and cyclic forms).
- **Drug Interactions**: 83 interactions were identified. The main ones are:
  - Major: deferiprone
  - Moderate: naltrexone, iodide I-131, iodide I-123, macimorelin, warfarin, dicoumarol, theophylline, aminophylline, oxtriphylline
  - Level not graded: pantoprazole, omeprazole, nystatin, rosuvastatin, prednisone, hydrocortisone, budesonide, dexamethasone, potassium chloride, clopidogrel

Please refer to the package insert for label warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone (L4). No clinical trials exist, the literature consists of mouse models and case reports that do not test PTU, and the mechanism suggests a risk of raising TSH and worsening goitre. The only New Zealand licence is cancelled, and the package insert safety data have not been obtained.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Clinical evidence that PTU benefits RTHβ patients, and an assessment of the TSH-rise risk
- A review of the hepatotoxicity risk and of New Zealand supply options, since no valid licence is currently held

---

### Supplementary Note: Other Predicted Indications

Two further predictions were also reviewed:

- **Neonatal thyrotoxicosis** (TxGNN score 99.40%, L4, recommendation: Research Question). The mechanistic link is strong, but the evidence is indirect (reviews on managing maternal hyperthyroidism in pregnancy, plus one cohort study, PMID 25747892), with no registered trials. Hepatotoxicity concerns limit direct neonatal use, and methimazole is generally preferred outside early pregnancy.
- **Hyperthyroxinemia** (TxGNN score 99.08%, L4, recommendation: Hold). This is a non-specific laboratory finding, and benefit depends on the cause. Lowering hormone would be inappropriate in familial dysalbuminemic hyperthyroxinemia, where patients are euthyroid. Evidence is limited to animal studies, case reports and observational reports, with no trials.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Hydrocortisone
parent: Model Prediction Only (L5)
nav_order: 224
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Hydrocortisone: predicted new indication Alopecia areata (evidence level L5). Approved use: Not stated in the New Zealand licence records"
---

# Hydrocortisone
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Hydrocortisone: From Autoimmune and Inflammatory Conditions to Alopecia Areata

## One-Sentence Summary

Hydrocortisone is a glucocorticoid originally used for conditions such as adrenocortical insufficiency, Crohn disease, inflammatory bowel disease, and other autoimmune and inflammatory disorders.
The TxGNN model predicts it may be effective for **alopecia areata**,
with **1 clinical trial** (where hydrocortisone served only as the low-potency comparator) and **20 publications** (mostly case reports and historical reports) currently associated with this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the New Zealand licence records (the knowledge graph lists, for example, adrenocortical insufficiency, Addison disease, Crohn disease, inflammatory bowel disease, and acute lymphoblastic leukemia) |
| Predicted New Indication | Alopecia areata |
| TxGNN Prediction Score | 99.97% |
| Evidence Level | L1 on paper (see note below); effectively weak |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 6 (all cancelled) |
| Recommended Decision | Hold |

*Note: The L1 label is assigned because a completed Phase 3 randomized trial exists. However, only one such trial was found (L1 formally requires at least two), and hydrocortisone was the comparator rather than the tested treatment. The evidence for hydrocortisone's own efficacy is therefore much weaker than the label suggests.*

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, hydrocortisone is a glucocorticoid with anti-inflammatory and immunosuppressive activity. Its efficacy in adrenal insufficiency and inflammatory or autoimmune conditions is well established.

Alopecia areata is a T-cell-mediated autoimmune disease in which the immune system attacks hair follicles. Corticosteroids can plausibly suppress this perifollicular inflammation, which is why topical and intralesional steroids are used in practice. This gives the prediction a reasonable mechanistic basis.

There are important caveats. Hydrocortisone is a weak-potency steroid and is expected to be less effective than higher-potency agents such as clobetasol. The only Phase 3 trial used hydrocortisone 1% as the comparator against clobetasol 0.05%, so it does not show that hydrocortisone works on its own. Historical reports from 1955 to 1973 describe hair regrowth after intralesional hydrocortisone, but these are low quality.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01453686](https://clinicaltrials.gov/study/NCT01453686) | Phase 3 | Completed | 41 | Randomized trial of clobetasol propionate 0.05% cream versus hydrocortisone 1% cream in children with alopecia areata. Hydrocortisone was the active comparator, so the trial does not establish hydrocortisone as the effective arm. Small sample size. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [24226568](https://pubmed.ncbi.nlm.nih.gov/24226568/) | 2014 | RCT | JAMA Dermatology | Clobetasol 0.05% versus hydrocortisone 1% for alopecia areata in children (the publication of the trial above) |
| [13368875](https://pubmed.ncbi.nlm.nih.gov/13368875/) | 1956 | Review / historical observation | Medical Times | Treatment of alopecia areata with cortisone, hydrocortisone, and analogs |
| [13525930](https://pubmed.ncbi.nlm.nih.gov/13525930/) | 1958 | Review | J Med Soc New Jersey | Overview of alopecia areata and other common scalp problems |
| [13610145](https://pubmed.ncbi.nlm.nih.gov/13610145/) | 1958 | Case series (historical) | Der Hautarzt | Hair regrowth reported after intracutaneous hydrocortisone injection |
| [15692503](https://pubmed.ncbi.nlm.nih.gov/15692503/) | 2005 | Case report | J Am Acad Dermatol | Four cases of congenital alopecia areata, treated with minoxidil and a range of topical agents |
| [19289325](https://pubmed.ncbi.nlm.nih.gov/19289325/) | 2009 | Case report | Endocr Pract | Autoimmune polyglandular syndrome type 3 with alopecia universalis and corticotropin deficiency (indirect) |
| [28094416](https://pubmed.ncbi.nlm.nih.gov/28094416/) | 2017 | Veterinary case report | Tierarztl Prax Ausg K | Canine alopecia areata; topical tacrolimus and hydrocortisone aceponate were ineffective |
| [24326563](https://pubmed.ncbi.nlm.nih.gov/24326563/) | 2013 | Preclinical (mouse) | J Investig Dermatol Symp Proc | Parathyroid hormone agonists for alopecia areata in mice (not hydrocortisone) |
| [4755919](https://pubmed.ncbi.nlm.nih.gov/4755919/) | 1973 | Historical report | Przegl Dermatol | Intralesional hydrocortisone acetate suspension for severe alopecia areata (no abstract) |
| [5989830](https://pubmed.ncbi.nlm.nih.gov/5989830/) | 1966 | Historical report | Vestn Dermatol Venerol | Intracutaneous hydrocortisone injections for alopecia areata and total alopecia (no abstract) |

---

## New Zealand Market Information

All six licences are cancelled, and no approved indication text is recorded for any of them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2645580 | Noumed | Cream 1% | Cancelled |
| 205648 | Douglas | Tablet 20 mg | Cancelled |
| 2646587 | Ethics | Cream 1% | Cancelled |
| 265284 | Solu-Cortef | Injection 100 mg vial | Cancelled |
| 2322420 | ABM | Powder | Cancelled |
| 205621 | Douglas | Tablet 5 mg | Cancelled |

---

## Safety Considerations

- **Contraindications suggested by the knowledge graph (not label text):** hypertension, epilepsy, gastric ulcer, kidney disease, liver disease, gallbladder disease, hyperglycemia, herpes simplex virus keratitis, measles, anogenital human papillomavirus infection, common wart, and several pituitary and growth hormone deficiency conditions.
- **Drug Interactions:** The interaction query returned 100 records. Examples include a **major** interaction with adalimumab, and moderate interactions with ibuprofen, ketorolac, nifedipine, isotretinoin, zidovudine, ethinylestradiol, aldesleukin, alemtuzumab, and several antidiabetic agents (acarbose, acetohexamide, albiglutide). Minor interactions include salbutamol and formoterol.

Package insert warnings and contraindications were not available. Please refer to the package insert for complete safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only Phase 3 trial used hydrocortisone as the weaker comparator, so it does not show that hydrocortisone is effective for alopecia areata. The remaining literature is mostly historical or low-quality case reports. In addition, all New Zealand licences are cancelled, and package insert safety data are missing, so safety screening cannot proceed.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications for a safety review
- Detailed mechanism of action data
- Direct comparative evidence of hydrocortisone against placebo or a vehicle control in alopecia areata
- Clarification of the regulatory pathway, since no valid New Zealand licence currently exists
- Confirmation of the intended route and formulation (topical or intralesional), which has not yet been assessed
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


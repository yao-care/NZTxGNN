---
layout: default
title: Hydrocortisone Acetate
parent: Model Prediction Only (L5)
nav_order: 225
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Hydrocortisone Acetate: predicted new indication Alopecia areata (evidence level L5). Approved use: Autoimmune disease, inflammatory bowel disease and Crohn disease…"
monitor_inn: "HYDROCORTISONE ACETATE"
predicted_indications: ["alopecia areata", "telogen effluvium", "alopecia mucinosa", "Quinquaud's folliculitis decalvans", "hereditary hypotrichosis with recurrent skin vesicles", "alopecia antibody deficiency", "alopecia-intellectual disability-hypergonadotropic hypogonadism syndrome", "atrichia with papular lesions", "seborrheic keratosis"]
---

# Hydrocortisone Acetate
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

# Hydrocortisone Acetate: From Autoimmune and Inflammatory Conditions to Alopecia Areata

## One-Sentence Summary

Hydrocortisone acetate is a corticosteroid originally used for autoimmune disease, inflammatory bowel disease (including Crohn disease), adrenocortical insufficiency and related conditions.
The TxGNN model predicts it may be effective for **alopecia areata**,
with **1 clinical trial** (in which hydrocortisone was the comparator) and **2 publications** (a 1973 case series and a 1979 review) currently supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Autoimmune disease, inflammatory bowel disease and Crohn disease, adrenocortical insufficiency, congenital adrenal hyperplasia (from the knowledge graph; the New Zealand licence has no approved indication text) |
| Predicted New Indication | Alopecia areata |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L1 (as assigned in the compiled data; see the note below) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

> **Note on the evidence level:** The L1 label would normally require two or more completed Phase 3 RCTs. Only one such trial was found, and hydrocortisone was the low-potency comparator rather than the test drug. In my judgement the real strength of evidence is closer to L2–L3 at best. This is why the recommendation below is more cautious than the label suggests.

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Based on known information, hydrocortisone acetate is a corticosteroid with anti-inflammatory and immunosuppressive effects. Its efficacy in autoimmune and inflammatory conditions is established.

Alopecia areata is an autoimmune, T-cell-mediated attack on hair follicles. Because hydrocortisone's original indications include autoimmune disease, the mechanistic link is plausible. Corticosteroids, given by intralesional injection or topically, are an established treatment class for alopecia areata. A 1973 report described intralesional injection of hydrocortisone acetate suspension in severe forms of the disease.

There is an important limitation. Hydrocortisone is a low-potency steroid and is generally considered less effective than higher-potency agents such as clobetasol. The prediction is therefore mechanistically sound, but the benefit of this specific drug is likely modest.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01453686](https://clinicaltrials.gov/study/NCT01453686) | Phase 3 | Completed | 41 | Randomized trial in children with alopecia areata comparing clobetasol propionate 0.05% cream with hydrocortisone 1% cream. Hydrocortisone was the low-potency comparator, and the trial used the 1% cream rather than the acetate salt. Results are not given in the available summary. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [4755919](https://pubmed.ncbi.nlm.nih.gov/4755919/) | 1973 | Case series | Przeglad dermatologiczny | Treatment of severe alopecia areata with intralesional injections of hydrocortisone acetate suspension (no abstract available). |
| [153470](https://pubmed.ncbi.nlm.nih.gov/153470/) | 1979 | Review | MMW, Munchener medizinische Wochenschrift | General review of topical skin therapy. It notes that the anti-inflammatory effect of fluocortin butyl ester is about equal to that of hydrocortisone acetate. Not specific to alopecia areata. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2611120 (cancelled) | Colifoam | Rectal foam 10%, CFC-Free (14 applications) | Not stated |

The only New Zealand authorization is a rectal foam, and it has been cancelled. It is not a formulation suited to treating alopecia areata, so a new product would likely be needed.

## Safety Considerations

- **Conditions flagged in the knowledge graph (not label text):** Prior drug-disease contraindication links include hypertension, epilepsy, gastric ulcer, kidney disease, liver disease, gallbladder disease, hyperglycemia, measles, herpes simplex virus keratitis, and pituitary or growth hormone deficiency disorders (including hypopituitarism, Kallmann syndrome and Sheehan syndrome).

No drug interaction data were found. For label-based warnings and contraindications, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic link to alopecia areata is plausible and the model score is high. However, the only trial used hydrocortisone as a low-potency comparator, and the supporting literature is dated and limited. The one New Zealand product is cancelled and is a rectal foam. The other nine predicted indications have model predictions only, with no supporting trials or literature.

**To proceed, the following is needed:**
- Trial results for NCT01453686, to see how hydrocortisone 1% performed against clobetasol
- Comparison against the higher-potency corticosteroids already used for alopecia areata
- Detailed mechanism of action data
- Package insert warnings and contraindications from Medsafe
- A suitable topical or intralesional formulation and a registration pathway in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


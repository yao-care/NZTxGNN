---
layout: default
title: Letrozole
parent: Model Prediction Only (L5)
nav_order: 266
evidence_level: L5
indication_count: 4
last_modified_at: 2026-11-19
monitor_inn: "LETROZOLE"
predicted_indications: ["nipple carcinoma", "Ehrlich tumor carcinoma", "breast fibrocystic disease", "benign mammary dysplasia"]
description: "Letrozole: predicted new indication Nipple carcinoma (evidence level L5). In New Zealand: 1 Pharmac Schedule item"
---

# Letrozole
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

## Letrozole: Nipple carcinoma
{: .text-alpha }

## One-Sentence Summary

Letrozole is an aromatase inhibitor that lowers estrogen production. The TxGNN model predicts it may be useful for **nipple carcinoma**, with a score of 0.9976. There are **no registered clinical trials** and only **8 publications** (mostly case reports), so the evidence is at **Level L5**: a model prediction without a study that tests letrozole in this disease.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Nipple carcinoma |
| TxGNN Prediction Score | 0.9976 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for letrozole: "The growth of some cancers of the breast is stimulated or maintained by estrogens. Treatment of breast cancer thought to be hormonally responsive (i.e., estrogen and/or progesterone receptor positive or receptor unknown) has included a variety of efforts to decrease estrogen levels (ovariectomy, adrenalectomy, hypophysectomy) or inhibit estrogen effects (antiestrogens and progestational agents). […] Letrozole is a nonsteroidal competitive inhibitor of the aromatase enzyme system; it inhibits the conversion of androgens to estrogens." ([DailyMed: Femara (letrozole) prescribing information, Novartis Pharmaceuticals Corporation](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=82b77d74-085f-45ac-a7dd-1f5c038bf406), label version 2026-06-11; accessed 2026-11-19.) Letrozole is known as an aromatase inhibitor, a class that blocks the conversion of androgens to estrogen, and this is the basis for the reasoning below.

Nipple and accessory breast carcinomas are breast-tissue tumours. If a tumour is hormone receptor positive, lowering estrogen is mechanistically plausible. However, the literature found consists of case reports and reviews of rare breast tumours, and none tests letrozole in nipple carcinoma. The high score probably reflects the close ontology relationship to breast carcinoma rather than disease-specific evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [33120755](https://pubmed.ncbi.nlm.nih.gov/33120755/) | 2020 | Cohort/case report | Medicine | Neoadjuvant endocrine therapy in poorly differentiated neuroendocrine carcinoma of the breast; effect of endocrine therapy in this disease is unclear |
| [41607681](https://pubmed.ncbi.nlm.nih.gov/41607681/) | 2026 | Case report | Clinical Case Reports | Nipple-originating solid papillary carcinoma in situ in a 73-year-old woman; limited evidence on standard treatment |
| [34495428](https://pubmed.ncbi.nlm.nih.gov/34495428/) | 2021 | Case report | Surgical Case Reports | Rare accessory breast cancer in the inframammary region, with literature review |
| [40395253](https://pubmed.ncbi.nlm.nih.gov/40395253/) | 2025 | Case report | Cureus | Axillary recurrence in breast cancer with bloody nipple discharge |
| [39682607](https://pubmed.ncbi.nlm.nih.gov/39682607/) | 2024 | Case report | Diagnostics | Synchronous metastatic infra-mammary accessory breast cancer and ipsilateral breast cancer |
| [42040393](https://pubmed.ncbi.nlm.nih.gov/42040393/) | 2026 | Case report | Clinical Case Reports | Tall cell carcinoma of the breast with reverse polarity and IDH1 R132 mutation |
| [42305961](https://pubmed.ncbi.nlm.nih.gov/42305961/) | 2026 | Case report | Frontiers in Medicine | Synchronous accessory breast cancer, papillary thyroid carcinoma and nasal papilloma |
| [42416948](https://pubmed.ncbi.nlm.nih.gov/42416948/) | 2026 | Case report | Cureus | Imaging findings of florid lobular carcinoma in situ |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2656655 | Letrole | Letrozole | Tab 2.5 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Hormonal (endocrine) therapy, not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Conditions of concern for use** (from drug-disease contraindication records, not label text): liver failure, cirrhosis of liver, osteoporosis, postmenopausal osteoporosis, hyperbilirubinemia, hereditary hyperbilirubinemia, Gilbert syndrome, cerebrovascular disorder.
- **Drug Interactions**: 100 potential interactions were identified. The only one graded Moderate is aprepitant. Others in the first results (for example calcitriol, glimepiride, lansoprazole, omeprazole, metformin, morphine, simvastatin, prednisone) have no assigned severity level.

Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No study tests letrozole in nipple carcinoma, and the supporting literature is limited to case reports of rare breast tumours. The evidence level is L5, so the prediction cannot yet support a decision to proceed.

**To proceed, the following is needed:**
- Studies that directly test letrozole in nipple carcinoma, with hormone receptor status reported
- Mechanism of action data
- Package insert warnings and contraindications
- A bone, liver and cardiovascular risk review for the target population

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Femara (letrozole) prescribing information, Novartis Pharmaceuticals Corporation](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=82b77d74-085f-45ac-a7dd-1f5c038bf406) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


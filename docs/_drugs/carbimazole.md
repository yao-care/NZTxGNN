---
layout: default
title: Carbimazole
parent: Model Prediction Only (L5)
nav_order: 85
evidence_level: L5
indication_count: 3
---

# Carbimazole
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

# Carbimazole: From Hyperthyroidism to Resistance to Thyroid Hormone (THR-beta Mutation)

## One-Sentence Summary

Carbimazole is an antithyroid drug (a prodrug of methimazole), originally used to treat hyperthyroidism, Graves disease and thyrotoxicosis.
The TxGNN model predicts it may be effective for **resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta**, but this top-ranked prediction is supported by **0 clinical trials** and only **1 publication** (a case-based diagnostic discussion that does not show benefit), so the evidence is very weak.
Two lower-ranked predictions, neonatal thyrotoxicosis (13 publications) and hyperthyroxinemia (6 publications), have more literature support and are discussed below.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hyperthyroidism, Graves disease, thyrotoxicosis (from the TxGNN knowledge graph; no approved indication text is available for the New Zealand licence) |
| Predicted New Indication | Resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the data compiled for this report. Based on known information, carbimazole is a prodrug of methimazole that inhibits thyroid peroxidase and reduces thyroid hormone synthesis. Its efficacy in hyperthyroidism is well established.

For resistance to thyroid hormone (RTH-beta), the mechanistic rationale is weak and potentially harmful. The defect lies at the receptor level, with elevated free T4 and non-suppressed TSH. Lowering hormone synthesis could in theory reduce circulating T4/T3, but it would also raise TSH through loss of negative feedback, risking goitre and thyrotroph stimulation. Pituitary resistance would make this worse. The high TxGNN score is a computational output only. The single retrieved article does not demonstrate benefit.

**Other predicted indications**

- **Neonatal thyrotoxicosis (score 99.41%, evidence level L3, "Research Question"):** This is usually caused by transplacental passage of maternal TSH-receptor stimulating antibodies in Graves disease. Carbimazole blocks hormone synthesis through the same mechanism as in its original indication, so the mechanistic link is strong. Evidence is observational, with no registered trials.
- **Hyperthyroxinemia (score 99.21%, evidence level L4, "Research Question"):** This overlaps with the original indication when it reflects true hyperthyroidism. The literature covers heterogeneous causes. In familial dysalbuminaemic hyperthyroxinaemia, patients are euthyroid and antithyroid drugs are not indicated and could cause harm. In TSH-secreting pituitary adenoma, thionamides are only a temporary measure before definitive therapy. Benefit depends on distinguishing true thyrotoxicosis from binding-protein abnormalities and central causes.

## Clinical Trial Evidence

Currently no related clinical trials registered for any of the three predicted indications.

## Literature Evidence

**Top-ranked prediction: resistance to thyroid hormone (THR-beta)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [24165508](https://pubmed.ncbi.nlm.nih.gov/24165508/) | 2013 | Case-based diagnostic discussion | BMJ Case Reports | Man with elevated free T4 and non-suppressed TSH, treated intermittently with carbimazole for 10 years. Addresses differential diagnosis, not carbimazole benefit in RTH. |

**Second-ranked prediction: neonatal thyrotoxicosis (most relevant 10 of 13)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [24251220](https://pubmed.ncbi.nlm.nih.gov/24251220/) | 2013 | Review | Indian J Endocrinol Metab | Fetal thyrotoxicosis is rare, with mortality of 12-20%, mostly from heart failure. Caused by transplacental transfer of thyroid-stimulating immunoglobulins. |
| [24622372](https://pubmed.ncbi.nlm.nih.gov/24622372/) | 2013 | Review | Lancet Diabetes Endocrinol | Hyperthyroidism in pregnancy; adverse outcomes can occur in mother and offspring. |
| [7523202](https://pubmed.ncbi.nlm.nih.gov/7523202/) | 1994 | Retrospective cohort | Eur J Obstet Gynecol Reprod Biol | 32 pregnancies with hyperthyroidism treated with carbimazole or propylthiouracil. |
| [1971773](https://pubmed.ncbi.nlm.nih.gov/1971773/) | 1990 | Cohort | Clin Endocrinol | 44 women with Graves disease on propylthiouracil or carbimazole; links TSH-receptor antibodies to maternal and neonatal thyroid function. |
| [11298090](https://pubmed.ncbi.nlm.nih.gov/11298090/) | 2001 | Case series | Clin Endocrinol | Congenital thyrotoxicosis in premature infants; mortality reported up to 25%. |
| [41191399](https://pubmed.ncbi.nlm.nih.gov/41191399/) | 2025 | Case report | Endocrinol Diabetes Metab Case Rep | Neonatal hypercalcaemia secondary to transient thyrotoxicosis; thyrotoxicosis responded to carbimazole. |
| [25952662](https://pubmed.ncbi.nlm.nih.gov/25952662/) | 2015 | Case report | Indian J Pediatr | Intrauterine carbimazole treatment in two siblings with fetal hyperthyroidism. |
| [12124735](https://pubmed.ncbi.nlm.nih.gov/12124735/) | 2002 | Case report (safety signal) | Am J Med Genet | Fourth published case of choanal atresia after maternal carbimazole treatment. |
| [27747714](https://pubmed.ncbi.nlm.nih.gov/27747714/) | 2015 | Case report | Drug Saf Case Rep | Neonate on carbimazole and propranolol developed severe hypotension after amlodipine. |
| [2315685](https://pubmed.ncbi.nlm.nih.gov/2315685/) | 1990 | Case report | Scott Med J | Neonatal thyrotoxicosis responded to carbimazole; maternal and neonatal TSH-receptor antibodies present. |

**Third-ranked prediction: hyperthyroxinemia (6 publications)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3709386](https://pubmed.ncbi.nlm.nih.gov/3709386/) | 1986 | Review | Drugs | Difficulties in diagnosing and treating thyroid disorders in the elderly. |
| [24847468](https://pubmed.ncbi.nlm.nih.gov/24847468/) | 2014 | Case report | Eur Thyroid J | TSH-secreting pituitary adenoma coexisting with Graves hyperthyroidism. |
| [32854689](https://pubmed.ncbi.nlm.nih.gov/32854689/) | 2020 | Case report | BMC Endocr Disord | Graves thyrotoxicosis followed by a probable TSHoma. |
| [8024995](https://pubmed.ncbi.nlm.nih.gov/8024995/) | 1994 | Case series/Review | Br J Clin Pract | Hyperthyroidism associated with hyperemesis gravidarum. |
| [3708869](https://pubmed.ncbi.nlm.nih.gov/3708869/) | 1986 | Mechanistic study | Clin Endocrinol | Albumin binding in familial dysalbuminaemic hyperthyroxinaemia. |
| [3985431](https://pubmed.ncbi.nlm.nih.gov/3985431/) | 1985 | Case report | Ann Fr Anesth Reanim | Post-traumatic hyperthyroxinaemia or hyperthyroidism. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 594423 (cancelled) | Neo-Mercazole | Tab 5 mg | Not available |

## Safety Considerations

- **Teratogenic concern:** The retrieved literature includes a case report of choanal atresia after fetal carbimazole exposure (PMID 12124735), consistent with the known concern about thionamides in early pregnancy.
- **Potential harm in specific predicted settings:** In RTH-beta, lowering hormone synthesis may raise TSH and risk goitre. In familial dysalbuminaemic hyperthyroxinaemia, patients are euthyroid and antithyroid drugs could cause harm.

No drug interaction records were found. Please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (RTH-beta) rests on a model score alone, has a weak and potentially harmful mechanistic rationale, and has no supporting trials. The neonatal thyrotoxicosis prediction has a plausible mechanism and observational support, but it comes from case reports and cohorts, includes a teratogenicity signal, and the only New Zealand licence is cancelled. It is best treated as a research question, not a basis for use.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, needed before any safety screening
- Mechanism-of-action data from DrugBank
- For neonatal thyrotoxicosis, a comparative evaluation against alternatives such as propylthiouracil and methimazole, with a specialist-supervised monitoring plan
- For hyperthyroxinemia, criteria to distinguish true thyrotoxicosis from binding-protein abnormalities and central causes
- Clarification of the regulatory pathway given the cancelled licence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


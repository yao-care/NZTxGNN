---
layout: default
title: Drug Repurposing Predictions
nav_order: 1
description: "AI-powered drug repurposing predictions for New Zealand. Evidence-based analysis of approved medicines for potential new therapeutic indications."
permalink: /
---

# Drug Repurposing Predictions — New Zealand

This project uses the TxGNN graph neural network to predict potential new therapeutic uses for medicines approved in New Zealand.

**Disclaimer**: This content is for research purposes only and does not constitute medical advice. All drug repurposing predictions require clinical validation before application.

---

<!-- local-home:start（母目錄 .claude/gen_local_home_20261029.py 產生，勿手改）-->
## New Zealand: what this site covers

This site publishes reports for **{{ site.data.drug_stats.total_drugs }}** medicines listed in the registration data of [Medsafe](https://www.medsafe.govt.nz/) (New Zealand).

Reports by evidence level: L1 {{ site.data.drug_stats.level_counts.L1 }} · L2 {{ site.data.drug_stats.level_counts.L2 }} · L3 {{ site.data.drug_stats.level_counts.L3 }} · L4 {{ site.data.drug_stats.level_counts.L4 }} · L5 {{ site.data.drug_stats.level_counts.L5 }}

### Highest evidence levels on this site

| Medicine | Evidence level |
|---|---|
| [Acetylcysteine](/drugs/acetylcysteine/) | L1 |
| [Bendamustine](/drugs/bendamustine/) | L1 |
| [Bupropion](/drugs/bupropion/) | L1 |
| [Busulfan](/drugs/busulfan/) | L1 |
| [Natamycin](/drugs/natamycin/) | L1 |
| [Rifabutin](/drugs/rifabutin/) | L1 |
| [Hydrocortisone](/drugs/hydrocortisone/) | L1 |
| [Hydrocortisone Acetate](/drugs/hydrocortisone-acetate/) | L1 |
| [Doxorubicin](/drugs/doxorubicin/) | L1 |
| [Icatibant](/drugs/icatibant/) | L1 |

### Medicines covered only by this site among the 30 TxGNN sites

| Medicine | Evidence level |
|---|---|
| [Saccharin](/drugs/saccharin/) | L5 |

<!-- local-home:end -->

## About the Developer

This platform is developed and operated by **藥提醒科技有限公司** (yao.care, company registration
number 83620786, 12F, No. 220, Sec. 2, Taiwan Blvd., West Dist., Taichung City, Taiwan).

NZTxGNN is the New Zealand site of the company's "TxGNN Drug Repurposing" product line.
The same system is deployed across 30 countries and regions, each named `{CC}TxGNN`
(JpTxGNN, UsTxGNN, DETxGNN, and so on) at `{cc}txgnn.yao.care`.
Product overview: <https://www.yao.care/medical/txgnn/>.

The TxGNN model itself was developed by the Zitnik Lab at Harvard Medical School and published
in *Nature Medicine*. This platform is the production system 藥提醒科技有限公司 built on top of that
model, covering national drug-registration data integration, deep-learning prediction that excludes uses already recorded in the knowledge graph, PubMed / ClinicalTrials evidence grading, and SMART on FHIR
electronic health record integration.

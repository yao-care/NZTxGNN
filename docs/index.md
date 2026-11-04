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

This site publishes reports for **{{ site.data.drug_stats.total_drugs }}** medicines on the [Pharmac Community Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) (New Zealand).

### New Zealand medicine data

| Item | Detail |
|------|--------|
| Dataset | Pharmac Community Pharmaceutical Schedule (CPSReporting.xlsx, "Community Medicines" sheet) |
| Publisher | Pharmac |
| Download | <https://schedule.pharmac.govt.nz/latest/CPSReporting.xlsx> |
| Nature of the data | A funding schedule, not a licence register: Medsafe licence status is not included |
| Update | Downloaded again every Monday by an automated workflow; listing status in drug reports is updated from the new file |

Details: [Data Sources]({{ '/sources/' | relative_url }}).

Reports by evidence level:

| Evidence level | Reports | Share |
|---|---|---|
| L1 | {{ site.data.drug_stats.level_counts.L1 }} | {{ site.data.drug_stats.level_counts.L1 | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |
| L2 | {{ site.data.drug_stats.level_counts.L2 }} | {{ site.data.drug_stats.level_counts.L2 | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |
| L3 | {{ site.data.drug_stats.level_counts.L3 }} | {{ site.data.drug_stats.level_counts.L3 | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |
| L4 | {{ site.data.drug_stats.level_counts.L4 }} | {{ site.data.drug_stats.level_counts.L4 | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |
| L5 | {{ site.data.drug_stats.level_counts.L5 }} | {{ site.data.drug_stats.level_counts.L5 | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |
{% assign na = site.data.drug_stats.total_drugs | minus: site.data.drug_stats.level_counts.L1 | minus: site.data.drug_stats.level_counts.L2 | minus: site.data.drug_stats.level_counts.L3 | minus: site.data.drug_stats.level_counts.L4 | minus: site.data.drug_stats.level_counts.L5 %}{% if na > 0 %}| N/A | {{ na }} | {{ na | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |{% endif %}

2 predicted drug–disease pairs on this site appear on no other of the 30 TxGNN sites.

### Highest evidence levels on this site

| Medicine | Evidence level |
|---|---|
| [Acetylcysteine](/drugs/acetylcysteine/) | L1 |
| [Bendamustine](/drugs/bendamustine/) | L1 |
| [Bupropion](/drugs/bupropion/) | L1 |
| [Busulfan](/drugs/busulfan/) | L1 |
| [Clomipramine](/drugs/clomipramine/) | L1 |
| [Doxorubicin](/drugs/doxorubicin/) | L1 |
| [Tacrolimus](/drugs/tacrolimus/) | L1 |
| [Dapsone](/drugs/dapsone/) | L1 |
| [Ketamine](/drugs/ketamine/) | L1 |
| [Diazepam](/drugs/diazepam/) | L2 |

### Medicines covered only by this site among the 30 TxGNN sites

| Medicine | Evidence level |
|---|---|
| [Saccharin](/drugs/saccharin/) | L5 |

<!-- local-home:end -->

## About the Developer

This platform is developed and operated by **藥提醒科技有限公司** (yao.care). See the
<a href="{{ '/about/' | relative_url }}">About</a> page for the full company and methodology background.

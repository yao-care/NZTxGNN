---
layout: default
title: About
nav_order: 90
permalink: /about/
description: "NZTxGNN is a drug repurposing prediction platform developed by 藥提醒科技有限公司 (yao.care), built on the Harvard TxGNN model, covering medicines on the Pharmac Community Pharmaceutical Schedule in New Zealand."
---

# About

<div class="key-takeaway">
Accelerating drug repurposing evidence validation with AI — from prediction to evidence at a glance.
</div>

---

<!-- local-facts:start（母目錄 .claude/gen_local_pages_20261119.py 產生，勿手改）-->
## New Zealand: this site's data in numbers

Local data: [Pharmac Community Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/). The local data are listings on the Pharmac Community Pharmaceutical Schedule, a funding schedule rather than a licence register. Medsafe licence status is not included in this data.

| Item | Value |
|---|---|
| Medicines with a published report | {{ site.data.local_facts.drugs_total }} |
| Medicines found in the local data | {{ site.data.local_facts.licence.drugs_with_data }} |
| …with at least one current entry | {{ site.data.local_facts.licence.drugs_with_current }} |
| …with entries but none current | {{ site.data.local_facts.licence.drugs_none_current }} |
| Medicines not in the local data | {{ site.data.local_facts.licence.drugs_without_data }} |
| Local entries (Pharmac Schedule listings) | {{ site.data.local_facts.licence.items_total }} |
| …of which current | {{ site.data.local_facts.licence.items_current }} |
| Review-log entries | {{ site.data.local_facts.reviews.records }} |
| Drug pages with review-log entries | {{ site.data.local_facts.reviews.public_pages_with_records }} |
| Predicted drug–disease pairs found on no other TxGNN site | {{ site.data.local_facts.cross.unique_pairs }} |

{% if site.data.local_facts.top_licensed.size > 0 %}Medicines with the most current Pharmac Schedule listings: {% for r in site.data.local_facts.top_licensed %}[{{ r.t }}]({{ r.u | relative_url }}) ({{ r.lv }}){% unless forloop.last %}, {% endunless %}{% endfor %}{% endif %}

{% if site.data.local_facts.recent_reviews.size > 0 %}Latest review-log corrections: {% for r in site.data.local_facts.recent_reviews limit: 8 %}[{{ r.t }}]({{ r.u | relative_url }}) ({{ r.d }}): {{ r.s }}{% unless forloop.last %}; {% endunless %}{% endfor %}{% endif %}

Examples of predictions that no other TxGNN site lists for the same medicine: [Saccharin]({{ '/drugs/saccharin/' | relative_url }}) → obsolete bundle branch block.

<!-- local-facts:end -->

## Background

<p class="key-answer" data-question="What is NZTxGNN?">
<strong>NZTxGNN</strong> is a research-support platform for drug repurposing, built on the TxGNN
model published in <em>Nature Medicine</em> by the Zitnik Lab at Harvard University. It predicts
indication expansion for medicines on the Pharmac Community Pharmaceutical Schedule in New Zealand. Beyond AI prediction scores,
the platform integrates clinical evidence from ClinicalTrials.gov and PubMed so researchers can
quickly assess how credible each prediction is.
</p>

---

<!-- local-condensed:start（母目錄 .claude/gen_local_pages_20261119.py；原共用段落縮寫，勿手改）-->
## About the Developer

This platform is developed and operated by **藥提醒科技有限公司** (yao.care, company registration number 83620786, Taichung City, Taiwan) as one of 30 country and region sites of its TxGNN drug repurposing product line ([product overview](https://www.yao.care/medical/txgnn/)).

---

## Method in brief

Predictions come from TxGNN, a graph neural network model from the Zitnik Lab at Harvard Medical School (Huang et al., *Nature Medicine* 2024, [DOI: 10.1038/s41591-024-03233-x](https://doi.org/10.1038/s41591-024-03233-x)); each predicted indication is graded L1–L5 against ClinicalTrials.gov and PubMed. Model, evidence levels and grading rules: [Methodology]({{ '/methodology/' | relative_url }}). Datasets, download links and licences: [Data Sources]({{ '/sources/' | relative_url }}).
<!-- local-condensed:end -->

---

## Contact

- **GitHub Issues**: <https://github.com/yao-care/NZTxGNN/issues>
- **Developer**: 藥提醒科技有限公司 (<https://www.yao.care>, service@yao.care)
- **Product overview**: <https://www.yao.care/medical/txgnn/>

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>

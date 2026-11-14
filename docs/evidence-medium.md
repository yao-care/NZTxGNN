---
layout: default
title: Moderate Evidence (L3-L4)
nav_order: 22
permalink: /evidence-medium/
description: "L3-L4 drug repurposing candidates in NZTxGNN, backed by observational or preclinical evidence."
---

# Moderate Evidence (L3-L4)

<!-- page-data:start（母目錄 .claude/gen_page_data_20261116.py 產生，勿手改）-->
{% assign pd_m = site.data.drug_stats.level_counts.L3 | plus: site.data.drug_stats.level_counts.L4 %}
This page lists **{{ pd_m }}** of the **{{ site.data.drug_stats.total_drugs }}** medicines with a published report on this site ({{ pd_m | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}%). Level definitions: [Methodology]({{ '/methodology/' | relative_url }}).
<!-- page-data:end -->

<p style="font-size: 1.25rem; color: #666; margin-bottom: 1.5rem;">
Candidates with preliminary evidence that require further validation
</p>

---

## Criteria

| Level | Definition | Clinical meaning |
|-------|------------|------------------|
| **L3** | Observational studies / large case series | Preliminary support; needs further validation |
| **L4** | Preclinical / mechanistic studies | Theoretical support; far from clinical use |

---

{% assign l3_drugs = site.drugs | where: "evidence_level", "L3" | where_exp: "d", "d.noindex != true" | sort: "title" %}
{% assign l4_drugs = site.drugs | where: "evidence_level", "L4" | where_exp: "d", "d.noindex != true" | sort: "title" %}

### L3 ({{ l3_drugs.size }} drugs)

| Drug | Indications | Link |
|---------|---------|------|
{% for drug in l3_drugs %}| **{{ drug.title }}** | {{ drug.indication_count }} | [View report]({{ drug.url | relative_url }}) |
{% endfor %}

### L4 ({{ l4_drugs.size }} drugs)

| Drug | Indications | Link |
|---------|---------|------|
{% for drug in l4_drugs %}| **{{ drug.title }}** | {{ drug.indication_count }} | [View report]({{ drug.url | relative_url }}) |
{% endfor %}

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>

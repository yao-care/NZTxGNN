---
layout: default
title: All Drugs
nav_order: 20
permalink: /drugs/
description: "All drug validation reports and evidence level statistics in NZTxGNN."
---
{% assign l1_count = site.drugs | where: "evidence_level", "L1" | size %}
{% assign l2_count = site.drugs | where: "evidence_level", "L2" | size %}
{% assign l3_count = site.drugs | where: "evidence_level", "L3" | size %}
{% assign l4_count = site.drugs | where: "evidence_level", "L4" | size %}
{% assign l5_count = site.drugs | where: "evidence_level", "L5" | size %}

# All Drugs

{{ site.drugs.size }} drug validation reports

---

## Evidence level breakdown

| Evidence level | Drugs | Description |
|---------|--------|------|
| **L1** | {{ l1_count }} | Multiple RCTs / systematic reviews |
| **L2** | {{ l2_count }} | Single RCT / Phase 2 trials |
| **L3** | {{ l3_count }} | Observational studies / large case series |
| **L4** | {{ l4_count }} | Preclinical / mechanistic studies |
| **L5** | {{ l5_count }} | Model prediction only |

---

## Full drug list

{% assign all_drugs = site.drugs | sort: 'title' %}

| Drug | Evidence level | Indications |
|---------|---------|---------|
{% for drug in all_drugs %}| [{{ drug.title }}]({{ drug.url | relative_url }}) | {{ drug.evidence_level }} | {{ drug.indication_count }} |
{% endfor %}

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>

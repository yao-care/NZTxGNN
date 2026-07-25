---
layout: default
title: Model Prediction Only (L5)
nav_order: 23
permalink: /evidence-low/
description: "L5 candidates in NZTxGNN: model prediction only, with no clinical or literature evidence yet."
---

# Model Prediction Only (L5)

<p style="font-size: 1.25rem; color: #666; margin-bottom: 1.5rem;">
Candidates with model prediction only and no human evidence yet
</p>

---

## Criteria

| Level | Definition | Clinical meaning |
|-------|------------|------------------|
| **L5** | Model prediction only | Hypothesis stage; no human evidence yet |

---

{% assign l5_drugs = site.drugs | where: "evidence_level", "L5" | sort: "title" %}

### L5 ({{ l5_drugs.size }} drugs)

| Drug | Indications | Link |
|---------|---------|------|
{% for drug in l5_drugs %}| **{{ drug.title }}** | {{ drug.indication_count }} | [View report]({{ drug.url | relative_url }}) |
{% endfor %}

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>

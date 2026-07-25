---
layout: default
title: High Evidence (L1-L2)
nav_order: 21
permalink: /evidence-high/
description: "L1-L2 drug repurposing candidates in NZTxGNN, supported by clinical trials or systematic reviews."
---

# High Evidence (L1-L2)

<p style="font-size: 1.25rem; color: #666; margin-bottom: 1.5rem;">
Candidates that may be prioritised for clinical evaluation
</p>

---

## Criteria

| Level | Definition | Clinical meaning |
|-------|------------|------------------|
| **L1** | Multiple Phase 3 RCTs / systematic reviews | Strong support; clinical use may be considered |
| **L2** | Single RCT or multiple Phase 2 trials | Moderate support; validation trials can be designed |

---

{% assign l1_drugs = site.drugs | where: "evidence_level", "L1" | sort: "title" %}
{% assign l2_drugs = site.drugs | where: "evidence_level", "L2" | sort: "title" %}

### L1 ({{ l1_drugs.size }} drugs)

| Drug | Indications | Link |
|---------|---------|------|
{% for drug in l1_drugs %}| **{{ drug.title }}** | {{ drug.indication_count }} | [View report]({{ drug.url | relative_url }}) |
{% endfor %}

### L2 ({{ l2_drugs.size }} drugs)

| Drug | Indications | Link |
|---------|---------|------|
{% for drug in l2_drugs %}| **{{ drug.title }}** | {{ drug.indication_count }} | [View report]({{ drug.url | relative_url }}) |
{% endfor %}

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>

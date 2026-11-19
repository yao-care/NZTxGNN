---
layout: default
title: Model Prediction Only (L5)
nav_order: 23
permalink: /evidence-low/
description: "365 medicines at evidence level L5 on this site (New Zealand), e.g. Olanzapine, Methylphenidate, Candesartan Cilexetil, with the main predicted new indication, local Pharmac Schedule listings and review-log entries for each."
---

# Model Prediction Only (L5)

<!-- tier-facts:start（母目錄 .claude/gen_local_pages_20261119.py 產生，勿手改）-->
{% assign ev_share = site.data.evidence_tier_facts.low.count | times: 100.0 | divided_by: site.data.drug_stats.total_drugs %}
This page lists **{{ site.data.evidence_tier_facts.low.count }}** of the **{{ site.data.drug_stats.total_drugs }}** medicines with a published report on this site ({{ site.data.evidence_tier_facts.low.count | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}%): L5 **{{ site.data.evidence_tier_facts.low.by_level.L5 }}**.
Median share at L5 across the 30 TxGNN sites: {{ site.data.evidence_tier_facts.low.cross.median_share }}%. {% if ev_share > site.data.evidence_tier_facts.low.cross.median_share %}This site is above the median.{% elsif ev_share < site.data.evidence_tier_facts.low.cross.median_share %}This site is below the median.{% else %}This site is at the median.{% endif %}

{% if site.data.evidence_tier_facts.low.count > 0 %}
In the local data (Pharmac Schedule listings), **{{ site.data.evidence_tier_facts.low.with_current }}** of these medicines have at least one current entry, **{{ site.data.evidence_tier_facts.low.none_current }}** have entries but none current, and **{{ site.data.evidence_tier_facts.low.without_data }}** are not in the local data. **{{ site.data.evidence_tier_facts.low.with_reviews }}** have entries in the review log (corrections of registration facts on the drug page). **{{ site.data.evidence_tier_facts.low.cross.unique_drugs }}** have a predicted indication that no other TxGNN site lists for the same medicine.
{% if site.data.evidence_tier_facts.low.none_current_list.size > 0 %}

With entries but none current: {% for r in site.data.evidence_tier_facts.low.none_current_list %}[{{ r.t }}]({{ r.u | relative_url }}){% unless forloop.last %}, {% endunless %}{% endfor %}.{% endif %}

Medicines with a current local entry are listed first.
{% endif %}

Definitions of all levels: [Methodology]({{ '/methodology/' | relative_url }}).
<!-- tier-facts:end -->

<p style="font-size: 1.25rem; color: #666; margin-bottom: 1.5rem;">
Candidates with model prediction only and no human evidence yet
</p>

---

## Criteria

| Level | Definition | Clinical meaning |
|-------|------------|------------------|
| **L5** | Model prediction only | Hypothesis stage; no human evidence yet |

---

{% assign l5_drugs = site.drugs | where: "evidence_level", "L5" | where_exp: "d", "d.noindex != true" | sort: "title" %}

### L5 ({{ site.data.evidence_tier_facts.low.by_level.L5 }} drugs)

<!-- tier-table:start L5（母目錄 .claude/gen_local_pages_20261119.py 產生，勿手改）-->

| Medicine | Level | Main predicted new indication | Pharmac Schedule listings: current / total | Review log | Only on this site |
|---|---|---|---|---|---|
{% for r in site.data.evidence_tier_facts.low.rows %}{% if r.l == "L5" %}| [{{ r.t | replace: '|', '/' }}]({{ r.u | relative_url }}) | {{ r.l }} | {{ r.i | replace: '|', '/' }} | {% if r.lt %}{{ r.lv }} / {{ r.lt }}{% else %}—{% endif %} | {{ r.r }}{% if r.rs and r.rs != '' %}: {{ r.rs | replace: '|', '/' }}{% endif %} | {% if r.x and r.x > 0 %}yes ({{ r.x }}){% else %}—{% endif %} |
{% endif %}{% endfor %}

<!-- tier-table:end -->

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>

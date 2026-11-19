---
layout: default
title: Moderate Evidence (L3-L4)
nav_order: 22
permalink: /evidence-medium/
description: "50 medicines at evidence level L3–L4 on this site (New Zealand), e.g. Brentuximab Vedotin, Calcipotriol, Clonazepam, with the main predicted new indication, local Pharmac Schedule listings and review-log entries for each."
---

# Moderate Evidence (L3-L4)

<!-- tier-facts:start（母目錄 .claude/gen_local_pages_20261119.py 產生，勿手改）-->
{% assign ev_share = site.data.evidence_tier_facts.medium.count | times: 100.0 | divided_by: site.data.drug_stats.total_drugs %}
This page lists **{{ site.data.evidence_tier_facts.medium.count }}** of the **{{ site.data.drug_stats.total_drugs }}** medicines with a published report on this site ({{ site.data.evidence_tier_facts.medium.count | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}%): L3 **{{ site.data.evidence_tier_facts.medium.by_level.L3 }}**, L4 **{{ site.data.evidence_tier_facts.medium.by_level.L4 }}**.
Median share at L3–L4 across the 30 TxGNN sites: {{ site.data.evidence_tier_facts.medium.cross.median_share }}%. {% if ev_share > site.data.evidence_tier_facts.medium.cross.median_share %}This site is above the median.{% elsif ev_share < site.data.evidence_tier_facts.medium.cross.median_share %}This site is below the median.{% else %}This site is at the median.{% endif %}

{% if site.data.evidence_tier_facts.medium.count > 0 %}
In the local data (Pharmac Schedule listings), **{{ site.data.evidence_tier_facts.medium.with_current }}** of these medicines have at least one current entry, **{{ site.data.evidence_tier_facts.medium.none_current }}** have entries but none current, and **{{ site.data.evidence_tier_facts.medium.without_data }}** are not in the local data. **{{ site.data.evidence_tier_facts.medium.with_reviews }}** have entries in the review log (corrections of registration facts on the drug page). **{{ site.data.evidence_tier_facts.medium.cross.unique_drugs }}** have a predicted indication that no other TxGNN site lists for the same medicine.
{% if site.data.evidence_tier_facts.medium.none_current_list.size > 0 %}

With entries but none current: {% for r in site.data.evidence_tier_facts.medium.none_current_list %}[{{ r.t }}]({{ r.u | relative_url }}){% unless forloop.last %}, {% endunless %}{% endfor %}.{% endif %}
{% endif %}

Definitions of all levels: [Methodology]({{ '/methodology/' | relative_url }}).
<!-- tier-facts:end -->

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

### L3 ({{ site.data.evidence_tier_facts.medium.by_level.L3 }} drugs)

<!-- tier-table:start L3（母目錄 .claude/gen_local_pages_20261119.py 產生，勿手改）-->

| Medicine | Level | Main predicted new indication | Pharmac Schedule listings: current / total | Review log | Only on this site |
|---|---|---|---|---|---|
{% for r in site.data.evidence_tier_facts.medium.rows %}{% if r.l == "L3" %}| [{{ r.t | replace: '|', '/' }}]({{ r.u | relative_url }}) | {{ r.l }} | {{ r.i | replace: '|', '/' }} | {% if r.lt %}{{ r.lv }} / {{ r.lt }}{% else %}—{% endif %} | {{ r.r }}{% if r.rs and r.rs != '' %}: {{ r.rs | replace: '|', '/' }}{% endif %} | {% if r.x and r.x > 0 %}yes ({{ r.x }}){% else %}—{% endif %} |
{% endif %}{% endfor %}

<!-- tier-table:end -->

### L4 ({{ site.data.evidence_tier_facts.medium.by_level.L4 }} drugs)

<!-- tier-table:start L4（母目錄 .claude/gen_local_pages_20261119.py 產生，勿手改）-->

| Medicine | Level | Main predicted new indication | Pharmac Schedule listings: current / total | Review log | Only on this site |
|---|---|---|---|---|---|
{% for r in site.data.evidence_tier_facts.medium.rows %}{% if r.l == "L4" %}| [{{ r.t | replace: '|', '/' }}]({{ r.u | relative_url }}) | {{ r.l }} | {{ r.i | replace: '|', '/' }} | {% if r.lt %}{{ r.lv }} / {{ r.lt }}{% else %}—{% endif %} | {{ r.r }}{% if r.rs and r.rs != '' %}: {{ r.rs | replace: '|', '/' }}{% endif %} | {% if r.x and r.x > 0 %}yes ({{ r.x }}){% else %}—{% endif %} |
{% endif %}{% endfor %}

<!-- tier-table:end -->

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>

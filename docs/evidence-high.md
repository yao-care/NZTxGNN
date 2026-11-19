---
layout: default
title: High Evidence (L1-L2)
nav_order: 21
permalink: /evidence-high/
description: "34 medicines at evidence level L1–L2 on this site (New Zealand), e.g. Acetylcysteine, Bendamustine, Bupropion, with the main predicted new indication, local Pharmac Schedule listings and review-log entries for each."
---

# High Evidence (L1-L2)

<!-- tier-facts:start（母目錄 .claude/gen_local_pages_20261119.py 產生，勿手改）-->
{% assign ev_share = site.data.evidence_tier_facts.high.count | times: 100.0 | divided_by: site.data.drug_stats.total_drugs %}
This page lists **{{ site.data.evidence_tier_facts.high.count }}** of the **{{ site.data.drug_stats.total_drugs }}** medicines with a published report on this site ({{ site.data.evidence_tier_facts.high.count | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}%): L1 **{{ site.data.evidence_tier_facts.high.by_level.L1 }}**, L2 **{{ site.data.evidence_tier_facts.high.by_level.L2 }}**.
Median share at L1–L2 across the 30 TxGNN sites: {{ site.data.evidence_tier_facts.high.cross.median_share }}%. {% if ev_share > site.data.evidence_tier_facts.high.cross.median_share %}This site is above the median.{% elsif ev_share < site.data.evidence_tier_facts.high.cross.median_share %}This site is below the median.{% else %}This site is at the median.{% endif %}

{% if site.data.evidence_tier_facts.high.count > 0 %}
In the local data (Pharmac Schedule listings), **{{ site.data.evidence_tier_facts.high.with_current }}** of these medicines have at least one current entry, **{{ site.data.evidence_tier_facts.high.none_current }}** have entries but none current, and **{{ site.data.evidence_tier_facts.high.without_data }}** are not in the local data. **{{ site.data.evidence_tier_facts.high.with_reviews }}** have entries in the review log (corrections of registration facts on the drug page). **{{ site.data.evidence_tier_facts.high.cross.unique_drugs }}** have a predicted indication that no other TxGNN site lists for the same medicine.
{% if site.data.evidence_tier_facts.high.none_current_list.size > 0 %}

With entries but none current: {% for r in site.data.evidence_tier_facts.high.none_current_list %}[{{ r.t }}]({{ r.u | relative_url }}){% unless forloop.last %}, {% endunless %}{% endfor %}.{% endif %}
{% endif %}

Definitions of all levels: [Methodology]({{ '/methodology/' | relative_url }}).
<!-- tier-facts:end -->

<p style="font-size: 1.25rem; color: #666; margin-bottom: 1.5rem;">
Candidates that may be prioritised for clinical evaluation
</p>

Which of these drugs have trial evidence for their predicted new indication, and how many registered trials are cited? See [New Zealand medicines with clinical-trial evidence for a new indication]({{ '/clinical-trial-evidence/' | relative_url }}).

---

## Criteria

| Level | Definition | Clinical meaning |
|-------|------------|------------------|
| **L1** | Multiple Phase 3 RCTs / systematic reviews | Strong support; clinical use may be considered |
| **L2** | Single RCT or multiple Phase 2 trials | Moderate support; validation trials can be designed |

---

{% assign l1_drugs = site.drugs | where: "evidence_level", "L1" | where_exp: "d", "d.noindex != true" | sort: "title" %}
{% assign l2_drugs = site.drugs | where: "evidence_level", "L2" | where_exp: "d", "d.noindex != true" | sort: "title" %}

### L1 ({{ site.data.evidence_tier_facts.high.by_level.L1 }} drugs)

<!-- tier-table:start L1（母目錄 .claude/gen_local_pages_20261119.py 產生，勿手改）-->

| Medicine | Level | Main predicted new indication | Pharmac Schedule listings: current / total | Review log | Only on this site |
|---|---|---|---|---|---|
{% for r in site.data.evidence_tier_facts.high.rows %}{% if r.l == "L1" %}| [{{ r.t | replace: '|', '/' }}]({{ r.u | relative_url }}) | {{ r.l }} | {{ r.i | replace: '|', '/' }} | {% if r.lt %}{{ r.lv }} / {{ r.lt }}{% else %}—{% endif %} | {{ r.r }}{% if r.rs and r.rs != '' %}: {{ r.rs | replace: '|', '/' }}{% endif %} | {% if r.x and r.x > 0 %}yes ({{ r.x }}){% else %}—{% endif %} |
{% endif %}{% endfor %}

<!-- tier-table:end -->

### L2 ({{ site.data.evidence_tier_facts.high.by_level.L2 }} drugs)

<!-- tier-table:start L2（母目錄 .claude/gen_local_pages_20261119.py 產生，勿手改）-->

| Medicine | Level | Main predicted new indication | Pharmac Schedule listings: current / total | Review log | Only on this site |
|---|---|---|---|---|---|
{% for r in site.data.evidence_tier_facts.high.rows %}{% if r.l == "L2" %}| [{{ r.t | replace: '|', '/' }}]({{ r.u | relative_url }}) | {{ r.l }} | {{ r.i | replace: '|', '/' }} | {% if r.lt %}{{ r.lv }} / {{ r.lt }}{% else %}—{% endif %} | {{ r.r }}{% if r.rs and r.rs != '' %}: {{ r.rs | replace: '|', '/' }}{% endif %} | {% if r.x and r.x > 0 %}yes ({{ r.x }}){% else %}—{% endif %} |
{% endif %}{% endfor %}

<!-- tier-table:end -->

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>

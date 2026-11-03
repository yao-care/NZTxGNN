---
layout: default
title: "New Zealand Drugs with Trial Evidence for a New Use"
nav_order: 25
permalink: /clinical-trial-evidence/
description: "New Zealand medicines (Pharmac Schedule) whose TxGNN-predicted new indication has clinical-trial evidence (L1-L2): indication, level, registered trials."
last_modified_at: 2026-11-03
---
{%- comment -%}
2026-11-03：回答 AI 搜尋常問的「本國藥物中，哪些新適應症有臨床試驗證據」。全表由 Liquid 從藥物頁產生，不寫死藥名或數字。
收錄＝公開藥物頁（排除 id_redirect 與 noindex）中 evidence_level 為 L1 或 L2（本站 evidence-high 標準：L1 多個 Phase 3 RCT／系統性回顧，
L2 單一 RCT 或多個 Phase 2 試驗）。預測新適應症與等級註記取自各藥物頁快速總覽表；下列欄名是本站藥物頁實際用到的寫法（建頁時逐頁核對），
新寫法抓不到時顯示「see drug report」、等級仍取 front matter。試驗數＝該藥物頁引用的不重複 NCT 編號數。
{%- endcomment -%}
{%- assign ind_labels = "Predicted New Indication" | split: "|" -%}
{%- assign lev_labels = "Evidence Level" | split: "|" -%}
{%- assign listed = site.drugs | where_exp: "d", "d.layout != 'id_redirect'" | where_exp: "d", "d.noindex != true" -%}
{%- assign l1 = listed | where: "evidence_level", "L1" | sort: "title" -%}
{%- assign l2 = listed | where: "evidence_level", "L2" | sort: "title" -%}
{%- assign trial_drugs = l1 | concat: l2 -%}
{%- assign latest = trial_drugs | sort: "last_modified_at" | last -%}
{%- assign latest_date = latest.last_modified_at | date: "%Y-%m-%d" -%}

# New Zealand medicines with clinical-trial evidence for a new indication

<p class="key-answer" data-question="Which medicines in New Zealand have clinical-trial evidence for a new indication?">
<strong>{{ trial_drugs.size }}</strong> of the {{ listed.size }} medicines with a NZTxGNN report have a TxGNN-predicted new indication graded at evidence level <strong>L1 or L2</strong> ({{ l1.size }} at L1, {{ l2.size }} at L2), meaning clinical-trial evidence (randomised trials, Phase 2–3 trials or systematic reviews) was found for it.
</p>

The medicines come from the [Pharmac Community Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) (a funding schedule, not a licence register: Medsafe licence status is not included); the list reflects the drug reports as last updated on {{ latest_date }}.

<div class="key-takeaway">
Trial evidence for a predicted indication is not proof of efficacy. Every candidate below still <strong>requires clinical validation</strong> and regulatory review before any clinical use.
</div>

---

## Medicines with trial evidence (L1–L2)

<div class="table-wrapper"><table>
<thead>
<tr><th>Drug</th><th>Predicted new indication</th><th>Evidence level</th><th>Registered trials cited (NCT)</th></tr>
</thead>
<tbody>
{%- for drug in trial_drugs -%}
{%- assign indication = "" -%}
{%- for lab in ind_labels -%}{%- if indication == "" -%}{%- assign needle = "<td>" | append: lab | append: "</td>" -%}{%- assign qo = drug.content | split: needle -%}{%- if qo.size > 1 -%}{%- assign indication = qo[1] | split: "</td>" | first | split: "<td>" | last | strip -%}{%- endif -%}{%- endif -%}{%- endfor -%}
{%- assign level_cell = "" -%}
{%- for lab in lev_labels -%}{%- if level_cell == "" -%}{%- assign needle = "<td>" | append: lab | append: "</td>" -%}{%- assign lv = drug.content | split: needle -%}{%- if lv.size > 1 -%}{%- assign level_cell = lv[1] | split: "</td>" | first | split: "<td>" | last | strip -%}{%- endif -%}{%- endif -%}{%- endfor -%}
{%- assign parts = drug.content | split: "NCT" -%}
{%- capture nct_ids -%}{%- for p in parts offset: 1 -%}{%- assign id = p | slice: 0, 8 -%}{%- assign rest = id | remove: "0" | remove: "1" | remove: "2" | remove: "3" | remove: "4" | remove: "5" | remove: "6" | remove: "7" | remove: "8" | remove: "9" -%}{%- if id.size == 8 and rest == "" -%}{{ id }},{%- endif -%}{%- endfor -%}{%- endcapture -%}
{%- assign nct_count = nct_ids | split: "," | uniq | size %}
<tr><td><a href="{{ drug.url | relative_url }}">{{ drug.title }}</a></td><td>{% if indication != "" %}{{ indication }}{% else %}see drug report{% endif %}</td><td>{{ drug.evidence_level }}{% if level_cell != "" and level_cell != drug.evidence_level %} <a href="{{ drug.url | relative_url }}" title="{{ level_cell | strip_html | escape }}">(with caveat)</a>{% endif %}</td><td>{{ nct_count }}</td></tr>
{%- endfor %}
</tbody>
</table></div>

## How to read this list

- **Evidence level** follows the site's [criteria]({{ '/evidence-high/' | relative_url }}): L1 = Multiple Phase 3 RCTs / systematic reviews; L2 = Single RCT or multiple Phase 2 trials. "(with caveat)" means the drug report qualifies the grade; read the note in the report before relying on it.
- **Predicted new indication** is the main prediction shown in each drug report. Indications and contraindications already recorded in the TxGNN knowledge graph are excluded, but some predictions may still overlap an established use; the report explains this where it applies.
- **Registered trials cited** counts the ClinicalTrials.gov (NCT) numbers cited in the drug report. Reports cite a selection of the trials found; 0 means the report cites no registered trial, and its literature evidence section shows the basis for the grade.
- For all levels and the full method, see [High Evidence (L1-L2)]({{ '/evidence-high/' | relative_url }}), [All Drugs]({{ '/drugs/' | relative_url }}) and the [methodology]({{ '/methodology/' | relative_url }}).

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>

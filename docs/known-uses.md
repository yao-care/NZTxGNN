---
layout: default
title: "Known Uses Only (No New Predictions)"
nav_order: 24
permalink: /known-uses/
description: "For these drugs, every candidate from the TxGNN model is already a known use (recorded in the knowledge graph or in a medicine licence), so there is no new…"
---

# Known Uses Only (No New Predictions)

For these drugs, every candidate from the TxGNN model is already a known use (recorded in the knowledge graph or in a medicine licence), so there is no new predicted indication. Each page lists the known uses and the local market status.

{% assign known_drugs = site.drugs | where: "evidence_level", "N/A" | sort: "title" %}

**{{ known_drugs.size }} drugs**

| Drug | Report |
|------|------|
{% for drug in known_drugs %}| **{{ drug.title }}** | [View page]({{ drug.url | relative_url }}) |
{% endfor %}

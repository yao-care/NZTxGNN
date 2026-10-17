---
layout: default
title: Colchicine
parent: Model Prediction Only (L5)
nav_order: 117
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-15
description: "Colchicine: predicted new indication Plasmodium falciparum malaria (evidence level L5). Approved use: Gout"
monitor_inn: "COLCHICINE"
predicted_indications: ["Plasmodium falciparum malaria", "dermatofibrosarcoma protuberans"]
---

# Colchicine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Colchicine: From Gout to Plasmodium falciparum Malaria

## One-Sentence Summary

Colchicine is an anti-inflammatory drug originally used for gout and familial Mediterranean fever (FMF).
The TxGNN model predicts it may be effective for **Plasmodium falciparum malaria**, but **no clinical trials** and **only 2 indirect laboratory publications** support this, and neither studies colchicine itself.
This is a computational prediction only and should be treated as a hypothesis.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Gout; familial Mediterranean fever (the New Zealand licence record has no indication text) |
| Predicted New Indication | Plasmodium falciparum malaria |
| TxGNN Prediction Score | 99.60% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorization) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. From general pharmacology, colchicine binds tubulin and disrupts microtubule polymerization, which reduces neutrophil chemotaxis and dampens inflammasome activation. This is the basis of its use in gout and FMF.

*P. falciparum* relies on microtubules for mitotic spindle formation and cell division, so a tubulin-targeting mechanism is biologically plausible. However, the link is weak. Gout and FMF are sterile inflammatory conditions, whereas malaria is a parasitic infection, so the original and new indications share little biology beyond the microtubule target. Tubulin is also highly conserved between parasite and host. Together with colchicine's narrow therapeutic index, this makes selective antiparasitic activity doubtful.

The two retrieved papers do not test colchicine. One studies curcumin's effects on parasite microtubules, and the other studies *pfmdr1* expression in mammalian cells in the context of chloroquine susceptibility. The high score (0.996) reflects the knowledge-graph model only.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23505424](https://pubmed.ncbi.nlm.nih.gov/23505424/) | 2013 | In vitro / preclinical | PLoS One | Curcumin (not colchicine) disrupts *P. falciparum* microtubules, supporting microtubules as a parasite target |
| [7511206](https://pubmed.ncbi.nlm.nih.gov/7511206/) | 1994 | In vitro / preclinical | Mol Cell Biol | *pfmdr1* expression in mammalian cells increases chloroquine susceptibility; no colchicine antimalarial testing |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2200503 | Colgout | Tab 500 mcg | Not recorded (status: cancelled) |

---

## Safety Considerations

- **Contraindications flagged in the drug–disease knowledge graph** (not taken from label text): liver failure, renal impairment, haematological disease (including thrombocytopenia, leukopenia, agranulocytosis, neutropenia and its congenital forms, aplastic anaemia), peptic ulcer disease, diarrhoeal disease, myopathy and other muscle disease (including rhabdomyolysis and familial periodic paralysis), and acute urate nephropathy.
- **Drug Interactions** (100 interactions found; examples from the first 20 shown):
  - **Major:** aprepitant, clarithromycin, eliglustat, rolapitant, rosuvastatin, simvastatin, cobicistat, deferiprone
  - **Moderate:** metronidazole, cimetidine, eluxadoline, miconazole, tinidazole, deferasirox, strontium chloride Sr-89, fostamatinib, ticagrelor, chloroquine, benznidazole, disulfiram
- Chloroquine is a common antimalarial, so its moderate interaction with colchicine is directly relevant to this predicted indication.

The knowledge-graph contraindications and the interaction list are not a substitute for the package insert, which was not available for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score and a plausible but nonspecific tubulin-targeting mechanism. There are no clinical trials and no studies testing colchicine against malaria. Colchicine's narrow therapeutic index and host toxicity make selective antiparasitic activity doubtful. It also has no current valid authorization in New Zealand.

**To proceed, the following is needed:**
- In vitro testing of colchicine against *P. falciparum* (including chloroquine-resistant strains) with a selectivity index relative to host cells
- Detailed mechanism of action data
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Review of the chloroquine interaction and other antimalarial combinations
- Clarification of the regulatory status of the cancelled licence

---

*Other predictions in the data (lower priority):*
- **Familial Mediterranean fever, autosomal dominant** (score 99.38%, L4): largely overlaps with an existing indication, as colchicine is already first-line for FMF. The literature is mainly reviews, with no trial data for this subtype.
- **Dermatofibrosarcoma protuberans** (score 99.37%, L5): no trials or literature. The established therapy is surgery and imatinib, so colchicine is an unlikely candidate.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


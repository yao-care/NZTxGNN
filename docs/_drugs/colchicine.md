---
layout: default
title: Colchicine
parent: Model Prediction Only (L5)
nav_order: 117
evidence_level: L5
indication_count: 2
monitor_inn: "COLCHICINE"
predicted_indications: ["Plasmodium falciparum malaria", "dermatofibrosarcoma protuberans"]
last_modified_at: 2026-10-28
description: "Colchicine: predicted new indication Plasmodium falciparum malaria (evidence level L5). Approved use: Not recorded in the local data"
---

# Colchicine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Colchicine: Tubulin-binding anti-inflammatory agent → Plasmodium falciparum malaria
{: .text-alpha }

## One-Sentence Summary

Colchicine is a tubulin-binding agent that disrupts microtubule polymerization.
The TxGNN model predicts it may be relevant for **Plasmodium falciparum malaria**,
but no clinical trials and no publications that directly study colchicine in this disease are currently available, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Plasmodium falciparum malaria |
| TxGNN Prediction Score | 0.9960 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not formally recorded in the data compiled for this report. From general pharmacology, colchicine binds tubulin and disrupts microtubule polymerization. *Plasmodium falciparum* relies on microtubules for mitotic spindle formation and cell division, so a tubulin-targeting mechanism is biologically plausible.

However, this link is only theoretical. The two retrieved papers do not test colchicine as an antimalarial: one studies curcumin's effects on parasite microtubules, and the other studies expression of the parasite *pfmdr1* gene in mammalian cells in the context of chloroquine susceptibility. Because tubulin is highly conserved, colchicine's narrow therapeutic index and host toxicity make selective antiparasitic activity doubtful. The high score is a computational prediction only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23505424](https://pubmed.ncbi.nlm.nih.gov/23505424/) | 2013 | In vitro / preclinical | PloS one | Curcumin (not colchicine) disrupts microtubules in *P. falciparum* |
| [7511206](https://pubmed.ncbi.nlm.nih.gov/7511206/) | 1994 | In vitro / preclinical | Molecular and cellular biology | Expression of parasite *pfmdr1* in mammalian cells increased chloroquine susceptibility; colchicine not tested as an antimalarial |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2200503 | Colgout | Tab 500 mcg | Not recorded in the local data |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Antimitotic tubulin binder; not a conventional anticancer agent |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert; blood counts, liver and renal function are generally relevant |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Drug Interactions**: 100 interactions were found in the interaction database. Major-level examples include aprepitant, clarithromycin, eliglustat, rolapitant, rosuvastatin, simvastatin, cobicistat and deferiprone. Moderate-level examples include metronidazole, cimetidine, eluxadoline, miconazole, tinidazole, deferasirox, fostamatinib, ticagrelor, chloroquine, benznidazole and disulfiram.

For other safety information (warnings and contraindications), please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has a very high model score but no direct clinical or laboratory study of colchicine in *P. falciparum* malaria. Colchicine's narrow therapeutic index and host toxicity further limit confidence, so the evidence level is L5.

**To proceed, the following is needed:**
- In vitro testing of colchicine against *P. falciparum*, including selectivity versus human cells
- Mechanism of action data for the drug
- Package insert warnings and contraindications from Medsafe
- A review of interaction risks, particularly with antimalarials such as chloroquine
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


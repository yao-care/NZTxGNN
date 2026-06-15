---
layout: default
title: Colchicine
parent: 僅模型預測 (L5)
nav_order: 86
evidence_level: L5
indication_count: 3
---

# Colchicine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Colchicine: From Gout to Plasmodium falciparum Malaria

## One-Sentence Summary

Colchicine is an ancient alkaloid derived from *Colchicum autumnale*, long established as the first-line treatment for acute gout and familial Mediterranean fever (FMF) through its microtubule-disrupting and anti-inflammatory properties.
The TxGNN model predicts it may also be effective for **Plasmodium falciparum malaria** — the most lethal form of human malaria — with **0 clinical trials** and **6 preclinical publications** currently identified, all of which are Tier 3 in vitro mechanistic studies.
Overall, evidence for this specific repurposing direction remains at the preclinical hypothesis stage (Evidence Level L4), and the current recommendation is **Hold** pending direct experimental validation.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Gout (acute flares); Familial Mediterranean Fever — internationally approved, but **not registered in Taiwan** |
| Predicted New Indication | Plasmodium falciparum malaria |
| TxGNN Prediction Score | 99.60% |
| Evidence Level | L4 |
| Taiwan Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Colchicine binds with high affinity to the **colchicine-binding site** on α/β-tubulin heterodimers, preventing GTP-dependent polymerization and collapsing the microtubule network. In human physiology, this disrupts neutrophil chemotaxis, inhibits NLRP3/Pyrin inflammasome assembly (which depends on cytoskeletal integrity), and reduces secretion of the pro-inflammatory cytokines IL-1β and IL-18. These mechanisms underpin its established efficacy in gout and FMF.

*Plasmodium falciparum* maintains its own α/β-tubulin system, independent from the host cell's cytoskeleton. The parasite depends on microtubules for **schizogony** (nuclear division within red blood cells) and for spindle apparatus formation during gametocyte development. In principle, a compound that disrupts plasmodial microtubule assembly could arrest parasite replication. Supporting this class-level rationale, in vitro studies on structurally related tubulin-binding compounds — including **Colcemid** (a colchicine analogue) and **tubulozole** isomers — have demonstrated activity against *P. falciparum*, providing indirect mechanistic plausibility.

However, the central challenge is **selectivity**: plasmodial tubulins share high structural similarity with human tubulins, and no published data demonstrates that colchicine itself can kill *P. falciparum* at sub-toxic human doses. None of the 6 identified publications directly test colchicine against the parasite — they describe structurally analogous compounds or general cytoskeletal biology in the malaria context. The TxGNN prediction likely reflects microtubule-targeting class similarity within the knowledge graph, rather than direct empirical evidence for colchicine itself.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for Colchicine in Plasmodium falciparum malaria.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [2655935](https://pubmed.ncbi.nlm.nih.gov/2655935/) | 1989 | In vitro pharmacology | Cell Biology International Reports | Nine tubulin-binding compounds tested against *P. falciparum* in vitro; plasmodial tubulins appear molecularly distinct from mammalian proteins; Tubulozole-T (inactive in mammals) shows antimalarial promise as indirect evidence for the drug class |
| [2670249](https://pubmed.ncbi.nlm.nih.gov/2670249/) | 1989 | In vitro pharmacology | Cell Biology International Reports | Confirms tubulin-binding compounds have in vitro antimalarial activity; cytochalasin B (actin-binding) also tested, establishing that cytoskeletal disruption broadly affects parasite viability |
| [2221861](https://pubmed.ncbi.nlm.nih.gov/2221861/) | 1990 | In vitro mechanistic | Antimicrobial Agents and Chemotherapy | Tubulozole isomers inhibit *P. falciparum* protein biosynthesis; **Colcemid** (a direct colchicine analogue) produces similar effects, suggesting the colchicine-binding site on plasmodial tubulin is a viable target |
| [23505424](https://pubmed.ncbi.nlm.nih.gov/23505424/) | 2013 | In vitro mechanistic | PLoS ONE | Curcumin disrupts *P. falciparum* microtubule structure; demonstrates that diverse microtubule-targeting agents can impair parasite viability, supporting the broader mechanistic hypothesis |
| [6362934](https://pubmed.ncbi.nlm.nih.gov/6362934/) | 1984 | Serological study | Clinical and Experimental Immunology | 82% of acute malaria patients show IgM antibodies to cytoskeletal intermediate filaments; highlights cytoskeletal involvement in malaria immunopathology, providing biological context |
| [7511206](https://pubmed.ncbi.nlm.nih.gov/7511206/) | 1994 | Molecular biology | Molecular and Cellular Biology | pfmdr1 (P-glycoprotein homologue) expression linked to chloroquine resistance; contextualizes how drug efflux mechanisms in *P. falciparum* may also limit microtubule-targeting agents |

---

## Taiwan Market Information

Colchicine currently has **no registered authorizations** in Taiwan (市場狀態：未上市). No product listings, dosage forms, or approved indications are available from the Taiwan TFDA database.

> Colchicine is approved in other jurisdictions — including the United States (**Colcrys®**, **Mitigare®**) for acute gout and familial Mediterranean fever — but these approvals do not apply to the Taiwan market.

---

## Safety Considerations

- **Narrow Therapeutic Index**: Colchicine has no clear dose boundary distinguishing non-toxic, toxic, and lethal exposure. Unintentional poisoning is common and associated with poor clinical outcomes (PMID [20586571](https://pubmed.ncbi.nlm.nih.gov/20586571/)). This is a critical constraint when evaluating any new indication, particularly one (malaria) that may require sustained dosing at concentrations approaching the toxic threshold.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
All 6 supporting publications are Tier 3 in vitro mechanistic studies involving related compounds rather than colchicine itself; no clinical trial data exists for this indication. Colchicine's narrow therapeutic index, combined with the structural similarity between plasmodial and human tubulins, creates a fundamental selectivity barrier that current evidence does not address.

**To proceed, the following is needed:**
- Direct in vitro testing of colchicine (not structural analogues) against *P. falciparum* at clinically achievable plasma concentrations
- Determination of the **selectivity index** (IC₅₀ parasite / IC₅₀ mammalian cytotoxicity); a value >10 would be the minimum threshold for further development
- Structural or biophysical studies confirming differential binding affinity to plasmodial vs. human α/β-tubulin
- In vivo proof-of-concept in an animal malaria model (e.g., *Plasmodium berghei* murine model)
- Full safety profiling and pharmacokinetic assessment at antimalarial-relevant doses
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


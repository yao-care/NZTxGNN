---
layout: default
title: Pirfenidone
parent: Model Prediction Only (L5)
nav_order: 369
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Pirfenidone: predicted new indication Extracutaneous mastocytoma (evidence level L5). Approved use: Idiopathic pulmonary fibrosis, idiopathic interstitial pneumonia…"
monitor_inn: "PIRFENIDONE"
predicted_indications: ["extracutaneous mastocytoma", "dermatofibrosarcoma protuberans", "aggressive systemic mastocytosis", "heart fibrosarcoma", "conventional fibrosarcoma", "autosomal recessive familial Mediterranean fever", "kidney fibrosarcoma", "hepatic infarction", "fibroblastic neoplasm", "low grade fibromyxoid sarcoma"]
---

# Pirfenidone
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Pirfenidone: From Idiopathic Pulmonary Fibrosis to Extracutaneous Mastocytoma

## One-Sentence Summary

Pirfenidone was originally used to treat idiopathic pulmonary fibrosis and related interstitial pneumonias.
The TxGNN model predicts it may be effective for **extracutaneous mastocytoma**, but this is a **model prediction only**, with **0 clinical trials** and **0 publications** supporting this specific indication.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Idiopathic pulmonary fibrosis, idiopathic interstitial pneumonia, acute interstitial pneumonia (from the knowledge graph; no approved indication text available for the New Zealand licences) |
| Predicted New Indication | Extracutaneous mastocytoma |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, pirfenidone is an anti-fibrotic drug whose efficacy in idiopathic pulmonary fibrosis is established. It is reported to suppress TGF-β signaling, which is the main basis for its anti-fibrotic action.

For extracutaneous mastocytoma, the mechanistic link is weak. Mast cell neoplasia is driven mainly by KIT signaling, and pirfenidone is not known to target KIT. The high score probably reflects patterns in the knowledge graph rather than a demonstrated biological connection. No trials or publications support this prediction.

The other top-ranked predictions are also unsupported by clinical data. Those with at least a plausible mechanism include:
- **Dermatofibrosarcoma protuberans** (score 99.41%): this tumour is driven by the COL1A1-PDGFB fusion, and pirfenidone has reported effects on PDGF and TGF-β signaling. No direct evidence was provided, and standard therapy is surgery or imatinib.
- **Fibroblastic neoplasm** (score 99.23%): this is the only prediction with any literature. That literature is indirect and includes possible harm signals (see below).

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature was found for extracutaneous mastocytoma. The only predicted indication with literature in the compiled data is **fibroblastic neoplasm** (rank 9), shown below for reference.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12907346](https://pubmed.ncbi.nlm.nih.gov/12907346/) | 2003 | Pilot clinical study | Am J Gastroenterol | Pilot project testing pirfenidone, a noncytotoxic oral anti-fibrotic, for desmoid tumours in familial adenomatous polyposis; results not available in the compiled data |
| [27835939](https://pubmed.ncbi.nlm.nih.gov/27835939/) | 2016 | In vitro study | BMC Musculoskelet Disord | Pirfenidone inhibits TGF-β1-driven activity in Dupuytren's disease-derived fibroblasts |
| [30927912](https://pubmed.ncbi.nlm.nih.gov/30927912/) | 2019 | In vitro study | BMC Musculoskelet Disord | Examines pirfenidone's effect on TGF-β1-stimulated non-SMAD signaling in Dupuytren's fibroblasts |
| [35129055](https://pubmed.ncbi.nlm.nih.gov/35129055/) | 2022 | Preclinical/Review | Pharm Dev Technol | Explores pirfenidone as a locally injected anti-fibrotic for Dupuytren's disease |
| [29702057](https://pubmed.ncbi.nlm.nih.gov/29702057/) | 2018 | Case report | Perm J | Undifferentiated pleomorphic sarcoma reported after pirfenidone use (possible safety signal) |
| [32572469](https://pubmed.ncbi.nlm.nih.gov/32572469/) | 2020 | Case report | Rheumatology (Oxford) | Multiple eruptive dermatofibromas aggravated by mycophenolate mofetil and pirfenidone in a patient with systemic sclerosis |

This evidence is preclinical and anecdotal. It does not support efficacy against fibroblastic neoplasms, and the two case reports raise possible harm signals.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2575833 | Esbriet (cancelled) | Tab 801 mg | Not available |
| 2602156 | Esbriet (cancelled) | Tab 267 mg | Not available |

## Safety Considerations

- **Drug Interactions**: 100 interactions were identified in the interaction database. The 20 reviewed here are all rated Moderate. They include:
  - Acid suppressants: omeprazole, esomeprazole, lansoprazole, cimetidine
  - Other agents: bupropion, naltrexone, ticlopidine, disulfiram, deferasirox, eliglustat, ivacaftor, miconazole, obeticholic acid, rolapitant

  Many of these are likely to involve metabolic (CYP) pathways, but the compiled data do not give mechanisms.
- **Case-report signals**: Reports of undifferentiated pleomorphic sarcoma and aggravated dermatofibromas after pirfenidone use (see Literature Evidence) warrant caution if the drug were considered for fibroblast-derived tumours.

Package insert warnings and contraindications were not available in the compiled data. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone (L5), with no clinical trials or literature for extracutaneous mastocytoma and no plausible link to KIT-driven disease. Approved KIT-directed therapies exist. No pirfenidone licence is currently valid in New Zealand.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, which are needed before any safety screening
- Detailed mechanism of action data (for example from DrugBank)
- Preclinical evidence (for example mast cell or KIT-driven models) showing a biological effect in this disease
- Review of the sarcoma and dermatofibroma case reports as a safety signal
- Clarification of the New Zealand licence status, given that both licences are cancelled
- Consideration of other predictions with a more plausible mechanism, such as dermatofibrosarcoma protuberans, though these also lack direct evidence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


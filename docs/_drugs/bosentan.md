---
layout: default
title: Bosentan
parent: 僅模型預測 (L5)
nav_order: 51
evidence_level: L5
indication_count: 9
---

# Bosentan
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
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

# Bosentan: From Pulmonary Arterial Hypertension to Rheumatoid Arthritis

## One-Sentence Summary

Bosentan is a dual endothelin receptor antagonist (ERA) originally developed for pulmonary arterial hypertension (PAH), where it reduces pulmonary vascular resistance by blocking both ETA and ETB receptors.
The TxGNN model predicts it may be effective for **Rheumatoid Arthritis (RA)**, supported by **1 clinical trial** (indirect, for Giant Cell Arteritis) and **16 publications** — primarily preclinical animal studies and reviews.
Current evidence is mechanistically plausible but lacks direct human RCT support; this remains a research hypothesis requiring prospective validation.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Pulmonary Arterial Hypertension (global approval; no New Zealand authorization on record) |
| Predicted New Indication | Rheumatoid Arthritis |
| TxGNN Prediction Score | 99.80% |
| Evidence Level | L3 (Preclinical + Observational) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the Evidence Pack. Based on known pharmacology, Bosentan is a dual ETA/ETB receptor antagonist that blocks the actions of endothelin-1 (ET-1), a potent vasoconstrictor and pro-fibrotic peptide. Its established efficacy in PAH is built on reducing pulmonary vascular remodelling driven by excessive ET-1 signalling.

The mechanistic link to RA is biologically plausible: ET-1 is significantly elevated in the synovial fluid of RA patients, where it activates NF-κB signalling to drive the release of pro-inflammatory cytokines (TNF-α, IL-1β) and sustains synovial hyperplasia. By blocking both ETA and ETB receptors, Bosentan could theoretically interrupt this inflammatory amplification loop within the joint microenvironment.

Animal models provide initial support for this hypothesis. In collagen-induced arthritis (CIA) and zymosan-induced arthritis mouse models, dual ET receptor blockade reduced joint inflammation, neutrophil accumulation, and oedema formation. However, no human clinical trial has directly tested Bosentan in RA patients. The sole registered trial (NCT06957002) targets Giant Cell Arteritis — a related but distinct autoimmune vasculitis — offering indirect mechanistic support rather than direct RA evidence.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|------------|--------------|
| [NCT06957002](https://clinicaltrials.gov/study/NCT06957002) | Phase 2 | Not Yet Recruiting | 40 | Bosentan + glucocorticoids vs. glucocorticoids alone in Giant Cell Arteritis (GCA); primary endpoint is failure-free survival at 12 months. GCA shares ET-1 overactivation with RA but is a distinct vasculitis entity — provides indirect mechanistic support only. Results not yet available. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|--------------|
| [22249931](https://pubmed.ncbi.nlm.nih.gov/22249931/) | 2012 | Animal Study | Inflammation Research | Bosentan (dual ETA/ETB antagonist) ameliorates collagen-induced arthritis in mice; TNF-α shown to upregulate endothelin system genes, establishing the ET-1/TNF-α axis in RA pathology |
| [18515326](https://pubmed.ncbi.nlm.nih.gov/18515326/) | 2008 | Animal Study | Journal of Leukocyte Biology | ET-1 levels elevated in plasma and synovial membrane of RA patients; selective ETA/ETB blockade reduces neutrophil accumulation and oedema in zymosan-induced arthritis model |
| [16766656](https://pubmed.ncbi.nlm.nih.gov/16766656/) | 2006 | Animal Study | PNAS | IL-15-induced mechanical hypernociception in RA inhibited by dual endothelin receptor blockade; identifies ET pathway as downstream mediator of RA-associated pain |
| [19969421](https://pubmed.ncbi.nlm.nih.gov/19969421/) | 2010 | Animal Study | Pain | IL-17 drives articular hypernociception in antigen-induced arthritis; contextualises the cytokine–ET-1 inflammatory network relevant to ET receptor antagonism |
| [20054770](https://pubmed.ncbi.nlm.nih.gov/20054770/) | 2009 | Case Report | Kardiologia Polska | 8.5-year-old with Eisenmenger syndrome treated with Bosentan concurrently diagnosed with juvenile rheumatoid arthritis; clinical improvement on Bosentan noted, though PAH was the primary indication |
| [24268012](https://pubmed.ncbi.nlm.nih.gov/24268012/) | 2014 | Review | Rheumatic Diseases Clinics of North America | PAH associated with CTDs including RA reviewed; ET-1-targeted therapies (including Bosentan) discussed in the context of connective tissue disease complications |
| [19487226](https://pubmed.ncbi.nlm.nih.gov/19487226/) | 2009 | Review | Rheumatology (Oxford) | Vascular disease in SLE, Sjögren's syndrome, and necrotizing vasculitides; ET-1 signalling central to CTD-associated PAH — mechanistic context for ERA use in rheumatic disease |
| [16218473](https://pubmed.ncbi.nlm.nih.gov/16218473/) | 2005 | Review | Lupus | PAH in CTDs including RA, dermatomyositis, Sjögren's; establishes prevalence of ET-1-driven vascular complications across rheumatic diseases |
| [19851110](https://pubmed.ncbi.nlm.nih.gov/19851110/) | 2010 | Review | Current Opinion in Rheumatology | Reviews pathophysiology and therapies of rheumatic skin diseases; skin used as primary endpoint for evaluating therapies — contextual background for ET-1 involvement |
| [18238768](https://pubmed.ncbi.nlm.nih.gov/18238768/) | 2008 | Review | AJHP | Current and emerging drug therapy for systemic sclerosis complications; Bosentan discussed as an ET receptor antagonist with potential disease-modifying applications across CTDs |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
While the ET-1/ETA/ETB mechanistic link to RA is biologically coherent and supported by two independent animal arthritis models, there is no direct human clinical trial evidence for Bosentan in RA. The single registered trial targets a related but different condition (GCA), and all literature consists of preclinical studies, reviews, and a single tangentially relevant case report — placing this at evidence level L3 with no clinical efficacy data to draw from.

**To proceed, the following is needed:**

- Retrieve Bosentan's full mechanism of action data (DrugBank API) and package insert warnings/contraindications (TFDA/Medsafe PDF) to complete the safety assessment
- Conduct a pilot open-label clinical study or at minimum a retrospective cohort analysis in RA patients who have received Bosentan for comorbid PAH or digital ulcers, to collect human efficacy signals
- Establish a clear disease differentiation rationale: given Bosentan already has strong evidence in limited systemic sclerosis (L1, RAPIDS-1 and RAPIDS-2 Phase 3 RCTs), prioritising SSc-related RA overlap patients may offer a more feasible clinical pathway
- Clarify drug interaction profile — Bosentan is a known CYP3A4/CYP2C9 inducer with significant interaction potential (warfarin, cyclosporin, glibenclamide), which is especially relevant if combined with standard RA biologics or DMARDs
- Define target patient subpopulation: RA patients with elevated synovial ET-1, concurrent Raynaud's phenomenon, or CTD overlap may be the most suitable candidates for a proof-of-concept study
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


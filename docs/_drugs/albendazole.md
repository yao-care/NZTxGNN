---
layout: default
title: Albendazole
parent: 僅模型預測 (L5)
nav_order: 19
evidence_level: L5
indication_count: 3
---

# Albendazole
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

# Albendazole: From Broad-Spectrum Anthelmintic to Alveolar Echinococcosis

## One-Sentence Summary

Albendazole is a broad-spectrum benzimidazole anthelmintic used globally to treat various parasitic worm infections. The TxGNN model predicts it may be effective for **Alveolar Echinococcosis** with a prediction score of **99.97%**, backed by **1 completed Phase 2 clinical trial** (n=194) directly targeting this indication and **20 publications** including expert consensus guidelines and systematic reviews.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Broad-spectrum anthelmintic (helminthic infections; no New Zealand authorizations on record) |
| Predicted New Indication | Alveolar Echinococcosis |
| TxGNN Prediction Score | 99.97% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Albendazole belongs to the benzimidazole class and works by selectively binding to parasite β-tubulin with much higher affinity than mammalian tubulin. This inhibits microtubule polymerization, disrupting the structural integrity of the parasite's cells, blocking intestinal glucose uptake, and arresting cell division. Its active metabolite, Albendazole Sulfoxide, is capable of penetrating *Echinococcus* cyst walls and reaching effective intracystic concentrations — a key pharmacokinetic requirement for treating alveolar echinococcosis (AE).

Alveolar echinococcosis is caused by the larval stage of *Echinococcus multilocularis*, a cestode (tapeworm) whose entire cellular architecture depends on β-tubulin. The mechanism that albendazole exploits is therefore not merely analogous to its anthelmintic activity — it is the same pathway at work. Albendazole exerts both direct parasitostatic activity against *E. multilocularis* protoscoleces and suppresses vesicular cyst growth, slowing disease progression in patients who cannot undergo surgery.

The clinical translation of this mechanism is well-documented: WHO expert consensus guidelines, multiple systematic reviews, and a completed Phase 2 trial in Kyrgyzstan (NCT07182305, n=194) all confirm albendazole as the only licensed antiparasitic agent for AE. In that sense, the TxGNN prediction is not so much a novel hypothesis as a validation of established but potentially under-utilized clinical practice in markets — like New Zealand — where the drug is not yet registered.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT07182305](https://clinicaltrials.gov/study/NCT07182305) | Phase 2 | Completed | 194 | Direct treatment of early-stage AE with albendazole in a high-prevalence area (Kyrgyzstan); albendazole demonstrated parasitostatic activity slowing disease progression |
| [NCT02876146](https://clinicaltrials.gov/study/NCT02876146) | N/A | Completed | 50 | EchinoVISTA prospective study: defined biological and imaging markers of parasite viability and optimal timing for albendazole withdrawal in hepatic AE |
| [NCT06483880](https://clinicaltrials.gov/study/NCT06483880) | N/A | Unknown | 24 | RCT of adjuvant albendazole after pulmonary hydatid cyst resection vs. placebo; evaluates recurrence reduction at 6-month follow-up |
| [NCT05824442](https://clinicaltrials.gov/study/NCT05824442) | N/A | Recruiting | 43 | Multiplex qPCR diagnostic evaluation for echinococcosis; albendazole is the standard treatment backbone in this patient population |
| [NCT07176598](https://clinicaltrials.gov/study/NCT07176598) | N/A | Completed | 1 | Case report of misdiagnosed primary intramuscular hydatid cyst; albendazole included in post-surgical management |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|------|---------|
| [19931502](https://pubmed.ncbi.nlm.nih.gov/19931502/) | 2010 | Expert Consensus | *Acta Tropica* | WHO-IWGE consensus guidelines on diagnosis, treatment and follow-up of cystic and alveolar echinococcosis; establishes albendazole as the recommended pharmacological treatment |
| [39311470](https://pubmed.ncbi.nlm.nih.gov/39311470/) | 2024 | Systematic Review | *Parasite* (Paris) | Benzimidazoles (albendazole/mebendazole) remain the only compounds recommended for AE; reviews current efficacy evidence and challenges including parasitostatic-only activity and hepatotoxicity |
| [40093668](https://pubmed.ncbi.nlm.nih.gov/40093668/) | 2025 | Clinical Practice Review | *World J Gastroenterol* | Surgical resection combined with long-term albendazole is the standard of care for hepatic echinococcosis; albendazole essential when curative surgery is not feasible |
| [30760475](https://pubmed.ncbi.nlm.nih.gov/30760475/) | 2019 | Review | *Clin Microbiol Rev* | Comprehensive review of 21st-century advances in echinococcosis epidemiology, diagnostics and treatment; albendazole central to all management algorithms |
| [34161992](https://pubmed.ncbi.nlm.nih.gov/34161992/) | 2021 | Clinical Review | *Semin Liver Dis* | Hepatic AE review; prolonged albendazole treatment since the 1990s has transformed prognosis from near-universally fatal to manageable chronic disease |
| [39508157](https://pubmed.ncbi.nlm.nih.gov/39508157/) | 2024 | Drug Repurposing Review | *Parasitology* | Albendazole is the exclusive antiparasitic option for AE and is only parasitostatic; identifies pyronaridine as a candidate add-on therapy through drug repurposing |
| [36974024](https://pubmed.ncbi.nlm.nih.gov/36974024/) | 2022 | Narrative Review | *Chinese J Schistosomiasis Control* | Reviews albendazole progress in AE treatment, including role in patients ineligible for surgery and advances in formulation to improve bioavailability |
| [38501660](https://pubmed.ncbi.nlm.nih.gov/38501660/) | 2024 | Pharmacological Study | *Antimicrob Agents Chemother* | Novel albendazole solubilizing formulations (ABZ-CSD, TABZ-HCl-H) significantly improve oral bioavailability and AE therapeutic outcomes in rat models |
| [34808118](https://pubmed.ncbi.nlm.nih.gov/34808118/) | 2022 | Review | *Acta Tropica* | Albendazole and mebendazole remain the only licensed non-surgical agents for AE and CE; surveys pipeline of experimental alternatives |
| [39254012](https://pubmed.ncbi.nlm.nih.gov/39254012/) | 2024 | Disease Review | *Tidsskrift for Den Norske Laegeforening* | AE clinical overview including Norwegian import cases; prolonged albendazole is standard adjunct to surgery and sole option for inoperable patients |

---

## New Zealand Market Information

Albendazole is **not currently registered with Medsafe** in New Zealand. There are no active product authorizations on record. Any use in New Zealand would require a named-patient supply or Section 29 unapproved medicine pathway.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Albendazole is already the globally recognized first-line pharmacological agent for alveolar echinococcosis, with a completed Phase 2 trial (n=194), WHO consensus guidelines, and multiple systematic reviews all endorsing its use. The gap is not clinical evidence but rather New Zealand regulatory standing and the absence of retrievable local safety documentation.

**To proceed, the following is needed:**
- Confirm a lawful access pathway in New Zealand (Medsafe registration or Section 29 unapproved medicine approval)
- Retrieve full package insert warnings and contraindications (FDA/EMA/TGA label review as proxy for missing local label data)
- Obtain DrugBank MOA and toxicity records to complete the formal evidence dossier
- Establish a safety monitoring plan covering liver function tests (LFTs), CBC with differential, and renal function — albendazole's main risks with long-term use are hepatotoxicity and myelosuppression
- Define a treatment duration protocol (AE typically requires months to years of continuous or cyclical albendazole therapy) and specify stopping/withdrawal criteria informed by imaging and serological markers (as studied in NCT02876146)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


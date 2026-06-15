---
layout: default
title: Efavirenz
parent: 僅模型預測 (L5)
nav_order: 130
evidence_level: L5
indication_count: 3
---

# Efavirenz
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

The `txgnn-pipeline` skill covers pipeline infrastructure — the current task is report generation, which is fully defined by the system prompt. Proceeding directly.

---

# Efavirenz: From HIV-1 Infection to Simian Immunodeficiency Virus Infection

## One-Sentence Summary

Efavirenz (EFV) is a non-nucleoside reverse transcriptase inhibitor (NNRTI) approved globally for HIV-1 infection, though not currently marketed in New Zealand.
The TxGNN model predicts it may be effective for **Simian Immunodeficiency Virus (SIV) Infection**, with **no directly relevant clinical trials** and **16 publications** — primarily non-human primate animal model studies — currently supporting this direction.
The mechanistic basis relies on RT-SHIV, a chimeric research virus carrying HIV-1 reverse transcriptase, rather than natural SIV, which substantially limits clinical translation potential.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | HIV-1 infection (globally approved; not registered in New Zealand) |
| Predicted New Indication | Simian Immunodeficiency Virus (SIV) Infection |
| TxGNN Prediction Score | 99.80% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the database. Based on known pharmacological information, Efavirenz is a first-generation NNRTI that binds the HIV-1 reverse transcriptase (RT) enzyme at an allosteric site known as the NNRTI binding pocket (NNIBP), blocking viral DNA synthesis. This mechanism is structurally specific to HIV-1 RT and does not generically apply to all retroviral reverse transcriptases.

The biological rationale behind the TxGNN prediction centres on **RT-SHIV** — a chimeric research virus in which the RT-encoding region of SIVmac239 is replaced with HIV-1 RT. Because RT-SHIV carries HIV-1 RT, it is directly susceptible to NNRTIs including efavirenz. Multiple non-human primate studies in rhesus and pigtail macaques have confirmed that EFV-based HAART regimens effectively suppress RT-SHIV viral loads, making it a well-validated animal model for studying HIV treatment strategies, drug resistance evolution, and viral reservoir dynamics.

However, a critical caveat must be recognised: **natural SIV reverse transcriptase** is structurally distinct from HIV-1 RT and lacks the NNIBP cavity that NNRTIs require for binding. Efavirenz does not effectively inhibit wild-type SIV RT (supported by PMID 15040537). The TxGNN model appears to have captured the strong co-occurrence between efavirenz and SIV-related literature without distinguishing the engineered RT-SHIV chimera from natural SIV. This prediction therefore reflects a research-tool association rather than a genuine cross-species therapeutic opportunity.

---

## Clinical Trial Evidence

No directly relevant clinical trials were identified for efavirenz in simian immunodeficiency virus infection. The one registered trial retrieved during evidence collection is not applicable to this evaluation:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|------------|--------------|
| [NCT00863668](https://clinicaltrials.gov/study/NCT00863668) | N/A | Withdrawn | 0 | Study of HIV decay kinetics using **raltegravir** (integrase inhibitor); efavirenz was not the test drug, the disease was HIV not SIV, and the trial was withdrawn before any enrollment. Not relevant to this evaluation. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [15328115](https://pubmed.ncbi.nlm.nih.gov/15328115/) | 2004 | Animal study (NHP in vivo) | Antimicrob Agents Chemother | EFV monotherapy in rhesus macaques infected with RT-SHIV; demonstrated direct antiviral activity against this HIV-1-RT-containing chimeric virus, establishing the core mechanistic foundation for this prediction |
| [15919889](https://pubmed.ncbi.nlm.nih.gov/15919889/) | 2005 | Animal study (NHP in vivo) | J Virology | EFV + lamivudine + tenofovir HAART in RT-SHIV-infected macaques; plasma viral RNA reduced by multiple log units in all 7 animals, establishing this regimen as a validated NHP model of HIV HAART |
| [24777106](https://pubmed.ncbi.nlm.nih.gov/24777106/) | 2014 | Animal study (pharmacodynamics) | Antimicrob Agents Chemother | Four- and five-drug HAART regimens (including EFV) in RT-SHIV macaques; enhanced combinations improved early viral decay kinetics compared to standard three-drug HAART |
| [19195672](https://pubmed.ncbi.nlm.nih.gov/19195672/) | 2009 | Animal study (transmission model) | Virology | Vaginal transmission of RT-SHIV characterised in Chinese rhesus macaques; viral RNA accumulated in lymph nodes and spleen with plasma viremia persisting up to one year, validating the model for mucosal transmission research |
| [19889213](https://pubmed.ncbi.nlm.nih.gov/19889213/) | 2009 | Animal study (viral dynamics) | Retrovirology | RT-SHIV subpopulation dynamics in pigtail macaques receiving short-course EFV monotherapy followed by combination ART; tracked emergence and fate of drug-resistant variants over time |
| [21084490](https://pubmed.ncbi.nlm.nih.gov/21084490/) | 2011 | Virological study | J Virology | SIV/HIV-1 RT genetic diversity persists in macaques despite ART; EFV monotherapy used before combination ART to characterise resistance evolution patterns |
| [22933296](https://pubmed.ncbi.nlm.nih.gov/22933296/) | 2012 | Virological study (resistance) | J Virology | Ultrasensitive allele-specific PCR detected preexisting EFV-resistance mutations at low frequency in RT-SHIV-infected macaques prior to ART initiation |
| [35856680](https://pubmed.ncbi.nlm.nih.gov/35856680/) | 2022 | Imaging/PK study | Antimicrob Agents Chemother | Mass spectrometry imaging of 6 ARVs (including EFV) in spleens of RT-SHIV-infected NHPs; quantified spatial relationship between drug tissue distribution and viral RNA reservoirs |
| [15040537](https://pubmed.ncbi.nlm.nih.gov/15040537/) | 2004 | In vitro | Antiviral Therapy | Evaluated 17 antiretroviral compounds against HIV-2, SIV, and SHIV strains; **EFV showed no meaningful activity against wild-type SIV**, confirming species-specific RT structural divergence as a barrier |
| [24505452](https://pubmed.ncbi.nlm.nih.gov/24505452/) | 2014 | Animal study | PLoS One | Residual viremia in RT-SHIV HAART model characterised by a predominant plasma clone and absence of viral evolution, suggesting that reservoir — not active replication — drives persistence during EFV-containing HAART |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN prediction is mechanistically coherent for the RT-SHIV chimeric virus research model, with well-replicated non-human primate evidence supporting efavirenz activity. However, this does not represent a clinically actionable repurposing opportunity: natural SIV is intrinsically resistant to efavirenz due to RT structural differences (confirmed in vitro), there is no veterinary therapeutic demand for an NNRTI targeting SIV, and no human or veterinary clinical trials have been registered for this indication. The prediction most likely reflects a bibliometric association rather than a genuine repurposing signal.

**To proceed, the following is needed:**
- Structural biology confirmation of whether any wild-type SIV RT variant retains an NNIBP susceptible to efavirenz
- Clarification of the intended use context — if this is a research tool application (RT-SHIV NHP model studies), the evidence is already sufficient for that purpose and no further development is needed
- If pursuing a veterinary SIV or feline FIV application, dedicated RT binding assays and RT pocket modelling would be required before animal studies
- Full safety profile review from the efavirenz package insert, particularly CNS toxicity (neuropsychiatric adverse effects, dizziness, vivid dreams) and teratogenicity (Pregnancy Category D/X in some jurisdictions), which would be relevant to any in-species use
- Mechanism of action data retrieval from DrugBank (DG002) to formally complete the evidence package
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


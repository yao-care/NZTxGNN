---
layout: default
title: Dolutegravir
parent: 僅模型預測 (L5)
nav_order: 124
evidence_level: L5
indication_count: 3
---

# Dolutegravir
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

The skill confirms this is TxGNN work but covers pipeline management — the report format is governed by my system prompt role definition (v5), which takes priority. Proceeding to generate the report.

---

# Dolutegravir: From HIV-1 Infection to Feline Acquired Immunodeficiency Syndrome

## One-Sentence Summary

Dolutegravir (DTG) is an integrase strand transfer inhibitor (INSTI) widely used in combination antiretroviral therapy for HIV-1 infection in humans.
The TxGNN model predicts it may be effective for **Feline Acquired Immunodeficiency Syndrome (Feline AIDS / FIV infection)**, with **5 clinical trials** (all in human HIV-1, providing indirect mechanistic support only) and **1 publication** directly evaluating DTG pharmacokinetics and outcomes in FIV-infected cats.
Current evidence is insufficient for clinical translation and dedicated feline PK/PD studies are required before proceeding.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | HIV-1 infection (combination antiretroviral therapy) |
| Predicted New Indication | Feline Acquired Immunodeficiency Syndrome |
| TxGNN Prediction Score | 99.85% |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Dolutegravir belongs to the integrase strand transfer inhibitor (INSTI) class of antiretrovirals. Its mechanism centers on binding to the active site of the HIV integrase enzyme and blocking the strand transfer step — the process by which viral DNA is permanently inserted into the host cell genome. Without successful integration, the virus cannot replicate or establish a latent reservoir.

Feline Immunodeficiency Virus (FIV) and HIV are both members of the *Lentivirus* genus within the family *Retroviridae*. Like HIV, FIV encodes its own integrase enzyme, which is essential for the viral replication cycle. This shared biochemical target provides the core mechanistic rationale: if DTG can block HIV integrase, it may theoretically also suppress FIV integrase. The TxGNN knowledge graph likely captured this phylogenetic and enzymatic relationship — along with the observation that FIV causes progressive immune dysfunction in cats analogous to AIDS in humans — when generating this prediction.

However, important caveats temper this rationale. FIV integrase shares only approximately 40–50% amino acid sequence homology with HIV-1 integrase, and the DTG binding pocket architecture may differ enough to reduce potency or alter resistance profiles. Beyond molecular compatibility, pharmacokinetic parameters (oral bioavailability, hepatic metabolism, plasma half-life, renal clearance) in domestic cats differ substantially from those in humans. A 2023 study (PMID 37112803, *Viruses*) directly evaluated a cART regimen including DTG at 2.5 mg/kg in FIV-infected cats, providing the first published in vivo feline data — but outcomes were limited to immunophenotypic characterisation without definitive virological efficacy endpoints. Independent dose-finding, safety, and efficacy studies in feline subjects remain necessary.

---

## Clinical Trial Evidence

> All trials retrieved are in human HIV-1 subjects (Grade C relevance — species mismatch). No veterinary or feline clinical trials were identified. The table below is provided for **mechanistic context only** and cannot be counted as direct evidence for FIV treatment.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|-----------|-------------|
| [NCT01499199](https://clinicaltrials.gov/study/NCT01499199) | Phase 3 | Completed | 13 | DTG 50 mg once daily + ABC/3TC in ART-naïve HIV-1 adults over 96 weeks with matched plasma/CSF PK sampling — demonstrates meaningful CNS penetration of DTG, relevant to lentiviral neurotropism |
| [NCT01263015](https://clinicaltrials.gov/study/NCT01263015) | Phase 3 | Completed | 844 | DTG + ABC/3TC vs. Atripla (EFV/FTC/TDF) in ART-naïve adults over 96 weeks; established non-inferiority of DTG-based regimen and confirmed durable virological suppression |
| [NCT01231516](https://clinicaltrials.gov/study/NCT01231516) | Phase 3 | Completed | 724 | DTG 50 mg once daily vs. raltegravir 400 mg twice daily in ART-experienced, INSTI-naïve adults; confirmed DTG's higher barrier to resistance within the INSTI class |
| [NCT00951015](https://clinicaltrials.gov/study/NCT00951015) | Phase 2 | Completed | 208 | Dose-finding study for DTG in ART-naïve HIV-1 adults; established optimal once-daily human dose — direct extrapolation to cats is not feasible due to species differences in metabolism |
| [NCT01227824](https://clinicaltrials.gov/study/NCT01227824) | Phase 3 | Completed | 828 | DTG vs. raltegravir in ART-naïve HIV-1 adults (SPRING-2 design); confirmed class-wide INSTI efficacy and DTG's superior resistance profile at 48 and 96 weeks |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [37112803](https://pubmed.ncbi.nlm.nih.gov/37112803/) | 2023 | Animal Study | *Viruses* | First published in vivo evaluation of cART (DTG 2.5 mg/kg + tenofovir 20 mg/kg + emtricitabine 40 mg/kg) in FIV-infected specific pathogen-free cats; assessed pharmacokinetics and immunophenotypic changes — provides direct but preliminary evidence for DTG use in feline lentiviral infection |

---

## New Zealand Market Information

Dolutegravir is **not currently registered or marketed in New Zealand**. No Medsafe product authorizations were identified in the regulatory query.

---

## Safety Considerations

- **Neural tube defect risk:** Dolutegravir carries a well-documented signal for neural tube defects (NTDs) in human pregnancies when used periconceptionally. While this directly concerns human use, this teratogenicity signal warrants consideration in veterinary research designs, particularly for breeding animals.
- **Feline-specific safety data:** No published feline toxicology data for dolutegravir are available in the retrieved literature. All safety parameters must be treated as unknown in cats.

For all other safety information, please refer to the package insert.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Although the mechanistic basis for DTG activity against FIV is biologically plausible — both viruses share lentiviral integrase as an essential replication enzyme — the approximately 40–50% sequence divergence between FIV and HIV-1 integrase, the complete absence of feline PK/PD validation, and the existence of only one preliminary animal study (PMID 37112803) without definitive efficacy endpoints collectively make the evidence insufficient to support veterinary clinical application at this stage.

**To proceed, the following is needed:**

- **In vitro susceptibility data:** IC₅₀ / EC₅₀ of DTG against FIV isolates in feline cell lines
- **Feline PK/PD studies:** Oral bioavailability, plasma half-life, and target tissue distribution in healthy cats before infected-animal studies
- **Dose-ranging and safety/toxicology:** Feline-specific dose-finding with haematological and hepatorenal monitoring
- **Controlled efficacy trial:** Randomised, controlled veterinary study in FIV-infected cats with virological (plasma viral load) and immunological (CD4⁺ T-cell count) endpoints
- **Structural biology confirmation:** Molecular docking or crystallography to confirm DTG binding geometry within FIV integrase
- **Regulatory pathway review:** Assessment under the New Zealand Agricultural Compounds and Veterinary Medicines (ACVM) Act for veterinary use authorisation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


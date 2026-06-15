---
layout: default
title: Emtricitabine
parent: 僅模型預測 (L5)
nav_order: 134
evidence_level: L5
indication_count: 3
---

# Emtricitabine
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

# Emtricitabine: From HIV-1 Infection to Feline Acquired Immunodeficiency Syndrome

## One-Sentence Summary

Emtricitabine (FTC) is a nucleoside reverse transcriptase inhibitor (NRTI) established as a cornerstone backbone agent in combination antiretroviral therapy for human HIV-1 infection.
The TxGNN model ranks **Feline Acquired Immunodeficiency Syndrome (Feline AIDS / FIV infection)** as its top predicted new indication with a score of **99.92%**,
currently supported by **1 direct veterinary clinical study** and **4 adjacent human HIV trials** — reflecting mechanistic plausibility rather than mature clinical evidence.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | HIV-1 infection (inferred from clinical context as combination ART backbone; no New Zealand regulatory approval on file) |
| Predicted New Indication | Feline Acquired Immunodeficiency Syndrome (FIV Infection) |
| TxGNN Prediction Score | 99.92% |
| Evidence Level | L4 (veterinary observational study + preclinical mechanistic basis) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold (Research Question) |

---

## Why is This Prediction Reasonable?

Emtricitabine is a cytidine analogue that acts as a nucleoside reverse transcriptase inhibitor (NRTI). It is incorporated into viral DNA during reverse transcription, where it acts as a chain terminator — blocking further elongation of the viral genome. In human medicine, it is universally co-formulated with tenofovir (as Truvada/Descovy) and serves as an anchor component of combination ART for HIV-1. Although formal MOA data was not retrieved in this evidence pack, its antiviral class and mechanism of action are well established in the clinical literature included below.

Feline Immunodeficiency Virus (FIV) belongs to the same Lentivirus genus as HIV, sharing a highly conserved reverse transcriptase structure. Because emtricitabine targets the RT enzyme active site — rather than species-specific viral proteins — its inhibitory action is theoretically transferable across lentiviral species. This structural conservation is the mechanistic foundation of the TxGNN prediction: the model has identified that FIV's RT biology is sufficiently similar to HIV-1's to make FTC a plausible therapeutic candidate.

Critically, a 2023 veterinary pharmacology study (*Viruses*, PMID 37112803) directly evaluated a combination ART regimen — dolutegravir (2.5 mg/kg) + tenofovir (20 mg/kg) + **emtricitabine (40 mg/kg)** — in FIV-infected domestic cats. The study reported measurable improvements in immune cell parameters (CD4+ T-cell immunophenotype), providing the first direct in vivo evidence that FTC-containing cART affects FIV disease biology. This veterinary data substantially raises the biological credibility of the TxGNN prediction, even though the study is small and observational in design.

---

## Clinical Trial Evidence

All four registered clinical trials are **indirect (Grade C)**: emtricitabine appears as a backbone drug in the comparator or background arm of human HIV-1 trials, not as the primary investigational agent for feline AIDS. No clinical trials in cats or veterinary settings were identified on ClinicalTrials.gov.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01263015](https://clinicaltrials.gov/study/NCT01263015) | Phase 3 | Completed | 844 | Dolutegravir + ABC/3TC vs. Efavirenz/Emtricitabine/TDF (Atripla) in ART-naïve HIV-1 adults; emtricitabine appears in comparator arm only — not primary investigational drug |
| [NCT00951015](https://clinicaltrials.gov/study/NCT00951015) | Phase 2 | Completed | 208 | Dose selection for dolutegravir administered with either ABC/3TC or TDF/emtricitabine backbone in ART-naïve HIV-1 subjects |
| [NCT02770508](https://clinicaltrials.gov/study/NCT02770508) | Phase 4 | Completed | 145 | Darunavir/ritonavir + lamivudine vs. darunavir/ritonavir + TDF/emtricitabine in naïve HIV-1 patients; emtricitabine in comparator arm only |
| [NCT01227824](https://clinicaltrials.gov/study/NCT01227824) | Phase 3 | Completed | 828 | Dolutegravir once daily vs. raltegravir twice daily, both with NRTI backbone (ABC/3TC or TDF/FTC), in ART-naïve HIV-1 adults |

> These trials collectively confirm emtricitabine's efficacy and safety as a standard-of-care NRTI in human lentiviral infection, providing indirect mechanistic support. None directly address FIV or feline patients.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|---------|
| [37112803](https://pubmed.ncbi.nlm.nih.gov/37112803/) | 2023 | Veterinary Observational/Experimental | *Viruses* | Combination ART (dolutegravir 2.5 mg/kg + tenofovir 20 mg/kg + emtricitabine 40 mg/kg) administered to FIV-infected domestic cats; study assessed pharmacokinetics and immune outcomes — reported measurable improvement in CD4+ T-cell immunophenotype, establishing proof-of-concept for cART in feline FIV |

> Only one publication directly addresses emtricitabine use in FIV-infected cats. This is the sole direct veterinary evidence base for this repurposing hypothesis.

---

## New Zealand Market Information

Emtricitabine is **not currently registered or marketed in New Zealand**. No pharmaceutical authorizations are on file in the regulatory dataset. Clinicians seeking access would need to consider unapproved medicine access pathways (e.g., Section 29 of the Medicines Act 1981) or veterinary-specific regulatory routes for feline use cases.

---

## Safety Considerations

Formal safety data (key warnings, contraindications, drug interactions) for the New Zealand regulatory jurisdiction was not retrieved in this evidence pack. All safety fields returned as unavailable.

> Please refer to established international prescribing information — such as the FDA label or EMA SmPC for Emtriva® / Descovy® — for comprehensive safety guidance, including renal dose adjustments, lactic acidosis risk, and hepatitis B flare warnings upon discontinuation, before any clinical or veterinary use.

---

## Conclusion and Next Steps

**Decision: Hold (Research Question)**

**Rationale:**
The mechanistic basis for emtricitabine activity against FIV is biologically sound and supported by a directly relevant 2023 veterinary study, but a single small observational study in cats does not constitute sufficient evidence to advance beyond an exploratory research question at this time.

**To proceed, the following is needed:**

- **Controlled veterinary trials**: Prospective, adequately powered trials assessing emtricitabine-containing cART in FIV-infected cats, with pre-specified virological and immunological endpoints
- **Feline pharmacokinetic studies**: Formal PK/PD characterisation in cats to establish optimal dosing (the 2023 paper used 40 mg/kg, far exceeding standard human dosing of 200 mg/day) and confirm target tissue drug exposures
- **MOA and safety gap remediation**: Retrieve TFDA package insert and DrugBank MOA data to resolve data gaps DG001 and DG002, enabling a complete mechanistic and safety assessment
- **Veterinary safety monitoring protocol**: Define haematological monitoring plan (CBC, renal function, liver enzymes) appropriate for long-term antiretroviral use in cats
- **Regulatory pathway scoping**: Assess feasibility of veterinary medicine authorisation (emtricitabine is currently approved only for human use) and determine whether compassionate use or research exemption frameworks apply
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


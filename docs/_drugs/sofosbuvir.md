---
layout: default
title: Sofosbuvir
parent: Moderate Evidence (L3-L4)
nav_order: 427
evidence_level: L3
indication_count: 8
last_modified_at: 2026-10-15
description: "Sofosbuvir: predicted new indication Hepatitis B virus infection (evidence level L3). Approved use: Chronic hepatitis C virus infection"
monitor_inn: "sofosbuvir"
predicted_indications: ["hepatitis B virus infection", "hepatitis E virus infection", "hepatitis, viral, animal", "hepatitis A virus infection", "Omsk hemorrhagic fever", "chronic hepatitis B virus infection", "Kyasanur forest disease", "HIV infectious disease"]
---

# Sofosbuvir
{: .fs-9 }

Evidence Level: **L3** | Predicted Indications: **8** 
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

# Sofosbuvir: From Chronic Hepatitis C to Hepatitis B Virus Infection

## One-Sentence Summary

Sofosbuvir is an HCV NS5B polymerase inhibitor, originally used to treat chronic hepatitis C virus infection (in New Zealand, as part of the ledipasvir/sofosbuvir combination, Harvoni).
The TxGNN model predicts it may be effective for **hepatitis B virus infection**, but the data compiled for this report show **10 related clinical trials** and **20 publications** that mostly concern HCV/HBV co-infection, not anti-HBV efficacy.
The mechanistic basis is weak, and the data point to HBV reactivation risk during HCV therapy rather than an antiviral benefit against HBV.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Chronic hepatitis C virus infection (the New Zealand licence record contains no approved-indication text; this comes from the knowledge graph) |
| Predicted New Indication | Hepatitis B virus infection |
| TxGNN Prediction Score | 99.77% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Cancelled only (no currently valid authorization) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, sofosbuvir is a nucleotide analogue inhibitor of the HCV NS5B RNA-dependent RNA polymerase, and its efficacy in hepatitis C is well established.

Mechanistically, however, the prediction is **not well supported**. HBV is a DNA virus that replicates through a reverse transcriptase, and sofosbuvir does not target that enzyme. The high TxGNN score most likely reflects the shared hepatitis/antiviral context in the knowledge graph rather than a true antiviral mechanism.

The clinical data come mainly from HCV/HBV co-infected patients, where HCV clearance is the intended effect. The main HBV-related findings are HBV reactivation risk and changes in HBV DNA and HBsAg kinetics during DAA therapy. A small Phase 2 pilot of ledipasvir/sofosbuvir in HBV (NCT03312023, n=21) was designed to test whether the HBsAg reductions seen in co-infection could be reproduced, but a small single-arm design cannot establish an anti-HBV effect.

For context, among the other predicted indications **hepatitis E virus infection** has the most credible mechanism, because HEV is an RNA virus with an RNA polymerase. Even there, clinical results are mixed and efficacy is not established.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03312023](https://clinicaltrials.gov/study/NCT03312023) | Phase 2 | Completed | 21 | Open-label ledipasvir/sofosbuvir for 12 weeks in HBV infection; directly relevant population but a small single-arm design |
| [NCT02555943](https://clinicaltrials.gov/study/NCT02555943) | Phase 2/3 | Completed | 23 | DAAs in HCV/HBV co-infection; studies HBV reactivation during anti-HCV treatment |
| [NCT04997564](https://clinicaltrials.gov/study/NCT04997564) | Phase 4 | Unknown | 120 | SOF/VEL with prophylactic TAF in HCV/HBV co-infection; TAF is the HBV-active agent and addresses reactivation |
| [NCT03186313](https://clinicaltrials.gov/study/NCT03186313) | Phase 3 | Completed | 72 | Sofosbuvir/daclatasvir (± ribavirin) in genotype 4 HCV; HBV evidence is indirect |
| [NCT02483156](https://clinicaltrials.gov/study/NCT02483156) | Phase 2/3 | Completed | 80 | Sofosbuvir plus ribavirin in genotype 4 HCV; HBV relevance only through co-infected patients |
| [NCT03261349](https://clinicaltrials.gov/study/NCT03261349) | Phase 2 | Unknown | 21 | Ledipasvir/sofosbuvir in HCV-associated indolent B-cell lymphoma; small pilot |
| [NCT02363517](https://clinicaltrials.gov/study/NCT02363517) | Phase 3 | Unknown | 420 | Ledipasvir/sofosbuvir for HCV in people who inject drugs; HBV is not an endpoint |
| [NCT03549312](https://clinicaltrials.gov/study/NCT03549312) | Phase 4 | Unknown | 25 | HIV/HCV switch study; the HBV-active component is TAF |
| [NCT03579576](https://clinicaltrials.gov/study/NCT03579576) | N/A | Completed | 803 | Simplified HCV treatment model in Myanmar; HBV is incidental |
| [NCT02768961](https://clinicaltrials.gov/study/NCT02768961) | Phase 4 | Completed | 64 | HCV screening and treatment in prisons; not relevant to HBV efficacy |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36045503](https://pubmed.ncbi.nlm.nih.gov/36045503/) | 2023 | Phase 2 open-label trial | J Med Virol | Ledipasvir/sofosbuvir for 12 weeks in HBV mono-infection, testing the HBsAg reduction seen in co-infected patients |
| [42558970](https://pubmed.ncbi.nlm.nih.gov/42558970/) | 2026 | Clinical study | J Clin Transl Hepatol | Safety and efficacy of SOF/VEL with TAF pre-treatment in HBV/HCV co-infection, focused on HBV reactivation |
| [35268497](https://pubmed.ncbi.nlm.nih.gov/35268497/) | 2022 | Cohort | J Clin Med | Different HBV-DNA and HBsAg kinetics in HCV co-infected patients during DAA therapy |
| [33523503](https://pubmed.ncbi.nlm.nih.gov/33523503/) | 2021 | Prospective observational | J Viral Hepat | HBV reactivation in cancer patients receiving DAAs for HCV |
| [26967675](https://pubmed.ncbi.nlm.nih.gov/26967675/) | 2016 | Review | J Clin Virol | HBV reactivation during DAA treatment of HCV as a further clinical challenge |
| [27621502](https://pubmed.ncbi.nlm.nih.gov/27621502/) | 2015 | Adverse-event report | Hosp Pharm | HBV reactivation during simeprevir plus sofosbuvir treatment of HCV |
| [31542053](https://pubmed.ncbi.nlm.nih.gov/31542053/) | 2019 | Case report | J Med Case Rep | HBV reactivation with a surface antigen immune-escape mutant during sofosbuvir/velpatasvir therapy |
| [25027705](https://pubmed.ncbi.nlm.nih.gov/25027705/) | 2014 | Review | Minerva Gastroenterol Dietol | Antivirals for hepatitis B and C and their effects on kidney function |

---

## Safety Considerations

- **Drug Interactions** (8 interactions identified in the DDI database):
  - **Major**: apalutamide, enzalutamide, lorlatinib
  - **Moderate**: fostamatinib, warfarin, dicoumarol, encorafenib
  - **Minor**: cyclosporine
- **HBV reactivation**: Published reports and cohort data show that treating HCV with sofosbuvir-based regimens can reactivate HBV in co-infected patients. HBV screening before therapy, monitoring, and prophylaxis (for example TAF) are clinically important.

For other safety information, please refer to the package insert.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Sofosbuvir has no plausible mechanism against HBV, and the available evidence (L3) consists of co-infection studies in which HCV clearance is the goal and HBV reactivation is the main concern. The high TxGNN score is not backed by evidence of anti-HBV efficacy. Sofosbuvir is also not currently marketed in New Zealand under this licence (cancelled).

**To proceed, the following is needed:**
- Mechanism of action data and package insert warnings and contraindications (not yet available in the data compiled for this report)
- A controlled study of anti-HBV efficacy in HBV mono-infection, with HBsAg and HBV DNA endpoints
- Clarification of the status of the cancelled New Zealand authorization
- If the programme is to be pursued at all, consider redirecting to **hepatitis E virus infection**, where a plausible RNA polymerase mechanism and in vitro and early clinical data exist (the pilot Phase 2 trial, NCT03282474, n=10, did not achieve sustained virologic response)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


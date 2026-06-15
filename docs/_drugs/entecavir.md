---
layout: default
title: Entecavir
parent: 僅模型預測 (L5)
nav_order: 136
evidence_level: L5
indication_count: 10
---

# Entecavir
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
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

# Entecavir: From Chronic Hepatitis B to Chronic Hepatitis C Virus Infection

## One-Sentence Summary

Entecavir is a potent guanosine nucleoside analogue approved globally for the treatment of chronic hepatitis B virus (HBV) infection, where it inhibits HBV DNA polymerase to suppress viral replication.
The TxGNN model predicts it may be effective for **Chronic Hepatitis C Virus Infection** with a score of 99.98%, and evidence searches retrieved **40 clinical trials** and **20 publications** — however, **none of these directly demonstrate Entecavir anti-HCV efficacy**; all evidence pertains to HBV management in co-infection contexts.
The prediction is assessed as a knowledge-graph false positive rather than a genuine repurposing signal.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Chronic Hepatitis B Virus Infection (globally approved; not registered in New Zealand) |
| Predicted New Indication | Chronic Hepatitis C Virus Infection |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Entecavir is a guanosine nucleoside analogue. After intracellular triphosphorylation, it competitively inhibits HBV DNA polymerase and blocks three critical steps of viral replication: polymerase priming, reverse transcription of pregenomic RNA (pgRNA), and DNA-dependent DNA synthesis. It is a globally recognised first-line therapy for chronic HBV with a high genetic barrier to resistance, confirmed by multiple completed Phase 3 RCTs.

The TxGNN model assigns a high prediction score (99.98%) for chronic HCV infection due to the proximity of HBV and HCV disease nodes within the knowledge graph — both viruses cause chronic hepatitis, share transmission routes (blood-borne, sexual), and frequently co-infect the same patient populations. This structural adjacency creates a spurious co-occurrence signal that the model cannot distinguish from a true mechanistic relationship.

**The mechanistic basis for this prediction is absent.** HCV is a positive-strand RNA virus that replicates exclusively via its NS5B RNA-dependent RNA polymerase — a fundamentally different enzyme from HBV's DNA polymerase (reverse transcriptase). Entecavir has no known inhibitory activity against HCV NS5B. Every clinical trial and publication retrieved in this search represents Entecavir managing the HBV component in co-infected patients or being co-administered with direct-acting antivirals (DAAs) for HCV, not acting directly against HCV itself. This is a confirmed knowledge-graph false positive and not a clinically actionable repurposing signal.

> **More Actionable Signal — Rank 2 (Hepatitis B Virus Infection):** Entecavir's own approved indication appears as the second-ranked prediction (TxGNN score 99.85%, Evidence Level L1, Decision: Proceed with Guardrails). This reflects Entecavir's well-established efficacy for HBV infection supported by robust Phase 3 RCT data. For New Zealand, the relevant regulatory question is potential Medsafe registration for HBV — not HCV repurposing.

---

## Clinical Trial Evidence

All trials below were retrieved with the query Entecavir + chronic HCV infection. **None directly assess Entecavir as an anti-HCV agent.** They represent its role in HBV management among patients receiving HCV treatment, or general HBV efficacy trials captured by co-infection search overlap.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02555943](https://clinicaltrials.gov/study/NCT02555943) | Phase 2/3 | Completed | 23 | Prospective study on HBV reactivation incidence during DAA anti-HCV treatment in HCV/HBV co-infected patients; Entecavir used for HBV suppression — no direct HCV efficacy assessment |
| [NCT04405011](https://clinicaltrials.gov/study/NCT04405011) | N/A | Unknown | 60 | Three-arm RCT examining whether prophylactic nucleoside analogue (NUC) prevents HBV reactivation during DAA therapy for chronic HCV; evaluates HBV management, not Entecavir anti-HCV activity |
| [NCT03662568](https://clinicaltrials.gov/study/NCT03662568) | Phase 1 | Completed | 56 | Drug-drug interaction study of Morphothiadine Mesilate/Ritonavir (anti-HCV) with Entecavir or TDF in healthy subjects; safety of co-administration only |
| [NCT00065507](https://clinicaltrials.gov/study/NCT00065507) | Phase 3 | Completed | 195 | Entecavir 1.0 mg vs Adefovir in HBV-infected patients with hepatic decompensation over 96 weeks; HBV-focused, HCV not the study target |
| [NCT01020565](https://clinicaltrials.gov/study/NCT01020565) | Phase 2 | Completed | 60 | Entecavir antiviral activity and safety in Japanese HBV patients (0.1 mg and 0.5 mg for 52 weeks); HBV-focused pharmacokinetic study |
| [NCT00412529](https://clinicaltrials.gov/study/NCT00412529) | Phase 3 | Completed | 44 | Exploratory comparison of Telbivudine vs Entecavir viral kinetics in HBeAg-positive compensated CHB over 12 weeks; HBV kinetics study |
| [NCT00096785](https://clinicaltrials.gov/study/NCT00096785) | Phase 3 | Completed | 69 | Entecavir vs Adefovir early viral load reductions in nucleoside-naive HBV adults; HBV efficacy comparator trial |
| [NCT01270178](https://clinicaltrials.gov/study/NCT01270178) | N/A | Unknown | 420 | Prospective Entecavir trial for chronic HBV in HCC patients post-radiofrequency ablation; evaluates tumour recurrence prevention, not HCV therapy |
| [NCT00371150](https://clinicaltrials.gov/study/NCT00371150) | Phase 4 | Completed | 131 | Observational study of Entecavir antiviral effect in Black/African-American and Hispanic populations with nucleoside-naive chronic HBV; ethnicity-specific efficacy data |
| [NCT01928511](https://clinicaltrials.gov/study/NCT01928511) | Phase 4 | Completed | 254 | Switch or add PEG-interferon in chronic HBV patients on long-term nucleoside therapy (SWAP Trial); evaluates HBsAg clearance strategies |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36146665](https://pubmed.ncbi.nlm.nih.gov/36146665/) | 2022 | Cohort | Viruses | HCV reactivation observed in anti-HCV antibody-positive CHB patients receiving nucleoside analogue (Nuc) therapy; Entecavir suppresses HBV while HCV may reactivate — highlights co-infection complexity |
| [28230928](https://pubmed.ncbi.nlm.nih.gov/28230928/) | 2017 | Cohort | J Gastroenterol Hepatol | HBV reactivation risk during DAA therapy for chronic HCV; nucleoside analogue prophylaxis (including Entecavir) recommended to prevent HBV rebound |
| [24773464](https://pubmed.ncbi.nlm.nih.gov/24773464/) | 2014 | Review | Expert Opin Pharmacother | HBV/HCV co-infection treatment: Entecavir for HBV component; DAAs for HCV — separate mechanistic targets |
| [25027705](https://pubmed.ncbi.nlm.nih.gov/25027705/) | 2014 | Review | Minerva Gastroenterol Dietol | FDA-approved antivirals for HBV (including Entecavir) and HCV reviewed alongside renal effects; no crossover anti-HCV activity reported for Entecavir |
| [32173307](https://pubmed.ncbi.nlm.nih.gov/32173307/) | 2020 | Review | Clin Res Hepatol Gastroenterol | HBV and HCV management in children; Entecavir cited as HBV therapy — no HCV indication discussed |
| [22959099](https://pubmed.ncbi.nlm.nih.gov/22959099/) | 2013 | Review | Clin Res Hepatol Gastroenterol | HBV/HCV co-infection as therapeutic challenge; case report illustrates complexities of managing both viruses with distinct drug classes |
| [35327336](https://pubmed.ncbi.nlm.nih.gov/35327336/) | 2022 | Review | Biomedicines | Therapy of chronic viral hepatitis (HBV/HCV/HDV); Entecavir and Tenofovir for HBV suppression; DAAs for HCV — mechanistically distinct treatments |
| [16937041](https://pubmed.ncbi.nlm.nih.gov/16937041/) | 2006 | Review | Wien Med Wochenschr | Early overview of chronic hepatitis B and C treatment; Entecavir emerging as potent HBV option with no anti-HCV role |
| [38450508](https://pubmed.ncbi.nlm.nih.gov/38450508/) | 2024 | Review | Rev Esp Enferm Dig | Hemophilia and hepatology: Entecavir/Tenofovir for HBV (approved 2005–2008) and DAAs for HCV (approved 2015) marked as independent treatment milestones |
| [36873880](https://pubmed.ncbi.nlm.nih.gov/36873880/) | 2023 | Case Report | Front Med | Unusual viral evolution in concurrent HBV/HCV infection: anti-HBV therapy (Entecavir) suppresses HBV but may unmask latent HCV — demonstrates viral interaction, not anti-HCV activity |

---

## New Zealand Market Information

Entecavir is **not currently registered with Medsafe** and holds no authorizations in New Zealand. No approved indication text is available from local regulatory records.

For reference, Entecavir (brand name Baraclude) is approved in major markets including the United States (FDA, 2005), the European Union, Japan, and Taiwan for the treatment of chronic HBV infection in adults with evidence of active viral replication and liver disease.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Critical Safety Note for HIV/HBV Co-infected Patients:** Although Entecavir is approved for HBV and not for HIV treatment, published evidence (PMID [17582071](https://pubmed.ncbi.nlm.nih.gov/17582071/), [18453854](https://pubmed.ncbi.nlm.nih.gov/18453854/)) demonstrates that Entecavir has partial anti-HIV-1 reverse transcriptase activity. Use of Entecavir as HBV monotherapy in HIV-positive patients **not on full antiretroviral therapy (ART)** can select for the HIV M184V resistance mutation, compromising future HIV treatment options. This "Entecavir surprise" phenomenon is an important prescribing guardrail in co-infected populations.

---

## Conclusion and Next Steps

**Decision: Hold** *(for the chronic HCV indication)*

**Rationale:**
The TxGNN prediction of Entecavir efficacy against chronic HCV infection is a mechanistic false positive. HCV is an RNA virus relying on NS5B RNA-dependent RNA polymerase for replication — an enzyme Entecavir does not inhibit. Zero clinical trials in the evidence base demonstrate direct anti-HCV activity for Entecavir, and no biological rationale supports further investigation in this direction.

**To proceed, the following is needed:**

- No further investigation is recommended for the HCV indication — the mechanistic mismatch is definitive and cannot be resolved by additional evidence gathering
- **The actionable signal is Rank 2 (Hepatitis B Virus Infection, Decision: Proceed with Guardrails):** Entecavir's globally established HBV indication is supported by multiple completed Phase 3 RCTs (L1 evidence); if NZ registration or PHARMAC listing is under consideration, this is the appropriate pathway to pursue
- Full package insert safety data (warnings, contraindications, black-box warnings if applicable) must be obtained before any regulatory submission
- For HIV/HBV co-infected patients, a clinical protocol ensuring concurrent ART coverage must be in place before Entecavir is initiated
- A pharmacoeconomic comparison with existing NZ-approved HBV therapies (e.g., Tenofovir) would be required to support PHARMAC funding consideration
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


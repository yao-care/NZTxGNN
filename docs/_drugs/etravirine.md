---
layout: default
title: Etravirine
parent: 僅模型預測 (L5)
nav_order: 142
evidence_level: L5
indication_count: 10
---

# Etravirine
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

# Etravirine: From HIV-1 Infection to Congenital Human Immunodeficiency Virus

## One-Sentence Summary

Etravirine (Intelence) is a second-generation non-nucleoside reverse transcriptase inhibitor (NNRTI) approved for treatment-experienced HIV-1 infected adults and children aged ≥2 years.
The TxGNN model predicts it may be effective for **Congenital Human Immunodeficiency Virus** (mother-to-child transmitted HIV-1),
with **13 clinical trials** and **1 publication** currently supporting this direction.

> **Multi-indication analysis note (TW-DB06414-multi):** Among the top-10 TxGNN predictions, ranks 1–2 (feline/simian immunodeficiency) are cross-species false positives due to RT binding pocket structural incompatibility; ranks 6–10 (benign prostate tumours, Brenner tumour, obsolete hyperlipidaemia ontology) are model graph artefacts with no biological rationale. This report focuses on **rank 4 (congenital HIV)** as the primary actionable prediction with the strongest mechanistic basis and evidence support. Ranks 3 and 5 are noted in context.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | HIV-1 infection (treatment-experienced adults and children ≥2 years) |
| Predicted New Indication | Congenital Human Immunodeficiency Virus |
| TxGNN Prediction Score | 99.79% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known information, Etravirine belongs to the NNRTI class of antiretrovirals. Its efficacy against HIV-1 in treatment-experienced patients has been clinically validated, and mechanistically it is directly applicable to congenital HIV — the causative pathogen (HIV-1) is biologically identical regardless of the route of transmission (sexual contact vs. mother-to-child vertical transmission).

Congenital HIV occurs when HIV-1 passes from mother to infant during pregnancy, delivery, or breastfeeding. The viral target — reverse transcriptase — is the same enzyme Etravirine inhibits. Etravirine's second-generation NNRTI design confers a flexible binding conformation that tolerates multiple resistance-associated mutations (RAMs) such as K103N, Y181C, and G190A, making it particularly valuable in settings where treatment-experienced mothers may have transmitted drug-resistant virus to their neonates.

The primary hurdle is not mechanistic but pharmacokinetic: neonates and infants under 24 months have immature CYP3A4/CYP2C enzyme systems, altered protein binding, and different body composition compared to older children. The FDA extension to children ≥2 years (2012) already demonstrates paediatric applicability; the evidence gap is specifically in neonates and infants below the current approved age threshold — exactly the population most affected by congenital HIV.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00855335](https://clinicaltrials.gov/study/NCT00855335) | Phase 3 | Completed | 77 | Single-arm open-label PK study directly assessing Etravirine (alone or with Darunavir/Ritonavir) in HIV-1 infected pregnant women — the most directly relevant trial measuring Etravirine exposure in the maternal-foetal setting |
| [NCT07412977](https://clinicaltrials.gov/study/NCT07412977) | N/A | Not yet recruiting | 5,160 | VIROPREG: French prospective multicentre cohort tracking viral infections (HIV, HBV, HCV, arboviruses) during pregnancy, including mother-to-child transmission rates and antiviral treatment effects on maternal and infant outcomes |
| [NCT00042289](https://clinicaltrials.gov/study/NCT00042289) | N/A | Completed | 1,578 | IMPAACT P1026s: Phase IV prospective PK study of ARV and TB drugs in pregnant women and their infants — provides contextual PK framework for ARV dosing in the perinatal HIV prevention setting |
| [NCT04273165](https://clinicaltrials.gov/study/NCT04273165) | Phase 2 | Completed | 30 | Etravirine in Friedreich Ataxia — off-indication use demonstrating Etravirine's ability to increase Frataxin protein expression via epigenetic mechanisms; confirms Etravirine's broader biological activity beyond HIV (supports rank 3 neurodevelopmental hypothesis) |
| [NCT02951052](https://clinicaltrials.gov/study/NCT02951052) | Phase 3 | Active, not recruiting | 618 | ATLAS: Long-acting Cabotegravir + Rilpivirine for virologically suppressed HIV-1 adults switching from NNRTI/INI/PI regimens — provides HIV treatment landscape background |
| [NCT01458132](https://clinicaltrials.gov/study/NCT01458132) | N/A | Completed | 19 | Long-term exposure registry for HIV-infected patients with neurological comorbidities including seizure — relevant to monitoring in paediatric HIV populations with CNS involvement |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [20587860](https://pubmed.ncbi.nlm.nih.gov/20587860/) | 2010 | Case Series | Antiviral Therapy | Two highly treatment-experienced HIV-positive pregnant women managed with Darunavir + Etravirine ± Raltegravir — both achieved viral suppression at delivery; no vertical transmission observed; safety signal acceptable in obstetric context |

---

## New Zealand Market Information

Etravirine is not currently approved or marketed in New Zealand. No Medsafe authorizations are on record (total licenses: 0). Any clinical use would require an individual regulatory pathway through Medsafe (provisional approval or Section 29 access).

---

## Safety Considerations

**Drug Interactions:** Etravirine is metabolized by CYP3A4, CYP2C9, and CYP2C19. Co-administration with strong CYP3A4 inducers (Rifampicin, Carbamazepine, Phenobarbital, Phenytoin) is contraindicated as it reduces Etravirine plasma levels below therapeutic thresholds. Strong CYP inhibitors may increase exposure. Lopinavir/ritonavir, tipranavir/ritonavir, and unboosted protease inhibitors are also incompatible.

**Paediatric PK Monitoring:** In neonates and infants <2 years, immature CYP metabolism means adult and older-child dosing tables are not directly applicable. Therapeutic drug monitoring (TDM) is recommended if used off-label in this age group.

**Resistance Screening:** Pre-treatment genotypic resistance testing for NNRTI-associated mutations is essential before initiating Etravirine, particularly for patterns conferring high-level cross-resistance (e.g., K101P, E138A/G/K/Q, V179L, Y181C/I/V, Y188L, G190A/S combined).

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Etravirine directly inhibits HIV-1 reverse transcriptase — the identical molecular target in congenital HIV — and FDA approval in children ≥2 years old already establishes paediatric precedent. The evidence gap is narrow and age-specific: pharmacokinetic and safety data in neonates and infants under 24 months. The VIROPREG cohort (NCT07412977, n=5,160) and the completed IMPAACT P1026s (NCT00042289, n=1,578) provide the contextual infrastructure; a targeted neonatal PK arm is the logical next step.

**To proceed, the following is needed:**

- **Neonatal/infant PK study**: Age-stratified dosing data for infants <2 years, ideally integrated into an existing perinatal HIV trial (e.g., IMPAACT network)
- **Pre-treatment resistance genotyping protocol**: Mandatory NNRTI RAM panel before initiation, with particular attention to patterns conferring reduced Etravirine susceptibility
- **Therapeutic drug monitoring plan**: TDM framework for neonates given CYP immaturity and variable drug exposure
- **Hepatic and haematological monitoring**: Liver function tests (ALT/AST/bilirubin) and CBC at baseline and follow-up — especially relevant in neonates where metabolic parameters differ substantially from older children
- **DDI management protocol**: Explicit guidance on incompatible co-medications in the neonatal ARV combination context (Rifampicin for TB co-infection is a common clinical scenario)
- **New Zealand regulatory pathway**: Medsafe provisional approval or Section 29 compassionate use pathway required before any clinical use in New Zealand — zero current authorizations means regulatory groundwork is the first practical step
- **Full package insert review**: Complete safety warnings, contraindications, and adverse event profile from the approved Etravirine prescribing information (Intelence) should be reviewed; TFDA package insert data was not available in this evidence pack
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


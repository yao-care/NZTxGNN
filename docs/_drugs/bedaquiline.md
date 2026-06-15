---
layout: default
title: Bedaquiline
parent: 僅模型預測 (L5)
nav_order: 45
evidence_level: L5
indication_count: 10
---

# Bedaquiline
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

# Bedaquiline: From MDR-TB Treatment to Inactive Tuberculosis (Latent TB Prevention)

## One-Sentence Summary

Bedaquiline (Sirturo) is a diarylquinoline antibiotic approved globally for multidrug-resistant tuberculosis (MDR-TB) treatment, though not currently registered in New Zealand.
The TxGNN model predicts it may be effective for **Inactive Tuberculosis** (latent TB prevention), with **3 registered clinical trials** — including one Phase 2/3 trial enrolling 2,530 participants — and **20 publications** currently supporting this direction, representing a meaningful clinical shift from treating active disease to preventing reactivation.

> **Note on TxGNN rankings:** The top-ranked prediction by TxGNN score is *tuberculosis, bovine* (rank 1, score 99.96%), followed closely by *tuberculous ascites* (rank 2), *tuberculoma* (rank 3), *inactive tuberculosis* (rank 4), and *avian tuberculosis* (rank 5) — all with near-identical scores (~99.96%). This report focuses on **inactive tuberculosis** (rank 4) as the primary subject because it carries the strongest clinical evidence (L2, Phase 2/3 RCT) and the most actionable repurposing rationale. Other TB-spectrum predictions are summarised in the Predicted Indication Landscape section below.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Multidrug-resistant tuberculosis (MDR-TB) — globally approved; not registered in New Zealand |
| Predicted New Indication | Inactive Tuberculosis (Latent TB Prevention) |
| TxGNN Prediction Score | 99.96% (rank 660/total) |
| Evidence Level | L2 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why Is This Prediction Reasonable?

Bedaquiline works by selectively inhibiting the mycobacterial F₀F₁-ATP synthase — specifically binding the *c*-subunit of the F₀ membrane rotor — thereby blocking the proton translocation required for ATP generation. This mechanism is lethal even to dormant, non-replicating *M. tuberculosis* bacilli that are in a low-energy metabolic state, a property that sharply distinguishes bedaquiline from most first-line TB drugs (isoniazid, rifampicin) whose bactericidal activity depends on active bacterial replication. The drug's selectivity is exceptional: human mitochondrial ATP synthase shows >20,000-fold lower sensitivity compared to mycobacterial ATP synthase, providing a large therapeutic window.

This unique activity against dormant bacilli is precisely why the leap from *treating* MDR-TB to *preventing reactivation* of latent TB (LTBI/inactive TB) is mechanistically coherent. Latent TB exists in a dormant state sustained by low-energy metabolism; bedaquiline's ability to sterilize even non-replicating organisms suggests it could clear the bacterial reservoir that drives reactivation. A 2022 mouse model study confirmed that long-acting bedaquiline formulations showed sustained antituberculosis activity for preventive therapy scenarios (PMID 34939891), directly validating the animal proof-of-concept.

The most significant safety challenge for this repurposing is the risk-benefit recalibration: current LTBI standard regimens (isoniazid, rifapentine) have well-established short-course safety profiles, while bedaquiline carries a risk of QT prolongation and hepatotoxicity that was accepted in the context of life-threatening MDR-TB, but requires more rigorous justification for a preventive indication in otherwise healthy or HIV-positive contacts. Defining the specific subpopulations — drug-resistant TB contacts, people living with HIV (PLHIV) — where the benefit clearly outweighs this risk is the central clinical question driving BREACH-TB.

---

## Clinical Trial Evidence

*(Inactive Tuberculosis — Rank 4)*

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06568484](https://clinicaltrials.gov/study/NCT06568484) | Phase 2/3 | Not Yet Recruiting | 2,530 | BREACH-TB: Seamless Phase 2/3 non-inferiority trial comparing 4-week bedaquiline vs. standard preventive regimen in PLHIV and high-risk contacts of DS-TB or RR-TB cases; 72-week follow-up for confirmed/probable TB disease |
| [NCT05766267](https://clinicaltrials.gov/study/NCT05766267) | Phase 2/3 | Active, Not Recruiting | 288 | CRUSH-TB: 17-week bedaquiline + moxifloxacin + pyrazinamide ± rifabutin/delamanid vs. standard 6-month regimen for pulmonary TB; efficacy/safety data directly informs dosing parameters relevant to preventive use |
| [NCT07069582](https://clinicaltrials.gov/study/NCT07069582) | Phase 1 | Not Yet Recruiting | 60 | Sub-study of SSTARLET: PK profiling of bedaquiline in breastfeeding women after single dose; provides safety data for a population relevant to TB preventive therapy (TPT) expansion |

---

## Literature Evidence

*(Inactive Tuberculosis — Rank 4; up to 10 most relevant)*

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [33299175](https://pubmed.ncbi.nlm.nih.gov/33299175/) | 2021 | Mechanism / Structural | *Nature* | Cryo-EM structure of mycobacterial ATP synthase bound to bedaquiline; confirms drug can sterilize latent *M. tuberculosis* by targeting dormant-phase energy metabolism |
| [39766559](https://pubmed.ncbi.nlm.nih.gov/39766559/) | 2024 | Review / Mechanism | *Antibiotics* | Comprehensive review of Mtb F-ATP synthase inhibitors; details bedaquiline's activity under dormant/low-energy conditions directly relevant to latent TB clearance |
| [34939891](https://pubmed.ncbi.nlm.nih.gov/34939891/) | 2022 | Preclinical | *Am J Respir Crit Care Med* | Long-acting bedaquiline formulation shows sustained antituberculosis activity for ≥12 weeks in validated mouse model of preventive therapy; proof-of-concept for LTBI indication |
| [39301910](https://pubmed.ncbi.nlm.nih.gov/39301910/) | 2025 | Review | *Infect Disord Drug Targets* | Reviews bedaquiline delivery systems for MDR-TB; explicitly notes ability to target persistent/latent TB forms that remain viable despite conventional therapy |
| [36982277](https://pubmed.ncbi.nlm.nih.gov/36982277/) | 2023 | Review | *Int J Mol Sci* | TB pathogenesis and treatment review; covers latent infection (~25% of global population) and emerging drug targets including bedaquiline for dormant bacilli |
| [39887565](https://pubmed.ncbi.nlm.nih.gov/39887565/) | 2025 | Review | *Respirology* | Updates TB disease spectrum concept (latent → subclinical → active); supports rationale for preventive therapy in subclinical/inactive TB using newer agents |
| [36915977](https://pubmed.ncbi.nlm.nih.gov/36915977/) | 2022 | Review | *J Zhejiang Univ Med Sci* | Progress on LTBI diagnosis and treatment; discusses current limitations of isoniazid/rifamycin regimens and need for novel agents |
| [29187395](https://pubmed.ncbi.nlm.nih.gov/29187395/) | 2018 | Review | *Clin Microbiol Rev* | Comprehensive review of therapeutic approaches to dormant *M. tuberculosis*; bedaquiline identified as a key candidate due to ATP synthase inhibition in non-replicating state |
| [38003836](https://pubmed.ncbi.nlm.nih.gov/38003836/) | 2023 | Review | *Pathogens* | Pediatric drug-resistant TB management; bedaquiline and delamanid highlighted for safe use in children — relevant to preventive therapy in pediatric contacts |
| [28256380](https://pubmed.ncbi.nlm.nih.gov/28256380/) | 2017 | Review | *Presse Médicale* | TB/HIV co-infection challenges; identifies PLHIV as highest-priority group for preventive therapy — directly corresponds to BREACH-TB target population |

---

## Predicted Indication Landscape

All 10 TxGNN predictions are summarised below for clinical decision-making context:

| Rank | Disease | TxGNN Score | Evidence Level | Decision | Rationale Summary |
|------|---------|-------------|----------------|----------|-------------------|
| 1 | Tuberculosis, Bovine | 99.96% | L4 | Research Question | *M. bovis* carries homologous ATP synthase target; 3 in-vitro papers confirm selectivity. No in vivo or clinical data. Veterinary regulatory pathway required. |
| 2 | Tuberculous Ascites | 99.96% | L5 | Hold | No evidence. Peritoneal PK (drug penetration) completely unknown. |
| 3 | Tuberculoma | 99.96% | L4 | Research Question | Case reports of bedaquiline in MDR/XDR-TB with CNS involvement. BBB penetration is poor (low CSF/plasma ratio) — the critical unknow. |
| **4** | **Inactive Tuberculosis** | **99.96%** | **L2** | **Proceed with Guardrails** | **Phase 2/3 BREACH-TB trial (n=2,530); mouse model proof-of-concept; 20 publications. Best evidence in this pack.** |
| 5 | Tuberculosis, Avian | 99.96% | L5 | Hold | *M. avium* ATP synthase has lower bedaquiline affinity (high MIC in vitro); no supporting evidence. |
| 6 | Vulvovaginal Candidiasis | 99.88% | L5 | Hold | Fungal ATP synthase structurally distinct; no mechanism; TxGNN graph noise (TB/immunosuppression co-occurrence). |
| 7 | Fascioliasis | 99.88% | L5 | Hold | Eukaryotic helminth — no mechanistic basis whatsoever. |
| 8 | Urea Cycle Disorder | 99.78% | L5 | Hold | Genetic metabolic disorder; completely unrelated to antimicrobial mechanism. |
| 9 | Cutaneous Tuberculosis | 99.70% | L4 | Research Question | *M. tuberculosis* causative — mechanism applies. Phase 3 LEOPARD trial (n=124,000) tests bedaquiline for *M. leprae* prophylaxis, providing cross-mycobacterial safety data. Skin penetration PK needed. |
| 10 | Esophageal Candidiasis | 99.68% | L5 | Hold | Same as rank 6 — fungal, no mechanism, graph confound via HIV co-infection. |

---

## Safety Considerations

Detailed New Zealand / Taiwan-specific package insert data is not available (bedaquiline is not registered in New Zealand). The following safety information reflects globally available regulatory and clinical data:

- **QT Prolongation**: Bedaquiline prolongs the QT interval; cardiac monitoring (baseline ECG and regular follow-up) is mandatory. Risk is amplified when co-administered with other QT-prolonging agents (e.g. fluoroquinolones, clofazimine, azithromycin).
- **Hepatotoxicity**: Clinically significant liver enzyme elevations have been reported. Liver function tests (AST, ALT, bilirubin) should be monitored at baseline and throughout treatment.
- **Long Half-life**: Terminal half-life is approximately 5.5 months (due to redistribution from tissues). Adverse effects and drug interactions may persist for months after the last dose — a particular consideration when designing LTBI preventive regimens that are inherently shorter.
- **Drug Interactions**: Bedaquiline is metabolised primarily by CYP3A4. Strong CYP3A4 inducers (e.g. rifampicin, efavirenz) significantly reduce bedaquiline plasma levels. This interaction is clinically critical in the TB/HIV co-infection setting targeted by BREACH-TB.
- **Mortality Signal**: A higher rate of all-cause mortality was observed in the bedaquiline arm vs. placebo in the pivotal Phase 2b trial (C208); the mechanistic explanation remains unclear. This signal requires careful monitoring in any preventive-use protocol.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails** *(for Inactive Tuberculosis / Latent TB Prevention)*

**Rationale:**
BREACH-TB (NCT06568484) is a rigorously designed Phase 2/3 non-inferiority trial specifically targeting this indication (n=2,530, PLHIV and high-risk contacts), demonstrating that the field has cleared the proof-of-concept threshold and entered formal clinical validation. The mechanistic basis — bedaquiline's unique killing activity against dormant mycobacteria — is supported by structural biology data and a validated mouse preventive therapy model. The path to regulatory evaluation is clear, albeit requiring a compelling safety-benefit analysis against the established LTBI standard of care.

**To proceed, the following is needed:**
- Await BREACH-TB primary results (estimated completion: September 2027)
- Clarify QT prolongation and hepatotoxicity risk in healthy or HIV-positive contacts receiving bedaquiline for prevention (vs. MDR-TB treatment context where higher risk is tolerated)
- Define subpopulations with net benefit: drug-resistant TB contacts and PLHIV are the leading candidates
- Investigate CYP3A4 drug interaction management in patients on antiretroviral therapy (ART)
- Evaluate long-acting injectable formulation feasibility (NCT34939891 mouse data) to improve adherence in preventive settings
- Obtain bedaquiline New Zealand regulatory registration for active TB first, as a prerequisite pathway to preventive indication

---

**For secondary indications requiring further research (Research Question):**
- *Tuberculoma / CNS TB*: Measure CSF/plasma ratio in MDR-TB patients receiving bedaquiline; evaluate whether adjunctive steroids + BDQ improves CNS penetration
- *Cutaneous TB*: Pharmacokinetic study of bedaquiline in skin tissue; LEOPARD trial safety data (n=124,000) can provide a safety bridge
- *Bovine TB*: Requires veterinary pharmacology programme; in vivo cattle model efficacy and food safety assessment before any regulatory consideration
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


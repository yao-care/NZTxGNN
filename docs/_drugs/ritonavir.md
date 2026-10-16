---
layout: default
title: Ritonavir
parent: Model Prediction Only (L5)
nav_order: 408
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-15
description: "Ritonavir: predicted new indication Feline acquired immunodeficiency syndrome (evidence level L5). Approved use: HIV infectious disease / AIDS"
monitor_inn: "RITONAVIR"
predicted_indications: ["feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter"]
---

# Ritonavir
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Ritonavir: From HIV/AIDS to Feline Acquired Immunodeficiency Syndrome

## One-Sentence Summary

Ritonavir is an HIV-1 protease inhibitor and pharmacokinetic booster, originally used to treat HIV infection/AIDS and hepatitis C virus infection.
The TxGNN model predicts it may be effective for **feline acquired immunodeficiency syndrome**, but the data compiled for this report contain **no clinical trials and no publications** for this indication, so the prediction rests on model output and mechanistic reasoning alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HIV infectious disease / AIDS; chronic hepatitis C virus infection (from the TxGNN knowledge graph; no approved indication text is available from the New Zealand licence record) |
| Predicted New Indication | Feline acquired immunodeficiency syndrome |
| TxGNN Prediction Score | 99.92% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Ritonavir is an HIV-1 protease inhibitor that is also widely used as a CYP3A4 inhibitor to boost the exposure of other antiviral drugs. Detailed mechanism-of-action data are not available in the data compiled for this report, so this description relies on general pharmacological knowledge.

Feline immunodeficiency virus (FIV) is a lentivirus, like HIV, and has a protease with similarity to the HIV-1 enzyme. This offers a plausible mechanistic rationale. The very high TxGNN score probably reflects the drug's similarity to approved HIV/AIDS use. However, the link cannot be verified: no trials or publications were retrieved for this indication.

Note also that two other predictions were returned and are weaker or only indirectly supported:
- **Simian immunodeficiency virus (SIV) infection** (score 99.92%, evidence level L4): this is mainly a non-human primate model of HIV, so it is useful as a research model rather than as a separate repurposing target.
- **A rare neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter** (score 99.92%, L5): no plausible mechanistic link is evident, and the score appears to be a graph-based artifact.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature was found for the top-ranked prediction (feline acquired immunodeficiency syndrome). For the second prediction (SIV infection), the following preclinical and indirect publications were retrieved. They are shown for context only:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12709355](https://pubmed.ncbi.nlm.nih.gov/12709355/) | 2003 | In vitro | Antimicrob Agents Chemother | SIVmac239 was inhibited by ritonavir (EC50 about 13 nM), comparable to HIV-1 (about 25 nM) |
| [15040537](https://pubmed.ncbi.nlm.nih.gov/15040537/) | 2004 | In vitro | Antivir Ther | Antiviral activity of approved drugs against HIV-2, SIV and SHIV strains, relevant to treatment and post-exposure prophylaxis |
| [16973590](https://pubmed.ncbi.nlm.nih.gov/16973590/) | 2006 | Animal study (macaques) | J Virol | Rapid viral decay in SIV-infected macaques given quadruple antiretroviral therapy |
| [22737073](https://pubmed.ncbi.nlm.nih.gov/22737073/) | 2012 | Animal study (macaques) | PLoS Pathog | Highly intensified ART produced long-term viral suppression in a simian AIDS model |
| [12951220](https://pubmed.ncbi.nlm.nih.gov/12951220/) | 2003 | Animal study (macaques) | J Virol Methods | Oral AZT, 3TC and lopinavir/ritonavir in SHIV-infected macaques; effect on the CD8 subset |
| [25033210](https://pubmed.ncbi.nlm.nih.gov/25033210/) | 2014 | Animal study (macaques) | PLoS One | Suppressive cART plus the HDAC inhibitor SAHA in SIV-infected rhesus macaques |
| [34903055](https://pubmed.ncbi.nlm.nih.gov/34903055/) | 2021 | Review / preclinical | mBio | Lentiviral infection persists in the brain despite effective ART |

These studies are indirect: most test multi-drug regimens, so ritonavir's specific contribution is unconfirmed.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2379317 (cancelled) | Norvir | Tab 100 mg | Not available |

## Cytotoxicity

Ritonavir is an antiviral and not an antineoplastic drug, so this section does not apply.

## Safety Considerations

- **Drug Interactions**: 100 interactions are recorded. Examples include:
  - **Major**: loperamide, triamcinolone, budesonide
  - **Moderate**: acarbose, hydrocortisone, albiglutide, alogliptin, metformin, alosetron, bupropion, aprepitant, dexamethasone, lorcaserin, betamethasone, metronidazole, canagliflozin, chlorpropamide, clarithromycin
- **Knowledge-graph contraindication signals** (from drug-disease relationships, not label text): conditions flagged include
  - long QT syndrome, torsades de pointes, heart conduction disease, atrioventricular block, sudden cardiac arrest
  - cardiomyopathy, myocardial ischemia, coronary artery disease
  - pancreatitis, liver disease, cirrhosis, gallbladder disease
  - diabetes, hyperglycemia, diabetic ketoacidosis
  - hypertriglyceridemia, hyperbilirubinemia, gout, hemophilia, chronic hepatitis B infection

Please refer to the package insert for formal warnings and contraindications, as label-based safety text was not available.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for feline acquired immunodeficiency syndrome has a high model score and a plausible lentiviral protease rationale, but there is no trial or literature support (evidence level L5). Ritonavir's only New Zealand licence is cancelled, and its interaction burden is heavy. Feline immunodeficiency is also a veterinary condition, so it falls outside human drug repurposing and would need veterinary regulatory review.

**To proceed, the following is needed:**
- Mechanism-of-action data for ritonavir (for example from DrugBank)
- Package insert warnings and contraindications from Medsafe
- Direct evidence of ritonavir activity against FIV (in vitro and veterinary studies)
- A decision on whether the target is human or veterinary use, since SIV is mainly a research model of HIV
- A review of the neurodevelopmental disorder prediction before any further work, given the absence of a mechanistic link
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


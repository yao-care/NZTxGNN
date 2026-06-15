---
layout: default
title: Eltrombopag
parent: 僅模型預測 (L5)
nav_order: 131
evidence_level: L5
indication_count: 1
---

# Eltrombopag
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Eltrombopag: From Thrombocytopenia (ITP) to HIV Infectious Disease

## One-Sentence Summary

Eltrombopag is a thrombopoietin receptor agonist (TPO-RA), approved internationally for the treatment of immune thrombocytopenia (ITP) and aplastic anaemia, though not currently registered in New Zealand or Taiwan.
The TxGNN model predicts it may be effective for **HIV Infectious Disease** — primarily targeting HIV-associated thrombocytopenia (HIV-ITP), with exploratory evidence also suggesting potential direct antiviral activity.
Currently **5 clinical trials** and **10 publications** support this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Immune Thrombocytopenia (ITP) / Thrombocytopenia (not registered in New Zealand) |
| Predicted New Indication | HIV Infectious Disease |
| TxGNN Prediction Score | 99.26% |
| Evidence Level | L2 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

The prediction is mechanistically supported by at least two distinct pathways. HIV infection causes thrombocytopenia (HIV-ITP) in approximately 30–40% of infected individuals, via immune-mediated platelet destruction and bone marrow suppression. As a TPO receptor agonist, eltrombopag stimulates the c-Mpl receptor on megakaryocytes to promote platelet production, directly addressing this common and clinically significant HIV complication. Multiple case reports and a case series (PMID 25504472, 22992580, 25333665) confirm its use as salvage therapy in HIV-ITP patients who have failed standard treatment including HAART optimisation.

A second mechanistic angle emerges from a 2020 high-throughput screen of FDA-approved drugs (PMID 32977702), which identified eltrombopag as a modulator of HIV-1 proviral transcription — suggesting potential direct antiviral activity beyond its haematological effects. Additionally, eltrombopag's known iron-chelating properties may suppress HIV replication by inhibiting iron-dependent viral enzymes. These converging mechanisms provide plausible biological grounding for the TxGNN model's prediction.

It should be noted that the primary and better-evidenced repurposing rationale is for HIV-associated haematological complications (ITP, aplastic anaemia), not direct antiviral therapy. The direct antiviral pathway remains at an exploratory, mechanistic stage and would require dedicated clinical investigation before any therapeutic claim can be made.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01636778](https://clinicaltrials.gov/study/NCT01636778) | Phase 2 | Completed | 45 | SB-497115 (eltrombopag development code) in thrombocytopenic HCV/cirrhosis patients; non-randomised open-label design; most representative prospective dataset applicable to HIV-ITP, assessing ability to raise and maintain platelet counts to enable antiviral therapy initiation |
| [NCT00529568](https://clinicaltrials.gov/study/NCT00529568) | Phase 3 | Completed | 759 | Randomised, placebo-controlled; eltrombopag in HCV-ITP patients initiating Peg-IFN alfa-2b + ribavirin; primary endpoint sustained virological response (SVR); large-scale trial providing strong indirect support for infection-related ITP management |
| [NCT00516321](https://clinicaltrials.gov/study/NCT00516321) | Phase 3 | Completed | 687 | Randomised, placebo-controlled; parallel design to NCT00529568 using Peg-IFN alfa-2a; confirms platelet maintenance benefit enabling antiviral therapy completion; supports general infection-related ITP evidence base |
| [NCT00996216](https://clinicaltrials.gov/study/NCT00996216) | Phase 3 | Completed | 27 | Open-label rollover study; long-term safety and tolerability of eltrombopag in HCV-ITP; very small sample (N=27) limits efficacy conclusions but contributes extended safety data |
| [NCT00678587](https://clinicaltrials.gov/study/NCT00678587) | Phase 3 | Terminated | 292 | Eltrombopag to reduce platelet transfusion need in chronic liver disease with thrombocytopenia; terminated early (only partial enrolment achieved); reason undisclosed — potential safety or futility signal; treat as a risk indicator rather than positive evidence |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25504472](https://pubmed.ncbi.nlm.nih.gov/25504472/) | 2015 | Case Series / Cohort | J Int Assoc Providers AIDS Care | First reported experience with TPO-RA (eltrombopag and romiplostim) in refractory HIV-ITP; eltrombopag effective as salvage therapy following failure of HAART optimisation and standard ITP treatment |
| [22992580](https://pubmed.ncbi.nlm.nih.gov/22992580/) | 2012 | Case Report | AIDS | Successful eltrombopag use without splenectomy in refractory HIV-related immune reconstitution thrombocytopenia; demonstrates feasibility in the specific HIV immune reconstitution context |
| [25333665](https://pubmed.ncbi.nlm.nih.gov/25333665/) | 2014 | Case Report | AIDS | First report of eltrombopag for HIV-associated aplastic anaemia; trilineage haematological response observed; immunomodulatory effects documented (↓Th1/Th17, ↑Treg ratio), suggesting mechanisms beyond TPO stimulation |
| [32977702](https://pubmed.ncbi.nlm.nih.gov/32977702/) | 2020 | Mechanistic Screen | Viruses | High-throughput screen of FDA-approved drugs identifies eltrombopag as a modulator of HIV-1 proviral transcription; provides mechanistic rationale for potential direct antiviral repurposing beyond haematological indication |
| [22185370](https://pubmed.ncbi.nlm.nih.gov/22185370/) | 2012 | Cohort (Registry) | Platelets | Danish real-world registry of TPO-RA use including off-label applications in secondary ITP (HIV, chronic lymphatic leukaemia); supports broader infection-related ITP use pattern |
| [24816314](https://pubmed.ncbi.nlm.nih.gov/24816314/) | 2014 | Cohort | Internal Medicine Journal | TPO receptor agonist use in ITP of <6 months duration including secondary ITP subgroups; includes clinical experience relevant to HIV-ITP |
| [19245929](https://pubmed.ncbi.nlm.nih.gov/19245929/) | 2009 | Review | Seminars in Hematology | Therapeutic strategies for HCV- and HIV-related immune thrombocytopenias; contextualises the role of TPO-RA in secondary ITP management |
| [19932434](https://pubmed.ncbi.nlm.nih.gov/19932434/) | 2009 | Review | Hematol Oncol Clin North Am | Infectious causes of chronic ITP (HCV, HIV, H. pylori); treating the primary infection often improves thrombocytopenia; eltrombopag cited as adjunct option in refractory cases |
| [24128106](https://pubmed.ncbi.nlm.nih.gov/24128106/) | 2013 | Case Report | Farmacia Hospitalaria | Two cases of eltrombopag for chronic hepatitis C-related thrombocytopenia; indirect support for infection-associated ITP management |
| [28043314](https://pubmed.ncbi.nlm.nih.gov/28043314/) | 2016 | Case Report | J Coll Physicians Surg Pak | Hepatitis B-associated megaloblastic anaemia and severe thrombocytopenia; illustrates complexity of infection-related thrombocytopenia differential diagnosis |

---

## New Zealand Market Information

Eltrombopag is not currently registered or authorised for use in New Zealand. No Medsafe authorisations were found in the database query conducted on 2026-03-29.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** Safety data (key warnings, contraindications, drug-drug interactions) were not retrievable in this evidence pack. Given that HIV patients typically receive multiple antiretrovirals and co-medications, a dedicated DDI review against common HAART regimens is strongly recommended before any clinical use.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple completed Phase 3 trials in infection-related ITP and direct HIV-ITP case evidence collectively support eltrombopag's haematological efficacy at L2 level; an additional mechanistic study raises the prospect of direct antiviral activity. However, the drug is not registered in New Zealand/Taiwan, safety data were not captured in this pack, and HIV-specific prospective RCT evidence is absent.

**To proceed, the following is needed:**
- Retrieve and review the full prescribing information (package insert) for key warnings and contraindications — especially hepatotoxicity risk, which is a known concern for eltrombopag and particularly relevant in HIV patients with potential co-existing liver disease
- Conduct a formal drug-drug interaction review against common HAART regimens (e.g. protease inhibitors, NNRTIs) and relevant co-medications
- Obtain complete MOA data from DrugBank to formalise the mechanistic link analysis
- Commission or identify a dedicated prospective cohort study or RCT in HIV-ITP patients; current evidence is primarily extrapolated from HCV-ITP trials and small case series
- Assess the regulatory pathway for New Zealand/Taiwan market authorisation, including whether data packages from existing international approvals (e.g. FDA, EMA) are sufficient to support a local filing
- Define a safety monitoring plan covering complete blood count, liver function tests, and thromboembolic event surveillance for any pilot use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


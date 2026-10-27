---
layout: default
title: Bleomycin
parent: Model Prediction Only (L5)
nav_order: 68
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-15
description: "Bleomycin: predicted new indication Top-ranked by score: cauda equina neoplasm (99.30%, L5… (evidence level L5). Approved use: Hodgkin lymphoma, non-Hodgkin lymphoma, head and neck cancer"
monitor_inn: "bleomycin"
predicted_indications: ["cauda equina neoplasm", "adult astrocytic tumour", "reticulum cell sarcoma", "primary pulmonary lymphoma", "pulmonary blastoma", "well-differentiated fetal adenocarcinoma of the lung"]
---

# Bleomycin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Bleomycin: From Hodgkin Lymphoma and Other Lymphomas to Reticulum Cell Sarcoma (Lymphoma)
{: .text-alpha }

## One-Sentence Summary

Bleomycin is a cytotoxic antibiotic originally used to treat Hodgkin and non-Hodgkin lymphomas and head and neck cancer.
The TxGNN model's top-ranked prediction is cauda equina neoplasm, but this has no supporting evidence. The best-supported candidate is **reticulum cell sarcoma** (an archaic term for non-Hodgkin lymphoma), backed by **11 clinical trials** and **20 publications**. This is largely a match to an existing indication rather than true repurposing.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hodgkin lymphoma, non-Hodgkin lymphoma, head and neck cancer (from the knowledge graph; no approved indication text is available in the New Zealand licence records) |
| Predicted New Indication | Top-ranked by score: cauda equina neoplasm (99.30%, L5, Hold). Best-supported: reticulum cell sarcoma (99.14%) |
| TxGNN Prediction Score | 99.30% (cauda equina neoplasm); 99.14% (reticulum cell sarcoma) |
| Evidence Level | L1 (reticulum cell sarcoma) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (0 valid, 2 cancelled) |
| Recommended Decision | Proceed with Guardrails (reticulum cell sarcoma); Hold for the top-ranked cauda equina neoplasm |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Bleomycin is a well-known cytotoxic agent that causes DNA strand breaks. Its efficacy in Hodgkin and non-Hodgkin lymphoma is established, and it is a component of regimens such as ABVD, ProMACE-CytaBOM, VACOP-B and m-BACOD.

Reticulum cell sarcoma is an outdated term for diffuse large B-cell or non-Hodgkin lymphoma, which already falls within bleomycin's established lymphoma indications. Early clinical reports from the 1970s describe bleomycin activity in reticulum cell sarcoma, and later trials tested bleomycin-containing combination regimens. Caveat: these trials test combinations, so bleomycin's individual contribution is not isolated, and bleomycin is no longer standard in many modern DLBCL regimens (e.g. R-CHOP).

The other predictions are weaker:
- **Cauda equina neoplasm (rank 1):** no trials, and the single retrieved paper concerns radiotherapy for CNS germinoma. Poor CNS penetration is a major barrier. (L5, Hold)
- **Adult astrocytic tumour (rank 2):** only early-phase or dated studies in glioma, including a phase I intratumoral infusion trial. A randomized trial showed no survival benefit from adding bleomycin. (L3, Research Question)
- **Primary pulmonary lymphoma (rank 4):** plausible, but no study evaluates bleomycin in this setting, and lung toxicity is especially relevant. (L4, Research Question)
- **Pulmonary blastoma (rank 5):** two case reports only. (L4, Hold)
- **Well-differentiated fetal adenocarcinoma of the lung (rank 6):** no evidence. (L5, Hold)

---

## Clinical Trial Evidence

Shown for reticulum cell sarcoma (the other predicted indications have no registered trials).

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00005867](https://clinicaltrials.gov/study/NCT00005867) | Phase 3 | Completed | 310 | CHOP vs PMitCEBO in good-risk aggressive non-Hodgkin lymphoma |
| [NCT00002835](https://clinicaltrials.gov/study/NCT00002835) | Phase 3 | Completed | 116 | Early intensification vs alternating triple therapy in poor-prognosis lymphoma |
| [NCT00002565](https://clinicaltrials.gov/study/NCT00002565) | Phase 3 | Completed | 61 | Alternating triple therapy vs CHOP in intermediate-grade/immunoblastic lymphoma; bleomycin contribution not isolated |
| [NCT00003815](https://clinicaltrials.gov/study/NCT00003815) | Phase 3 | Unknown | N/A | High-dose chemoradiotherapy with autologous transplant in high-grade NHL; bleomycin role unclear |
| [NCT00003110](https://clinicaltrials.gov/study/NCT00003110) | Phase 2 | Completed | 5 | 72-hour continuous-infusion bleomycin as salvage in AIDS-related and immunocompetent NHL; bleomycin-specific but very small |
| [NCT00002571](https://clinicaltrials.gov/study/NCT00002571) | Phase 2 | Completed | 52 | ProMACE-CytaBOM plus antiviral therapy and G-CSF in AIDS-related lymphoma |
| [NCT00002524](https://clinicaltrials.gov/study/NCT00002524) | Phase 2 | Completed | 46 | Combination chemotherapy pilot in AIDS-related lymphoma |
| [NCT00002657](https://clinicaltrials.gov/study/NCT00002657) | Phase 2 | Completed | 20 | ProMACE-CytaBOM with immunomodulation in post-cardiac-transplant lymphoproliferation |
| [NCT00032149](https://clinicaltrials.gov/study/NCT00032149) | Phase 1/2 | Unknown | 30 | PMitCEBO plus G-CSF in good-prognosis HIV-related lymphoma |
| [NCT00031902](https://clinicaltrials.gov/study/NCT00031902) | Phase 1 | Unknown | N/A | Rituximab plus PVB chemotherapy in HIV-related NHL |

---

## Literature Evidence

Shown for the indications with the most relevant publications; reticulum cell sarcoma and adult astrocytic tumour are the main ones.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [4109401](https://pubmed.ncbi.nlm.nih.gov/4109401/) | 1972 | Clinical study | Br Med J | Single-agent bleomycin in 54 Hodgkin, 17 lymphosarcoma, 22 reticulum cell sarcoma and 7 mycosis fungoides patients after prior therapy; 29% of Hodgkin patients remitted |
| [4107632](https://pubmed.ncbi.nlm.nih.gov/4107632/) | 1971 | Clinical study | Arch Dermatol | Bleomycin in mycosis fungoides and reticulum cell lymphoma |
| [14962711](https://pubmed.ncbi.nlm.nih.gov/14962711/) | 2004 | Randomised trials (pooled) | Eur J Cancer | Long-term efficacy of the bleomycin-containing CHVmP/BV regimen in 936 aggressive NHL patients across three EORTC trials |
| [24014242](https://pubmed.ncbi.nlm.nih.gov/24014242/) | 2013 | Pooled analysis | Blood | Treatment factors in 1,546 HIV-associated NHL patients from 19 prospective trials |
| [27888880](https://pubmed.ncbi.nlm.nih.gov/27888880/) | 2016 | Review | Hematol Oncol Clin North Am | Optimizing outcomes in primary mediastinal B-cell lymphoma |
| [15202521](https://pubmed.ncbi.nlm.nih.gov/15202521/) | 2003 | Review | Leuk Lymphoma | MACOP-B (includes bleomycin) followed by radiotherapy may be a better induction strategy in PMBL |
| [6187214](https://pubmed.ncbi.nlm.nih.gov/6187214/) | 1983 | Case report | Am J Med | Fatal bleomycin-induced hyperpyrexia in a lymphoma patient |
| [6164465](https://pubmed.ncbi.nlm.nih.gov/6164465/) | 1981 | Randomised trial | Cancer | Grade III-IV astrocytoma: radiotherapy was valuable, but bleomycin did not improve survival |
| [12416544](https://pubmed.ncbi.nlm.nih.gov/12416544/) | 2002 | Phase I trial | J Neurooncol | Continuous intratumoral bleomycin infusion in recurrent glioblastoma; feasibility study |
| [31668826](https://pubmed.ncbi.nlm.nih.gov/31668826/) | 2020 | Cohort | J Geriatr Oncol | Age and bleomycin pulmonary toxicity risk in Hodgkin lymphoma |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2204215 | Baxter | Inj 1,000 iu for ECP | Not available (status: cancelled) |
| 2343207 | DBL Bleomycin Sulfate | Inj 15,000 iu, vial | Not available (status: cancelled) |

Both licences are cancelled, so there is currently no valid marketed bleomycin product in New Zealand.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (antitumour antibiotic; causes DNA strand scission) |
| Myelosuppression Risk | Low relative to other cytotoxics (the 1972 report notes marrow-sparing properties); please refer to the package insert |
| Emetogenicity Classification | Low |
| Monitoring Items | Pulmonary function and chest imaging (pulmonary toxicity is the key concern), plus CBC, liver and renal function |
| Handling Protection | Must follow cytotoxic drug handling regulations |

---

## Safety Considerations

- **Pulmonary toxicity:** The literature repeatedly reports bleomycin-induced interstitial pneumonitis and pulmonary toxicity, with risk increasing with age (PMIDs 4118423, 31668826, 27742539). This is especially relevant for lung-based indications.
- **Hyperpyrexia:** A fatal acute hyperpyrexic reaction has been reported, and a test dose before therapy has been recommended (PMID 6187214).
- **Drug interactions:** 205 interactions were identified. The ones listed in the data are mostly of unknown severity (e.g. pantoprazole, omeprazole, morphine, prednisone, vancomycin, ondansetron, clarithromycin); levofloxacin is classified as minor.

The package insert warnings and contraindications could not be retrieved. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails (reticulum cell sarcoma / non-Hodgkin lymphoma); Hold for cauda equina neoplasm and other low-evidence predictions**

**Rationale:**
- Reticulum cell sarcoma has multiple completed Phase 3 trials and historical single-agent data, but it is effectively an existing lymphoma indication, and the trials test combination regimens, so bleomycin's own contribution is unclear. The top-scoring cauda equina neoplasm prediction has no supporting evidence and faces CNS penetration barriers.
- Both New Zealand licences are cancelled, so a supply route would also need to be established.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (currently a blocking gap for safety screening)
- Detailed mechanism of action data
- Confirmation of the current New Zealand supply and licensing situation
- A pulmonary toxicity monitoring plan
- A review of bleomycin's role in modern lymphoma regimens
- For glioma and primary pulmonary lymphoma: further research before any clinical consideration
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Gemcitabine
parent: Model Prediction Only (L5)
nav_order: 218
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Gemcitabine: predicted new indication Female breast carcinoma (evidence level L5). Approved use: Ovarian cancer"
monitor_inn: "Gemcitabine"
predicted_indications: ["female breast carcinoma", "rectum mucinous adenocarcinoma", "colon mucinous adenocarcinoma", "endometrial mixed adenocarcinoma", "villoglandular endometrial endometrioid adenocarcinoma", "endometrial mucinous adenocarcinoma", "cervical mucinous adenocarcinoma", "gallbladder mucinous adenocarcinoma", "mucin-rich endometrial endometrioid adenocarcinoma"]
---

# Gemcitabine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Gemcitabine: From Ovarian Cancer to Female Breast Carcinoma

## One-Sentence Summary

Gemcitabine is a nucleoside-analogue chemotherapy drug, originally used to treat ovarian cancer.
The TxGNN model predicts it may be effective for **female breast carcinoma**,
with **33 clinical trials** and **20 publications** currently supporting this direction, including a completed Phase 3 randomized trial in metastatic breast cancer.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Ovarian cancer (no approved indication text is available in the New Zealand licence records; the original indication comes from the knowledge graph) |
| Predicted New Indication | Female breast carcinoma |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L1 (see note below) |
| New Zealand Market Status | Cancelled only (no currently valid authorization) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Proceed with Guardrails |

*Note on evidence level: the compiled data assign L1. The completed Phase 3 trial NCT00006459 (gemcitabine plus paclitaxel versus paclitaxel) and the large Phase 3 trial NCT00093795 (4,894 participants) both completed. However, no published results were retrieved for either, so L1 should be confirmed once the results are reviewed.*

## Why is This Prediction Reasonable?

Detailed mechanism of action data are not available in the compiled data. Pharmacology records show that gemcitabine acts on ribonucleotide reductase (subunits RRM1 and RRM2). It is a nucleoside analogue that interferes with DNA synthesis, which gives it broad activity against rapidly dividing solid tumours.

Ovarian cancer and breast cancer are both common solid tumours treated with cytotoxic combination regimens. Gemcitabine is already used with carboplatin for relapsed ovarian cancer and with paclitaxel for metastatic breast cancer, according to the pharmacology records. Breast carcinoma therefore has the strongest clinical support of all predicted indications.

Use should be limited to the established metastatic setting and to combination regimens. Toxicities such as myelosuppression and retinopathy need attention.

## Clinical Trial Evidence

The most relevant trials of the 33 retrieved are listed below. Many others are early-phase studies of other agents in which gemcitabine is only a combination partner.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00006459](https://clinicaltrials.gov/study/NCT00006459) | Phase 3 | Completed | Not reported | Gemcitabine plus paclitaxel vs paclitaxel in unresectable, locally recurrent or metastatic breast cancer |
| [NCT00093795](https://clinicaltrials.gov/study/NCT00093795) | Phase 3 | Completed | 4,894 | Adjuvant regimens in node-positive breast cancer, including dose-dense AC followed by paclitaxel plus gemcitabine |
| [NCT00408408](https://clinicaltrials.gov/study/NCT00408408) | Phase 3 | Unknown | 1,206 | Neoadjuvant trial adding capecitabine or gemcitabine to docetaxel before AC |
| [NCT00039546](https://clinicaltrials.gov/study/NCT00039546) | Phase 3 | Unknown | Not reported | tAnGo: gemcitabine added to paclitaxel, epirubicin and cyclophosphamide adjuvant chemotherapy in early breast cancer |
| [NCT00070278](https://clinicaltrials.gov/study/NCT00070278) | Phase 3 | Unknown | 800 | Neoadjuvant epirubicin/cyclophosphamide then paclitaxel with or without gemcitabine in poor-risk early breast cancer |
| [NCT00006007](https://clinicaltrials.gov/study/NCT00006007) | Phase 2 | Completed | 59 | Pemetrexed plus gemcitabine in metastatic breast cancer |
| [NCT00110084](https://clinicaltrials.gov/study/NCT00110084) | Phase 2 | Completed | 50 | Nab-paclitaxel plus gemcitabine in metastatic breast cancer |
| [NCT00003540](https://clinicaltrials.gov/study/NCT00003540) | Phase 2 | Completed | 30 | Gemcitabine alone in metastatic breast cancer previously treated with doxorubicin and paclitaxel |
| [NCT00244933](https://clinicaltrials.gov/study/NCT00244933) | Phase 2 | Completed | 19 | Gemcitabine plus genistein in metastatic breast cancer (small, biomarker-driven) |
| [NCT07528768](https://clinicaltrials.gov/study/NCT07528768) | Phase 2 | Not yet recruiting | 750 | Gemcitabine vs paclitaxel in metastatic triple-negative breast cancer in women of African ancestry |

## Literature Evidence

No randomized controlled trial reports were retrieved. The literature consists mainly of reviews and early clinical studies.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15685819](https://pubmed.ncbi.nlm.nih.gov/15685819/) | 2004 | Review | Oncology (Williston Park) | Gemcitabine plus paclitaxel in metastatic breast cancer; response rate of 52% (114 of 221 patients) in phase II trials |
| [15685821](https://pubmed.ncbi.nlm.nih.gov/15685821/) | 2004 | Review | Oncology (Williston Park) | Gemcitabine with platinum compounds in metastatic breast cancer |
| [15685820](https://pubmed.ncbi.nlm.nih.gov/15685820/) | 2004 | Review | Oncology (Williston Park) | Gemcitabine plus docetaxel in metastatic breast cancer |
| [14768404](https://pubmed.ncbi.nlm.nih.gov/14768404/) | 2003 | Review | Oncology (Williston Park) | Gemcitabine, anthracycline and taxane combinations for advanced breast cancer |
| [12138397](https://pubmed.ncbi.nlm.nih.gov/12138397/) | 2002 | Review | Seminars in Oncology | Single-agent response rates of 16% to 37%; higher with combinations |
| [12057045](https://pubmed.ncbi.nlm.nih.gov/12057045/) | 2002 | Clinical study | Clinical Breast Cancer | Neoadjuvant gemcitabine therapy for breast cancer |
| [14754469](https://pubmed.ncbi.nlm.nih.gov/14754469/) | 2004 | Review | Clinical Breast Cancer | Gemcitabine and trastuzumab in HER2-overexpressing metastatic breast cancer |
| [40779028](https://pubmed.ncbi.nlm.nih.gov/40779028/) | 2025 | Phase I clinical study | Breast Cancer Res Treat | Carboplatin, gemcitabine and mifepristone in advanced breast and recurrent ovarian cancer |
| [28961673](https://pubmed.ncbi.nlm.nih.gov/28961673/) | 2017 | Case report (safety) | Retina | Gemcitabine-induced retinopathy |

## New Zealand Market Information

All three authorizations are cancelled, and approved indication text is not available in the records.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2207532 | Baxter | Inj 1 mg for ECP | Not available |
| 2320304 | Gemcitabine Ebewe | Inj 1 g | Not available |
| 2521423 | DBL Gemcitabine | Inj 43.3 mg per ml (equivalent to 38 mg per ml gemcitabine), 26.3 ml vial | Not available |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (nucleoside analogue / antimetabolite) |
| Myelosuppression Risk | Medium to high (myelosuppression is a known concern; the knowledge graph links the drug to leukopenia, thrombocytopenia and anaemia) |
| Emetogenicity Classification | Low to medium |
| Monitoring Items | Complete blood count with differential, liver and renal function |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: kidney disease, liver disease, gallbladder disease, thrombocytopenia, leukopenia, anaemia (including macrocytic anaemia), infectious disease, interstitial lung disease, pyoureter, and haemolytic-uremic syndrome (including typical form).
- **Reported adverse effect**: retinopathy (case report, PMID 28961673).
- **Drug interactions**: the interaction search found no drug-drug interactions, only pharmacological targets (ribonucleotide reductase subunits RRM1 and RRM2).

Package insert warnings and contraindications were not available, so please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Breast carcinoma is supported by a completed Phase 3 randomized trial of gemcitabine plus paclitaxel in metastatic disease, several completed Phase 2 combination studies and multiple reviews. However, all New Zealand authorizations are cancelled, and no safety label data were available. Use should be restricted to the metastatic setting and to combination regimens, with close haematological monitoring.

**To proceed, the following is needed:**
- Published results of NCT00006459 and NCT00093795 to confirm the evidence level
- Package insert warnings and contraindications from Medsafe
- Detailed mechanism of action data
- Clarification of the regulatory pathway, since no valid New Zealand authorization currently exists
- A monitoring plan for myelosuppression, renal and hepatic function, and retinal toxicity
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


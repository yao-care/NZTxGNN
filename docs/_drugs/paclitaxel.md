---
layout: default
title: Paclitaxel
parent: High Evidence (L1-L2)
nav_order: 344
evidence_level: L2
indication_count: 10
---

# Paclitaxel
{: .fs-9 }

Evidence Level: **L2** | Predicted Indications: **10** 
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

# Paclitaxel: From Ovarian Cancer to Female Breast Carcinoma

## One-Sentence Summary

Paclitaxel is a microtubule-stabilising cytotoxic chemotherapy drug, originally used for ovarian cancer.
The TxGNN model predicts it may be effective for **female breast carcinoma**,
with **43 clinical trials** and **20 publications** compiled for this prediction. However, paclitaxel is already an established standard of care in breast cancer, so this is not a true repurposing candidate. The finding largely confirms existing clinical practice.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Ovarian cancer (the data compiled for this report did not include approved indication text from New Zealand licences; the original indication is taken from the knowledge-graph indication list) |
| Predicted New Indication | Female breast carcinoma |
| TxGNN Prediction Score | 99.995% |
| Evidence Level | L2 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 11 (all cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on well-known pharmacology, paclitaxel stabilises microtubules, causing mitotic arrest and apoptosis in rapidly dividing tumour cells. This action does not depend on tumour type or hormone-receptor status.

Ovarian cancer and breast cancer are both epithelial solid tumours that respond to taxane-based chemotherapy. Reviews in the compiled literature (e.g. PMID 10791395 and 9282422) describe taxanes as evidence-based components of therapy for advanced breast, lung and ovarian carcinomas, and note that paclitaxel's licence was extended to breast cancer.

The predicted indication therefore fits the mechanism well. Because paclitaxel is already used in breast cancer, the prediction mainly validates the model rather than pointing to a new use. The supplied trial list also shows mostly Phase 1/2 evidence for paclitaxel-specific effects in this setting.

Other predicted indications in the data (ranked 2 to 10) are mostly breast cancer subtypes or anatomical variants. Ehrlich tumour (a mouse model), bilateral and nipple carcinoma, and two rhabdomyosarcoma entries have only preclinical, case-report or no evidence, and were rated Hold.

## Clinical Trial Evidence

The most relevant trials for paclitaxel in female breast carcinoma (the data contained many more, including several Phase 3 combination studies):

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00003539](https://clinicaltrials.gov/study/NCT00003539) | Phase 2 | Completed | 50 | Weekly paclitaxel plus trastuzumab in metastatic breast cancer; single-arm efficacy evidence |
| [NCT00006120](https://clinicaltrials.gov/study/NCT00006120) | Phase 2 | Unknown | Not reported | Randomised comparison of two schedules of docetaxel or paclitaxel in metastatic breast cancer |
| [NCT00006459](https://clinicaltrials.gov/study/NCT00006459) | Phase 3 | Completed | Not reported | Gemcitabine plus paclitaxel versus paclitaxel in unresectable or metastatic breast cancer |
| [NCT00667251](https://clinicaltrials.gov/study/NCT00667251) | Phase 3 | Completed | 652 | Taxane-based chemotherapy with lapatinib or trastuzumab as first-line therapy for HER2-positive metastatic breast cancer |
| [NCT00553358](https://clinicaltrials.gov/study/NCT00553358) | Phase 3 | Completed | 455 | Neo-ALTTO: neoadjuvant lapatinib, trastuzumab or both plus paclitaxel in HER2-positive breast cancer |
| [NCT01822314](https://clinicaltrials.gov/study/NCT01822314) | Phase 3 | Completed | 632 | Neoadjuvant nab-paclitaxel versus solvent-based paclitaxel followed by anthracycline regimens |
| [NCT00513292](https://clinicaltrials.gov/study/NCT00513292) | Phase 3 | Completed | 280 | Neoadjuvant FEC then paclitaxel plus trastuzumab compared with the reverse sequence in HER2-positive operable breast cancer |
| [NCT00431080](https://clinicaltrials.gov/study/NCT00431080) | Phase 3 | Completed | 478 | Dose-dense adjuvant FE75C followed by docetaxel versus paclitaxel in node-positive breast cancer |
| [NCT00075270](https://clinicaltrials.gov/study/NCT00075270) | Phase 3 | Completed | 580 | Lapatinib plus paclitaxel versus paclitaxel alone in first-line metastatic breast cancer |
| [NCT05020860](https://clinicaltrials.gov/study/NCT05020860) | Phase 2 | Recruiting | 185 | Neoadjuvant systemic therapy study; paclitaxel-specific contribution unclear |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10791395](https://pubmed.ncbi.nlm.nih.gov/10791395/) | 2000 | Review | Lancet | Paclitaxel and docetaxel are accepted evidence-based components of therapy for advanced breast, lung and ovarian cancers; paclitaxel approved in the USA for adjuvant node-positive breast cancer |
| [9282422](https://pubmed.ncbi.nlm.nih.gov/9282422/) | 1997 | Review | Drug and Therapeutics Bulletin | Licence extended to first-line ovarian cancer and metastatic breast cancer |
| [11147586](https://pubmed.ncbi.nlm.nih.gov/11147586/) | 2000 | Phase 2 clinical study | Cancer | Doxorubicin plus paclitaxel in metastatic breast carcinoma; efficacy and toxicity, with prior anthracycline therapy influencing results |
| [11745249](https://pubmed.ncbi.nlm.nih.gov/11745249/) | 2001 | Clinical study | Cancer | Paclitaxel in multimodality treatment of inflammatory breast carcinoma |
| [9296218](https://pubmed.ncbi.nlm.nih.gov/9296218/) | 1997 | Dose-finding study | Annals of Oncology | Maximum tolerated doses of 3-hour paclitaxel with cyclophosphamide in advanced breast cancer |
| [10898354](https://pubmed.ncbi.nlm.nih.gov/10898354/) | 2000 | Review | Cancer | Developments in breast cancer chemotherapy, including taxanes |
| [24823476](https://pubmed.ncbi.nlm.nih.gov/24823476/) | 2014 | Translational | Nature Communications | TEKT4 germline variants enriched after treatment, linked to paclitaxel resistance |
| [39317691](https://pubmed.ncbi.nlm.nih.gov/39317691/) | 2024 | Computational/preclinical | Chemical Biology & Drug Design | Paclitaxel combinations and in vivo biomarkers in breast carcinoma |
| [20665703](https://pubmed.ncbi.nlm.nih.gov/20665703/) | 2011 | Preclinical | Journal of Cellular Physiology | ZD6474 enhances paclitaxel antiproliferative and apoptotic effects in breast carcinoma cells |
| [32086991](https://pubmed.ncbi.nlm.nih.gov/32086991/) | 2020 | Preclinical | Cancer Science | Targeting claudin-4 enhances chemosensitivity in triple-negative breast cancer |

## New Zealand Market Information

All 11 licences are cancelled; none are currently valid. Approved indication text was not available for any of them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2271834 | Anzatax | Inj 6 mg per ml, 16.7 ml vial | Cancelled |
| 2271826 | Anzatax | Inj 150 mg | Cancelled |
| 2207664 | Paclitaxel Ebewe | Inj 6 mg per ml, 16.7 ml vial | Cancelled |
| 2207672 | Paclitaxel Ebewe | Inj 150 mg | Cancelled |
| 2394596 | Paclitaxel Actavis | Inj 6 mg per ml, 50 ml vial | Cancelled |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (taxane, microtubule stabiliser) |
| Myelosuppression Risk | High (neutropenia is the main dose-limiting toxicity; the knowledge graph also links paclitaxel to neutropenia, agranulocytosis, thrombocytopenia and anaemia) |
| Emetogenicity Classification | Low |
| Monitoring Items | Complete blood count with differential, liver function, renal function, and neurological assessment for peripheral neuropathy |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

- **Disease associations flagged in the knowledge graph (not label text):** neutropenia, agranulocytosis, thrombocytopenia, anaemia, liver disease, kidney disease, gallbladder disease, peripheral neuropathy, myocardial infarction, sudden cardiac arrest, hypotensive disorder, asthma, diabetes mellitus and diabetic ketoacidosis, psoriasis, hyperbilirubinaemia (including Gilbert syndrome), and several psychiatric conditions (anxiety, neurotic and dysthymic disorders). These should be treated as signals to verify against the package insert, not confirmed contraindications.
- **Drug interactions:** the interaction query returned 100 interacting drugs; the first 20 are shown in the data compiled for this report. Moderate-level interactions include aprepitant, cimetidine, clarithromycin, clotrimazole, dexamethasone, eliglustat, metronomidazole, miconazole, rolapitant, rosuvastatin, simvastatin, tinidazole and troglitazone. Minor: ascorbic acid and levofloxacin. Several others (glimepiride, doxycycline, morphine, metformin, omeprazole) are of unknown severity.
- The literature also documents paclitaxel-related adverse events, including pneumonitis, peripheral neuropathy, corneal lesions, uveitis and hand-foot-type skin reactions.

Please refer to the package insert for formal warnings and contraindications.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Paclitaxel has a clear mechanistic fit and extensive trial activity in breast cancer, including several completed Phase 3 trials. However, most of those trials test combinations, so paclitaxel's own contribution is not isolated, and it is already standard of care. This is therefore confirmation of existing use rather than new repurposing. Evidence is rated L2 for this indication because of the limited paclitaxel-specific Phase 2/3 data in the compiled set. Lower-ranked predictions (Ehrlich tumour, bilateral and nipple carcinoma, rhabdomyosarcoma subtypes) should be held.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (a blocking data gap for safety screening), from the Medsafe data sheet
- Mechanism of action data from DrugBank
- Clarification of the original indication and of the New Zealand regulatory position, since all licences are cancelled and no approved indication text is available
- Confirmation of the paclitaxel-specific effect in breast cancer from monotherapy or head-to-head Phase 3 data
- A myelosuppression and neuropathy monitoring plan, and a review of drug interactions (particularly CYP-active agents)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


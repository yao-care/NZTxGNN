---
layout: default
title: Docetaxel
parent: Model Prediction Only (L5)
nav_order: 160
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Docetaxel: predicted new indication Female breast carcinoma (evidence level L5). Approved use: Non-small cell lung cancer"
monitor_inn: "DOCETAXEL"
predicted_indications: ["Ewing sarcoma", "well-differentiated fetal adenocarcinoma of the lung", "primary pulmonary lymphoma", "botryoid-type embryonal rhabdomyosarcoma of the vagina", "pulmonary blastoma", "rhabdomyosarcoma (disease)", "embryonal extrahepatic bile duct rhabdomyosarcoma", "parameningeal embryonal rhabdomyosarcoma"]
---

# Docetaxel
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

# Docetaxel: From Non-Small Cell Lung Cancer to Female Breast Carcinoma

## One-Sentence Summary

Docetaxel is a taxane chemotherapy drug, originally used for non-small cell lung cancer, prostate cancer, head and neck cancer, gastric cancer and breast cancer.
The TxGNN model predicts it may be effective for **female breast carcinoma**, although breast carcinoma is already a recognised use, so this is closer to confirmation than true repurposing.
The data compiled for this report list **46 clinical trials** for this indication (the top 40 are shown here, including 5 Phase 3 trials) and **20 publications**.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Non-small cell lung cancer (approved-indication text is not available from the New Zealand licence records; taken from the knowledge-graph indication list) |
| Predicted New Indication | Female breast carcinoma |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L2 (as assigned in the data; several large completed Phase 3 trials appear in the list, see the note below) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 5 (all cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, docetaxel is a taxane that stabilises microtubules and blocks cells from completing mitosis, so rapidly dividing tumour cells stop growing or die. Its efficacy in non-small cell lung cancer and other solid tumours is established.

Breast carcinoma is already among docetaxel's listed indications, so the prediction is consistent with existing use. The same antimitotic mechanism is applicable to breast tumours. Many of the trials below use docetaxel as part of established regimens (for example with doxorubicin and cyclophosphamide, or with trastuzumab).

Other predicted indications (rank 2 onward) are weaker. Ewing sarcoma and rhabdomyosarcoma have only small Phase 2 signals, mostly from gemcitabine plus docetaxel combinations. Small cell lung carcinoma, primary pulmonary lymphoma and several rare tumours have little or no specific evidence, and appear to be driven by disease-ontology proximity.

## Clinical Trial Evidence

The 10 most relevant trials are listed below, prioritising completed, larger studies where docetaxel is a central component.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00321633](https://clinicaltrials.gov/study/NCT00321633) | Phase 2 | Completed | 148 | Randomised comparison of carboplatin versus docetaxel in metastatic breast cancer (BRCA-related) |
| [NCT00003352](https://clinicaltrials.gov/study/NCT00003352) | Phase 2 | Completed | 89 | Doxorubicin, docetaxel and cyclophosphamide in metastatic or locally advanced breast cancer |
| [NCT00003953](https://clinicaltrials.gov/study/NCT00003953) | Phase 2 | Completed | 39 | Preoperative dose-dense doxorubicin then docetaxel in stage II-IIIB breast cancer |
| [NCT00379015](https://clinicaltrials.gov/study/NCT00379015) | Phase 2 | Completed | 38 | Neoadjuvant epirubicin/cyclophosphamide then docetaxel plus trastuzumab in HER2-positive disease |
| [NCT00333775](https://clinicaltrials.gov/study/NCT00333775) | Phase 3 | Completed | 736 | Bevacizumab plus docetaxel versus docetaxel plus placebo, first-line HER2-negative metastatic breast cancer |
| [NCT00667251](https://clinicaltrials.gov/study/NCT00667251) | Phase 3 | Completed | 652 | Taxane chemotherapy with lapatinib versus trastuzumab in HER2-positive metastatic breast cancer |
| [NCT00003519](https://clinicaltrials.gov/study/NCT00003519) | Phase 3 | Completed | 2,778 | Adriamycin/docetaxel versus adriamycin/cyclophosphamide, adjuvant, node-positive or high-risk breast cancer |
| [NCT00021255](https://clinicaltrials.gov/study/NCT00021255) | Phase 3 | Completed | 3,222 | AC-T versus AC-TH versus TCH (docetaxel, carboplatin, trastuzumab) in adjuvant HER2-positive breast cancer |
| [NCT00003782](https://clinicaltrials.gov/study/NCT00003782) | Phase 3 | Completed | 5,351 | Three adjuvant regimens (AC-T, AT, ATC) in node-positive breast cancer |
| [NCT00002707](https://clinicaltrials.gov/study/NCT00002707) | Phase 3 | Completed | 2,411 | Preoperative AC with or without docetaxel, before or after surgery |

Note: the data show at least five completed Phase 3 trials in which docetaxel is part of the regimen. However, in most of them docetaxel is part of a combination, and the relevance grading for these trials was not completed in the data. The evidence level assigned in the data (L2) is therefore conservative and should be reviewed.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28398846](https://pubmed.ncbi.nlm.nih.gov/28398846/) | 2017 | RCT | J Clin Oncol | ABC trials: docetaxel plus cyclophosphamide (TC) compared with standard anthracycline-taxane regimens in early breast cancer |
| [10791395](https://pubmed.ncbi.nlm.nih.gov/10791395/) | 2000 | Review | Lancet | Taxanes, including docetaxel, are evidence-based components of therapy for advanced breast, lung and ovarian cancers |
| [18624687](https://pubmed.ncbi.nlm.nih.gov/18624687/) | 2008 | Review | Expert Opin Drug Metab Toxicol | Lessons from docetaxel development: pharmacology, pharmacokinetics and pharmacogenetics |
| [12868800](https://pubmed.ncbi.nlm.nih.gov/12868800/) | 2003 | Review | Breast Cancer Res Treat | Docetaxel-anthracycline combinations in metastatic breast cancer; high activity and non-overlapping toxicity |
| [9775340](https://pubmed.ncbi.nlm.nih.gov/9775340/) | 1998 | Review | Am J Health Syst Pharm | Docetaxel pharmacology, trials and adverse effects in metastatic breast cancer |
| [9282422](https://pubmed.ncbi.nlm.nih.gov/9282422/) | 1997 | Review | Drug Ther Bull | Paclitaxel and docetaxel in breast and ovarian cancer |
| [16020974](https://pubmed.ncbi.nlm.nih.gov/16020974/) | 2005 | Phase 2 | Oncology | Weekly docetaxel plus gemcitabine as first-line treatment for metastatic breast cancer |
| [12599222](https://pubmed.ncbi.nlm.nih.gov/12599222/) | 2003 | Phase 2 | Cancer | Capecitabine with docetaxel and epirubicin in previously untreated advanced breast carcinoma |
| [27997437](https://pubmed.ncbi.nlm.nih.gov/27997437/) | 2017 | Retrospective | Anti-Cancer Drugs | Association between adjuvant docetaxel-based chemotherapy and breast cancer-related lymphedema |
| [19173816](https://pubmed.ncbi.nlm.nih.gov/19173816/) | 2008 | Clinical study | Zhonghua Zhong Liu Za Zhi | Docetaxel plus capecitabine in anthracycline-resistant advanced breast carcinoma |

## New Zealand Market Information

All five licences are cancelled; none is currently valid. Approved-indication text is not available for any of them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2571161 | Docetaxel Accord | Inj 20 mg per ml, 4 ml vial | Not available |
| 2440830 | DBL Docetaxel | Inj 10 mg per ml, 8 ml vial | Not available |
| 2207559 | Baxter | Inj 1 mg for ECP | Not available |
| 2428148 | Docetaxel Sandoz | Inj 20 mg | Not available |
| 2428156 | Docetaxel Sandoz | Inj 80 mg | Not available |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (taxane, antimitotic microtubule stabiliser) |
| Myelosuppression Risk | High. Neutropenia, thrombocytopenia, anaemia and agranulocytosis appear among the knowledge-graph contraindicated conditions |
| Emetogenicity Classification | Low to moderate (general class estimate; not stated in the compiled data) |
| Monitoring Items | Full blood count with differential, liver function and bilirubin, renal function, signs of neuropathy, fluid retention and stomatitis |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

The package insert warnings and contraindications were not retrieved, so please refer to the package insert for authoritative safety information. The following comes from knowledge-graph and interaction data.

- **Conditions flagged in the knowledge graph (not label text)**: liver disease, gallbladder disease, thrombocytopenia, neutropenia, agranulocytosis, anaemia, peripheral neuropathy and other peripheral nervous system disease, motor neuritis, hyperbilirubinaemia (including Gilbert syndrome), diarrhoeal disease, pulmonary oedema, stomatitis, and cardiac tamponade.
- **Drug Interactions**: 100 interactions were recorded; 20 are shown in the data. The only one rated Major is **clarithromycin**. Moderate interactions include aprepitant, metronidazole, cimetidine, eliglustat, miconazole, rolapitant, rosuvastatin, simvastatin, tinidazole and troglitazone. Minor interactions include dexamethasone, levofloxacin, prednisolone and prednisone. These are consistent with docetaxel being metabolised through CYP3A4, so strong CYP3A4 inhibitors need particular care.
- **Other literature signals**: the data include reports of fluid retention and peripheral oedema, interstitial pneumonitis, and ethanol content in the formulation causing measurable breath alcohol after infusion.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Breast carcinoma is an established use of docetaxel, supported by many completed trials, including large Phase 3 studies of docetaxel-containing regimens, and by multiple reviews. Its New Zealand licences are all cancelled, and safety documentation is incomplete, so use should be conditional on those gaps being closed.

**To proceed, the following is needed:**
- Confirm the current New Zealand supply and regulatory status, since no licence is valid.
- Obtain the package insert warnings and contraindications (blocking data gap for safety screening).
- Obtain detailed mechanism of action data.
- Review the Phase 3 trials to confirm docetaxel's contribution, and re-assess the evidence level.
- Set up a safety monitoring plan covering myelosuppression, neuropathy, hepatic impairment and CYP3A4 interactions.
- Treat the other predicted indications (Ewing sarcoma and rhabdomyosarcoma subtypes) as research questions only, and hold small cell lung carcinoma, primary pulmonary lymphoma, pulmonary blastoma and the rare tumours with no specific evidence.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


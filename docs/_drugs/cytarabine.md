---
layout: default
title: Cytarabine
parent: Model Prediction Only (L5)
nav_order: 129
evidence_level: L5
indication_count: 9
monitor_inn: "CYTARABINE"
predicted_indications: ["small cell lung carcinoma", "primary pulmonary lymphoma", "well-differentiated fetal adenocarcinoma of the lung", "pulmonary blastoma", "upper aerodigestive tract neoplasm", "ganglioneuroblastoma (disease)", "vertebral anomalies and variable endocrine and T-cell dysfunction", "retroperitoneal neoplasm", "neuroblastoma"]
last_modified_at: 2026-10-30
description: "Cytarabine: predicted new indication small cell lung carcinoma (evidence level L5). Approved use: Not recorded in the local data"
---

# Cytarabine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

## Cytarabine: pyrimidine nucleoside analogue (antimetabolite) → small cell lung carcinoma
{: .text-alpha }

## One-Sentence Summary

Cytarabine is a pyrimidine nucleoside analogue that blocks DNA synthesis in rapidly dividing cells and is used as a cytotoxic chemotherapy agent.
The TxGNN model predicts it may be effective for **small cell lung carcinoma**, but **no clinical trials** and **no cytarabine-specific publications on this disease** are in the data compiled for this report.
The score is high, yet the supporting evidence is indirect.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | small cell lung carcinoma |
| TxGNN Prediction Score | 0.9978 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (6 of 6 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 6 (valid: 6) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on general knowledge, cytarabine is a pyrimidine nucleoside analogue. It interferes with DNA synthesis, which makes it plausible as an agent against highly proliferative tumours such as small cell lung carcinoma.

However, the supporting literature does not study cytarabine in this disease. Most retrieved papers concern non-small cell lung cancer. These are 1990 phase II studies of cytarabine with cisplatin, in which the overall response rate was modest (14% in one study). Other papers cover related antimetabolites such as gemcitabine and decitabine, or topotecan.

One 1987 report, "High-dose cytarabine in small cell lung cancer" (PMID 3030547), directly names the disease. No abstract is available, so no findings can be drawn from it. Because nothing in the data demonstrates efficacy in this disease, the evidence level is L5, and the high TxGNN score should be treated as a hypothesis, not as confirmation.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3030547](https://pubmed.ncbi.nlm.nih.gov/3030547/) | 1987 | Clinical report (type not classified) | Cancer Treat Rep | High-dose cytarabine in small cell lung cancer; no abstract available, so findings cannot be summarised |
| [2158614](https://pubmed.ncbi.nlm.nih.gov/2158614/) | 1990 | Randomized phase II | Med Pediatr Oncol | Cisplatin-cytarabine vs cisplatin-vinblastine in 151 patients with advanced NSCLC (not SCLC) |
| [2156598](https://pubmed.ncbi.nlm.nih.gov/2156598/) | 1990 | Phase II | Cancer | High-dose cytarabine plus cisplatin in 37 untreated NSCLC patients; response rate 14%, Grade IV myelosuppression in 32% |
| [2157307](https://pubmed.ncbi.nlm.nih.gov/2157307/) | 1990 | Phase II | Tumori | Cisplatin, vindesine and cytarabine in 32 patients with advanced NSCLC (not SCLC) |
| [1332622](https://pubmed.ncbi.nlm.nih.gov/1332622/) | 1992 | Review | Gan To Kagaku Ryoho | Gemcitabine and DMDC are related to cytarabine; many solid tumours are insensitive to cytarabine |
| [28223673](https://pubmed.ncbi.nlm.nih.gov/28223673/) | 2017 | Case report | Gan To Kagaku Ryoho | Meningeal carcinomatosis with small cell lung carcinoma treated with a multidisciplinary approach |
| [1360876](https://pubmed.ncbi.nlm.nih.gov/1360876/) | 1992 | In vitro | Cancer Chemother Pharmacol | Drug sensitivity patterns in SCLC cell lines (doxorubicin, etoposide, vincristine, carmustine) |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 786365 | Pfizer | Inj 20 mg per ml, 5 ml vial | Not recorded |
| 2359537 | Pfizer | Inj 100 mg per ml, 20 ml vial | Not recorded |
| 2695758 | Pfizer | Inj 20 mg per ml, 5 ml vial | Not recorded |
| 2204231 | Baxter | Inj 100 mg intrathecal syringe for ECP | Not recorded |
| 341711 | Cytarabine DBL | Inj 100 mg per ml, 20 ml vial | Not recorded |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (pyrimidine antimetabolite) |
| Myelosuppression Risk | High; a cited phase II study reported Grade IV myelosuppression in 32% of patients on high-dose cytarabine with cisplatin |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Complete blood count with differential, liver and renal function |
| Handling Protection | Must follow cytotoxic drug handling regulations |

---

## Safety Considerations

- **Drug Interactions**: Interaction screening returned 100 records. Those listed include naltrexone (Moderate) and levofloxacin (Minor). Others, including mannitol, pantoprazole, metformin, morphine, omeprazole, vancomycin and prednisone, have an unknown severity level.

For label warnings and contraindications, please refer to the package insert.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No clinical trials are registered for this prediction, and the retrieved publications mostly address non-small cell lung cancer or other drugs. The very high TxGNN score is not backed by cytarabine-specific clinical data in small cell lung carcinoma.

**To proceed, the following is needed:**
- Retrieve and review the full text of the 1987 high-dose cytarabine study in small cell lung cancer (PMID 3030547)
- Mechanism of action data and a direct mechanistic rationale for small cell lung carcinoma
- Package insert warnings and contraindications from Medsafe
- Preclinical or early-phase clinical data of cytarabine in small cell lung carcinoma, including comparison with current standard therapy
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


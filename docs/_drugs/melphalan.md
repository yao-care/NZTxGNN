---
layout: default
title: Melphalan
parent: Model Prediction Only (L5)
nav_order: 284
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-28
monitor_inn: "MELPHALAN"
predicted_indications: ["female breast carcinoma", "rectum mucinous adenocarcinoma", "colon mucinous adenocarcinoma", "gallbladder mucinous adenocarcinoma", "cervical mucinous adenocarcinoma", "endometrial mucinous adenocarcinoma"]
description: "Melphalan: predicted new indication Female breast carcinoma (evidence level L5). Approved use: Not recorded in the local data"
---

# Melphalan
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

## Melphalan: Female breast carcinoma
{: .text-alpha }

## One-Sentence Summary

with **7 clinical trials** and **1 publication** currently linked to this prediction. Most of the trials test melphalan only as part of combination high-dose regimens, and the single publication is a 1964 paper with no abstract.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Female breast carcinoma |
| TxGNN Prediction Score | 0.9967 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 3 of 3) |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Melphalan is a bifunctional nitrogen mustard that alkylates DNA and forms interstrand crosslinks. Detailed mechanism-of-action data were not available in the data compiled for this report, so this description reflects the general understanding of the drug class.

Germ cell tumours are generally very sensitive to DNA-damaging agents. This is the main reason melphalan has been tried in high-dose regimens with autologous stem cell rescue for relapsed or poor-prognosis disease. Ovarian cancer, the original indication, shares a gonadal origin and some chemosensitivity features with gonadal germ cell tumours, which fits the TxGNN prediction.

An important caveat: in the available trials melphalan is given within multi-drug regimens, so its individual contribution cannot be separated out.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00936936](https://clinicaltrials.gov/study/NCT00936936) | Phase 2 | Completed | 64 | Two cycles of high-dose chemotherapy (gemcitabine, docetaxel, melphalan, carboplatin; then ifosfamide, carboplatin, etoposide) in poor-prognosis relapsed germ-cell tumours. Directly on-indication, but combination-based. |
| [NCT00060255](https://clinicaltrials.gov/study/NCT00060255) | Phase 2 | Completed | 451 | Eight high-dose chemotherapy regimens with autologous transplant in haematologic malignancies and selected solid tumours. The germ cell tumour subset and melphalan's role are not specified. |
| [NCT00003425](https://clinicaltrials.gov/study/NCT00003425) | Phase 1/2 | Completed | 25 | Escalating-dose melphalan with stem cell support and amifostine in mixed cancers. Melphalan is the central agent. |
| [NCT00638898](https://clinicaltrials.gov/study/NCT00638898) | Phase 1 | Completed | 25 | Busulfan, melphalan and topotecan with autologous transplant in advanced and recurrent tumours. |
| [NCT00536601](https://clinicaltrials.gov/study/NCT00536601) | N/A | Completed | 174 | High-dose chemotherapy with or without total-body irradiation before autologous transplant. Germ cell evidence is non-specific. |
| [NCT00002750](https://clinicaltrials.gov/study/NCT00002750) | Phase 1 | Completed | 6 | Intrathecal melphalan in neoplastic meningitis. Different route and setting. |
| [NCT00003926](https://clinicaltrials.gov/study/NCT00003926) | Phase 1 | Terminated | 13 | Amifostine chemoprotection with transplant in paediatric solid tumours. Focuses on toxicity mitigation, not efficacy. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [14151951](https://pubmed.ncbi.nlm.nih.gov/14151951/) | 1964 | Other | Acta - Unio Internationalis Contra Cancrum | Early study of hormonal and alkylating drugs and their effect on pituitary follicle-stimulating function. No abstract is available, and it offers little direct support for efficacy. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 783781 | Alkeran (valid) | Inj 50 mg | Not available |
| 200271 | Alkeran (valid) | Tab 2 mg | Not available |
| 2647699 | Melpha (valid) | Inj 50 mg | Not available |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (alkylating agent, nitrogen mustard class) |
| Myelosuppression Risk | High. Leukopenia, neutropenia, thrombocytopenia and anaemia appear among the knowledge-graph contraindications, consistent with marrow toxicity. High-dose regimens require stem cell rescue. |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Complete blood count with differential, liver and renal function |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

- **Conditions flagged in the knowledge graph** (drug-disease contraindication links, not label text): kidney disease, thrombocytopenia, anaemia (including macrocytic and haemolytic), leukopenia, neutropenia and agranulocytosis (including congenital and idiopathic forms), interstitial lung disease, pulmonary fibrosis, drug-induced liver injury, vasculitis, and pyoureter.
- **Drug Interactions**: The query returned 100 interactions, 20 of which were retrieved for this report. Moderate-level interactions are with amphotericin B (conventional, lipid complex, cholesteryl sulfate and liposomal formulations). Minor interactions are with cimetidine, levofloxacin, zinc sulfate and zinc gluconate. Others, such as lansoprazole, omeprazole, pantoprazole, famotidine, prednisone, morphine and metformin, are of unknown severity.
- **Long-term risk**: One review in the literature notes that alkylating agents such as melphalan carry some of the highest rates of second malignancies, mostly acute leukaemias.

Please refer to the package insert for label warnings and contraindications, which were not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high and one completed Phase 2 trial is directly on-indication, but melphalan is only one part of a combination regimen and its own contribution is unknown. The study is best treated as a research question rather than a candidate ready for development.

**To proceed, the following is needed:**
- Results and subgroup data from NCT00936936, ideally separating melphalan's contribution
- Retrieval of the package insert warnings and contraindications (a blocking data gap for safety screening)
- Mechanism-of-action data from DrugBank
- A review of why the New Zealand licences were cancelled and whether any product is available for import or special access
- A safety and monitoring plan for high-dose use, including stem cell support and management of marrow suppression
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


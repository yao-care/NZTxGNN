---
layout: default
title: Vinblastine
parent: Moderate Evidence (L3-L4)
nav_order: 481
evidence_level: L3
indication_count: 10
last_modified_at: 2026-10-15
description: "Vinblastine: predicted new indication Rhabdomyosarcoma (evidence level L3). Approved use: Lymphomas (Hodgkin, non-Hodgkin, mantle cell…"
monitor_inn: "vinblastine"
predicted_indications: ["rhabdomyosarcoma (disease)", "botryoid-type embryonal rhabdomyosarcoma of the vagina", "parameningeal embryonal rhabdomyosarcoma", "embryonal extrahepatic bile duct rhabdomyosarcoma", "prostate embryonal rhabdomyosarcoma", "extrahepatic bile duct rhabdomyosarcoma", "liver sarcoma", "monocytic leukemia", "neuroblastoma", "ganglioneuroblastoma (disease)"]
---

# Vinblastine
{: .fs-9 }

Evidence Level: **L3** | Predicted Indications: **10** 
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

# Vinblastine: From Hodgkin Lymphoma and Other Lymphomas to Rhabdomyosarcoma

## One-Sentence Summary

Vinblastine is a vinca alkaloid chemotherapy drug, originally used for lymphomas (including Hodgkin lymphoma and mycosis fungoides) and Langerhans cell histiocytosis.
The TxGNN model predicts it may be effective for **rhabdomyosarcoma**,
but no clinical trials are registered, and the **7 publications** compiled for this report are mostly case reports, preclinical work and indirect studies.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Lymphomas (Hodgkin, non-Hodgkin, mantle cell, cutaneous T-cell lymphoma/mycosis fungoides) and Langerhans cell histiocytosis (from the knowledge graph; no approved-indication text is available from New Zealand licences) |
| Predicted New Indication | Rhabdomyosarcoma |
| TxGNN Prediction Score | 99.86% |
| Evidence Level | L3 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, vinblastine is a vinca alkaloid that binds tubulin and inhibits microtubule assembly. This causes mitotic arrest in rapidly dividing cells.

Rhabdomyosarcoma is a highly proliferative tumour, so an antimitotic mechanism is plausible. Vinca alkaloids, mainly vincristine, are already a backbone of rhabdomyosarcoma regimens. Vinblastine itself appears in a pediatric metronomic regimen and in case-level combination reports. Preclinical work also suggests synthetic lethality between PLK1 inhibition and microtubule-destabilizing drugs in rhabdomyosarcoma models.

Important caveat: the clinical reports are mostly combination regimens, so vinblastine's own contribution cannot be separated out. The predicted indication also lacks any controlled evidence.

Other predictions in the list (rank 2–10) are mostly rhabdomyosarcoma subtypes with prediction-only support (L5). Two others have limited literature: monocytic leukaemia (L3, old small uncontrolled series) and neuroblastoma (L4, preclinical only).

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38050209](https://pubmed.ncbi.nlm.nih.gov/38050209/) | 2023 | Case report/Review | Medicine | Adult perianal rhabdomyosarcoma; partial response after nivolumab, dacarbazine, cisplatin and vinblastine, followed by surgery |
| [41216926](https://pubmed.ncbi.nlm.nih.gov/41216926/) | 2026 | Cohort (indirect) | Pediatric Blood & Cancer | CWS-96 and CWS-2002P trials in non-rhabdomyosarcoma soft tissue sarcoma; risk stratification and chemotherapy evaluation |
| [22156656](https://pubmed.ncbi.nlm.nih.gov/22156656/) | 2011 | Pilot study | Oncotarget | Pilot of a pediatric metronomic 4-drug regimen |
| [2451411](https://pubmed.ncbi.nlm.nih.gov/2451411/) | 1987 | Case report/Review | Hinyokika Kiyo | Refractory prostatic rhabdomyosarcoma in a child; rapid mass reduction with cisplatin, vinblastine and peplomycin |
| [26024389](https://pubmed.ncbi.nlm.nih.gov/26024389/) | 2015 | Preclinical | Cell Death & Differentiation | Synthetic lethality of PLK1 inhibitors with microtubule-destabilizing drugs in rhabdomyosarcoma models |
| [3329524](https://pubmed.ncbi.nlm.nih.gov/3329524/) | 1987 | Preclinical | Anti-Cancer Drug Design | Therapeutic selectivity of vinca alkaloids examined in rhabdomyosarcoma xenografts in mice |
| [14328283](https://pubmed.ncbi.nlm.nih.gov/14328283/) | 1965 | Historical review | British Journal of Clinical Practice | Overview of tumour chemotherapy in children |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 779989 | Hospira | Inj 1 mg per ml, 10 ml vial | Not available (cancelled) |
| 2204061 | Baxter | Inj 1 mg for ECP | Not available (cancelled) |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (vinca alkaloid, antimitotic) |
| Myelosuppression Risk | High (neutropenia is the main dose-limiting toxicity; the knowledge graph also flags thrombocytopenia, neutropenia and agranulocytosis) |
| Emetogenicity Classification | Low to moderate (please refer to the package insert warnings and precautions) |
| Monitoring Items | CBC with differential, liver function (hepatic impairment is flagged), renal function, neurological assessment for peripheral neuropathy |
| Handling Protection | Must follow cytotoxic drug handling regulations; intravenous use only |

---

## Safety Considerations

- **Contraindications (from the knowledge graph, not label text):** liver failure, thrombocytopenia, neutropenia, agranulocytosis, peripheral neuropathy and other peripheral nervous system disease, hyperbilirubinemia (including Gilbert syndrome), lung disease, myocardial ischemia/coronary artery disease, and infectious diseases (including fungal infection and dermatophytosis). Anxiety, neurotic and dysthymic disorders are also listed. These edges should be verified against the official label.
- **Drug Interactions:** 371 interactions were identified. Notable examples:
  - **Major:** clarithromycin.
  - **Moderate:** aprepitant, dexamethasone, metronidazole, eliglustat, miconazole, rolapitant, rosuvastatin, simvastatin, tinidazole, glycerol phenylbutyrate, troglitazone.
  - **Minor:** levofloxacin.
  - Several others (for example pantoprazole, omeprazole, prednisone, ondansetron) have an unknown severity level.
- **Neurotoxicity:** literature on neuronal tumours notes a neurotoxicity concern linked to β-III tubulin, and a case of severe ileus was reported with cisplatin plus vinblastine infusion.

Package insert warnings and contraindications were not available and should be obtained for a full safety review.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The antimitotic mechanism makes rhabdomyosarcoma biologically plausible and vinca alkaloids are already used in this disease. However, no clinical trials exist, the clinical evidence is limited to case reports and indirect studies in combination regimens, and vinblastine currently has no valid New Zealand licence. The evidence supports a research question (L3), not a development decision.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- A systematic comparison of vinblastine with vincristine, the standard vinca alkaloid in rhabdomyosarcoma regimens
- Literature review separating vinblastine's contribution from combination regimens
- Assessment of regulatory and supply feasibility, given that both New Zealand licences are cancelled
- A safety plan covering myelosuppression, neurotoxicity, hepatic impairment and CYP3A4-related interactions
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Ruxolitinib
parent: Model Prediction Only (L5)
nav_order: 415
evidence_level: L5
indication_count: 10
---

# Ruxolitinib
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

# Ruxolitinib: From Myeloproliferative Neoplasms to Uterine Corpus Perivascular Epithelioid Cell Tumor

## One-Sentence Summary

Ruxolitinib is a JAK1/2 inhibitor originally used for myeloproliferative neoplasms such as polycythemia vera and myelofibrosis.
The TxGNN model's top-ranked prediction is **uterine corpus perivascular epithelioid cell tumor**, which has **no clinical trials and no publications** behind it, so it rests on model prediction alone.
The best-supported alternative direction is **hemophagocytic lymphohistiocytosis (HLH)**, with **20 publications** (no registered trials), mostly case series, retrospective cohorts and one prospective stratified study.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Polycythemia vera, myelofibrosis and other myeloproliferative neoplasms (from the knowledge graph; no approved-indication text in the New Zealand licence records) |
| Predicted New Indication | Uterine corpus perivascular epithelioid cell tumor |
| TxGNN Prediction Score | 99.73% |
| Evidence Level | L5 (for the top-ranked prediction) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the data compiled for this report. Based on general knowledge, ruxolitinib inhibits JAK1 and JAK2, which blocks JAK-STAT signalling. This is the pathway that drives myeloproliferative neoplasms and many cytokine-mediated inflammatory states.

For the top-ranked prediction (uterine PEComa), the link is speculative. PEComa is mainly associated with mTOR/TSC pathway activation, and any crosstalk with JAK-STAT is unsupported by the supplied data. The high TxGNN score most likely reflects similarity to neighbouring nodes in the knowledge graph. The same applies to the other PEComa-family and lymphangioleiomyomatosis predictions (ranks 2–4, 7). In lymphangioleiomyomatosis the established therapy targets mTOR, and no JAK-inhibitor evidence was provided. Benign PEComa generally does not require systemic therapy, so its clinical rationale is weak.

Two other predictions have a more credible rationale:

- **Hemophagocytic lymphohistiocytosis (HLH; ranks 9–10):** HLH is a hyperinflammatory syndrome driven by interferon-gamma and cytokine signalling through JAK-STAT. Murine models show a mechanistic benefit with ruxolitinib, and clinical reports exist, mainly in infection-associated (EBV) HLH.
- **Liposarcoma (rank 5):** Preclinical work shows JAK-STAT signalling controls cancer stem cell properties and chemotherapy resistance in myxoid liposarcoma. This supports a research hypothesis only, with no clinical data.

## Clinical Trial Evidence

Currently no related clinical trials registered for any of the ten predicted indications.

## Literature Evidence

No literature was provided for the top-ranked prediction (uterine PEComa). The table below covers the best-supported prediction, HLH associated with infection (rank 9). It lists a selection of 10 of the 20 publications, prioritising clinical studies. Study types are inferred from titles and abstracts and should be verified against the full texts.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35344583](https://pubmed.ncbi.nlm.nih.gov/35344583/) | 2022 | Prospective stratified study | Blood | Ruxolitinib as first-line agent in paediatric HLH, with treatment stratified by early response |
| [40665481](https://pubmed.ncbi.nlm.nih.gov/40665481/) | 2025 | Retrospective cohort | Br J Haematol | Ruxolitinib-based regimen (n=53) compared with adjusted HLH-94 chemotherapy (n=42) in paediatric EBV-HLH |
| [33523540](https://pubmed.ncbi.nlm.nih.gov/33523540/) | 2021 | Case series | Eur J Haematol | Ruxolitinib as adjunctive therapy in secondary HLH |
| [31879790](https://pubmed.ncbi.nlm.nih.gov/31879790/) | 2020 | Retrospective cohort | Ann Hematol | Ruxolitinib for steroid-refractory acute GVHD in 12 patients with EBV-HLH undergoing allo-HSCT |
| [37787838](https://pubmed.ncbi.nlm.nih.gov/37787838/) | 2023 | Case analysis | Ann Hematol | Sintilimab plus ruxolitinib as compassionate therapy in 12 adults with EBV-HLH |
| [41934358](https://pubmed.ncbi.nlm.nih.gov/41934358/) | 2026 | Retrospective survey | Pediatr Hematol Oncol | Nationwide Japanese survey of ruxolitinib use in 17 children with HLH |
| [38691058](https://pubmed.ncbi.nlm.nih.gov/38691058/) | 2024 | Case report | J Pediatr Hematol Oncol | Emapalumab plus ruxolitinib and dexamethasone in EBV-HLH with multiorgan damage and severe infection |
| [36263041](https://pubmed.ncbi.nlm.nih.gov/36263041/) | 2022 | Case report | Front Immunol | Ruxolitinib as first-line therapy for secondary HLH in patients with AIDS |
| [31015190](https://pubmed.ncbi.nlm.nih.gov/31015190/) | 2019 | Preclinical (murine) | Blood | Mechanisms of ruxolitinib action in murine HLH models |
| [31943120](https://pubmed.ncbi.nlm.nih.gov/31943120/) | 2020 | Review | QJM | Review of adult HLH |

For liposarcoma (rank 5), only two preclinical studies on JAK-STAT signalling in myxoid liposarcoma are available (PMIDs [35186752](https://pubmed.ncbi.nlm.nih.gov/35186752/) and [30650179](https://pubmed.ncbi.nlm.nih.gov/30650179/)).

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2554763 | Jakavi | Tab 5 mg | Cancelled; indication text not available |
| 2627817 | Jakavi | Tab 10 mg | Cancelled; indication text not available |
| 2554771 | Jakavi | Tab 15 mg | Cancelled; indication text not available |
| 2554798 | Jakavi | Tab 20 mg | Cancelled; indication text not available |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (JAK1/2 kinase inhibitor), not a conventional cytotoxic agent |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions (typically blood counts and liver and renal function) |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Drug Interactions:** The interaction screen returned 100 interacting drugs. The sample reviewed includes the following:
  - **Major:** clarithromycin, cobicistat, deferasirox, deferiprone, ibritumomab tiuxetan, tositumomab, tositumomab (I-131), samarium (153Sm) lexidronam
  - **Moderate:** dexamethasone, rosuvastatin, dexfenfluramine, fenfluramine, sibutramine, troglitazone, palifermin, strontium chloride Sr-89

  Dexamethasone is a notable moderate interaction because it is part of standard HLH therapy.

Package insert warnings and contraindications could not be retrieved, so please refer to the package insert for them.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (uterine PEComa) is supported only by a model score, with no trials or publications, and a mechanistic link that is speculative. Ruxolitinib is also currently not validly licensed in New Zealand (all four licences cancelled). HLH is the most credible repurposing direction (L3 evidence, mostly non-randomized), but it is a lower-ranked prediction and would need its own evaluation. Liposarcoma remains a preclinical research question.

**To proceed, the following is needed:**
- Mechanism-of-action data for ruxolitinib to support the mechanistic-link analysis
- Package insert warnings and contraindications from Medsafe, which are required before any safety screening
- Clarification of why the licences were cancelled, and the current New Zealand supply route
- For HLH: full-text verification of study designs, and ideally a prospective or randomised trial before any decision beyond a research question
- For uterine PEComa: preclinical or clinical evidence of JAK-STAT dependence, compared against mTOR-targeted therapy
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Fluorouracil
parent: Model Prediction Only (L5)
nav_order: 207
evidence_level: L5
indication_count: 10
---

# Fluorouracil
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

# Fluorouracil: From Gastric and Colorectal Cancer to Rhabdomyosarcoma

## One-Sentence Summary

Fluorouracil is an antimetabolite chemotherapy drug, originally used for gastrointestinal cancers (gastric, pancreatic, colon metastases) and, in topical form, for skin conditions such as actinic keratosis and superficial basal cell carcinoma.
The TxGNN model predicts it may be effective for **botryoid-type embryonal rhabdomyosarcoma of the vagina**, but this is a model prediction only, with **0 clinical trials** and **0 publications** directly supporting it.
The related predicted indication "rhabdomyosarcoma (disease)" has 4 loosely related publications, none providing controlled evidence for fluorouracil.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Gastric cancer, pancreatic adenocarcinoma, colon cancer metastases, actinic keratosis, superficial basal cell carcinoma (no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Botryoid-type embryonal rhabdomyosarcoma of the vagina |
| TxGNN Prediction Score | 99.75% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 5 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, fluorouracil is an antimetabolite that inhibits thymidylate synthase and is incorporated into RNA and DNA. This disrupts DNA synthesis in rapidly dividing cells. Its efficacy in gastrointestinal cancers is established.

The predicted indications are mostly rhabdomyosarcoma subtypes (vaginal botryoid, parameningeal, prostate, and bile duct), plus liver sarcoma and three sickle cell–hemoglobin syndromes. For the sarcomas, the rationale is a generic antimetabolite effect on proliferative tumours. This is probably a subtype-level extrapolation from related knowledge-graph nodes rather than evidence of specific activity. Standard rhabdomyosarcoma regimens (e.g., vincristine, actinomycin D, cyclophosphamide) do not rely on fluorouracil. For the bile duct subtype, fluorouracil's role in biliary adenocarcinoma does not demonstrate activity in this rare embryonal sarcoma. For the sickle cell syndromes, the only theoretical link is fetal hemoglobin induction (as with hydroxyurea). This is unproven for fluorouracil, and its myelosuppressive toxicity is a concern in chronic non-malignant disease.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

No literature was retrieved for the top-ranked prediction. For the second-ranked prediction, rhabdomyosarcoma (disease), the following papers were retrieved:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3884137](https://pubmed.ncbi.nlm.nih.gov/3884137/) | 1985 | Review | Cancer | Adjuvant chemotherapy for childhood sarcomas, including rhabdomyosarcoma; no fluorouracil-specific evidence |
| [6277228](https://pubmed.ncbi.nlm.nih.gov/6277228/) | 1981 | Review | Ann Acad Med Singapore | General review of curability of cancers with chemotherapy, listing rhabdomyosarcoma among curable childhood cancers |
| [42483493](https://pubmed.ncbi.nlm.nih.gov/42483493/) | 2026 | Case report | Case Rep Oncol Med | Adult lingual rhabdomyosarcoma; cisplatin plus 5-fluorouracil was followed by disease progression |
| [3951398](https://pubmed.ncbi.nlm.nih.gov/3951398/) | 1986 | Cohort | Med Pediatr Oncol | Childhood nasopharyngeal carcinoma series; not directly relevant to rhabdomyosarcoma |

The only paper reporting fluorouracil use in rhabdomyosarcoma shows disease progression on a cisplatin plus fluorouracil regimen, so the literature provides no support for efficacy.

---

## New Zealand Market Information

All five licences are cancelled, and approved indication text is not available.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 253065 | Efudix | Cream 5% | Cancelled |
| 2618435 | Fluorouracil Accord | Injection 50 mg/ml, 20 ml vial | Cancelled |
| 2674092 | Fluorouracil Accord | Injection 50 mg/ml, 50 ml vial | Cancelled |
| 2618427 | Fluorouracil Accord | Injection 50 mg/ml, 100 ml vial | Cancelled |
| 2370123 | Baxter | Injection 1 mg for ECP | Cancelled |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (fluoropyrimidine antimetabolite) |
| Myelosuppression Risk | High to moderate; the knowledge graph links fluorouracil to thrombocytopenia, neutropenia, leukopenia and agranulocytosis as contraindicated conditions |
| Emetogenicity Classification | Low to moderate |
| Monitoring Items | CBC with differential, liver and renal function, electrolytes; consider DPD deficiency screening |
| Handling Protection | Follow cytotoxic drug handling regulations for injectable forms |

---

## Safety Considerations

- **Contraindications (from the knowledge graph, not label text)**: kidney disease, liver disease, gallbladder disease, pyoureter, thrombocytopenia, neutropenia, agranulocytosis, leukopenia, diarrheal disease, active peptic ulcer disease, pharyngitis, stomatitis, and dihydropyrimidine dehydrogenase (DPD) deficiency.
- **Drug Interactions**: 100 interactions were found in total. The notable ones are Moderate: metronidazole and tinidazole. Minor: cimetidine and levofloxacin. Many others have unknown severity (e.g., calcitriol, doxycycline, omeprazole, pantoprazole, metformin, morphine).

Please also refer to the package insert, as label warnings were not available.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predictions are supported only by a high model score (about 99.7%) with no registered trials and no direct controlled literature. The one case report involving fluorouracil in rhabdomyosarcoma showed progression. Standard rhabdomyosarcoma therapy does not use fluorouracil. In addition, all New Zealand licences are cancelled, and the drug carries significant myelosuppressive toxicity.

**To proceed, the following is needed:**
- Preclinical data (e.g., in vitro sensitivity of rhabdomyosarcoma cell lines) and a systematic literature search for fluorouracil in rhabdomyosarcoma
- Detailed mechanism of action data
- Package insert warnings and contraindications for current New Zealand-available products, and confirmation of the current regulatory and supply status
- A comparison against standard rhabdomyosarcoma regimens to justify any role for fluorouracil
- A DPD deficiency screening and haematological monitoring plan if any clinical use is considered
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


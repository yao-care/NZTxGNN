---
layout: default
title: Everolimus
parent: High Evidence (L1-L2)
nav_order: 190
evidence_level: L2
indication_count: 10
last_modified_at: 2026-10-15
description: "Everolimus: predicted new indication Liposarcoma (evidence level L2). Approved use: Renal cell carcinoma (also breast carcinoma, kidney angiomyolipoma…"
monitor_inn: "EVEROLIMUS"
predicted_indications: ["liposarcoma", "ovarian myxoid liposarcoma", "dermatofibrosarcoma protuberans", "parameningeal embryonal rhabdomyosarcoma", "botryoid-type embryonal rhabdomyosarcoma of the vagina", "embryonal extrahepatic bile duct rhabdomyosarcoma", "rhabdomyosarcoma (disease)", "prostate embryonal rhabdomyosarcoma", "unclassified renal cell carcinoma", "renal cell carcinoma associated with Xp11.2 translocations/TFE3 gene fusions"]
---

# Everolimus
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

# Everolimus: From Renal Cell Carcinoma to Liposarcoma

## One-Sentence Summary

Everolimus is an mTOR inhibitor originally used for renal cell carcinoma, breast cancer, and tuberous sclerosis-related conditions.
The TxGNN model predicts it may be effective for **liposarcoma**, with **1 clinical trial** (a Phase 2 combination study) and **4 publications** currently supporting this direction. Only one of these publications reports clinical results; the others are preclinical or not specific to everolimus.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Renal cell carcinoma (also breast carcinoma, kidney angiomyolipoma, tuberous sclerosis) |
| Predicted New Indication | Liposarcoma |
| TxGNN Prediction Score | 99.88% |
| Evidence Level | L2 |
| New Zealand Market Status | Cancelled only (no currently valid authorizations) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Proceed with Guardrails (research-stage only) |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known information, everolimus inhibits mTORC1, a key node downstream of PI3K/AKT signalling that drives tumour growth and survival. Its efficacy in renal cell carcinoma is established.

Dedifferentiated liposarcoma is characterised by CDK4/MDM2 amplification. Combining mTOR inhibition with the CDK4/6 inhibitor ribociclib targets two converging proliferative pathways: cell cycle control and growth/survival signalling. Combination and preclinical data support the pathway relevance.

An important caveat is that the supporting clinical evidence is a single-arm combination study, so the contribution of everolimus alone cannot be separated out.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03114527](https://clinicaltrials.gov/study/NCT03114527) | Phase 2 | Active, not recruiting | 48 | Ribociclib + everolimus in advanced dedifferentiated liposarcoma (Arm A) and leiomyosarcoma (Arm B) after at least one prior systemic therapy; two-centre, single-arm design assessing anti-tumour activity |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37967116](https://pubmed.ncbi.nlm.nih.gov/37967116/) | 2024 | Phase 2 trial report | Clin Cancer Res | Ribociclib + everolimus in advanced dedifferentiated liposarcoma and leiomyosarcoma; the combination showed biological synergy in tumour models |
| [36003796](https://pubmed.ncbi.nlm.nih.gov/36003796/) | 2022 | Review | Front Oncol | Patient-derived orthotopic xenograft models identify combination therapies with the CDK inhibitor palbociclib in sarcomas |
| [29848686](https://pubmed.ncbi.nlm.nih.gov/29848686/) | 2018 | Preclinical | Anticancer Res | Eribulin combined with other anticancer agents; not everolimus-specific |
| [41991999](https://pubmed.ncbi.nlm.nih.gov/41991999/) | 2026 | Preclinical | Oncogene | XPO1 inhibitor KPT-330 in dedifferentiated liposarcoma; not everolimus-specific |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2334879 | Afinitor (cancelled) | Tab 10 mg | Not available |
| 2334860 | Afinitor (cancelled) | Tab 5 mg | Not available |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (mTOR inhibitor), not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions (the knowledge graph links everolimus with neutropenia, thrombocytopenia, leukopenia, and anaemia) |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | CBC, liver and renal function, blood glucose and lipids (suggested by the knowledge graph contraindication signals) |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: liver failure and liver disease, kidney disease, thrombocytopenia, neutropenia, leukopenia, anaemia, hyperglycaemia and diabetes mellitus, diabetic ketoacidosis, hyperlipidaemia (including hypertriglyceridaemia and familial hyperlipidaemia), interstitial lung disease, congestive heart failure, stomatitis, haemolytic-uraemic syndrome, pericardial effusion, and lymphoma. These are knowledge-graph signals and should be verified against the official label.
- **Drug Interactions** (100 interactions recorded; examples):
  - Major: amphotericin B, aprepitant, dexamethasone, clarithromycin
  - Moderate: antidiabetic agents (metformin, acarbose, glimepiride, SGLT2 inhibitors, DPP-4 inhibitors, GLP-1 agonists), cimetidine, eliglustat, eluxadoline
  - Minor: zinc sulfate

Package insert warnings and contraindications were not available in the compiled data. Please refer to the package insert for authoritative safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails** (as a research question, not for clinical adoption)

**Rationale:**
- A completed-stage Phase 2 trial with published results suggests activity in dedifferentiated liposarcoma. However, it is a single-arm combination study, so everolimus-specific benefit is unproven. Everolimus has no currently valid New Zealand authorization.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data
- Randomized or everolimus-specific evidence in liposarcoma
- Review of the cancelled New Zealand authorizations, and a regulatory pathway for any use

**Other predicted indications (for reference):**
- **Unclassified renal cell carcinoma** has the strongest evidence among them (L2). The data include the randomized Phase 2 ASPEN trial (everolimus vs sunitinib) and several everolimus combination studies in non-clear-cell RCC. It is a reasonable candidate for follow-up.
- **Rhabdomyosarcoma** has L2 evidence from early-phase pediatric trials only (NCT00187174 Phase 1, NCT03245151 Phase 1/2), in mixed solid tumour populations.
- The remaining predictions (ovarian myxoid liposarcoma, dermatofibrosarcoma protuberans, rhabdomyosarcoma subtypes, and Xp11.2 translocation RCC) rest on model prediction with little or no supporting evidence, so the recommendation is Hold.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


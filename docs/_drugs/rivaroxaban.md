---
layout: default
title: Rivaroxaban
parent: Model Prediction Only (L5)
nav_order: 409
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-15
description: "Rivaroxaban: predicted new indication Rheumatoid arthritis (evidence level L5). Approved use: Not stated in the licence records compiled for this report (rivaroxab…"
monitor_inn: "RIVAROXABAN"
predicted_indications: ["rheumatoid arthritis", "gout", "HIV infectious disease", "brachydactyly-syndactyly syndrome"]
---

# Rivaroxaban
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

# Rivaroxaban: From Anticoagulation (Thromboembolic Disease) to Rheumatoid Arthritis

## One-Sentence Summary

Rivaroxaban is an oral Factor Xa inhibitor anticoagulant. The licence records compiled for this report do not list an approved indication text, and the drug's established use is in preventing and treating thromboembolic disease.
The TxGNN model predicts it may be effective for **rheumatoid arthritis**, but there are **0 clinical trials** and only **4 publications** (reviews, a cohort study and a case report), none showing disease-modifying benefit in this condition.
The prediction rests on model output alone (**evidence level L5**).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the licence records compiled for this report (rivaroxaban is a Factor Xa inhibitor anticoagulant) |
| Predicted New Indication | Rheumatoid arthritis |
| TxGNN Prediction Score | 99.57% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Rivaroxaban is known as a direct Factor Xa inhibitor, so it reduces thrombin generation and clot formation. This general pharmacology is background knowledge, not something the compiled data confirms.

The link to rheumatoid arthritis (RA) is indirect. Autoimmune disease is associated with a thrombo-inflammatory state, with increased thrombin generation and crosstalk between coagulation and inflammation. Patients with RA also have a higher risk of venous thromboembolism. Rivaroxaban could address the thrombotic component of this picture, but not RA disease activity itself.

None of the retrieved literature shows disease-modifying efficacy in RA. The very high TxGNN score (99.57%) is a network-based prediction without clinical support. It should not be read as evidence of efficacy.

Other predictions in the data (gout, HIV infection, brachydactyly-syndactyly syndrome) are weaker or are safety questions rather than repurposing opportunities. They are not evaluated further here.

## Clinical Trial Evidence

Currently no related clinical trials registered for rheumatoid arthritis.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [33141212](https://pubmed.ncbi.nlm.nih.gov/33141212/) | 2020 | Review | JAMA | Review of lower extremity venous thromboembolism diagnosis and treatment; not specific to RA |
| [34175144](https://pubmed.ncbi.nlm.nih.gov/34175144/) | 2021 | Review | La Revue de médecine interne | Thrombin generation assay for assessing hypercoagulability in autoimmune disease |
| [29621248](https://pubmed.ncbi.nlm.nih.gov/29621248/) | 2018 | Cohort | PLoS One | Adherence to rivaroxaban versus apixaban in non-valvular atrial fibrillation; not an RA study |
| [41918541](https://pubmed.ncbi.nlm.nih.gov/41918541/) | 2026 | Case report | Cureus | Recurrent embolic events despite anticoagulation in an 88-year-old woman with RA taking oral steroids and with chronic kidney disease |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2545152 | Xarelto | Tab 10 mg | Not stated (licence cancelled) |
| 2425947 | Xarelto | Tab 20 mg | Not stated (licence cancelled) |
| 2425920 | Xarelto | Tab 15 mg | Not stated (licence cancelled) |

## Safety Considerations

- **Disease associations from the knowledge graph (not label text)**: The TxGNN knowledge graph links rivaroxaban to contraindication-type relationships with liver failure, liver disease, kidney disease, gallbladder disease, pyoureter and blood coagulation disease. These need verification against the official product information.
- **Drug Interactions**: 100 interactions were identified. Examples of **Major** interactions include acetylsalicylic acid (aspirin), cobicistat, deferasirox, ibritumomab tiuxetan and tositumomab (I-131). **Moderate** interactions include aprepitant, dexamethasone, cimetidine, clarithromycin, miconazole and clotrimazole.
- **Bleeding risk with CYP3A4/P-gp inhibitors**: Literature in the compiled data notes that ritonavir- or cobicistat-boosted regimens can raise rivaroxaban exposure and bleeding risk.
- **Relevance to RA**: Patients with RA often take corticosteroids and NSAIDs, so the dexamethasone and aspirin interactions would be practical concerns.

Please also refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has the highest model score but no clinical trials and no literature showing disease-modifying benefit in RA (evidence level L5). Rivaroxaban would address only the thrombotic component, not RA activity. All three New Zealand licences are cancelled, and bleeding-related interactions with common RA co-medications add safety concerns.

**To proceed, the following is needed:**
- Mechanism of action data and a clearer mechanistic rationale for any effect on RA inflammation
- Preclinical or early-phase clinical evidence in RA, for example thromboprophylaxis or thrombo-inflammatory biomarker studies
- Package insert warnings and contraindications from Medsafe, and the approved indications from the licence records
- A review of current New Zealand availability, since all licences are cancelled
- A bleeding-risk assessment for RA patients using corticosteroids, NSAIDs or aspirin
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


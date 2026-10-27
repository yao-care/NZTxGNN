---
layout: default
title: Fulvestrant
parent: Model Prediction Only (L5)
nav_order: 216
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Fulvestrant: predicted new indication HIV infectious disease (evidence level L5). Approved use: Not recorded in the licence data"
monitor_inn: "FULVESTRANT"
predicted_indications: ["HIV infectious disease", "multiple endocrine neoplasia", "feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter", "rheumatoid arthritis", "acne (disease)", "brachydactyly-syndactyly syndrome", "hemoglobinopathy", "colobomatous microphthalmia-rhizomelic dysplasia syndrome"]
---

# Fulvestrant
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

## Fulvestrant: From Breast Cancer Endocrine Therapy to HIV Infectious Disease
{: .text-alpha }

## One-Sentence Summary

Fulvestrant is a selective estrogen receptor degrader (SERD) supplied as an injectable. The TxGNN model predicts it may be effective for **HIV infectious disease**, but this rests on **0 clinical trials** and **2 publications** (one preclinical mechanistic study, one tangential review of HTLV-1), so the evidence is very early.

The original indication is not recorded in the New Zealand licence data compiled for this report.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the licence data |
| Predicted New Indication | HIV infectious disease |
| TxGNN Prediction Score | 99.91% |
| Evidence Level | L4 (preclinical/mechanistic only) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. From general knowledge, fulvestrant binds the estrogen receptor and promotes its degradation, which is why it is used with other agents in hormone receptor-positive, HER2-negative breast cancer.

The link to HIV is indirect. A 2026 study (PMID 42430506) examined how estrogen, fulvestrant and HIV latency-reversing agents affect estrogen receptor α in CD4+ T cells. This suggests estrogen receptor signalling may influence HIV transcription and latency. However, the work is in vitro, and it is unclear whether degrading the receptor would reverse or reinforce latency. The result could go either way, so the mechanistic rationale is a hypothesis rather than support for treatment.

The other top-ranked predictions are weaker. Several are rare congenital syndromes or veterinary and animal-model diseases with no plausible mechanism. Rheumatoid arthritis has animal data suggesting that estrogen blockade may worsen inflammation, which is a potential harm signal.

## Clinical Trial Evidence

Currently no related clinical trials registered for HIV infectious disease.

(Trials found under the second-ranked prediction, multiple endocrine neoplasia, are all in breast cancer and do not address that prediction.)

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [42430506](https://pubmed.ncbi.nlm.nih.gov/42430506/) | 2026 | In vitro / mechanistic | JCI Insight | Evaluated how estrogen, fulvestrant and HIV latency-reversing agents affect estrogen receptor α in CD4+ T cells; suggests a link between ER signalling and HIV latency |
| [40343334](https://pubmed.ncbi.nlm.nih.gov/40343334/) | 2025 | Multi-cohort omics analysis | Research Square | Concerns HTLV-1-associated myelopathy, a different retrovirus; only tangentially relevant |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2586371 | Faslodex | Inj 50 mg per ml, 5 ml prefilled syringe | Not recorded (cancelled) |
| 2663139 | Fulvestrant EVER Pharma | Inj 50 mg per ml, 5 ml prefilled syringe | Not recorded (cancelled) |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Endocrine therapy (SERD); not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Disease associations from the knowledge graph (not label text)**: liver disease, gallbladder disease, thrombocytopenia and blood coagulation disease are flagged as contraindication-related in the TxGNN knowledge graph. These should be verified against the product label.
- **Drug Interactions**: 92 potential interactions were identified, all with an unclassified severity level. Examples include warfarin, simvastatin, metformin, dexamethasone, prednisone, morphine, bupropion, and several proton pump inhibitors and antiemetics (pantoprazole, omeprazole, lansoprazole, ondansetron, palonosetron). Warfarin deserves attention given the coagulation-related flags above.
- **Possible harm signal**: in an animal model of rheumatoid arthritis, estrogen blockade with fulvestrant worsened inflammation. This is not directly relevant to HIV, but it shows estrogen blockade may have immune effects.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The HIV prediction has a high model score but only preclinical, indirect support and no clinical trials. The direction of effect on HIV latency is unknown. Fulvestrant has no currently valid New Zealand licence, which further limits near-term feasibility.

**To proceed, the following is needed:**
- Mechanistic data on whether ER degradation reverses or reinforces HIV latency (in vitro and ex vivo CD4+ T-cell work)
- Mechanism of action data and the original approved indication
- The package insert warnings and contraindications from Medsafe, so safety screening can proceed
- Review of the interaction severity levels and of interactions with antiretroviral agents
- Clarification of the regulatory status in New Zealand, since both licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


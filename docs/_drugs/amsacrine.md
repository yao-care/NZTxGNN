---
layout: default
title: Amsacrine
parent: Model Prediction Only (L5)
nav_order: 38
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Amsacrine: predicted new indication Plasma cell myeloma (evidence level L5). Approved use: Not recorded in the New Zealand licence data"
monitor_inn: "AMSACRINE"
predicted_indications: ["hyperthyroidism", "plasma cell myeloma", "indolent plasma cell myeloma", "resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta", "bronchitis"]
---

# Amsacrine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

# Amsacrine: From Antineoplastic Use (Original Indication Not Recorded) to Plasma Cell Myeloma

## One-Sentence Summary

Amsacrine is a cytotoxic anticancer drug (a topoisomerase II inhibitor and DNA intercalator). The New Zealand licence records compiled for this report do not list an approved indication.
The TxGNN model's top-ranked prediction is hyperthyroidism, which has no supporting evidence and no plausible mechanism. The most evidence-supported prediction is **plasma cell myeloma** (rank 2), with **0 registered clinical trials** and **4 publications**, including 2 Phase II studies that showed limited activity.

The report below focuses on plasma cell myeloma, because the rank 1 prediction has no evidence and is mechanistically implausible.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the New Zealand licence data |
| Predicted New Indication | Plasma cell myeloma (rank 2; the rank 1 prediction, hyperthyroidism, has no supporting evidence) |
| TxGNN Prediction Score | 99.32% (plasma cell myeloma); 99.37% (hyperthyroidism, rank 1) |
| Evidence Level | L3 (plasma cell myeloma); L5 (hyperthyroidism) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. From known pharmacology, amsacrine is a topoisomerase II poison and DNA intercalator. It damages DNA in rapidly dividing cells, which makes it biologically plausible against proliferating haematological malignancies such as myeloma.

The link to myeloma is therefore mechanistic, as both are haematological cancers. Two Phase II studies tested amsacrine in myeloma. A 1983 multi-tumour study (solid tumours, myeloma, lymphoma) reported only 12 partial responses among 221 evaluable patients. A 1989 myeloma-specific trial in 74 previously treated patients found a good response in only 2 patients (3%) and improvement in 3 (4%). Severe toxicity occurred in 33% of patients who received three or more courses, and the authors concluded the dose and schedule were usually ineffective in multiple myeloma.

Modern myeloma therapies (proteasome inhibitors, immunomodulatory drugs, anti-CD38 antibodies and BCMA-directed agents) have largely superseded this type of agent. The model score is high, but the real-world evidence does not show meaningful benefit.

The other predictions (hyperthyroidism, thyroid hormone resistance, bronchitis, indolent myeloma) have no clinical or literature support. There is no recognised mechanism linking a DNA-damaging cytotoxic drug to these conditions, and its toxicity would be disproportionate for them.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [2913481](https://pubmed.ncbi.nlm.nih.gov/2913481/) | 1989 | Phase II trial (single-arm) | Med Pediatr Oncol | 74 previously treated myeloma patients received 120 mg/m² every 3 weeks; good response in 3%, improvement in 4%; severe toxicity in 33% with three or more courses; dose and schedule usually ineffective |
| [6688199](https://pubmed.ncbi.nlm.nih.gov/6688199/) | 1983 | Phase II trial | Cancer Treat Rep | Heavily pretreated solid tumours, myeloma and lymphoma; 12 partial responses in 221 evaluable patients at 120 mg/m² every 28 days; some antitumour activity |
| [2064323](https://pubmed.ncbi.nlm.nih.gov/2064323/) | 1991 | Review | Anticancer Res | Review of new drugs for acute and chronic leukaemia with emphasis on m-AMSA; myeloma is mentioned only in relation to other agents |
| [1958848](https://pubmed.ncbi.nlm.nih.gov/1958848/) | 1991 | In vitro preclinical | Anti-Cancer Drugs | Compared cytotoxicity of DNA intercalating agents in myeloma cells and rat heart cells; raises cardiotoxicity concern |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2680521 | AmsaLyo | Inj 75 mg | Not recorded (cancelled) |
| 2577437 | Amsidine | Inj 50 mg per ml, 1.5 ml ampoule | Not recorded (cancelled) |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (topoisomerase II inhibitor / DNA intercalator) |
| Myelosuppression Risk | High (severe toxicity was reported in 33% of myeloma patients receiving three or more courses) |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | CBC with differential, liver and renal function, electrolytes, cardiac function (cardiotoxicity concern) |
| Handling Protection | Must follow cytotoxic drug handling regulations |

---

## Safety Considerations

- **Cardiotoxicity**: An in vitro study (PMID 1958848) compared heart-cell and tumour-cell toxicity of DNA intercalating agents and raised a cardiotoxicity concern.
- **Severe toxicity**: In the 1989 Phase II myeloma trial, 33% of patients who received three or more courses had severe toxicity.

Please refer to the package insert for further safety information, including warnings, contraindications and drug interactions.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for plasma cell myeloma rests on only two older single-arm Phase II studies, which showed low response rates and notable toxicity. There are no registered trials, the drug is no longer marketed in New Zealand, and modern myeloma treatments are far more effective. The remaining predictions have no supporting evidence.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- The original approved indication from the licence records
- Evidence of benefit over current standard myeloma therapies, which is unlikely given the existing Phase II results
- A cardiac and haematological safety assessment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


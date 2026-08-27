---
layout: default
title: Melphalan
parent: 僅模型預測 (L5)
nav_order: 215
evidence_level: L5
indication_count: 10
---

# Melphalan
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Melphalan: From Multiple Myeloma to Gonadal Germ Cell Tumor

## One-Sentence Summary

> Melphalan (DrugBank DB01042) is a classic alkylating chemotherapy agent originally established for multiple myeloma and as a high-dose conditioning agent before autologous stem cell transplant.
> The TxGNN model predicts it may be effective for **Gonadal Germ Cell Tumor**,
> with **8 clinical trials** and **4 publications** currently supporting this direction.

*Note: The evidence pack's structured `original_indications` field and MOA field are both empty/data-gap; the original-indication context above is drawn from established public drug knowledge (DrugBank), not from this Evidence Pack.*

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in Evidence Pack (`taiwan_regulatory.licenses` empty); commonly known indication is Multiple Myeloma |
| Predicted New Indication | Gonadal Germ Cell Tumor |
| TxGNN Prediction Score | 99.77% |
| Evidence Level | L3 (multiple completed early-phase clinical trials + supporting literature, no completed Phase 3 RCT) |
| New Zealand Market Status | 未上市 (Not marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the Evidence Pack (MOA = Data Gap, severity: High). Based on generally known pharmacology, melphalan is a bifunctional alkylating agent (a phenylalanine derivative of nitrogen mustard) that cross-links DNA strands, causing strand breaks and apoptosis in rapidly dividing cells. This mechanism underlies its established use in multiple myeloma and as a high-dose conditioning agent prior to autologous hematopoietic stem cell transplantation (ASCT).

The link to gonadal germ cell tumor is directly supported by the trial evidence itself: melphalan already appears as a component of high-dose salvage chemotherapy regimens for relapsed/refractory germ-cell tumors — for example NCT00936936 combines melphalan with gemcitabine, docetaxel, and carboplatin as part of a two-cycle high-dose regimen with stem cell rescue, and NCT00638898 pairs melphalan with busulfan and topotecan for the same purpose. Germ cell tumors are highly chemosensitive, and high-dose alkylator-based regimens with ASCT support are an established salvage strategy in this population, which is mechanistically consistent with melphalan's cytotoxic, DNA-damaging activity.

Because these are largely single-arm, dose-finding, or pilot studies rather than randomized controlled trials, the mechanistic plausibility is well supported but the confirmatory efficacy evidence remains at an early/observational stage.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00002750](https://clinicaltrials.gov/study/NCT00002750) | Phase 1 | Completed | 6 | Intrathecal melphalan for recurrent/persistent neoplastic meningitis |
| [NCT00936936](https://clinicaltrials.gov/study/NCT00936936) | Phase 2 | Completed | 64 | Two-cycle high-dose chemo (gemcitabine/docetaxel/melphalan/carboplatin, then ifosfamide/carboplatin/etoposide) for poor-prognosis relapsed germ-cell tumors |
| [NCT01272817](https://clinicaltrials.gov/study/NCT01272817) | N/A | Completed | 36 | Nonmyeloablative allogeneic HSCT using melphalan+cladribine or TLI conditioning across various malignancies including germ-cell-relevant settings |
| [NCT00638898](https://clinicaltrials.gov/study/NCT00638898) | Phase 1 | Completed | 25 | High-dose busulfan/melphalan/topotecan followed by autologous HSCT in advanced/recurrent tumors |
| [NCT00060255](https://clinicaltrials.gov/study/NCT00060255) | Phase 2 | Completed | 451 | Eight high-dose chemo regimens ± TBI before autologous transplant in hematologic and selected solid tumors |
| [NCT00003425](https://clinicaltrials.gov/study/NCT00003425) | Phase 1/2 | Completed | 25 | Escalating-dose melphalan with autologous stem cell support and amifostine cytoprotection in cancer patients |
| [NCT00003926](https://clinicaltrials.gov/study/NCT00003926) | Phase 1 | Terminated | 13 | Amifostine chemoprotection with autologous stem cell transplant for high-risk/relapsed pediatric solid and brain tumors |
| [NCT00536601](https://clinicaltrials.gov/study/NCT00536601) | N/A | Completed | 174 | High-dose regimens ± TBI before autologous transplant in hematologic malignancies and selected solid tumors |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [4270380](https://pubmed.ncbi.nlm.nih.gov/4270380/) | 1973 | Pending classification | Oncology | Chemotherapy of testicular germinal tumors (abstract not available in Evidence Pack) |
| [24913](https://pubmed.ncbi.nlm.nih.gov/24913/) | 1977 | Pending classification | The Urologic Clinics of North America | Seminoma review (abstract not available) |
| [13392619](https://pubmed.ncbi.nlm.nih.gov/13392619/) | 1956 | Pending classification | Voprosy Onkologii | Experience treating testicular seminoma and metastases with sarcolysin (melphalan) (abstract not available) |
| [14151951](https://pubmed.ncbi.nlm.nih.gov/14151951/) | 1964 | Pending classification | Acta - Unio Internationalis Contra Cancrum | Influence of hormonal and alkylating drugs on pituitary follicle-stimulating function (abstract not available) |

---

## New Zealand Market Information

Melphalan is currently not marketed in New Zealand (0 authorizations on file in this Evidence Pack).

---

## Cytotoxicity

Melphalan is a classic conventional cytotoxic chemotherapy agent (alkylating agent, nitrogen mustard class), meeting the antineoplastic criteria used for this section.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (Alkylating agent — nitrogen mustard class) |
| Myelosuppression Risk | High — myelosuppression is the well-recognized dose-limiting toxicity of melphalan; specific institutional toxicity data not available in this Evidence Pack |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | CBC with differential, platelet count, renal function; specific monitoring protocol not available in this Evidence Pack |
| Handling Protection | Must follow cytotoxic drug handling regulations |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple completed early-phase clinical trials (8 total) show melphalan already used as a component of high-dose salvage regimens for relapsed/refractory germ-cell tumors, consistent with the drug's established chemosensitivity profile in this tumor type, but none are Phase 3 RCTs and TFDA/Medsafe safety documentation is currently a blocking data gap.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert with warnings, contraindications, and DDI data (currently Blocking data gap)
- Confirmed mechanism of action data from DrugBank (currently High-severity data gap)
- New Zealand regulatory filing/market status confirmation, since the drug is currently unmarketed
- Classification/relevance grading of the pending clinical trials and literature (currently marked "pending" in the Evidence Pack)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


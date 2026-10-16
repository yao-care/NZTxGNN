---
layout: default
title: Mitomycin
parent: Model Prediction Only (L5)
nav_order: 306
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Mitomycin: predicted new indication Solid pseudopapillary carcinoma of the pancreas (evidence level L5). Approved use: Pancreatic adenocarcinoma"
monitor_inn: "mitomycin"
predicted_indications: ["solid pseudopapillary carcinoma of pancreas", "osteoclastic giant cell tumor of pancreas", "pancreatic carcinoma with mixed differentiation", "pancreatic intraductal papillary-mucinous carcinoma", "mixed ductal-endocrine carcinoma of pancreas", "pancreatic signet ring cell adenocarcinoma", "undifferentiated pancreatic carcinoma", "malignant exocrine pancreas neoplasm", "pancreatic intraductal papillary-mucinous neoplasm", "pancreatic acinar cell cystadenocarcinoma"]
---

# Mitomycin
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

# Mitomycin: From Pancreatic Adenocarcinoma to Solid Pseudopapillary Carcinoma of the Pancreas

## One-Sentence Summary

Mitomycin is a cytotoxic chemotherapy drug, originally used for pancreatic adenocarcinoma.
The TxGNN model predicts it may be effective for **solid pseudopapillary carcinoma of the pancreas**,
but there are currently **no clinical trials** and **no publications** directly supporting this prediction, so it rests on model prediction alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Pancreatic adenocarcinoma (from the knowledge graph; no approved indication text is available in the New Zealand licence records) |
| Predicted New Indication | Solid pseudopapillary carcinoma of the pancreas |
| TxGNN Prediction Score | 99.86% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 5 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Based on known information, mitomycin is a DNA cross-linking alkylating agent. Its efficacy in pancreatic adenocarcinoma is the basis of its original indication, and mechanistically its general cytotoxic activity may be applicable to other pancreatic tumours.

The predicted indication is a rare pancreatic tumour that is usually indolent and often cured by surgery, so cytotoxic therapy is rarely needed. The link to the original indication is therefore a general one (both are pancreatic neoplasms) and has not been confirmed by any study. The other top-ranked predictions are also pancreatic tumour subtypes, such as osteoclastic giant cell tumour, mixed-differentiation carcinoma, signet ring cell adenocarcinoma and undifferentiated carcinoma. All of them have scores of about 99.8% and rest on the same extrapolation from the parent indication. Ten predictions were compiled in total. None has clinical trial support, and only four (ranks 4, 7, 8 and 9) have any literature, which is indirect.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

There is no literature for the top-ranked prediction. The table below lists the indirect publications linked to other predicted pancreatic indications. None of them demonstrates a mitomycin effect in those diseases.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10897253](https://pubmed.ncbi.nlm.nih.gov/10897253/) | 2000 | Review | Strahlenther Onkol | Adjuvant and neoadjuvant radiochemotherapy in ductal pancreatic carcinoma; concerns ductal carcinoma, not the predicted tumour types |
| [8361472](https://pubmed.ncbi.nlm.nih.gov/8361472/) | 1993 | Clinical study | Nihon Geka Gakkai Zasshi | Tamoxifen added to immuno-chemotherapy (including mitomycin) after resection of pancreatic carcinoma; not mitomycin-specific |
| [15983445](https://pubmed.ncbi.nlm.nih.gov/15983445/) | 2005 | Case report | Pancreatology | Pseudomyxoma peritonei with a pancreatic intraductal papillary mucinous neoplasm, managed with intraperitoneal hyperthermic chemoperfusion; connection to mitomycin unclear |
| [2695183](https://pubmed.ncbi.nlm.nih.gov/2695183/) | 1989 | Review | Bull Cancer | General review of unknown primary tumours; not specific to pancreatic carcinoma or mitomycin |

## New Zealand Market Information

All five licences are cancelled, and no approved indication text is recorded for them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2601761 | Teva | Inj 20 mg vial | Cancelled |
| 2677016 | Mitomycin (Fresenius Kabi) | Inj 5 mg vial | Cancelled |
| 2137844 | Accord | Inj 5 mg vial | Cancelled |
| 2686937 | Mitomycin (Sagent) | Inj 5 mg vial | Cancelled |
| 2207605 | Baxter | Inj 1 mg for ECP | Cancelled |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (DNA cross-linking alkylating agent) |
| Myelosuppression Risk | High; the knowledge graph links mitomycin to thrombocytopenia, leukopenia and anaemia |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Complete blood count with platelets, renal function, and signs of haemolytic-uraemic syndrome |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

- **Disease-level cautions (knowledge graph, not label text)**: Kidney disease, thrombocytopenia, blood coagulation disease, anaemia (including macrocytic anaemia), leukopenia, haemolytic-uraemic syndrome (including the typical form), congestive heart failure and heart failure, stomatitis, and pyoureter.
- **Drug Interactions**: 193 interactions were identified. Most are of unknown severity, and the main ones with a stated severity are:
  - Major: deferiprone, samarium (153Sm) lexidronam
  - Moderate: palifermin, strontium chloride Sr-89, fostamatinib
  - Minor: levofloxacin

  Warfarin, leucovorin, and several common supportive medicines (such as dexamethasone and ondansetron) are also listed, with unknown severity.

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone (L5), with no trials or direct literature. The target tumour is rare, usually indolent and mostly cured by surgery. In addition, all New Zealand licences for mitomycin are cancelled, and its safety profile includes serious myelosuppression and renal risks.

**To proceed, the following is needed:**
- Mechanism of action data
- Package insert warnings and contraindications for a formal safety screen
- Published case series or trial evidence of mitomycin in solid pseudopapillary neoplasms, or a clear clinical need for cytotoxic therapy in this setting
- Confirmation of whether any mitomycin product can be supplied in New Zealand, and by which route
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


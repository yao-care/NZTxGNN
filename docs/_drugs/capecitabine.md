---
layout: default
title: Capecitabine
parent: 僅模型預測 (L5)
nav_order: 61
evidence_level: L5
indication_count: 10
---

# Capecitabine
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

# Capecitabine: From Colorectal Cancer to Gastric Adenocarcinoma and Proximal Polyposis of the Stomach

## One-Sentence Summary

Capecitabine is an oral fluoropyrimidine prodrug internationally established as a backbone chemotherapy for colorectal cancer, breast cancer, and gastric cancer.
The TxGNN model predicts it may be effective for **Gastric Adenocarcinoma and Proximal Polyposis of the Stomach (GAPPS)**, a rare hereditary gastric cancer syndrome;
however, **no clinical trials or published literature** currently support this specific indication, making the overall evidence level **L5 (model prediction only)**.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | No New Zealand registration; globally established for colorectal cancer, breast cancer, and gastric cancer |
| Predicted New Indication | Gastric Adenocarcinoma and Proximal Polyposis of the Stomach (GAPPS) |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why Is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on established pharmacological knowledge, Capecitabine is an orally administered fluoropyrimidine carbamate prodrug that undergoes a three-step enzymatic conversion to 5-fluorouracil (5-FU) preferentially in tumor tissue. The final activation step depends on thymidine phosphorylase (TP), which is overexpressed in many solid tumors, enabling selective drug activation at the tumor site. The active 5-FU metabolite inhibits thymidylate synthase (TS), blocking de novo dTMP synthesis and thereby disrupting DNA replication and repair; it also incorporates into RNA, further impairing cell function.

GAPPS is an ultra-rare hereditary gastric cancer syndrome caused by point mutations in the APC gene promoter (1B region), characterized by fundic gland polyposis and proximal gastric adenocarcinoma. Although Capecitabine's TP/TS mechanism is theoretically applicable to epithelial adenocarcinomas broadly, GAPPS-associated carcinomas display distinctive molecular features — predominantly diffuse-type histology, with possible lower TP expression compared to intestinal-type gastric adenocarcinoma. Whether this translates to reduced fluoropyrimidine activation efficiency in GAPPS tumors has not been studied.

The TxGNN model's high prediction score (99.94%, rank 876) most likely reflects shared "gastric cancer" network linkages in the knowledge graph rather than GAPPS-specific biological evidence. This prediction should currently be treated as hypothesis-generating only, pending molecular characterization of GAPPS tumors with respect to fluoropyrimidine sensitivity.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## Cytotoxicity

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic (Fluoropyrimidine class — oral prodrug of 5-FU) |
| Myelosuppression Risk | Low to moderate (neutropenia and thrombocytopenia may occur; generally milder than intravenous 5-FU due to oral route and tumor-selective activation) |
| Emetogenicity Classification | Low |
| Monitoring Items | CBC with differential (before each cycle), liver function tests (ALT/AST/bilirubin), renal function (serum creatinine / creatinine clearance — dose reduction required if CrCl < 50 mL/min), hand-foot syndrome assessment |
| Handling Protection | Must follow institutional cytotoxic drug handling and disposal regulations |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
GAPPS is an ultra-rare hereditary syndrome with unique molecular characteristics (predominantly diffuse-type adenocarcinoma driven by APC promoter mutation); no clinical trial or published literature currently examines Capecitabine in this indication, making any evidence-based efficacy or safety assessment impossible at this stage.

**To proceed, the following is needed:**

- **Tumor molecular profiling:** Characterize TP and TS expression in GAPPS-associated adenocarcinoma to determine fluoropyrimidine activation potential
- **Clinical case data:** Identify registry entries or case reports documenting outcomes of systemic fluoropyrimidine chemotherapy in GAPPS patients
- **Histological subtype clarification:** Determine whether GAPPS adenocarcinoma shares the diffuse-type molecular features that confer reduced 5-FU sensitivity (e.g., lower TP expression, TS overexpression)
- **MOA data retrieval:** Query DrugBank API to obtain complete Capecitabine mechanism of action for formal mechanistic linkage analysis
- **Safety data retrieval:** Download and parse the full package insert (Medsafe NZ or international regulatory equivalent) to complete contraindication and drug interaction profiling before any clinical consideration
- **Regulatory pathway:** Since Capecitabine is not registered in New Zealand, any clinical use in GAPPS would require a Special Authority or Named Patient access pathway pending the above evidence gaps being addressed
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


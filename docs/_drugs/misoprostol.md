---
layout: default
title: Misoprostol
parent: 僅模型預測 (L5)
nav_order: 230
evidence_level: L5
indication_count: 2
---

# Misoprostol
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
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

# Misoprostol: From Undocumented Original Indication to Amenorrhea (Disease)

## One-Sentence Summary

Misoprostol's original approved indication and mechanism of action (MOA) are not documented in the current evidence pack (both flagged as data gaps). The TxGNN model predicts potential relevance for **Amenorrhea (disease)**, with **0 clinical trials** and **7 publications** currently linked — however, the literature largely describes misoprostol's established role in early pregnancy termination, where "amenorrhea" refers to gestational dating rather than amenorrhea as a treatment target, raising concern about a disease-label mapping artifact.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in evidence pack (data gap) |
| Predicted New Indication | Amenorrhea (disease) |
| TxGNN Prediction Score | 99.64% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available, and misoprostol's original approved indication is also not documented in this evidence pack. Based on information within the evidence pack's own rationale field, misoprostol is a PGE1 (prostaglandin E1) analogue with uterotonic and cervix-ripening activity, used clinically to induce uterine evacuation of pregnancy tissue — e.g., management of missed/incomplete abortion and, in combination with mifepristone, medical termination of early pregnancy.

This points toward a directional mismatch: misoprostol's known pharmacology promotes uterine contraction and expulsion of pregnancy contents — the opposite of what would be needed to treat amenorrhea (absence of menstruation). The apparent link to "amenorrhea" in the supporting literature stems from the term being used as a **gestational-age descriptor** (e.g., "amenorrhea ≤35 days" meaning days since last menstrual period, a standard way of dating early pregnancy in these trials), not as a diagnosed condition being treated.

Taken together, this is most consistent with a **disease-ontology mapping error** between the TxGNN prediction label and the literature corpus, rather than genuine mechanistic support for misoprostol as a treatment for amenorrhea. The mechanistic linkage is weak and the therapeutic direction is questionable; this evidence does not substantiate the predicted indication.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [27678099](https://pubmed.ncbi.nlm.nih.gov/27678099/) | 2017 | RCT | Reproductive Sciences | RCT (n=744) of low-dose mifepristone + self-administered misoprostol for ultra-early medical abortion (amenorrhea ≤35 days used as gestational-dating criterion) |
| [25394644](https://pubmed.ncbi.nlm.nih.gov/25394644/) | 2015 | RCT | Reproductive Sciences | Dose-ranging RCT (n=2500) of mifepristone (50–150 mg) + misoprostol 200 µg for termination of ultra-early pregnancy |
| [29974571](https://pubmed.ncbi.nlm.nih.gov/29974571/) | 2018 | RCT | J Obstet Gynaecol Res | Safety/efficacy of self-administered low-dose mifepristone + misoprostol for early medical abortion |
| [26405260](https://pubmed.ncbi.nlm.nih.gov/26405260/) | 2015 | Cohort | Human Reproduction | Feasibility of low-dose mifepristone + misoprostol given before expected menstruation to prevent/interrupt unintended pregnancy |
| [37113350](https://pubmed.ncbi.nlm.nih.gov/37113350/) | 2023 | Case Report | Cureus | Case report of acute fatty liver of pregnancy; patient presented with amenorrhea as a pregnancy symptom — misoprostol not the treatment focus |
| [26001691](https://pubmed.ncbi.nlm.nih.gov/26001691/) | 2015 | Review/Guideline | J Obstet Gynaecol Can | Guideline on endometrial ablation for abnormal uterine bleeding; misoprostol not a primary focus |
| [1486304](https://pubmed.ncbi.nlm.nih.gov/1486304/) | 1992 | Cohort/Case Series | BMJ | Medical management of missed abortion and anembryonic pregnancy |

---

## New Zealand Market Information

Misoprostol is not currently marketed in New Zealand — 0 product authorizations are on file.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is numerically high, but the supporting literature does not actually treat amenorrhea as a therapeutic target — "amenorrhea" appears as a gestational-dating term within early-pregnancy-termination studies, and misoprostol's known uterotonic mechanism runs counter to the predicted indication. No clinical trials directly support this indication, and MOA, original indication, and regulatory safety data are all unavailable.

**To proceed, the following is needed:**
- TFDA/regulatory package insert (blocking gap: warnings, contraindications)
- Confirmed mechanism of action (DrugBank) to properly assess the original-to-new indication rationale
- Verification/correction of the disease-ontology mapping between the literature corpus and the "amenorrhea (disease)" label before this candidate is advanced
- Dedicated literature search specifically targeting amenorrhea as a treatment indication (current search set appears mismatched)
- DDI and contraindication data
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


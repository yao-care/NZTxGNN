---
layout: default
title: Tranexamic Acid
parent: Model Prediction Only (L5)
nav_order: 460
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Tranexamic Acid: predicted new indication Amenorrhea (disease) (evidence level L5). Approved use: Endometriosis / endometriosis of uterus"
---

# Tranexamic Acid
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

# Tranexamic Acid: From Endometriosis to Amenorrhea

## One-Sentence Summary

Tranexamic acid is an antifibrinolytic drug. The knowledge graph lists endometriosis as its original indication, although the New Zealand licence records contain no approved indication text.
The TxGNN model predicts it may be useful for **amenorrhea (disease)**, but there are currently **0 clinical trials** and **2 publications** (both reviews), so the evidence is **model prediction with indirect literature support only**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Endometriosis / endometriosis of uterus (from the knowledge graph; no approved indication text in the New Zealand licence records) |
| Predicted New Indication | Amenorrhea (disease) |
| TxGNN Prediction Score | 99.19% |
| Evidence Level | L4 (no trials; only indirect review literature and a mechanistic rationale) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled; 0 currently valid) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available in the data compiled for this report. Based on established pharmacology, tranexamic acid is a lysine analogue that blocks plasminogen binding to fibrin. This reduces fibrinolysis and bleeding, including menstrual blood loss.

The link to "amenorrhea" is **indirect**. Tranexamic acid does not act on the hormonal or endometrial pathways that would cause true amenorrhea, so any benefit would be a reduction in bleeding volume rather than correction of an underlying cause. The label "amenorrhea" is also ambiguous. It can mean the condition itself, or a desired therapeutic state of menses suppression, for example prophylaxis in premenopausal haematology patients at risk of heavy bleeding. The second reading is the more plausible one for this drug.

The high TxGNN score (0.99) is a model prediction only and should not be treated as clinical evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|------|---------|
| [21701432](https://pubmed.ncbi.nlm.nih.gov/21701432/) | 2011 | Review | Menopause | Evidence-based overview of medical therapy for abnormal uterine bleeding. Treatment choice depends on cause, bleeding amount, contraception needs, fertility preservation and perimenopausal status. Non-hormonal agents are included. |
| [39043214](https://pubmed.ncbi.nlm.nih.gov/39043214/) | 2024 | Review | J Oncol Pharm Pract | Systematic approach to menses prophylaxis and suppression in premenopausal haematologic cancer patients. Data comparing therapies are scarce in the cancer setting. |

Both publications are reviews on bleeding management. Neither directly tests tranexamic acid for amenorrhea.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2574020 | Mercury Pharma | Tab 500 mg | Not recorded (status: cancelled) |
| 2497174 | Tranexamic-AFT | Inj 100 mg per ml, 10 ml ampoule | Not recorded (status: cancelled) |

---

## Safety Considerations

**Contraindications / disease associations (from the knowledge graph, not label text):** the knowledge graph links tranexamic acid to the following conditions, which should be reviewed against the official label.
- Cardiovascular and thrombotic: thrombophlebitis, coronary artery disease, stroke and cerebrovascular disorder, hypertension
- Hormone-sensitive cancers: female breast carcinoma, endometrial cancer and other uterine corpus neoplasms
- Metabolic: diabetes mellitus, diabetic ketoacidosis, hyperglycemia, hyperlipidemia, hypertriglyceridemia, obesity
- Other: porphyria, liver disease, gallbladder disease, migraine, brain edema, anxiety and neurotic disorders, dysthymic disorder

**Drug Interactions:** 35 interactions were identified in total. The main ones are:
- **Major:**
  - Oestrogens and hormonal contraceptives (ethinylestradiol, estradiol, conjugated estrogens, esterified estrogens, estrone, diethylstilbestrol, norethisterone, drospirenone, etonogestrel). These raise the risk of thrombosis.
  - Pro-coagulant products (anti-inhibitor coagulant complex, Factor IX complex).
  - Carfilzomib.
  - Estramustine.
- **Moderate:** Thrombolytics (alteplase, anistreplase), which have opposing pharmacological effects.

Please refer to the package insert for warnings and official contraindications. The label data were not available for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score and an indirect mechanistic argument. There are no registered trials, and the only literature is two general reviews on bleeding management. All New Zealand licences are cancelled, and the dominant concern is thrombotic risk, particularly with concurrent hormonal therapy.

**To proceed, the following is needed:**
- Clarify the target indication. Decide whether it is true amenorrhea or menses suppression/prophylaxis (for example in haematology patients), and define the outcome measure.
- Obtain the package insert from Medsafe to confirm warnings and contraindications. This is a blocking gap for safety screening.
- Obtain mechanism of action data from DrugBank.
- Search for trials or studies of tranexamic acid for menses suppression.
- Review the thrombotic risk, especially in patients on hormonal therapy.
- Confirm the regulatory pathway, because all current New Zealand licences are cancelled.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


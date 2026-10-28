---
layout: default
title: Misoprostol
parent: Model Prediction Only (L5)
nav_order: 305
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-28
description: "Misoprostol: predicted new indication Amenorrhea (evidence level L4). Approved use: Not stated in the licence data"
monitor_inn: "MISOPROSTOL"
predicted_indications: ["amenorrhea (disease)", "atypical coarctation of aorta"]
---

# Misoprostol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

## Misoprostol: From an Unspecified Original Indication to Amenorrhea
{: .text-alpha }

## One-Sentence Summary

Misoprostol is a prostaglandin E1 analogue. The New Zealand licence data compiled for this report do not state its approved indication.
The TxGNN model predicts it may be effective for **amenorrhea**, but there are **no registered clinical trials** and only **7 publications**, none of which show that misoprostol treats amenorrhea. The prediction rests on model output rather than clinical evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the licence data |
| Predicted New Indication | Amenorrhea |
| TxGNN Prediction Score | 0.9964 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Misoprostol is a prostaglandin E1 analogue that induces uterine contraction and cervical ripening, and it is used in obstetric and gynaecological settings such as medical abortion and management of missed abortion.

The retrieved literature is about pregnancy termination and pregnancy-related conditions. Amenorrhea appears in these papers mainly as an eligibility criterion (for example, "amenorrhea ≤35 days" in early pregnancy studies), not as a condition being treated. One regimen, low-dose mifepristone plus misoprostol before expected menstruation, was studied to prevent unintended pregnancy, which is only loosely related to menstrual regulation.

Amenorrhea is a symptom with many possible causes (pregnancy, hypothalamic, pituitary, ovarian or uterine), and there is no established mechanism by which a prostaglandin agonist would restore menstrual cycles. The high TxGNN score most likely reflects network proximity to reproductive and pregnancy-related terms rather than a true therapeutic link. The mechanistic rationale is therefore weak.

A second prediction, atypical coarctation of the aorta (score 0.9930), has no trials or literature. It is a structural congenital anomaly treated by surgical or endovascular repair, and misoprostol's vasodilatory activity is not expected to correct it. It is not considered further.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [27678099](https://pubmed.ncbi.nlm.nih.gov/27678099/) | 2017 | RCT | Reproductive Sciences | 744 women with ultra-early pregnancy (amenorrhea ≤35 days) compared hospital-administered and self-administered misoprostol after low-dose mifepristone for medical abortion |
| [25394644](https://pubmed.ncbi.nlm.nih.gov/25394644/) | 2015 | RCT (dose-ranging) | Reproductive Sciences | 2,500 women with ultra-early pregnancy received mifepristone at 5 decreasing doses followed by oral misoprostol 200 µg; the primary endpoint was complete abortion without surgery |
| [26405260](https://pubmed.ncbi.nlm.nih.gov/26405260/) | 2015 | Interventional study | Human Reproduction | Low-dose mifepristone plus misoprostol before expected menstruation, studied for preventing unintended pregnancy |
| [29974571](https://pubmed.ncbi.nlm.nih.gov/29974571/) | 2018 | Interventional study | J Obstet Gynaecol Res | Safety and efficacy of low-dose mifepristone with self-administered misoprostol for early pregnancy termination |
| [1486304](https://pubmed.ncbi.nlm.nih.gov/1486304/) | 1992 | Review | BMJ | Medical management of missed abortion and anembryonic pregnancy |
| [26001691](https://pubmed.ncbi.nlm.nih.gov/26001691/) | 2015 | Review | J Obstet Gynaecol Can | Endometrial ablation for abnormal uterine bleeding; not specific to misoprostol |
| [37113350](https://pubmed.ncbi.nlm.nih.gov/37113350/) | 2023 | Case report | Cureus | Acute fatty liver of pregnancy in a patient presenting with amenorrhea; not a misoprostol treatment study |

None of these studies tests misoprostol as a treatment for amenorrhea.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 561630 (cancelled) | Cytotec | Tab 200 mcg | Not stated |

---

## Safety Considerations

- **Disease associations from the knowledge graph (not label text)**: PrimeKG records drug-disease contraindication links for misoprostol with the following conditions:
  - Cardiovascular: hypertension, congestive heart failure, myocardial infarction, coronary thrombosis, stroke and cerebrovascular disorders, cerebral infarction, and other heart disease
  - Hepatic and gastrointestinal: liver failure, liver disease, drug-induced liver injury, inflammatory bowel disease, and active peptic ulcer disease
  - Respiratory and haematological: asthma (including intrinsic and allergic forms), blood coagulation disease, hemoglobinopathy, and anaemia (including macrocytic)
  - Other: gallbladder disease, substance abuse/dependence, and non-infectious meningitis
- **Drug Interactions** (100 interactions were found; the main ones are listed):
  - **Major**: dinoprostone (topical)
  - **Moderate**: magnesium-containing antacids (magnesium oxide, magnesium carbonate, magnesium hydroxide, magaldrate) and quinapril
  - **Minor**: sodium citrate, aluminium hydroxide, calcium carbonate, sodium bicarbonate and naproxen
- **Pregnancy**: the data compiled for this report note a teratogenic risk, including vascular disruption defects.

Please refer to the package insert for label warnings and contraindications, because these were not available.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has no registered trials, and the published studies concern pregnancy termination rather than treatment of amenorrhea. No plausible mechanism links a prostaglandin E1 analogue to restoring menstrual cycles. The only New Zealand licence is cancelled, and the label safety data could not be retrieved.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Evidence that misoprostol can treat a specific cause of amenorrhea, such as preclinical or clinical studies
- Clarification of the original approved indication and the regulatory status of the cancelled licence
- A review of the pregnancy-related teratogenic risk against any proposed use in women of reproductive age
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


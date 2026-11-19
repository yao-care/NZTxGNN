---
layout: default
title: Emicizumab
parent: Model Prediction Only (L5)
nav_order: 174
evidence_level: L5
indication_count: 10
monitor_inn: "EMICIZUMAB"
predicted_indications: ["pseudo-von Willebrand disease", "primary release disorder of platelets", "Glanzmann thrombasthenia", "Scott syndrome", "acquired coagulation factor deficiency", "bleeding diathesis due to a collagen receptor defect", "hemorrhagic disorder due to a constitutional thrombocytopenia", "thrombotic thrombocytopenic purpura", "fetal and neonatal alloimmune thrombocytopenia", "flood factor deficiency"]
last_modified_at: 2026-11-19
description: "Emicizumab: predicted new indication pseudo-von Willebrand disease (evidence level L5). In New Zealand: 4 Pharmac Schedule items"
---

# Emicizumab
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

## Emicizumab: bispecific factor VIIIa-mimetic antibody → pseudo-von Willebrand disease
{: .text-alpha }

## One-Sentence Summary

Emicizumab is a bispecific antibody that bridges activated factor IX and factor X, mimicking the function of activated factor VIII.
The TxGNN model predicts it may be relevant to **pseudo-von Willebrand disease**, with a score of 0.9999.
**No clinical trials and no publications** directly studying this disease were found, so this remains a model prediction only.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | pseudo-von Willebrand disease |
| TxGNN Prediction Score | 0.9999 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (4 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 4 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Emicizumab bridges activated factor IX and factor X, replacing the cofactor function of activated factor VIII in the intrinsic tenase complex. According to the Mechanism of Action section of the US FDA-approved prescribing information for emicizumab: "HEMLIBRA bridges activated factor IX and factor X to restore the function of missing activated factor VIII that is needed for effective hemostasis." ([DailyMed: Hemlibra (emicizumab) prescribing information, Genentech, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=2483adba-fab6-4d1b-96c5-c195577ed071), label version 2025-07-11; accessed 2026-11-19.) This bridging role is described in the pharmacology information.

Pseudo-von Willebrand disease, however, is a platelet-type disorder. It is caused by a gain-of-function change in the platelet GP1BA receptor, which increases binding to von Willebrand factor. The primary defect lies in the platelet–VWF interaction, not in factor VIII function. No plausible mechanistic link to emicizumab is evident.

The high TxGNN score therefore reflects a model prediction only. It should not be read as evidence of therapeutic benefit, and no clinical or published evidence supports it.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2553554 | Hemlibra | Emicizumab | Inj 105 mg in 0.7 ml vial | Yes |
| 2553538 | Hemlibra | Emicizumab | Inj 150 mg in 1 ml vial | Yes |
| 2553511 | Hemlibra | Emicizumab | Inj 30 mg in 1 ml vial | Yes |
| 2553546 | Hemlibra | Emicizumab | Inj 60 mg in 0.4 ml vial | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

---

## Safety Considerations

- **Drug Interactions**:
  - **Major**: anti-inhibitor coagulant complex (activated prothrombin complex concentrate). Thrombotic microangiopathy and thrombosis are known concerns with this combination.
  - **Moderate**: recombinant antihemophilic factor (factor VIII) and recombinant coagulation factor VIIa.
- **Other safety signal**: Thrombotic thrombocytopenic purpura is also among the model's predictions. Emicizumab promotes coagulation and carries a known thrombotic microangiopathy risk, so that prediction is a potential harm, not an opportunity.

For warnings and contraindications, please refer to the package insert.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No clinical trials or publications address pseudo-von Willebrand disease, and the disease mechanism (a platelet receptor gain-of-function) does not match how emicizumab works. The prediction rests on the model score alone.

**To proceed, the following is needed:**
- Any direct preclinical or clinical evidence in pseudo-von Willebrand disease
- A plausible mechanistic rationale linking the platelet–VWF defect to emicizumab's tenase-complex mechanism
- Package insert warnings and contraindications for a formal safety review
- Detailed mechanism-of-action data

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Detailed mechanism-of-action data were not available in the data compiled for this report, but t" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Hemlibra (emicizumab) prescribing information, Genentech, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=2483adba-fab6-4d1b-96c5-c195577ed071) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


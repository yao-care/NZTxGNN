---
layout: default
title: Exemestane
parent: Model Prediction Only (L5)
nav_order: 191
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Exemestane: predicted new indication Antithrombin deficiency type 2 (evidence level L5). Approved use: Not stated in the available licence data"
monitor_inn: "EXEMESTANE"
predicted_indications: ["antithrombin deficiency type 2", "amenorrhea (disease)", "factor 5 excess with spontaneous thrombosis", "heparin cofactor 2 deficiency", "thrombophilia", "migraine disorder", "migraine with brainstem aura"]
---

# Exemestane
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

## Exemestane: From Breast Cancer (Aromatase Inhibition) to Antithrombin Deficiency Type 2
{: .text-alpha }

## One-Sentence Summary

Exemestane is a steroidal, irreversible aromatase inhibitor that lowers estrogen levels. It is used in hormone-sensitive breast cancer, although the New Zealand licence data compiled for this report contain no indication text.
The TxGNN model's top prediction is **antithrombin deficiency type 2** (score 0.9983), but there are **0 clinical trials** and **0 publications** supporting it. This is a computational prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the available licence data |
| Predicted New Indication | Antithrombin deficiency type 2 |
| TxGNN Prediction Score | 0.9983 |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the data compiled for this report. Based on general knowledge, exemestane irreversibly inhibits aromatase and so lowers circulating estrogen. This is the basis of its use in estrogen-dependent breast cancer.

Antithrombin deficiency type 2 is a hereditary thrombophilia caused by a functional defect in the SERPINC1 protein. No plausible mechanistic link to estrogen lowering was identified. Estrogen-containing therapies raise thrombotic risk, but that does not show that exemestane would correct antithrombin function. The high score (0.9983) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

The other predictions follow the same pattern:

| Predicted Indication | Score | Assessment |
|------|------|------|
| Amenorrhea | 0.9982 | The literature describes amenorrhea as a treatment effect in breast cancer patients, not a condition exemestane treats. Exemestane could worsen estrogen deficiency. |
| Factor 5 excess with spontaneous thrombosis | 0.9982 | No mechanistic link. No trials or literature. |
| Heparin cofactor 2 deficiency | 0.9982 | No mechanistic link. No trials or literature. |
| Thrombophilia | 0.9967 | No direct link. Aromatase inhibitors have been associated with thromboembolic events in some settings, so a safety concern may exist. |
| Migraine disorder | 0.9902 | A weak, speculative estrogen-related link. Estrogen deprivation and headaches are reported adverse effects of aromatase inhibitors. |
| Migraine with brainstem aura | 0.9901 | Speculative, mirrors the general migraine entry. Symptoms could plausibly worsen. |

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature is available for the top-ranked indication, antithrombin deficiency type 2. The only literature found concerns the second-ranked prediction, amenorrhea, and it is indirect. Amenorrhea appears there as an outcome or context in breast cancer care, not as a target of exemestane therapy.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23108951](https://pubmed.ncbi.nlm.nih.gov/23108951/) | 2013 | Cohort | Annals of Oncology | Incidence and predictors of ovarian function recovery in breast cancer patients with chemotherapy-induced amenorrhea who switched from tamoxifen to exemestane |
| [26178334](https://pubmed.ncbi.nlm.nih.gov/26178334/) | 2015 | Review | Oncology (Williston Park) | Role of ovarian suppression in premenopausal women with hormone receptor-positive early breast cancer |
| [26951320](https://pubmed.ncbi.nlm.nih.gov/26951320/) | 2016 | Review | Journal of Clinical Oncology | Whether estradiol monitoring is needed in women receiving ovarian suppression for breast cancer |
| [31379370](https://pubmed.ncbi.nlm.nih.gov/31379370/) | 2019 | Review | Recenti Progressi in Medicina | Current role of LHRH analogues in premenopausal breast cancer |
| [28118723](https://pubmed.ncbi.nlm.nih.gov/28118723/) | 2016 | Review | Klinicka Onkologie | Aromatase inhibitor treatment in postmenopausal breast cancer and management of side effects |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2500647 (cancelled) | Pfizer Exemestane | Tab 25 mg | Not stated |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted hormonal therapy (steroidal aromatase inhibitor), not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Drug Interactions**: The interaction screen returned 100 interacting drugs. The two with a Moderate severity level among those shown are **dexamethasone** and **troglitazone**. The others shown have no assigned severity, including pantoprazole, morphine, metformin, omeprazole, simvastatin, aspirin, and rosuvastatin.
- **Class-related considerations**: Aromatase inhibitors have been associated with thromboembolic events in some settings and with estrogen-deprivation effects such as vasomotor symptoms and headache. This matters for the thrombophilia and migraine predictions.

Package insert warnings and contraindications were not available. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
All predictions are supported only by the model score, with no clinical trials. The one set of literature, for amenorrhea, is indirect and reverses the repurposing logic. No plausible mechanistic link was found for the leading thrombophilia predictions, and estrogen deprivation could even be harmful. The only New Zealand licence is cancelled.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications
- Mechanism-of-action data from DrugBank
- Any mechanistic or preclinical evidence linking aromatase inhibition to antithrombin function, or a re-evaluation of whether a more plausible indication should be prioritised
- A thromboembolic safety review, given the thrombophilia-related predictions
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


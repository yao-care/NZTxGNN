---
layout: default
title: Desmopressin
parent: Model Prediction Only (L5)
nav_order: 141
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Desmopressin: predicted new indication Congenital prothrombin deficiency (evidence level L5). Approved use: Hereditary von Willebrand disease, nocturnal enuresis, haemophilia"
monitor_inn: "DESMOPRESSIN"
predicted_indications: ["congenital prothrombin deficiency", "inherited thrombophilia", "Glanzmann thrombasthenia", "primary release disorder of platelets", "Scott syndrome", "flood factor deficiency"]
---

# Desmopressin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

# Desmopressin: From Von Willebrand Disease and Nocturnal Enuresis to Congenital Prothrombin Deficiency

## One-Sentence Summary

Desmopressin was originally used for hereditary von Willebrand disease, haemophilia and nocturnal enuresis.
The TxGNN model's top-ranked prediction is **congenital prothrombin deficiency**, but there are **0 clinical trials** and only **1 review article** behind it, and the mechanism does not fit well.
Among the seven predictions, **primary release disorder of platelets** has the most credible supporting evidence (reviews, a paediatric cohort and a case report).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hereditary von Willebrand disease, nocturnal enuresis, haemophilia (from the knowledge graph; no approved-indication text is available in the New Zealand licence records) |
| Predicted New Indication | Congenital prothrombin deficiency |
| TxGNN Prediction Score | 99.70% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 5 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the compiled data. From the literature, desmopressin raises plasma von Willebrand factor (vWF) and factor VIII by promoting their release from endothelial cells through the V2 receptor. This is why it works in von Willebrand disease and mild haemophilia A.

For the top-ranked prediction, congenital prothrombin deficiency (factor II), this mechanism does not apply. Prothrombin is not released through this pathway, so the high score probably reflects the disease's proximity to other bleeding disorders in the knowledge graph rather than a specific therapeutic rationale.

The other predictions differ in plausibility:

| Predicted Indication | Score | Evidence Level | Assessment |
|------|------|------|------|
| Congenital prothrombin deficiency | 99.70% | L5 | Weak mechanistic link; network proximity only |
| Inherited thrombophilia | 99.44% | L5 | Mechanistically contradictory (prothrombotic condition); a thrombotic safety signal |
| Glanzmann thrombasthenia | 99.30% | L4 | Indirect evidence; cannot correct the integrin αIIbβ3 defect; may be an adjunct |
| Primary release disorder of platelets | 99.26% | L3 | Most plausible; improves primary haemostasis; small non-randomised data |
| Pseudo-von Willebrand disease | 99.16% | L4 | Safety concern; may worsen platelet clumping and thrombocytopenia |
| Scott syndrome | 99.16% | L5 | Defect is not addressed by vWF/FVIII release |
| "Flood factor deficiency" | 99.15% | L4 | Likely a mapping artefact; if it maps to von Willebrand disease it is not repurposing |

## Clinical Trial Evidence

Currently no related clinical trials registered, for the top-ranked prediction or any of the other six.

## Literature Evidence

**Congenital prothrombin deficiency (top prediction)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7684674](https://pubmed.ncbi.nlm.nih.gov/7684674/) | 1993 | Review | Drugs | Rational treatment options for common inherited bleeding disorders (haemophilia A, von Willebrand disease); no specific data on prothrombin deficiency |

**Most relevant literature for the other predictions**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36656570](https://pubmed.ncbi.nlm.nih.gov/36656570/) | 2023 | Review | Eur J Haematol | Desmopressin releases vWF and FVIII, enhances platelet adhesion and shortens bleeding time; can cause hyponatraemia and, rarely, arterial thrombosis |
| [21509710](https://pubmed.ncbi.nlm.nih.gov/21509710/) | 2011 | Cohort | Klin Padiatr | Effect of desmopressin on primary haemostasis in children with aspirin-like defect (primary release disorder) |
| [23051555](https://pubmed.ncbi.nlm.nih.gov/23051555/) | 2013 | Mechanistic study | Haemophilia | More vWF is bound to microparticles after desmopressin, which may explain effects in platelet disorders |
| [10509036](https://pubmed.ncbi.nlm.nih.gov/10509036/) | 1999 | Laboratory study | Haematologica | In vitro and ex vivo effects of desmopressin on platelet function |
| [1785670](https://pubmed.ncbi.nlm.nih.gov/1785670/) | 1991 | Clinical study | Am J Pediatr Hematol Oncol | Desmopressin plus ethamsylate shortened bleeding time in 12 patients with various bleeding disorders (including Glanzmann and pseudo-von Willebrand disease) who responded poorly to desmopressin alone |
| [16444446](https://pubmed.ncbi.nlm.nih.gov/16444446/) | 2006 | Case report | Clin Appl Thromb Hemost | Glanzmann thrombasthenia: desmopressin was ineffective for dental extraction; recombinant factor VIIa succeeded |
| [22057877](https://pubmed.ncbi.nlm.nih.gov/22057877/) | 2011 | Case report | Hamostaseologie | Refractory menorrhagia as first sign of Hermansky-Pudlak syndrome (platelet storage pool defect) |
| [16460464](https://pubmed.ncbi.nlm.nih.gov/16460464/) | 2006 | Review | J Thromb Haemost | Link between desmopressin and inherited thrombotic thrombocytopenic purpura; a safety signal rather than supporting evidence |
| [22726101](https://pubmed.ncbi.nlm.nih.gov/22726101/) | 2012 | Review | Haemophilia | Treatment of inherited platelet disorders; platelet transfusion is often needed in severe cases |
| [39054329](https://pubmed.ncbi.nlm.nih.gov/39054329/) | 2024 | Review | Nat Rev Dis Primers | General review of von Willebrand disease |

## New Zealand Market Information

All five licences are cancelled; no approved-indication text is recorded.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 492620 | Minirin | Tablet 100 mcg | Not recorded |
| 492639 | Minirin | Tablet 200 mcg | Not recorded |
| 338192 | Minirin | Injection 4 mcg per ml, 1 ml | Not recorded |
| 2702916 | Desmopressin-PH&T | Nasal spray 10 mcg per dose, 6 ml | Not recorded |
| 2603268 | Minirin Melt | Wafer 120 mcg | Not recorded |

## Safety Considerations

- **Contraindication-related conditions (knowledge graph, not label text)**: hypertension, coronary artery disease, impaired renal function, cystic fibrosis, allergic and vasomotor rhinitis, pharyngitis, common cold, respiratory tract infection, von Willebrand disease (certain subtypes), dipsogenic diabetes insipidus, polydipsia and water intoxication.
- **Drug interactions**: 100 interactions were found in total. The main ones listed:
  - **Major**: corticosteroids, including hydrocortisone, dexamethasone, prednisolone, prednisone, triamcinolone, betamethasone, beclomethasone dipropionate and budesonide.
  - **Moderate**: epinephrine, loperamide, morphine (including liposomal), opium and chlorpropamide.
  - **Minor**: acetylsalicylic acid.
- **Literature-reported risks**: hyponatraemia and, rarely, arterial thrombotic events (PMID 36656570). In inherited thrombophilia and platelet-type (pseudo) von Willebrand disease, use is mechanistically concerning.

Package insert warnings and contraindications were not available; please refer to the package insert for the full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (congenital prothrombin deficiency) has no clinical trials, one general review and a weak mechanistic link, so the evidence is L5. No New Zealand licence is currently valid. The most credible direction is **primary release disorder of platelets** (L3, small non-randomised data), which would be worth pursuing as a separate research question. Inherited thrombophilia and pseudo-von Willebrand disease carry safety concerns and should not be pursued.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Verification of the "flood factor deficiency" mapping
- For primary release disorder of platelets: a systematic literature review and, if justified, a prospective study
- Clarification of current New Zealand availability, since all licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


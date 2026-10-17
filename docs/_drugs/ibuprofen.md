---
layout: default
title: Ibuprofen
parent: Model Prediction Only (L5)
nav_order: 227
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Ibuprofen: predicted new indication Acromesomelic dysplasia, Hunter-Thompson type (evidence level L5). Approved use: Rheumatoid arthritis, osteoarthritis, juvenile idiopathic arthritis…"
monitor_inn: "IBUPROFEN"
predicted_indications: ["acromesomelic dysplasia, Hunter-Thompson type", "brachyolmia-amelogenesis imperfecta syndrome", "myosclerosis", "brachyolmia", "brachydactyly-syndactyly syndrome", "pseudoachondroplasia", "colobomatous microphthalmia-rhizomelic dysplasia syndrome"]
---

# Ibuprofen
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

# Ibuprofen: From Rheumatoid Arthritis and Pain Conditions to Acromesomelic Dysplasia, Hunter-Thompson Type

## One-Sentence Summary

Ibuprofen is a non-steroidal anti-inflammatory drug (NSAID) originally used for rheumatoid arthritis, osteoarthritis, headache, juvenile idiopathic arthritis and patent ductus arteriosus.
The TxGNN model predicts it may be effective for **acromesomelic dysplasia, Hunter-Thompson type**, a rare skeletal disorder, but there are **0 clinical trials** and **0 publications** supporting this prediction, so it rests on model output alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Rheumatoid arthritis, osteoarthritis, juvenile idiopathic arthritis, headache disorder, patent ductus arteriosus (per the knowledge graph; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Acromesomelic dysplasia, Hunter-Thompson type |
| TxGNN Prediction Score | 99.74% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this report. Ibuprofen is generally known as a non-selective cyclooxygenase (COX) inhibitor with anti-inflammatory and analgesic effects, and its use in arthritis and musculoskeletal pain is well established.

Hunter-Thompson type acromesomelic dysplasia is a rare skeletal disorder caused by loss-of-function variants in *GDF5*, which impair cartilage formation and limb growth. No plausible link between COX inhibition and this GDF5/BMP signalling defect is evident. The high score most likely reflects the disease's proximity in the knowledge graph to musculoskeletal indications such as arthropathy and osteoarthritis, not a mechanistic rationale.

At most, ibuprofen could offer symptomatic relief of joint pain, which would be supportive care rather than disease modification. COX inhibition might also affect bone and cartilage metabolism adversely. The other top-ranked predictions (brachyolmia-amelogenesis imperfecta syndrome, myosclerosis, brachyolmia, brachydactyly-syndactyly syndrome, pseudoachondroplasia, colobomatous microphthalmia-rhizomelic dysplasia syndrome) are all rare skeletal or developmental disorders with the same prediction-only status. Of these, pseudoachondroplasia has the most plausible supportive role, for secondary joint pain and osteoarthritis.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2551985 | Ethics | Oral liquid 20 mg per ml | Not available (cancelled) |
| 2690276 | Ibuprofen SR BNM | Long-acting tablet 800 mg | Not available (cancelled) |
| 2526212 | Relieve | Tablet 200 mg | Not available (cancelled) |

All three authorizations are cancelled, so no valid ibuprofen product is currently registered in the data compiled for this report.

---

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: Hypertension, liver failure and liver disease, kidney disease, epilepsy, COPD, thrombocytopenia, peptic ulcer disease, inflammatory bowel disease, systemic lupus erythematosus, diabetes mellitus and diabetic ketoacidosis, blood coagulation disorders, stroke and cerebrovascular disorders, cerebral infarction, myocardial infarction, hypothyroidism, potassium deficiency, benign prostatic hyperplasia, hypotensive disorder, gallbladder disease and substance abuse/dependence.
- **Drug Interactions**: The interaction query returned 100 records. The sample reviewed includes:
  - **Major**: Acetylsalicylic acid (aspirin).
  - **Moderate**: Corticosteroids (betamethasone, budesonide, dexamethasone, hydrocortisone); sulfonylureas (acetohexamide, chlorpropamide, glimepiride, glipizide, glyburide); exenatide; aprepitant; balsalazide; fenfluramine and dexfenfluramine; kanamycin; levofloxacin.
  - **Minor**: Famotidine, ranitidine, cimetidine.
- **Pregnancy**: NSAID exposure during pregnancy carries known fetal safety concerns, which is relevant to congenital developmental disorders.

Please refer to the package insert for label warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the TxGNN model score, with no clinical trials or publications, and no plausible mechanistic link to the GDF5 defect. There are also no currently valid New Zealand licences for ibuprofen in the data compiled for this report.

**To proceed, the following is needed:**
- Mechanism of action data and a mechanistic assessment of any link to GDF5/BMP signalling or cartilage biology
- Package insert warnings and contraindications from Medsafe
- Preclinical or clinical evidence for ibuprofen in this disease, or in the supportive-care setting of skeletal dysplasia pain
- Confirmation of current New Zealand registration and available dosage forms and routes
- A safety assessment for this population, including the effects of COX inhibition on bone and cartilage and exposure during pregnancy
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


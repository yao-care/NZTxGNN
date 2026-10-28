---
layout: default
title: Naproxen
parent: Model Prediction Only (L5)
nav_order: 316
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-15
monitor_inn: "NAPROXEN"
predicted_indications: ["brachydactyly-syndactyly syndrome", "colobomatous microphthalmia-rhizomelic dysplasia syndrome", "acromesomelic dysplasia, Hunter-Thompson type", "brachyolmia-amelogenesis imperfecta syndrome"]
description: "Naproxen: predicted new indication Brachydactyly-syndactyly syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Naproxen
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

## Naproxen: Brachydactyly-syndactyly syndrome
{: .text-alpha }

## One-Sentence Summary

Naproxen is a non-steroidal anti-inflammatory drug (NSAID) originally used for arthritis, gout, tendinitis, bursitis, migraine and other pain and inflammatory conditions.
The TxGNN model predicts it may be effective for **brachydactyly-syndactyly syndrome**,
but there are **0 clinical trials** and **0 publications** supporting this prediction, so it rests on a computational prediction alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Brachydactyly-syndactyly syndrome |
| TxGNN Prediction Score | 0.9935 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 5 of 5) |
| Number of Authorizations | 5 (valid: 5) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on general pharmacology, naproxen is a non-selective COX-1/COX-2 inhibitor that reduces prostaglandin synthesis. This explains its proven benefit in inflammatory and painful musculoskeletal conditions.

Brachydactyly-syndactyly syndromes are congenital limb malformations caused by developmental gene variants. An anti-inflammatory and analgesic drug has no plausible disease-modifying role in such a structural condition. The high score (0.9935, rank 5394) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. NSAID exposure in pregnancy also raises fetal safety concerns.

The other top predictions show the same pattern: colobomatous microphthalmia-rhizomelic dysplasia syndrome (99.2%), acromesomelic dysplasia Hunter-Thompson type (99.2%) and brachyolmia-amelogenesis imperfecta syndrome (99.1%). All are genetic developmental disorders with no trials, no literature and no mechanistic link to COX inhibition. At most, naproxen could give symptomatic pain relief for secondary musculoskeletal complaints, which is not a repurposing indication.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2654458 | Noflam 250 | Tab 250 mg | Valid; indication text not available |
| 2654466 | Noflam 500 | Tab 500 mg | Valid; indication text not available |
| 2518619 | Naprosyn SR 1000 | Tab long-acting 1 g | Valid; indication text not available |
| 2725495 | Noflam 250 | Tab 250 mg | Valid; indication text not available |
| 2518627 | Naprosyn SR 750 | Tab long-acting 750 mg | Valid; indication text not available |

## Safety Considerations

- **Contraindication signals from the knowledge graph (not label text):** hypertension, liver failure and liver disease, kidney disease, congestive heart failure, coronary artery disease, myocardial infarction, stroke and cerebrovascular disorders, blood coagulation disorders, anaemia, diabetes mellitus and diabetic ketoacidosis, epilepsy, osteoporosis, obesity, gallbladder disease, benign prostatic hyperplasia, and substance abuse or dependence.
- **Drug Interactions:** The interaction query returned 100 records. The main ones listed are all Moderate:
  - Acetylsalicylic acid (aspirin)
  - Corticosteroids: hydrocortisone, dexamethasone, betamethasone, budesonide, triamcinolone
  - Metformin and chlorpropamide
  - Acid-suppressing drugs: famotidine, ranitidine, cimetidine, rabeprazole, dexlansoprazole
  - Mesalazine and balsalazide
  - Aprepitant
  - Potassium citrate and potassium bicarbonate
  - Bowel preparations: picosulfuric acid, polyethylene glycol with electrolytes
- **Pregnancy:** NSAID exposure carries fetal safety concerns, which matters for a congenital-malformation indication.

Please also refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is based only on a TxGNN model (deep learning) score, with no clinical trials, no literature and no plausible mechanistic link. The target condition is a congenital structural disorder that COX inhibition would not be expected to change. The drug is marketed in New Zealand, with 5 of 5 licences valid, and naproxen carries a notable cardiovascular, renal and gastrointestinal risk profile.

**To proceed, the following is needed:**
- Mechanism of action data confirmed from DrugBank
- Package insert warnings and contraindications from Medsafe, which are needed for safety screening
- Any preclinical or clinical evidence linking naproxen to this condition
- Confirmation of the current New Zealand regulatory and supply status, given that the drug is marketed with 5 of 5 licences valid
- A pregnancy and paediatric safety assessment, given the congenital nature of the target disease
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


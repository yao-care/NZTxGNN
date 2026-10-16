---
layout: default
title: Mefenamic Acid
parent: High Evidence (L1-L2)
nav_order: 283
evidence_level: L2
indication_count: 8
last_modified_at: 2026-10-15
description: "Mefenamic Acid: predicted new indication Rheumatoid arthritis (evidence level L2). Approved use: Not stated in the available New Zealand licence data"
---

# Mefenamic Acid
{: .fs-9 }

Evidence Level: **L2** | Predicted Indications: **8** 
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

# Mefenamic Acid: From Pain and Inflammation (NSAID) to Rheumatoid Arthritis

## One-Sentence Summary

Mefenamic acid is a nonsteroidal anti-inflammatory drug (NSAID) that is marketed in New Zealand as Ponstan capsules.
The TxGNN model predicts it may be effective for **rheumatoid arthritis**,
with **no registered clinical trials** but **20 publications** (several older double-blind comparative trials) supporting symptomatic benefit.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the available New Zealand licence data |
| Predicted New Indication | Rheumatoid arthritis |
| TxGNN Prediction Score | 99.73% |
| Evidence Level | L2 |
| New Zealand Market Status | ✓ Marketed |
| Number of Authorizations | 2 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, mefenamic acid is an NSAID that inhibits COX-1 and COX-2, which lowers prostaglandin synthesis. Prostaglandins mediate pain and inflammation in rheumatoid arthritis, so symptomatic relief is mechanistically plausible.

Because mefenamic acid is already an NSAID, this is closer to an extension within the same drug class than a true repurposing. It would relieve symptoms only and is not expected to modify the course of the disease.

Other predictions in the model output include osteoarthritis susceptibility, headache disorder, migraine disorder, and several rare syndromes. Of these, migraine has the most supporting literature (double-blind trials in menstrual migraine, in acute attacks versus acetaminophen, and in prophylaxis). The rare congenital syndromes (colobomatous microphthalmia-rhizomelic dysplasia and brachydactyly-syndactyly) have no plausible mechanistic link and are likely knowledge-graph artefacts.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [373989](https://pubmed.ncbi.nlm.nih.gov/373989/) | 1979 | RCT (double-blind crossover) | Curr Med Res Opin | In 24 patients, mefenamic acid, sulindac and flurbiprofen were all significantly better than placebo for pain, joint tenderness and morning stiffness |
| [796645](https://pubmed.ncbi.nlm.nih.gov/796645/) | 1976 | RCT (double-blind crossover) | Med J Aust | Mefenamic acid 1500 mg/day compared favourably with ibuprofen; side effects were mild and mainly gastrointestinal |
| [330287](https://pubmed.ncbi.nlm.nih.gov/330287/) | 1977 | RCT (double-blind) | J Int Med Res | In 40 patients, analgesic and anti-inflammatory effect was not significantly different from ibuprofen |
| [326491](https://pubmed.ncbi.nlm.nih.gov/326491/) | 1977 | Double-blind crossover | Curr Med Res Opin | In 18 patients, mefenamic acid and indomethacin were both significantly better than placebo on most measures |
| [4294443](https://pubmed.ncbi.nlm.nih.gov/4294443/) | 1967 | Clinical study | Ann Rheum Dis | Early clinical study of mefenamic acid in rheumatoid arthritis (no abstract available) |
| [5920657](https://pubmed.ncbi.nlm.nih.gov/5920657/) | 1966 | Comparative study | Br Med J | Mefenamic acid and flufenamic acid compared with aspirin and phenylbutazone (no abstract available) |
| [1455792](https://pubmed.ncbi.nlm.nih.gov/1455792/) | 1992 | Clinical study | Vopr Kurortol Fizioter Lech Fiz Kult | Electrophoresis of mefenamic acid reported complete or partial response in 75% of 125 patients (uncontrolled) |
| [5676955](https://pubmed.ncbi.nlm.nih.gov/5676955/) | 1968 | Case report (safety) | Br Med J | Three patients developed autoimmune haemolytic anaemia during mefenamic acid therapy; all recovered after withdrawal |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 616168 | Ponstan | Cap 250 mg | Not available in the licence data |
| 2469499 | Ponstan | Cap 250 mg | Not available in the licence data |

## Safety Considerations

- **Disease-related cautions (knowledge graph, not label text)**: Knowledge-graph contraindication links include hypertension, kidney disease, liver disease, gallbladder disease, active peptic ulcer disease, blood coagulation disorders, myocardial infarction and other cardiovascular or heart disease, and haemoglobinopathy.
- **Drug Interactions**: 100 interactions were identified. Those listed include moderate-level interactions with acetylsalicylic acid, corticosteroids (hydrocortisone, dexamethasone, betamethasone, triamcinolone, budesonide), metformin, chlorpropamide, mesalazine, balsalazide, aprepitant, potassium citrate and potassium bicarbonate. Minor interactions include famotidine, ranitidine and cimetidine.
- **Literature-reported risks**: Autoimmune haemolytic anaemia, enteropathy with villous atrophy after prolonged use, analgesic-associated renal disease, and medication-overuse headache.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Several double-blind comparative trials show symptomatic benefit in rheumatoid arthritis, comparable to ibuprofen, indomethacin and sulindac. These trials are old and small, and none are registered clinical trials. Benefit is symptomatic only, and gastrointestinal, renal and cardiovascular risks apply.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, which are currently unavailable and block formal safety screening
- Approved indication text for the two New Zealand licences
- Detailed mechanism of action data (for example from DrugBank)
- Modern, adequately powered trials versus current NSAIDs
- A monitoring plan for gastrointestinal, renal and cardiovascular risk, with limits on treatment duration
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


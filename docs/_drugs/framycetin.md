---
layout: default
title: Framycetin
parent: Model Prediction Only (L5)
nav_order: 215
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Framycetin: predicted new indication Sclerosing cholangitis (evidence level L5). Approved use: Not specified in the licence data compiled for this report (marketed…"
monitor_inn: "framycetin"
predicted_indications: ["sclerosing cholangitis", "urinary tract infection (disease)", "congenital prothrombin deficiency", "Ureaplasma urethritis", "gonococcal urethritis", "uterine inflammatory disease", "xanthogranulomatous pyelonephritis"]
---

# Framycetin
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

## Framycetin: From Topical Antibacterial Use to Sclerosing Cholangitis
{: .text-alpha }

## One-Sentence Summary

Framycetin is an aminoglycoside antibiotic, marketed in New Zealand as ear/eye drops.
The TxGNN model predicts it may be effective for **sclerosing cholangitis**,
but **no clinical trials and no publications** currently support this prediction. It is a model-based hypothesis only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not specified in the licence data compiled for this report (marketed as ear/eye drops) |
| Predicted New Indication | Sclerosing cholangitis |
| TxGNN Prediction Score | 99.66% |
| Evidence Level | L5 |
| New Zealand Market Status | ✓ Marketed |
| Number of Authorizations | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Based on known information, framycetin is an aminoglycoside antibiotic. Aminoglycosides bind the bacterial 30S ribosomal subunit and block protein synthesis. Framycetin is poorly absorbed when taken orally and is used mainly topically.

A possible link to sclerosing cholangitis is gut-directed antibiotic modulation of the microbiome, an approach explored with oral vancomycin in primary sclerosing cholangitis. No direct evidence supports this for framycetin, and its similarity to the original indication has not been assessed. The prediction should be treated as a hypothesis.

Other predicted indications, all also at L5 with no trials or literature:

| Rank | Predicted Indication | TxGNN Score | Comment |
|------|------|------|------|
| 2 | Urinary tract infection | 99.42% | Plausible as an aminoglycoside class effect. Systemic use is limited by nephrotoxicity and ototoxicity. |
| 3 | Congenital prothrombin deficiency | 99.41% | No plausible mechanism. Likely a graph-based artifact. |
| 4 | Ureaplasma urethritis | 99.28% | Ureaplasma lacks a cell wall, and aminoglycoside activity against it is generally poor. |
| 5 | Gonococcal urethritis | 99.28% | Class-level activity only. Poor systemic bioavailability limits use. |
| 6 | Uterine inflammatory disease | 99.18% | Plausible for polymicrobial infection, but topical-only use limits relevance. |
| 7 | Xanthogranulomatous pyelonephritis | 99.18% | Plausible against the associated bacteria, but nephrotoxicity is a concern in a diseased kidney. |

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 254894 | Soframycin | Ear/Eye drops 0.5% | Not specified in the data |
| 208728 | Sofradex | Ear/Eye drops (dexamethasone 500 mcg, framycetin sulphate 5 mg and gramicidin 50 mcg per ml) | Not specified in the data |

Both authorizations are valid and cover ophthalmic/otic routes only. Route compatibility with the predicted indication has not been assessed. A topical ear/eye product is unlikely to reach the biliary tract.

## Cytotoxicity

Not applicable. Framycetin is not an antineoplastic drug, so this section is omitted from the assessment.

## Safety Considerations

Please refer to the package insert for safety information.

For context only, aminoglycosides as a class are associated with nephrotoxicity and ototoxicity when absorbed systemically. This is general class knowledge, not drawn from the data compiled for this report. No drug interaction records were found.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the model score (L5), with no clinical trials or literature. The available formulations are topical ear/eye drops, which are not compatible with a biliary indication. The mechanistic link is speculative.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Approved indication text for the two New Zealand licences
- Route compatibility assessment, including whether an oral or systemic formulation would be needed
- A targeted literature search for aminoglycoside or gut-directed antibiotic use in sclerosing cholangitis
- A comparison with better-supported options such as oral vancomycin before any further investment
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


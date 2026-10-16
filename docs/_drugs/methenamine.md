---
layout: default
title: Methenamine
parent: Model Prediction Only (L5)
nav_order: 289
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-15
description: "Methenamine: predicted new indication Ureaplasma urethritis (evidence level L5). Approved use: Urinary tract infection"
monitor_inn: "METHENAMINE"
predicted_indications: ["Ureaplasma urethritis", "gonococcal urethritis", "uterine inflammatory disease", "xanthogranulomatous pyelonephritis"]
---

# Methenamine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

# Methenamine: From Urinary Tract Infection to Ureaplasma Urethritis

## One-Sentence Summary

Methenamine is a urinary antiseptic originally used for urinary tract infection.
The TxGNN model predicts it may be effective for **Ureaplasma urethritis**,
but **no clinical trials and no supporting publications** were found, so this is a computational prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Urinary tract infection (the New Zealand licence records contain no approved indication text; this indication comes from the knowledge graph) |
| Predicted New Indication | Ureaplasma urethritis |
| TxGNN Prediction Score | 99.79% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on general pharmacology, methenamine is a urinary antiseptic prodrug. In acidic urine (pH below 6) it breaks down into formaldehyde, which has nonspecific antibacterial activity. Its efficacy in urinary tract infection is the basis for the prediction.

Urethritis is also a lower urinary or genital tract infection, so a conceptual link exists. However, the link is weak in practice:

- Ureaplasma has no cell wall and lives mainly inside cells or attached to mucosal surfaces.
- Urine contact time in the urethra is short, and urine acidification is unreliable, so effective formaldehyde exposure is doubtful.

The high TxGNN score is a model output only and is not backed by trials or literature.

### Other Predicted Indications (for reference)

| Predicted Indication | TxGNN Score | Evidence Level | Assessment |
|------|------|------|------|
| Gonococcal urethritis | 99.79% | L5 | Mucosal infection treated with systemic antibiotics. Transient urinary antiseptic exposure and the need for reliable cure make methenamine unsuitable. |
| Uterine inflammatory disease | 99.77% | L4 | Methenamine does not reach uterine tissue at effective levels. The only article retrieved is a report on actinomycotic endometritis, which does not address methenamine efficacy. |
| Xanthogranulomatous pyelonephritis | 99.73% | L4 | Methenamine acts only in bladder urine and does not reach renal tissue. The only article retrieved is a pathology survey with no methenamine evidence. |

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no literature directly supports the top predicted indication (Ureaplasma urethritis).

Two articles were retrieved for lower-ranked predictions. Neither provides evidence of methenamine efficacy:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [29369919](https://pubmed.ncbi.nlm.nih.gov/29369919/) | 2019 | Case report/Review | Int J Gynecol Pathol | Seven cases of actinomycotic endometritis, mostly with a long-standing intrauterine device. Describes the disease only; no methenamine treatment data. |
| [19642728](https://pubmed.ncbi.nlm.nih.gov/19642728/) | 2009 | Pathology case series/Review | Arch Pathol Lab Med | Nonneoplastic kidney diseases in tumour nephrectomy specimens. No methenamine evidence. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 209538 | Hiprex | Tab 1 g | Not recorded (cancelled) |
| 2671077 | Hiprex | Tab 1 g | Not recorded (cancelled) |

## Safety Considerations

- **Drug Interactions**: 14 interactions were identified. The main ones are:
  - **Major**: sulfasalazine and sulfamethizole (sulfonamides can form an insoluble precipitate with formaldehyde in urine).
  - **Moderate**: urinary alkalinising agents and antacids, including sodium bicarbonate, sodium citrate, sodium acetate, potassium citrate, tromethamine, calcium carbonate, aluminium hydroxide, magaldrate, and magnesium carbonate, hydroxide and oxide. These raise urine pH and reduce formaldehyde release. Picosulfuric acid is also listed as moderate.
- **Conditions flagged in the knowledge graph (not label text)**: liver disease or failure, kidney disease, congestive heart failure, diabetes mellitus and diabetic ketoacidosis, G6PD deficiency, and others. These come from drug-disease contraindication links in the knowledge graph and must be verified against the official label.

Package insert warnings and contraindications were not available and should be consulted.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone (L5), with no trials and no supporting literature. Methenamine's action is limited to acidic bladder urine, which makes effective exposure doubtful for Ureaplasma or gonococcal urethritis. It has no valid New Zealand licence at present.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe
- Detailed mechanism of action data (for example from DrugBank)
- In vitro susceptibility data for Ureaplasma against formaldehyde or methenamine under urinary conditions
- Evidence on urethral formaldehyde exposure and urine acidification
- Clarification of why both New Zealand licences were cancelled and whether the product is currently available
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


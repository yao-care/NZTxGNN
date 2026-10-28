---
layout: default
title: Miconazole
parent: Moderate Evidence (L3-L4)
nav_order: 299
evidence_level: L4
indication_count: 1
last_modified_at: 2026-10-15
monitor_inn: "MICONAZOLE"
predicted_indications: ["acne (disease)"]
description: "Miconazole: predicted new indication Acne (disease) (evidence level L4). Approved use: Not recorded in the local data"
---

# Miconazole
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **1** 
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

## Miconazole: Acne (disease)
{: .text-alpha }

## One-Sentence Summary

Miconazole is an imidazole antifungal originally used for fungal and yeast infections of the skin, nails and vagina (for example tinea pedis, cutaneous candidiasis and vulvovaginal candidiasis).
The TxGNN model predicts it may be useful for **acne**, but there are currently **no registered clinical trials** and only **4 publications**, which are mostly reviews, in vitro work or small studies. This is best treated as a research question rather than a validated indication.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Acne (disease) |
| TxGNN Prediction Score | 0.9954 |
| Evidence Level | L4 |
| New Zealand Market Status | Marketed (valid licences: 5 of 5) |
| Number of Authorizations | 5 (valid: 5) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, miconazole is an azole antifungal that inhibits fungal CYP51 (lanosterol 14-alpha-demethylase) and disrupts ergosterol synthesis. Its efficacy in superficial fungal skin infections is established, and it may be mechanistically applicable to acne through the pathways below.

The link between the original and new indications is indirect, and three lines of reasoning support it:
- **Anti-bacterial activity:** Azole antifungals show in vitro activity against *Cutibacterium (Propionibacterium) acnes*, the bacterium involved in acne vulgaris (PMID 20045949).
- **Overlap with yeast-driven disease:** *Malassezia* yeasts cause Pityrosporum folliculitis, an acne-like condition that is often misdiagnosed as acne vulgaris (PMID 8593718). An antifungal would logically help here, but this is a different disease.
- **Anti-inflammatory effects:** Miconazole has been reported to have anti-inflammatory effects on skin (PMID 18627330), which may be relevant to inflammatory acne lesions.

These pathways are plausible but not confirmed as the mechanism of benefit in acne vulgaris. The very high TxGNN score is a computational prediction and does not substitute for clinical evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [18627330](https://pubmed.ncbi.nlm.nih.gov/18627330/) | 2008 | Review | Expert Opin Pharmacother | Overview of the multifaceted effects of miconazole on skin disorders; miconazole nitrate is a long-established imidazole antifungal |
| [15536660](https://pubmed.ncbi.nlm.nih.gov/15536660/) | 2004 | Clinical study (split-face, likely small) | Skin Res Technol | Split-face assessment in mild inflammatory catamenial acne in young women |
| [8593718](https://pubmed.ncbi.nlm.nih.gov/8593718/) | 1995 | Clinical study / therapeutic trial | Clin Exp Dermatol | 62 patients with Pityrosporum folliculitis, a condition often misdiagnosed as acne vulgaris; diagnosis and therapeutic trials (a different but acne-like condition) |
| [20045949](https://pubmed.ncbi.nlm.nih.gov/20045949/) | 2010 | In vitro study | Biol Pharm Bull | In vitro activity of azole antifungals against *P. acnes* isolated from patients with acne vulgaris |

---

## New Zealand Market Information

Indication text is not available for these authorizations. All products listed are single-ingredient miconazole.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 253294 | Daktarin | Tincture 2% | Valid |
| 253278 | Daktarin | Lotion 2% | Valid |
| 697648 | Micreme | Vaginal cream 2% with applicator | Valid |
| 2405008 | Decozol | Oral gel 20 mg per g | Valid |
| 2184354 | Multichem | Cream 2% | Valid |

---

## Safety Considerations

- **Contraindications (from the knowledge graph, not label text):** liver disease and gallbladder disease are listed as drug-disease contraindication links.
- **Drug Interactions:** 100 interactions were found in the DDInter database. Most listed are Moderate; a few are Minor. Examples:
  - Moderate: paclitaxel, fentanyl, alfentanil, hydrocodone, acalabrutinib, nifedipine, amlodipine, amiodarone, amitriptyline, alprazolam, alfuzosin, aliskiren, alosetron, ethinylestradiol, estradiol
  - Minor: dolutegravir, loratadine, amphotericin B, ambrisentan

Please refer to the package insert for other safety information, including warnings and label contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high (0.9954), but the supporting evidence is limited to a review, in vitro data, a small split-face study and a study of a related but different condition. There are no registered clinical trials for acne (Evidence Level L4), and the safety review cannot be completed without the package insert. The idea is plausible and worth exploring as a research question, not yet as a development decision.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications, which are required for safety screening
- Detailed mechanism of action data (for example from DrugBank) to strengthen the mechanistic link
- Controlled clinical evidence in acne vulgaris specifically, such as a randomised trial of a topical formulation
- Confirmation of which topical formulations and indications are currently valid in New Zealand, and a route-compatibility check for acne treatment
- Review of the interaction profile, particularly if systemic exposure is expected
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


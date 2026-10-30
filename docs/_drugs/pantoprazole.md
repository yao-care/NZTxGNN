---
layout: default
title: Pantoprazole
parent: Model Prediction Only (L5)
nav_order: 348
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-30
monitor_inn: "PANTOPRAZOLE"
predicted_indications: ["duodenal obstruction"]
description: "Pantoprazole: predicted new indication Duodenal obstruction (evidence level L5). Approved use: Not recorded in the local data"
---

# Pantoprazole
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Pantoprazole: Duodenal obstruction
{: .text-alpha }

## One-Sentence Summary

Pantoprazole is a proton pump inhibitor, originally used for peptic ulcer disease, gastroesophageal reflux disease, esophagitis and Zollinger-Ellison syndrome.
This is essentially an on-label use rather than true repurposing, so the prediction mostly confirms known pharmacology.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Duodenal obstruction |
| TxGNN Prediction Score | 0.9921 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. The mechanism is well established in the literature, however. Pantoprazole irreversibly inhibits the H+/K+ ATPase (proton pump) in gastric parietal cells. This suppresses gastric acid secretion, which is the key driver of ulcer formation and persistence.

The original indications (peptic ulcer disease, reflux disease, esophagitis) are all acid-related disorders. The TxGNN score mainly reflects that the drug already belongs to this disease area.

Other predicted indications are weaker:
- **Gastrojejunal (marginal) ulcer and peptic ulcer perforation:** acid suppression is plausible, but the evidence is extrapolated from general ulcer data. No trials are registered, so these remain research questions.
- **Duodenogastric reflux:** the benefit would be partial, because acid suppression does not address bile-related injury.
- **Duodenal obstruction:** there is no clear mechanistic link. The high score likely reflects network proximity to duodenal ulcer.
- **Duodenal ulcer:** supported by L1-level evidence, including double-blind trials against ranitidine and omeprazole.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02084420](https://clinicaltrials.gov/study/NCT02084420) | Phase 3 | Completed | 323 | Ilaprazole vs pantoprazole triple therapy for 7 days in H. pylori-positive gastric and/or duodenal ulcer patients; multicentre, randomised, double-blind, active-controlled |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38345252](https://pubmed.ncbi.nlm.nih.gov/38345252/) | 2024 | Systematic review / network meta-analysis | Am J Gastroenterol | P-CAB vs PPI for healing grade C/D esophagitis |
| [38384180](https://pubmed.ncbi.nlm.nih.gov/38384180/) | 2024 | RCT | Gut Liver | Tegoprazan vs active control for endoscopic resection-induced artificial ulcers |
| [10638560](https://pubmed.ncbi.nlm.nih.gov/10638560/) | 2000 | Comparative trial | Am J Gastroenterol | Pantoprazole vs ranitidine in duodenal ulcer, multicentre study in Brazil |
| [15244210](https://pubmed.ncbi.nlm.nih.gov/15244210/) | 2003 | Comparative study | Hepato-gastroenterology | Lansoprazole vs pantoprazole in active duodenal ulcer and H. pylori eradication |
| [19938880](https://pubmed.ncbi.nlm.nih.gov/19938880/) | 2009 | Review | Clin Drug Investig | Pantoprazole irreversibly binds the proton pump; no drug interactions identified in numerous interaction studies |
| [21534455](https://pubmed.ncbi.nlm.nih.gov/21534455/) | 2011 | Review | Med Arh | PPIs in GERD management |
| [15989651](https://pubmed.ncbi.nlm.nih.gov/15989651/) | 1997 | Review | Expert Opin Investig Drugs | Faster pain relief and ulcer healing than H2 antagonists |
| [8930575](https://pubmed.ncbi.nlm.nih.gov/8930575/) | 1996 | Review | Eur J Gastroenterol Hepatol | Acid-activated, selective mechanism of action of pantoprazole |
| [38652367](https://pubmed.ncbi.nlm.nih.gov/38652367/) | 2024 | Animal study | Inflammopharmacology | Pantoprazole plus mesenchymal stem cells in rat gastric ulcer |
| [16961157](https://pubmed.ncbi.nlm.nih.gov/16961157/) | 2006 | Review | Int J Clin Pharmacol Ther | Impact of CYP2C19 polymorphism on PPI action |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2637154 | Panzop Relief | Tab EC 40 mg | Not recorded (valid) |
| 2637146 | Panzop Relief | Tab EC 20 mg | Not recorded (valid) |

---

## Safety Considerations

- **Disease associations flagged in the knowledge graph (not label text):** osteoporosis, postmenopausal osteoporosis, bone fracture and pernicious anaemia are listed as contraindication-type associations.
- **Drug Interactions:** 100 interactions were identified. Major-level interactions include:
  - Acalabrutinib
  - Atazanavir

  Moderate interactions include hydrochlorothiazide, atorvastatin, fenofibrate, amphotericin B, amikacin, bosutinib, bumetanide and apalutamide. Minor interactions include acetylsalicylic acid and cyanocobalamin.

Please refer to the package insert for warnings and formal contraindications.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Active peptic ulcer disease is an established, on-label use of pantoprazole. It is supported by a completed Phase 3 randomised trial and by older double-blind comparative trials in duodenal ulcer.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications, which are currently missing and are a blocking gap for safety screening
- Detailed mechanism of action data from DrugBank
- A review of the major DDIs (acalabrutinib, atazanavir) and of the bone-health and vitamin B12/pernicious anaemia concerns for long-term use
- A manual check of the trial title and comparator details for NCT02084420
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


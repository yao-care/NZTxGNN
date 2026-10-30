---
layout: default
title: Sucralfate
parent: High Evidence (L1-L2)
nav_order: 430
evidence_level: L2
indication_count: 2
last_modified_at: 2026-10-30
description: "Sucralfate: predicted new indication Duodenogastric reflux (evidence level L2). Approved use: Duodenal ulcer"
monitor_inn: "SUCRALFATE"
predicted_indications: ["duodenogastric reflux", "duodenal obstruction"]
---

# Sucralfate
{: .fs-9 }

Evidence Level: **L2** | Predicted Indications: **2** 
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

## Sucralfate: From Duodenal Ulcer to Duodenogastric Reflux
{: .text-alpha }

## One-Sentence Summary

Sucralfate was originally used to treat duodenal ulcer.
The TxGNN model predicts it may be effective for **duodenogastric reflux**,
supported by **no registered clinical trials** but **10 publications**, including **2 randomized controlled trials** in alkaline reflux gastritis.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Duodenal ulcer (from the knowledge graph; the New Zealand licence has no indication text recorded) |
| Predicted New Indication | Duodenogastric reflux |
| TxGNN Prediction Score | 0.9937 |
| Evidence Level | L2 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 (valid: 1) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, sucralfate is a mucosal-protective agent, its efficacy in duodenal ulcer has been established, and mechanistically it may be applicable to duodenogastric reflux. This reasoning draws on general pharmacological knowledge rather than the data compiled for this report.

Sucralfate is generally understood to form a protective, adherent barrier over damaged gastric and oesophageal mucosa and to bind bile salts and pepsin. Duodenogastric reflux (alkaline or bile reflux) injures the mucosa through duodenal contents, so a barrier and bile-binding action plausibly reduces that injury. This is consistent with its original use in duodenal ulcer, where mucosal protection is also central.

The very high TxGNN score is supported by the retrieved literature, which includes randomized trials of sucralfate in patients with alkaline reflux gastritis. Results should still be read cautiously, as these studies are small and old.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3839973](https://pubmed.ncbi.nlm.nih.gov/3839973/) | 1985 | RCT | Am J Med | Double-blind trial of sucralfate 6 g/day vs placebo for 6 weeks in 23 post-gastric-surgery patients with alkaline reflux gastritis symptoms, followed by open-label sucralfate |
| [12923369](https://pubmed.ncbi.nlm.nih.gov/12923369/) | 2003 | RCT | Eur J Gastroenterol Hepatol | Sucralfate vs rabeprazole vs no treatment for post-cholecystectomy alkaline reactive gastritis, assessing dyspeptic symptoms and endoscopic/histological signs |
| [1391144](https://pubmed.ncbi.nlm.nih.gov/1391144/) | 1992 | Clinical study | Minerva Gastroenterol Dietol | 18 patients with duodenogastric reflux gastritis; cisapride 30 mg/day vs sucralfate 4 g/day for two months |
| [3616071](https://pubmed.ncbi.nlm.nih.gov/3616071/) | 1987 | Case series | Rev Esp Enferm Apar Dig | 50 cases of post-surgical biliary reflux gastritis treated with sucralfate |
| [3475771](https://pubmed.ncbi.nlm.nih.gov/3475771/) | 1987 | Clinical report | Scand J Gastroenterol Suppl | Randomized sucralfate vs placebo trial in gastritis, comparing gastroesophageal with duodenogastric reflux |
| [14723838](https://pubmed.ncbi.nlm.nih.gov/14723838/) | 2004 | Review | Curr Treat Options Gastroenterol | Duodenogastric reflux-induced oesophagitis; proton-pump inhibitors described as best medical treatment |
| [6372664](https://pubmed.ncbi.nlm.nih.gov/6372664/) | 1984 | Review | Annu Rev Med | Alkaline reflux gastritis and oesophagitis: features, pathophysiology and management |
| [3838414](https://pubmed.ncbi.nlm.nih.gov/3838414/) | 1985 | Committee report | Am J Gastroenterol | Non-ulcer uses of sucralfate (gastritis, oesophagitis) are promising but not clearly established |
| [12836018](https://pubmed.ncbi.nlm.nih.gov/12836018/) | 2003 | Cohort/Descriptive | Eur J Pediatr | Primary duodenogastric reflux in six children and adolescents (not a sucralfate study) |
| [10228771](https://pubmed.ncbi.nlm.nih.gov/10228771/) | 1999 | Surgical series | Hepatogastroenterology | Duodenal switch operation for pathologic duodenogastric reflux (not drug-related) |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2602288 | Carafate | Tab 1 g | Not recorded |

## Safety Considerations

- **Conditions flagged in the knowledge graph (not label text)**: hyperglycaemia, chronic kidney disease, chronic renal failure syndrome and end-stage renal failure. Caution is reasonable in patients with these conditions.
- **Drug Interactions** (100 interactions found; examples below):
  - **Major**: dolutegravir, bictegravir
  - **Moderate**: tetracycline, doxycycline, alendronic acid, risedronic acid, ibandronate, acarbose, pioglitazone, alogliptin, albiglutide, allopurinol, ascorbic acid, calcifediol, baloxavir marboxil
  - **Minor**: atenolol, acebutolol, betaxolol, bisoprolol, anagrelide

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Two small randomized trials and several clinical reports suggest a benefit of sucralfate in alkaline reflux gastritis, and the mechanism is plausible. However, no registered trials exist, the studies are old and small, and mechanism and label safety data are missing.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Critical appraisal of the two RCTs, and ideally a larger modern trial
- A dosing and interaction management plan (e.g. spacing from interacting drugs) and caution in renal impairment and diabetes

---

**Note on the second prediction (duodenal obstruction):** Its TxGNN score is high (0.9930), but the evidence level is L5 and the recommendation is **Hold**. There is no clear mechanistic link, because sucralfate's mucosal protection does not treat a mechanical obstruction. None of the retrieved literature shows sucralfate treating obstruction. The score likely reflects network proximity to duodenal ulcer.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


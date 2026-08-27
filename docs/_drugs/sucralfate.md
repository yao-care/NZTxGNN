---
layout: default
title: Sucralfate
parent: 僅模型預測 (L5)
nav_order: 325
evidence_level: L5
indication_count: 2
---

# Sucralfate
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Sucralfate: From Peptic Ulcer Disease to Duodenogastric Reflux

## One-Sentence Summary

Sucralfate (DrugBank DB00364) is a mucosal-protective agent historically used in the management of peptic (duodenal/gastric) ulcer disease, based on the drug's own literature record rather than a New Zealand regulatory filing, since the drug currently holds no marketing authorization in New Zealand.
The TxGNN model predicts it may be effective for **Duodenogastric Reflux**, with **0 registered clinical trials** and **13 relevant publications** currently supporting this direction.
A second candidate indication, duodenal obstruction, scored similarly (99.30%) but has even thinner supporting literature and is not covered further below.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in New Zealand regulatory records (drug not marketed); literature in this evidence pack references historical use in duodenal/gastric ulcer and erosive gastroduodenitis |
| Predicted New Indication | Duodenogastric reflux |
| TxGNN Prediction Score | 99.37% (rank 5283) |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in this evidence pack (data gap). Based on the retrieved literature itself, sucralfate is described as having "cytoprotective features in addition to its known antipepsin and antacid effects" (PMID 3838414), and case/randomized studies describe its use for mucosal protection against gastric erosion and ulceration.

Duodenogastric reflux involves retrograde flow of alkaline duodenal (bile-containing) content into the stomach, producing mucosal injury (alkaline/bile reflux gastritis) that is mechanistically related to the acid-peptic mucosal injury sucralfate is traditionally used to treat. Multiple retrieved publications directly studied sucralfate for this indication — including randomized trials against placebo (PMID 3475771) and against rabeprazole (PMID 12923369) — which is consistent with the TxGNN prediction and suggests the model captured a genuine, previously-studied mechanistic link rather than a purely novel association.

Because taiwan_regulatory contains no New Zealand licenses for sucralfate, this rationale relies on literature evidence within the pack rather than an official approved-indication text, and should be treated as lower-confidence until a formal MOA and regulatory record are obtained.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3839973](https://pubmed.ncbi.nlm.nih.gov/3839973/) | 1985 | RCT | The American Journal of Medicine | Randomized, double-blind study of sucralfate 6g/day vs. placebo in 23 patients with alkaline reflux gastritis symptoms after Billroth I/II or vagotomy/pyloroplasty |
| [12923369](https://pubmed.ncbi.nlm.nih.gov/12923369/) | 2003 | RCT | European Journal of Gastroenterology & Hepatology | Randomized trial of sucralfate vs. rabeprazole vs. no treatment for post-cholecystectomy alkaline reactive gastritis |
| [3475771](https://pubmed.ncbi.nlm.nih.gov/3475771/) | 1987 | RCT | Scandinavian Journal of Gastroenterology. Supplement | Prospective randomized trial of sucralfate vs. placebo in patients with symptomatic/macroscopic gastritis, including duodenogastric reflux comparison |
| [1391144](https://pubmed.ncbi.nlm.nih.gov/1391144/) | 1992 | Comparative study | Minerva Gastroenterologica e Dietologica | Compared cisapride (prokinetic) vs. sucralfate (cytoprotective) for dyspeptic symptoms in 18 patients with duodenogastric reflux gastritis |
| [17285081](https://pubmed.ncbi.nlm.nih.gov/17285081/) | 2006 | Review | Journal de Chirurgie | Review of duodenogastric and gastroesophageal bile reflux pathophysiology, diagnosis (24h bile monitoring), and therapeutic management |
| [14723838](https://pubmed.ncbi.nlm.nih.gov/14723838/) | 2004 | Review | Current Treatment Options in Gastroenterology | Review of duodenogastric reflux-induced (alkaline) esophagitis; notes PPIs as best medical treatment, difficulty of DGER management |
| [6372664](https://pubmed.ncbi.nlm.nih.gov/6372664/) | 1984 | Review | Annual Review of Medicine | Review of alkaline reflux (bile) gastritis and esophagitis, pathophysiology and diagnostic features |
| [3552846](https://pubmed.ncbi.nlm.nih.gov/3552846/) | 1987 | Review | Gastroenterologie Clinique et Biologique | Pharmacologic basis of medical treatment for duodenogastric reflux (abstract not available) |
| [3838414](https://pubmed.ncbi.nlm.nih.gov/3838414/) | 1985 | Review | The American Journal of Gastroenterology | ACG committee review of sucralfate's non-ulcer uses, including cytoprotective, antipepsin, and antacid effects |
| [3616071](https://pubmed.ncbi.nlm.nih.gov/3616071/) | 1987 | Case series | Revista Española de las Enfermedades del Aparato Digestivo | 50 cases of postsurgical biliary reflux gastritis treated with sucralfate (abstract not available) |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- Sucralfate is currently not marketed in New Zealand (0 licenses) and package-insert-level safety/warning data is marked as a Blocking data gap, so the candidate cannot yet pass an initial safety screen; supporting evidence for duodenogastric reflux is limited to small, unregistered historical trials and reviews rather than modern registered clinical trials.

**To proceed, the following is needed:**
- TFDA/manufacturer package insert (warnings, contraindications, DDI) to clear the Blocking data gap (DG001)
- Confirmed drug MOA from DrugBank or equivalent source (DG002)
- Evaluation of whether a New Zealand marketing pathway exists for sucralfate before further repurposing investment
- A contemporary, registered clinical trial (ClinicalTrials.gov/ICTRP) confirming efficacy in duodenogastric reflux, since none currently exist
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Pantoprazole
parent: 僅模型預測 (L5)
nav_order: 265
evidence_level: L5
indication_count: 6
---

# Pantoprazole
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
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

# Pantoprazole: From Acid-Suppression Therapy (Original Indication Data Unavailable) to Active Peptic Ulcer Disease

## One-Sentence Summary

Pantoprazole is a proton pump inhibitor (PPI); no formal original-indication text is available in this evidence pack because the drug is not currently marketed in this jurisdiction. The TxGNN model's top prediction is **Active Peptic Ulcer Disease**, supported by **3 clinical trials** and **19 publications** — though it should be noted this largely reflects pantoprazole's already-established pharmacological class effect (acid suppression for peptic ulcer disease) rather than a novel repurposing signal.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available (no approved license/indication text on file; drug not marketed) |
| Predicted New Indication | Active Peptic Ulcer Disease |
| TxGNN Prediction Score | 99.69% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, pantoprazole is a member of the proton pump inhibitor (PPI) class, which binds irreversibly to the gastric parietal cell H+/K+-ATPase to reduce gastric acid secretion. This is the well-established mechanism underlying PPI efficacy across acid-related gastrointestinal disorders.

Active peptic ulcer disease is a classic acid-related condition, and acid suppression via H+/K+-ATPase inhibition is the standard pharmacological rationale for ulcer healing and H. pylori eradication regimens. Multiple completed trials in this evidence pack (including a Phase 3 active-controlled RCT against ilaprazole) directly test pantoprazole in gastric/duodenal ulcer populations.

**Important caveat:** the model's own rationale for this candidate explicitly notes that acid suppression for peptic ulcer disease is pantoprazole's already-approved use, not a newly discovered indication. The high score and strong evidence level (L1) here should therefore be read as a validation of known pharmacology rather than a genuine repurposing opportunity, and any Go/Guardrail decision should be framed accordingly.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02197039](https://clinicaltrials.gov/study/NCT02197039) | N/A | Completed | 316 | Prospective study identifying risk factors for poor stigmata fading/early rebleeding after endoscopic hemostasis plus high-dose PPI infusion, to guide selection for second-look endoscopy |
| [NCT00930670](https://clinicaltrials.gov/study/NCT00930670) | Phase 4 | Completed | 320 | Evaluated the effect of various PPIs (including pantoprazole) and statins on clopidogrel antiplatelet activity in patients undergoing PCI with dual antiplatelet therapy |
| [NCT02084420](https://clinicaltrials.gov/study/NCT02084420) | Phase 3 | Completed | 323 | Multicenter, randomized, double-blind, active-controlled trial comparing ilaprazole vs. pantoprazole triple therapy for 7-day H. pylori eradication in H. pylori-positive gastric and/or duodenal ulcer patients |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [18824852](https://pubmed.ncbi.nlm.nih.gov/18824852/) | 2008 | RCT | Digestion | Prospective RCT comparing intermittent vs. continuous pantoprazole infusion for prevention of peptic ulcer rebleeding after endoscopic hemostasis |
| [12752349](https://pubmed.ncbi.nlm.nih.gov/12752349/) | 2003 | RCT | Aliment Pharmacol Ther | Compared efficacy of three pantoprazole-based triple therapy regimens for H. pylori eradication and gastric ulcer healing |
| [15244210](https://pubmed.ncbi.nlm.nih.gov/15244210/) | 2003 | Cohort | Hepato-gastroenterology | Compared efficacy of lansoprazole vs. pantoprazole in treatment of active duodenal ulcer and H. pylori eradication |
| [38384180](https://pubmed.ncbi.nlm.nih.gov/38384180/) | 2024 | Cohort | Gut and Liver | Multicenter, randomized, active-controlled study validating acid suppressant therapy (PPI-class comparator) for ESD-induced artificial ulcer healing |
| [19938880](https://pubmed.ncbi.nlm.nih.gov/19938880/) | 2009 | Review | Clinical Drug Investigation | Comprehensive review of pantoprazole pharmacology; notes no clinically significant drug-drug interactions identified across numerous interaction studies |
| [9017763](https://pubmed.ncbi.nlm.nih.gov/9017763/) | 1997 | Review | Pharmacotherapy | Review of PPI mechanism (H+/K+-ATPase inhibition) and comparative superiority over H2-receptor antagonists in acid-related disease control |
| [38345252](https://pubmed.ncbi.nlm.nih.gov/38345252/) | 2024 | Review | Am J Gastroenterology | Systematic review/network meta-analysis comparing P-CAB vs. PPI efficacy and safety in healing Grade C/D esophagitis |
| [10983736](https://pubmed.ncbi.nlm.nih.gov/10983736/) | 2000 | Review | Drugs | Review of esomeprazole, comparing intragastric pH control against omeprazole, lansoprazole, and pantoprazole in GORD trials |
| [11693467](https://pubmed.ncbi.nlm.nih.gov/11693467/) | 2001 | Review | Drugs | Update on lansoprazole's role in acid-related disorder management, including peptic ulcer disease and H. pylori eradication regimens |
| [38652367](https://pubmed.ncbi.nlm.nih.gov/38652367/) | 2024 | Preclinical | Inflammopharmacology | Rat model study of combined pantoprazole + mesenchymal stem cell therapy on experimentally induced gastric ulcer, examining oxidative stress, inflammation, and apoptosis pathways |

---

## New Zealand Market Information

Pantoprazole currently has no registered authorizations on file — market status is Not Marketed, with 0 total licenses recorded in this evidence pack.

---

## Safety Considerations

Please refer to the package insert for safety information. (No TFDA warnings, contraindications, or drug-drug interaction data were retrievable at the time of this evidence pack's compilation.)

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Evidence level L1 is supported by a directly relevant Phase 3 active-controlled RCT and multiple RCT/cohort studies, but this strength primarily confirms pantoprazole's already-established PPI-class efficacy in peptic ulcer disease rather than revealing a novel repurposing opportunity — this distinction must be explicit in any downstream use of this candidate. Separately, a **Blocking**-severity data gap (missing TFDA package-insert warnings/contraindications) currently prevents completion of the S1 safety initial screen.

**To proceed, the following is needed:**
- TFDA package insert (warnings, contraindications) — Blocking gap, required before S1 safety screening can complete
- Confirmed mechanism-of-action documentation from DrugBank — needed for a rigorous, non-generic mechanistic-relevance analysis
- Explicit reviewer note that "Active Peptic Ulcer Disease" reflects known PPI pharmacology, not a novel signal, before this candidate is used to justify new-indication claims
- Market/licensing status update if commercial launch in this jurisdiction is planned, since 0 authorizations currently exist
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


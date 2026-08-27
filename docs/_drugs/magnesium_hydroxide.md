---
layout: default
title: Magnesium Hydroxide
parent: 僅模型預測 (L5)
nav_order: 211
evidence_level: L5
indication_count: 6
---

# Magnesium Hydroxide
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

Using superpowers:using-superpowers — checked for applicable skills; this is a fully-specified document-generation task (fill in an exact given template from structured data), no coding/debugging/design skill applies, so proceeding directly.

# Magnesium Hydroxide: From Antacid/Laxative Use to Active Peptic Ulcer Disease

## One-Sentence Summary

Magnesium hydroxide is a long-established inorganic antacid/laxative (e.g. "milk of magnesia") with no formal indication record in this evidence pack and no current New Zealand market authorization. The TxGNN model's top prediction is **Active Peptic Ulcer Disease**, essentially reaffirming its classical acid-neutralizing use, supported by **0 registered clinical trials** but **20 identified publications**, several of them controlled human trials from the 1980s–90s.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not formally recorded in this evidence pack (no NZ/Medsafe license on file); classically used as an OTC antacid and laxative agent |
| Predicted New Indication | Active Peptic Ulcer Disease |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Structured mechanism-of-action data (DrugBank MOA field) is not available for this candidate. However, the evidence pack's own mechanistic rationale is well characterized: magnesium hydroxide directly neutralizes gastric acid, raises intragastric pH, and reduces pepsin activity. Human and animal studies further show that antacids containing Mg(OH)₂ can induce endogenous prostaglandin and EGF/EGF-receptor expression, contributing an additional mucosal cytoprotective effect beyond simple acid buffering.

This is not really a *novel* repurposing signal — acid neutralization and ulcer-related mucosal protection are the textbook, decades-old rationale for antacid use in peptic ulcer disease. TxGNN's high score here largely reflects recovery of an already-established pharmacological relationship rather than an unexpected new indication. That said, it validates the model's mechanistic grounding and confirms the drug's continued relevance as adjunct/symptomatic therapy in an era dominated by PPIs and H2-receptor antagonists.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7034155](https://pubmed.ncbi.nlm.nih.gov/7034155/) | 1981 | RCT | Scandinavian Journal of Gastroenterology | 12-week double-blind trial (n=72): antacid/anticholinergic vs cimetidine vs placebo in active duodenal/prepyloric ulcers; antacid regimen showed 50% healing at 3 weeks vs placebo |
| [6086186](https://pubmed.ncbi.nlm.nih.gov/6086186/) | 1984 | RCT | Clinics in Gastroenterology | Review/trial data on antacids and anticholinergics in duodenal ulcer treatment |
| [1526089](https://pubmed.ncbi.nlm.nih.gov/1526089/) | 1992 | RCT | Clinical Pharmacology and Therapeutics | Multicenter double-blind comparison in active benign gastric ulcer disease, using antacid-era comparator design |
| [22950493](https://pubmed.ncbi.nlm.nih.gov/22950493/) | 2013 | Review | Current Pharmaceutical Design | Updated review of cellular/molecular mechanisms of antacid-mediated gastric cytoprotection and ulcer healing |
| [2595273](https://pubmed.ncbi.nlm.nih.gov/2595273/) | 1989 | Animal study | Scandinavian Journal of Gastroenterology | Al(OH)₃/Mg(OH)₂ antacid gastroprotection in rats against ethanol/aspirin/stress ulcers, mediated by endogenous prostanoids |
| [3018068](https://pubmed.ncbi.nlm.nih.gov/3018068/) | 1986 | Clinical study | Journal of Clinical Gastroenterology | Postprandial buffering comparison of sodium bicarbonate vs aluminum-magnesium hydroxide in duodenal ulcer patients |
| [8260735](https://pubmed.ncbi.nlm.nih.gov/8260735/) | 1993 | Review | Journal of Physiology and Pharmacology | Clinical pharmacology review of magnesium/aluminium-containing antacids, including cytoprotective mechanisms |
| [37146](https://pubmed.ncbi.nlm.nih.gov/37146/) | 1979 | Review | Fortschritte der Medizin | Overview of antacid therapy: acid-neutralizing capacity and dosing relative to meals in peptic ulcer disease |
| [2686073](https://pubmed.ncbi.nlm.nih.gov/2686073/) | 1989 | Clinical study | Terapevticheskii Arkhiv | Effect of almagel (Al/Mg antacid) on gastric/duodenal acidity and protease activity in duodenal ulcer patients |
| [9305482](https://pubmed.ncbi.nlm.nih.gov/9305482/) | 1997 | Clinical study | Alimentary Pharmacology & Therapeutics | H2-receptor antagonists and Al/Mg(OH)₃ antacids show an aggravating effect on H. pylori gastritis in duodenal ulcer patients |

## New Zealand Market Information

No active Medsafe authorizations on record — magnesium hydroxide is currently not marketed in New Zealand under this evidence pack, so no product/license table can be produced.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanistic link (acid neutralization + mucosal cytoprotection) is textbook-level and directly supported by multiple older human RCTs and mechanism studies, but no modern registered clinical trials exist and the drug has no current NZ market presence, so guardrails (safety/regulatory data) are needed before advancing.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert warnings and contraindications (currently blocking — DG001)
- Formal DrugBank mechanism-of-action confirmation (currently high-priority gap — DG002)
- Drug interaction (DDI) data, as the DDI query returned no results
- Confirmation of formal original indication text, since no license record currently exists

*Note — other TxGNN-predicted indications for this drug in the same evidence pack:* gastric ulcer (disease) and gastroduodenitis also scored L2/Proceed with Guardrails; gastrojejunal ulcer scored L3/Research Question; stomach disease scored L3/Research Question; peptic ulcer perforation scored L5/Hold (mechanistically implausible — perforation is a surgical emergency, not treatable by antacid).
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Ceftriaxone
parent: 僅模型預測 (L5)
nav_order: 68
evidence_level: L5
indication_count: 7
---

# Ceftriaxone
{: .fs-9 }

證據等級: **L5** | 預測適應症: **7** 個
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

# CEFTRIAXONE: From Bacterial Infections to Hyperamylasemia

## One-Sentence Summary

Ceftriaxone is a third-generation cephalosporin antibiotic widely used for treating serious bacterial infections including meningitis, pneumonia, and sepsis.
The TxGNN model predicts it may have utility in **Hyperamylasemia**, with **0 clinical trials** and **3 publications** currently supporting this direction — all via indirect mechanistic links rather than direct therapeutic evidence.
At Evidence Level L4, this prediction warrants a **Hold** decision pending mechanistic clarification.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Serious bacterial infections (not registered in New Zealand) |
| Predicted New Indication | Hyperamylasemia |
| TxGNN Prediction Score | 99.39% |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the Evidence Pack. Based on established pharmacological knowledge, ceftriaxone is a third-generation cephalosporin beta-lactam antibiotic. It exerts its bactericidal effect by binding to penicillin-binding proteins (PBPs) and inhibiting bacterial cell wall synthesis, leading to cell lysis. Its long half-life (6–9 hours) permits once-daily dosing, and its broad spectrum covers gram-positive organisms (including penicillin-resistant *Streptococcus pneumoniae*) and many gram-negative pathogens.

The connection between ceftriaxone and hyperamylasemia is not a direct therapeutic one. The mechanistic link is indirect and operates through two pathways: first, leptospirosis (Weil's syndrome) is a known cause of secondary hyperamylasemia, and ceftriaxone is the drug of choice for treating leptospirosis — treating the underlying infection may resolve the associated enzyme elevation. Second, prophylactic ceftriaxone administered after endoscopic papillosphincterotomy (EPS) may prevent secondary biliary infections, thereby reducing post-procedural pancreatic enzyme elevation.

These relationships represent coincidental or secondary effects rather than a direct mechanism for treating hyperamylasemia per se. The high TxGNN score (99.39%) most likely reflects disease co-occurrence patterns in the knowledge graph — specifically, the shared nodes between infectious disease entities and pancreatic enzyme abnormalities — rather than a genuine repurposing signal. Further mechanistic hypothesis generation is needed before this candidate can advance.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for ceftriaxone in hyperamylasemia.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [10458061](https://pubmed.ncbi.nlm.nih.gov/10458061/) | 1999 | RCT/Observational | *Bratislavske lekarske listy* | Prophylactic ceftriaxone 1 g in 30 patients after endoscopic papillosphincterotomy and gallstone extraction; compared with controls without prophylaxis. Most common bile isolates were *Pseudomonas aeruginosa* and *E. coli*, all sensitive to ceftriaxone. Prophylactic effect on post-procedural biliary/pancreatic enzyme changes was assessed. |
| [7522351](https://pubmed.ncbi.nlm.nih.gov/7522351/) | 1994 | Observational | *Southern Medical Journal* | Evaluated elevated pancreatic amylase and lipase in 38 patients with intracranial bleeding without pancreatitis; 25 had elevated lipase, 17/25 also had elevated amylase. Describes hyperamylasemia as a neurogenic phenomenon unrelated to antibiotic therapy. |
| [36263834](https://pubmed.ncbi.nlm.nih.gov/36263834/) | 2023 | Case Report | *Revista española de enfermedades digestivas* | Weil syndrome (leptospirosis) presenting with upper GI bleeding; demonstrates that leptospirosis — for which ceftriaxone is first-line treatment — can cause multi-organ involvement including pancreatic enzyme elevation. Ceftriaxone not directly evaluated for hyperamylasemia. |

---

## New Zealand Market Information

Ceftriaxone has no registered product authorizations in New Zealand based on the current Evidence Pack data.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The three retrieved publications link ceftriaxone to hyperamylasemia only through indirect mechanisms — treating an underlying infection (leptospirosis) or preventing post-procedural biliary complications — with no study directly evaluating ceftriaxone as a therapy for hyperamylasemia itself. The TxGNN signal most likely reflects knowledge graph co-occurrence rather than a repurposing-actionable therapeutic relationship.

**To proceed, the following is needed:**

- A coherent mechanistic hypothesis explaining how ceftriaxone could directly modulate serum amylase levels, independent of infection treatment
- Preclinical data (in vitro or animal models) demonstrating any direct effect on pancreatic acinar cell function or amylase secretion
- A systematic literature review to determine whether any population receiving ceftriaxone has shown measurable amylase reduction as a primary outcome
- Clarification of ceftriaxone's full MOA profile (DrugBank data) to identify any off-target effects relevant to pancreatic physiology
- Regulatory baseline: obtain package insert warnings and contraindications before any clinical exploration
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


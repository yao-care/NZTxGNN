---
layout: default
title: Benzbromarone
parent: 僅模型預測 (L5)
nav_order: 47
evidence_level: L5
indication_count: 1
---

# Benzbromarone
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Benzbromarone: From Hyperuricemia to Renal Hypouricemia

## One-Sentence Summary

Benzbromarone is a uricosuric agent classically used to treat hyperuricemia and gout by blocking uric acid reabsorption in the kidney.
The TxGNN model predicts it may be relevant to **Renal Hypouricemia** with a score of 99.07%, supported by **0 clinical trials** and **20 publications**.
However, mechanistic analysis strongly suggests this is a **false-positive signal**: benzbromarone appears in the literature as a diagnostic pharmacological probe — not a therapeutic agent — for this condition, making the prediction an artefact of co-occurrence rather than a genuine repurposing opportunity.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Hyperuricemia / Gout (uricosuric agent; not registered in New Zealand) |
| Predicted New Indication | Hypouricemia, Renal |
| TxGNN Prediction Score | 99.07% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the dataset. Based on established pharmacology, benzbromarone is a potent uricosuric agent that works by inhibiting URAT1 (SLC22A12) and related urate transporters on the apical membrane of renal proximal tubule cells, thereby blocking tubular reabsorption of uric acid and increasing its urinary excretion. This mechanism is effective in hyperuricemia, where excess uric acid reabsorption raises serum levels.

Renal hypouricemia, however, arises from the exact opposite pathophysiology: loss-of-function mutations in URAT1 (SLC22A12) or GLUT9 (SLC2A9) abolish uric acid reabsorption from the outset, causing pathologically low serum urate and excessive urinary excretion. The core defect is already a failure of the very transporter that benzbromarone inhibits. Administering a URAT1 inhibitor to a patient who lacks functional URAT1 would provide no pharmacological benefit and could further aggravate hyperuricosuria, increasing the risk of the condition's most serious complication: exercise-induced acute renal failure.

The high TxGNN score almost certainly reflects **methodological co-occurrence** in the literature. Benzbromarone is widely used as a **pharmacological probe** in the pyrazinamide–benzbromarone test, a standard diagnostic tool to subtype renal hypouricemia by characterising the site of the tubular transport defect (pre-secretory vs. post-secretory reabsorption). Multiple papers in the evidence set use benzbromarone for this diagnostic purpose only. TxGNN likely learned this strong co-occurrence without distinguishing diagnostic use from therapeutic intent.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [31650389](https://pubmed.ncbi.nlm.nih.gov/31650389/) | 2020 | Review | Clinical Rheumatology | Narrative review of hypouricemia for rheumatologists; covers etiology, URAT1/GLUT9 mutations, and diagnostic workup including benzbromarone as a probe |
| [14694169](https://pubmed.ncbi.nlm.nih.gov/14694169/) | 2004 | Cohort / Molecular | JASN | SLC22A12 gene sequencing in 32 patients; established URAT1 mutation as primary cause of renal hypouricemia in Japan |
| [14747372](https://pubmed.ncbi.nlm.nih.gov/14747372/) | 2004 | Basic Science | JASN | Mouse URAT1 homologue (RST) localisation and function; benzbromarone confirmed as URAT1 inhibitor in oocyte expression system |
| [18670416](https://pubmed.ncbi.nlm.nih.gov/18670416/) | 2008 | Clinical Study | Am J Hypertension | Losartan's uricosuric action via URAT1 inhibition; contextualises the role of URAT1 inhibitors in modulating urate handling |
| [8893184](https://pubmed.ncbi.nlm.nih.gov/8893184/) | 1996 | Case Series | Acta Paediatrica | Recurrent exercise-induced ARF in renal hypouricemia; benzbromarone/pyrazinamide test used to localise tubular defect |
| [9144014](https://pubmed.ncbi.nlm.nih.gov/9144014/) | 1997 | Case Series | Internal Medicine | Two renal hypouricemia patients with nephrolithiasis; benzbromarone suppression test performed for subtype diagnosis |
| [3380222](https://pubmed.ncbi.nlm.nih.gov/3380222/) | 1988 | Case Report / Mechanistic | Nephron | Isolated renal urate transport defect; benzbromarone paradoxically increased urate clearance, confirming absent reabsorption |
| [8302413](https://pubmed.ncbi.nlm.nih.gov/8302413/) | 1993 | Case Report / Mechanistic | Nephron | Renal hypouricemia with enhanced tubular secretion and urolithiasis; benzbromarone used as pharmacological probe |
| [1501741](https://pubmed.ncbi.nlm.nih.gov/1501741/) | 1992 | Case Series | Nephron | Two subjects in whom pyrazinamide failed to inhibit benzbromarone's uricosuric action; mechanistic investigation of URAT1 transport components |
| [11676906](https://pubmed.ncbi.nlm.nih.gov/11676906/) | 2001 | Case Report | Anales Españoles de Pediatría | 12-month-old infant with renal hypouricemia and uric acid urolithiasis; positive benzbromarone response identified presecretory defect |

---

## New Zealand Market Information

Benzbromarone is **not registered** in New Zealand. No Medsafe authorizations exist. This drug is unavailable through the standard regulatory pathway and would require special access arrangements (e.g., provisional consent) for any clinical use.

---

## Safety Considerations

Please refer to the package insert for safety information. No drug interaction data was retrieved from the available sources.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN prediction score is high (99.07%), but this is almost certainly a mechanistic false positive: benzbromarone is a URAT1 inhibitor, while renal hypouricemia is caused by URAT1 loss of function — making pharmacological inhibition of an already non-functional transporter both irrational and potentially harmful. The 20 publications identified document benzbromarone's role as a diagnostic probe, not a therapeutic candidate.

**To proceed, the following would be needed:**

- A formal mechanistic re-evaluation to definitively exclude any residual URAT1 function in patient subtypes where benzbromarone might conceivably have effect (e.g., partial/incomplete transport defects such as PMID 7933674)
- Confirmation from a clinical expert in renal urate transport disorders that no therapeutic application is plausible for any subtype
- If the signal is to be preserved for documentation, flag this entry in the TxGNN output database as a **diagnostic co-occurrence artefact** to prevent future re-evaluation cycles
- Consider whether this case should be used to improve TxGNN's ability to discriminate diagnostic/probe drug use from therapeutic use in training data
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


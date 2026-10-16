---
layout: default
title: Ferrous Sulfate Anhydrous
parent: Model Prediction Only (L5)
nav_order: 198
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-15
description: "Ferrous Sulfate Anhydrous: predicted new indication Vitamin B12- and folate-independent constitutional megaloblastic anem… (evidence level L5). Approved use: Iron deficiency anemia, folic acid defici…"
monitor_inn: "Ferrous sulfate anhydrous"
predicted_indications: ["vitamin B12- and folate-independent constitutional megaloblastic anemia", "Plummer-Vinson syndrome", "biotin metabolic disease", "non-syndromic esophageal malformation"]
---

# Ferrous Sulfate Anhydrous
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

# Ferrous Sulfate Anhydrous: From Iron Deficiency Anemia to Vitamin B12- and Folate-Independent Constitutional Megaloblastic Anemia

## One-Sentence Summary

Ferrous sulfate anhydrous is an oral iron supplement, originally used to treat iron deficiency anemia and related nutritional anemias.
The TxGNN model predicts it may be effective for **vitamin B12- and folate-independent constitutional megaloblastic anemia**,
but there are currently **0 clinical trials** and **0 publications** supporting this prediction, so it rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Iron deficiency anemia, folic acid deficiency anemia, microcytic anemia, hypochromic anemia, anemia of prematurity, vitamin deficiency disorder (no approved indication text in the New Zealand licence record) |
| Predicted New Indication | Vitamin B12- and folate-independent constitutional megaloblastic anemia |
| TxGNN Prediction Score | 99.78% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, ferrous sulfate is an iron replacement salt, its efficacy in iron deficiency anemia is well established, and the model links it to other anemias through network proximity.

The link to the predicted indication is weak. Megaloblastic anemia is classically driven by impaired DNA synthesis from vitamin B12 or folate deficiency. The constitutional form predicted here is, by definition, independent of both, and no iron-dependent mechanism is established. The high score most likely reflects proximity in the knowledge graph to approved anemia indications (folic acid deficiency anemia, microcytic anemia) rather than a genuine pharmacological rationale. Iron would not be expected to correct a B12/folate-independent defect.

**Other predictions reviewed**

| Rank | Predicted Indication | Score | Evidence Level | Assessment |
|------|------|------|------|------|
| 2 | Plummer-Vinson syndrome | 99.77% | L4 | Biologically plausible: the syndrome combines iron deficiency anemia, dysphagia and upper esophageal webs, and iron repletion is the established standard of care, with case-series reports of improved dysphagia and anemia. Evidence is limited to case reports and small series, with no RCTs. This is effectively an extension of the existing iron deficiency anemia indication, not a novel repurposing. Recommendation: Research Question |
| 3 | Biotin metabolic disease | 99.51% | L5 | No established role for iron; these disorders are treated with biotin. Likely reflects shared nutrient-deficiency neighbours in the graph. Recommendation: Hold |
| 4 | Non-syndromic esophageal malformation | 99.49% | L5 | Structural malformations need surgical or endoscopic management; iron cannot correct them. Likely spurious association via Plummer-Vinson syndrome. Recommendation: Hold |

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2664429 | Ferro-Liquid (cancelled) | Oral liquid 30 mg (6 mg elemental iron) per 1 ml | Not recorded |

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (not taken from label text): peptic ulcer disease, sickle cell anemia, hereditary persistence of fetal hemoglobin, primary hyperoxaluria, hereditary hemochromatosis, hemosiderosis, iron metabolism disease, Wilson disease, ulcerative colitis, inflammatory bowel disease, hemolytic anemia, dyskinesia of esophagus, gastroenteritis.
- **Drug Interactions**: 37 interactions were identified. The 20 listed in the data compiled for this report are mostly moderate:
  - Proton pump inhibitors: rabeprazole, omeprazole, lansoprazole, dexlansoprazole, pantoprazole, esomeprazole (moderate)
  - Tetracyclines: doxycycline, tetracycline, minocycline (moderate)
  - Quinolone: levofloxacin (moderate)
  - Antacids and minerals: magnesium oxide, calcium carbonate, aluminum hydroxide, magaldrate (moderate)
  - Others: pancrelipase, sodium sulfate, triethylenetetramine (moderate)
  - Minor: cimetidine, zinc sulfate, zinc acetate
- Oral iron is also noted in the analysis as having GI tolerability issues.

Package insert warnings and contraindications have not been obtained. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction is supported only by a model score, with no trials or literature, and has no plausible iron-dependent mechanism. The product's only New Zealand licence is cancelled. The one biologically credible candidate, Plummer-Vinson syndrome (rank 2), is essentially an extension of the existing iron deficiency anemia use and is better handled as a research question than as a repurposing decision.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data (for example from DrugBank)
- Literature and trial searches for Plummer-Vinson syndrome, and for constitutional megaloblastic anemia if it is to be pursued
- Clarification of current New Zealand availability, since the only licence is cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Nitrofurantoin
parent: Model Prediction Only (L5)
nav_order: 325
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Nitrofurantoin: predicted new indication Rheumatoid arthritis (evidence level L5). Approved use: Not listed in the New Zealand licence records (known use: urinary tra…"
monitor_inn: "NITROFURANTOIN"
predicted_indications: ["rheumatoid arthritis", "autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome", "brain small vessel disease 1 with or without ocular anomalies", "diabetic nephropathy", "brachydactyly-syndactyly syndrome", "colobomatous microphthalmia-rhizomelic dysplasia syndrome", "sclerosing cholangitis", "methemoglobinemia, alpha type", "gout", "methemoglobinemia"]
---

# Nitrofurantoin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Nitrofurantoin: From Urinary Tract Infection to Rheumatoid Arthritis

## One-Sentence Summary

Nitrofurantoin is an antibacterial drug. The data compiled for this report do not list an approved indication, but its known use is for urinary tract infections.
The TxGNN model predicts it may be effective for **rheumatoid arthritis**, but there are **0 clinical trials** and no publications showing therapeutic benefit.
The literature retrieved mostly describes harms, so the prediction is probably an artifact of the knowledge graph.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not listed in the New Zealand licence records (known use: urinary tract infection) |
| Predicted New Indication | Rheumatoid arthritis |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorisation) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Nitrofurantoin is a nitrofuran antibacterial. Its metabolites are generally understood to damage bacterial DNA and ribosomal proteins, and its efficacy in urinary tract infections is established.

This mechanism has no plausible therapeutic link to rheumatoid arthritis, which is an autoimmune inflammatory joint disease. The very high TxGNN score (0.999) is most likely a knowledge-graph artifact and is not supported by clinical evidence.

The retrieved literature points the other way. It describes drug-induced pulmonary fibrosis, an interaction between nitrofurantoin and methotrexate (a standard rheumatoid arthritis drug) causing irreversible pulmonary fibrosis, and an association between antibiotic use and rheumatoid arthritis flares.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31222078](https://pubmed.ncbi.nlm.nih.gov/31222078/) | 2019 | Cohort (self-controlled case series) | Scientific Reports | Examined the association between antibiotic use and rheumatoid arthritis flares in a UK cohort of 31,992 newly diagnosed patients |
| [35145797](https://pubmed.ncbi.nlm.nih.gov/35145797/) | 2022 | Case report | Cureus | A 94-year-old woman on long-term methotrexate developed irreversible pulmonary fibrosis after nitrofurantoin |
| [3335140](https://pubmed.ncbi.nlm.nih.gov/3335140/) | 1988 | Cohort | Chest | Interstitial lung fibrosis requiring hospitalisation in rheumatoid arthritis patients was rare and carried a poor prognosis |
| [15195196](https://pubmed.ncbi.nlm.nih.gov/15195196/) | 2004 | Review | Saudi Medical Journal | Drug-induced pulmonary fibrosis; nitrofurantoin is among the causative drugs |
| [25362778](https://pubmed.ncbi.nlm.nih.gov/25362778/) | 2014 | Review | La Revue du Praticien | Drug-induced interstitial lung diseases; nitrofurantoin is among the implicated antibiotics |
| [41635325](https://pubmed.ncbi.nlm.nih.gov/41635325/) | 2026 | Case report | Cureus | Autoimmune hepatitis versus drug-induced liver injury; nitrofurantoin is listed as a cause to rule out |
| [11937933](https://pubmed.ncbi.nlm.nih.gov/11937933/) | 2002 | Case report | Annales de Dermatologie et de Vénéréologie | Drug-induced sialadenitis; nitrofurantoin is among the reported drugs |
| [8104358](https://pubmed.ncbi.nlm.nih.gov/8104358/) | 1993 | Case report | Revue de Pneumologie Clinique | Gold salt-induced pneumonitis with CD4 alveolitis; not directly about nitrofurantoin |

None of these papers shows a therapeutic benefit of nitrofurantoin in rheumatoid arthritis.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 254738 | Nifuran | Tab 50 mg | Not recorded (cancelled) |
| 254754 | Nifuran | Tab 100 mg | Not recorded (cancelled) |
| 2493624 | Macrobid | Cap modified-release 100 mg | Not recorded (cancelled) |

---

## Safety Considerations

- **Disease-level contraindication signals (knowledge graph, not label text)**: The knowledge graph links nitrofurantoin to the following conditions:
  - Renal: anuria, impaired renal function
  - Metabolic: diabetes mellitus, diabetic ketoacidosis, G6PD deficiency, glycogen storage disease, cystinuria, galactosemia
  - Neurological: peripheral neuropathy, peripheral nervous system disease, optic neuritis
  - Pulmonary: interstitial lung disease, pulmonary fibrosis, postinflammatory pulmonary fibrosis
  - Hepatobiliary: drug-induced liver injury, hepatitis, obstructive jaundice, biliary tract disease
  - Gastrointestinal: Clostridium difficile colitis, perinatal necrotizing enterocolitis
  - Nutritional: vitamin B12 deficiency, vitamin B deficiency, beriberi, ariboflavinosis
- **Drug Interactions**: 100 interactions were found. Those listed include:
  - Moderate: balsalazide, metronidazole, picosulfuric acid, naltrexone, rosuvastatin, simvastatin
  - Minor: anticholinergics (hyoscyamine, atropine, glycopyrronium, scopolamine and others) and magnesium-containing antacids
- **Evidence from the retrieved literature**: Reports of pulmonary toxicity (including with methotrexate), liver injury, and hemolysis or methemoglobinemia in G6PD deficiency are relevant to a rheumatoid arthritis population. Many such patients take methotrexate, and the lung is a common site of rheumatoid arthritis complications.

Please also refer to the package insert for label warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score. There are no clinical trials and no literature showing benefit, and the mechanism has no plausible link to rheumatoid arthritis. The available evidence instead raises safety concerns, especially pulmonary and hepatic toxicity and the methotrexate interaction. All New Zealand authorisations are cancelled.

**To proceed, the following is needed:**
- A plausible mechanistic rationale for anti-rheumatic activity, supported by preclinical data
- Mechanism of action data and the package insert warnings and contraindications
- A safety assessment for patients on methotrexate or with pulmonary or hepatic disease
- Confirmation of regulatory status, since no valid New Zealand authorisation currently exists

The other predicted indications reviewed (including diabetic nephropathy, sclerosing cholangitis, methemoglobinemia and several rare genetic syndromes) also have only model-prediction support. For several of them the evidence points to harm, notably methemoglobinemia, where nitrofurantoin is a documented cause. None of them supports advancing the drug.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


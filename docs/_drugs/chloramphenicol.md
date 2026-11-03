---
layout: default
title: Chloramphenicol
parent: Model Prediction Only (L5)
nav_order: 95
evidence_level: L5
indication_count: 8
last_modified_at: 2026-11-03
monitor_inn: "CHLORAMPHENICOL"
predicted_indications: ["diffuse scleroderma", "postinfectious vasculitis", "post-bacterial disorder", "post-infectious syndrome", "infective urethral stricture", "Chagas cardiomyopathy", "infection-related hemolytic uremic syndrome", "punctate epithelial keratoconjunctivitis"]
description: "Chloramphenicol: predicted new indication Diffuse scleroderma (evidence level L5). Approved use: Not recorded in the local data"
---

# Chloramphenicol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
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

## Chloramphenicol: Diffuse scleroderma
{: .text-alpha }

## One-Sentence Summary

Chloramphenicol is a broad-spectrum antibiotic originally used for systemic bacterial and rickettsial infections such as typhoid fever, salmonellosis, meningococcal infection and typhus.
Topical use in bacterial conjunctivitis is already well established, so this is closer to an existing use than to true repurposing.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Diffuse scleroderma |
| TxGNN Prediction Score | 0.9965 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, chloramphenicol is a bacteriostatic antibiotic that inhibits protein synthesis by binding the 50S ribosomal subunit. It is active against many common ocular pathogens.

The original indications are systemic bacterial infections, so the extension to bacterial conjunctivitis is a natural one. Topical chloramphenicol has been used for conjunctivitis for decades, particularly in the UK, and the New Zealand licence records are for eye products (eye drops 0.5% and eye ointment 1%). The prediction therefore largely reflects an established use rather than a new discovery.

Other predicted indications are much weaker. Diffuse scleroderma appears to be a literature artefact: the papers are in vitro studies in which chloramphenicol was probably used as a reporter-assay tool (the CAT gene), not as a therapy. Post-infectious vasculitis, post-bacterial disorder, post-infectious syndrome, infective urethral stricture, Chagas cardiomyopathy, infection-related haemolytic uraemic syndrome, and punctate epithelial keratoconjunctivitis have no meaningful clinical support. Several of these conditions are also ones in which antibiotics are not established treatments, or in which chloramphenicol's haematological toxicity is a particular concern.

## Clinical Trial Evidence

Currently no related clinical trials registered for conjunctivitis.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [32959365](https://pubmed.ncbi.nlm.nih.gov/32959365/) | 2020 | Systematic review | Cochrane Database Syst Rev | Interventions for preventing ophthalmia neonatorum (neonatal conjunctivitis) |
| [38511104](https://pubmed.ncbi.nlm.nih.gov/38511104/) | 2024 | Comparative clinical study | Curr Ther Res | Moxifloxacin compared with chloramphenicol for bacterial eye infections |
| [3300139](https://pubmed.ncbi.nlm.nih.gov/3300139/) | 1987 | Randomized comparison (open) | Acta Ophthalmol | In Tanzania, fusidic acid had higher clinical success (93%) than chloramphenicol (48%) and framycetin (74%), with higher in vitro resistance to chloramphenicol |
| [11270936](https://pubmed.ncbi.nlm.nih.gov/11270936/) | 2001 | Review | Drugs | Comparative review of 16 topical ophthalmic antibacterial preparations |
| [8800624](https://pubmed.ncbi.nlm.nih.gov/8800624/) | 1996 | Safety review | Drug Saf | Debated link between topical ocular chloramphenicol and aplastic anaemia |
| [7498557](https://pubmed.ncbi.nlm.nih.gov/7498557/) | 1995 | Review/commentary | Eye (Lond) | Use and possible overuse of topical chloramphenicol |
| [25833086](https://pubmed.ncbi.nlm.nih.gov/25833086/) | 2015 | Commentary | Clin Exp Ophthalmol | Over-the-counter chloramphenicol |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2368137 | Chlorafast | Eye drops 0.5% | Not recorded (valid) |
| 2576902 | Devatis | Eye ointment 1% | Not recorded (valid) |

## Cytotoxicity

Chloramphenicol is not an antineoplastic drug, so cytotoxic-drug classification does not apply.

## Safety Considerations

- **Conditions flagged as contraindications in the knowledge graph** (derived from drug-disease relationships, not from label text):
  - Haematological: aplastic anaemia (including idiopathic), agranulocytosis, neutropenia (including congenital forms), thrombocytopenia, anaemia, paroxysmal nocturnal haemoglobinuria
  - Organ disease: kidney disease, liver disease, gallbladder disease
  - Neurological: peripheral neuropathy, optic neuritis
  - Metabolic: acute intermittent porphyria
- **Drug Interactions** (100 interactions identified; examples from the list provided):
  - Major: cisapride, naloxegol
  - Moderate: sulfonylureas (glimepiride, glipizide, glyburide, chlorpropamide, acetohexamide), repaglinide, saxagliptin, simvastatin, rosuvastatin, metronidazole, budesonide, aprepitant, iron
- **Literature safety signal**: the possible link between topical ocular chloramphenicol and aplastic anaemia remains controversial, and use is restricted in some countries.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
However, there are no registered trials, both New Zealand licences are cancelled, and the haematological safety concerns are significant. The other predicted indications are unsupported and should not be pursued.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Reasons for the cancellation of the two ophthalmic licences, and whether any product is currently available
- A comparison with currently marketed ophthalmic antibiotics in New Zealand, including a safety and benefit-risk review of topical use
- Confirmation of current local resistance patterns in common ocular pathogens
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


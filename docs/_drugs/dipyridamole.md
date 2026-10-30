---
layout: default
title: Dipyridamole
parent: Model Prediction Only (L5)
nav_order: 157
evidence_level: L5
indication_count: 8
monitor_inn: "DIPYRIDAMOLE"
predicted_indications: ["thrombotic disease", "sick sinus syndrome 2, autosomal dominant", "transient ischemic attack (disease)", "sarcoglycanopathy", "Wildervanck syndrome", "macrocephaly, dysmorphic facies, and psychomotor retardation", "cavernous sinus thrombosis", "lateral sinus thrombosis"]
last_modified_at: 2026-10-30
description: "Dipyridamole: predicted new indication Thrombotic disease (evidence level L5). Approved use: Atrial fibrillation"
---

# Dipyridamole
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

## Dipyridamole: Thrombotic Disease
{: .text-alpha }

## One-Sentence Summary

Dipyridamole is an antiplatelet and vasodilator drug. Atrial fibrillation is not a label-supported approved indication.
The TxGNN model predicts it may be effective for **thrombotic disease**, a broad indication that has **no registered clinical trials** and **20 publications**, mostly reviews of antiplatelet drugs that are indirect evidence.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Atrial fibrillation (a relation recorded in the knowledge graph; no approved indication text in the New Zealand licence) |
| Predicted New Indication | Thrombotic disease |
| TxGNN Prediction Score | 0.9994 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 (valid: 1) |
| Number of Approvals | 1 (single-ingredient product) |
| Recommended Decision | Hold |

*Evidence level note:* the cited literature is general antiplatelet reviews, a few case-level reports in thrombotic thrombocytopenic purpura, and off-topic items. None directly studies "thrombotic disease" as a defined disease with dipyridamole. Under the evidence rules this is capped at L5, although the data compiled for this report had suggested L4.

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Published reviews describe dipyridamole as a phosphodiesterase inhibitor that also blocks adenosine reuptake. This raises cAMP and cGMP in platelets and vascular cells and reversibly inhibits platelet aggregation (Kim & Liao, 2008).

Because platelet aggregation is central to thrombosis, a link to thrombotic disease is plausible. Dipyridamole is also already used as an adjunct antithrombotic, for example with aspirin. The prediction therefore overlaps with known pharmacology rather than describing a new mechanism.

The link between atrial fibrillation and the predicted indication is thromboembolic risk. This connection is inferred and has not been shown by the supplied evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered for thrombotic disease.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [18174451](https://pubmed.ncbi.nlm.nih.gov/18174451/) | 2008 | Review | Arterioscler Thromb Vasc Biol | Dipyridamole raises cAMP/cGMP, reversibly inhibits platelet aggregation and may enhance nitric oxide-mediated vascular protection |
| [382146](https://pubmed.ncbi.nlm.nih.gov/382146/) | 1979 | Review | Postgrad Med | Aspirin, dipyridamole and sulfinpyrazone show antithrombotic effects in humans |
| [12765506](https://pubmed.ncbi.nlm.nih.gov/12765506/) | 2003 | Review | Med J Aust | Antiplatelet drugs protect against myocardial infarction, stroke and other vascular events in high-risk patients |
| [15494585](https://pubmed.ncbi.nlm.nih.gov/15494585/) | 2004 | Review | JAMA | Antiplatelet agents are effective in preventing recurrent vascular events in established vascular disease |
| [2182246](https://pubmed.ncbi.nlm.nih.gov/2182246/) | 1990 | Review | Clin Cardiol | Aspirin plus dipyridamole in coronary thrombosis: aspirin is partially effective, with limitations |
| [2440111](https://pubmed.ncbi.nlm.nih.gov/2440111/) | 1987 | Review | Semin Hematol | Antiplatelet agents reported to improve thrombotic thrombocytopenic purpura in many cases |
| [559478](https://pubmed.ncbi.nlm.nih.gov/559478/) | 1977 | Case report | Arch Intern Med | Two thrombotic thrombocytopenic purpura patients; one remitted on dipyridamole with aspirin and sulfinpyrazone |
| [25697566](https://pubmed.ncbi.nlm.nih.gov/25697566/) | 2015 | Review | Curr Top Med Chem | Dipyridamole has unrecognised antioxidant activity alongside antithrombotic and vasodilator use |
| [20484211](https://pubmed.ncbi.nlm.nih.gov/20484211/) | 2010 | Review | Am J Health Syst Pharm | Antiplatelet and anticoagulant drugs in sickle cell disease complications |
| [11945125](https://pubmed.ncbi.nlm.nih.gov/11945125/) | 2002 | Review | Curr Med Chem | Overview of antiplatelet agents in thromboembolic disease |

Several other retrieved items were off-topic and are not listed, for example the bevacizumab mesothelioma trial (PMID 26719230).

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2710919 | Dipyridamole - Strides | Modified-release capsule 200 mg | Not stated in the licence record |

## Safety Considerations

- **Contraindications recorded in the knowledge graph (not label text):** the knowledge graph lists drug-disease contraindication relations including peptic ulcer disease, hypotensive disorder, thrombocytopenia, hemophilia, acute coronary syndrome, myocardial infarction, kidney disease, liver disease and gout. These need to be checked against the New Zealand package insert.
- **Drug Interactions:** 100 interactions are recorded. Those classed as Moderate are dexfenfluramine, sibutramine and fenfluramine. Others, such as acetylsalicylic acid, cimetidine, omeprazole, metformin and simvastatin, have an unclassified severity level.
- **Pharmacology-based caution:** dipyridamole raises adenosine levels, which is a potential concern in sinus node disease and coronary artery disease.

The package insert warnings and contraindications could not be retrieved, so please refer to the package insert for authoritative safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is biologically plausible and overlaps with dipyridamole's known antiplatelet pharmacology. However, "thrombotic disease" is very broad, no trials are registered for it, and the supplied literature is indirect. Evidence is therefore at L5. Among the other predictions, only transient ischemic attack has strong direct support (the ESPRIT randomised trial, n=4500, and meta-analyses), and that could be evaluated as a separate, more specific candidate.

**To proceed, the following is needed:**
- A specific thrombotic indication (for example secondary stroke prevention) with directly relevant randomised trials or meta-analyses
- Mechanism of action data confirmed from DrugBank
- Medsafe package insert warnings and contraindications, which are currently missing and block safety screening
- The approved indication text for the New Zealand licence, which is currently blank
- A review of high-risk drug interactions, especially with anticoagulants and other antiplatelets
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


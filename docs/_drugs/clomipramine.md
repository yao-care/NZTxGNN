---
layout: default
title: Clomipramine
parent: Model Prediction Only (L5)
nav_order: 110
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Clomipramine: predicted new indication Anxiety disorder (evidence level L5). Approved use: Obsessive-compulsive disorder"
monitor_inn: "Clomipramine"
predicted_indications: ["anxiety disorder", "benign paroxysmal torticollis of infancy", "schizoid personality disorder", "histrionic personality disorder (disease)", "paranoid personality disorder", "schizotypal personality disorder", "agoraphobia", "major depressive disorder", "endogenous depression", "attention deficit-hyperactivity disorder"]
---

# Clomipramine
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

# Clomipramine: From Obsessive-Compulsive Disorder to Anxiety Disorder

## One-Sentence Summary

Clomipramine is a tricyclic antidepressant originally used to treat obsessive-compulsive disorder (OCD).
The TxGNN model predicts it may be effective for **anxiety disorder**, a prediction that is supported mainly by **literature** (especially for panic disorder and agoraphobia), while the **8 clinical trials** retrieved are mostly OCD studies and give only indirect support.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Obsessive-compulsive disorder (from the knowledge graph; no approved indication text is available from the New Zealand licence) |
| Predicted New Indication | Anxiety disorder |
| TxGNN Prediction Score | 99.93% |
| Evidence Level | L1 (as scored in the data compiled for this report; see caveat below) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Proceed with Guardrails |

**Caveat on the evidence level:** the L1 grade is the one assigned in the compiled data. Under the stated rules, L1 requires at least two completed Phase 3 RCTs testing the predicted indication. The only completed Phase 3 trials retrieved (NCT00074815, NCT00254735) are OCD studies, and in NCT00254735 clomipramine is background therapy rather than the tested agent. Direct anxiety evidence is literature-based, which corresponds more closely to L3. Read the evidence level as "strong indirect and literature support", not as confirmed trial-level proof in anxiety disorders.

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, clomipramine is a potent serotonin reuptake inhibitor, and its active metabolite desmethylclomipramine adds noradrenergic activity. Pharmacology data also show binding to the serotonin transporter (SERT), norepinephrine transporter (NET), dopamine transporter (DAT) and α1A-adrenoceptor.

OCD shares circuitry with anxiety disorders and was formerly classified as one. Serotonergic and noradrenergic modulation is relevant to panic, generalized anxiety and phobic anxiety. Network meta-analyses and the WFSBP guidelines support its use in panic disorder. Comparative studies against moclobemide, diazepam and imipramine, along with serum-level and dose-finding studies, support efficacy in panic disorder with or without agoraphobia.

The main limitation is that the listed trials are largely OCD trials, so direct evidence for the broader "anxiety disorder" category comes mainly from the literature rather than registered trials.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00074815](https://clinicaltrials.gov/study/NCT00074815) | Phase 3 | Completed | 124 | Cognitive behavioural therapy added to SRI treatment in children with OCD who partially responded |
| [NCT00004310](https://clinicaltrials.gov/study/NCT00004310) | Phase 2 | Unknown | 76 | Intravenous versus oral clomipramine pulse loading in OCD |
| [NCT00466609](https://clinicaltrials.gov/study/NCT00466609) | Phase 4 | Completed | 54 | Fluoxetine alone vs fluoxetine plus quetiapine or clomipramine in OCD non-responders |
| [NCT00254735](https://clinicaltrials.gov/study/NCT00254735) | Phase 3 | Completed | 44 | Quetiapine added to SSRI/clomipramine in severe OCD (clomipramine is background therapy) |
| [NCT01404871](https://clinicaltrials.gov/study/NCT01404871) | N/A | Completed | 26 | Predicting response to clomipramine or escitalopram in OCD |
| [NCT00564564](https://clinicaltrials.gov/study/NCT00564564) | Phase 4 | Completed | 21 | Quetiapine vs clomipramine augmentation of SSRIs in OCD |
| [NCT05737511](https://clinicaltrials.gov/study/NCT05737511) | Phase 4 | Not yet recruiting | 80 | Hydroxyzine vs treatment as usual in panic disorder (clomipramine at most part of usual care) |
| [NCT02374567](https://clinicaltrials.gov/study/NCT02374567) | Phase 3 | Terminated | 407 | Pharmacovigilance in elderly psychiatric inpatients; may add safety information in older adults |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38014714](https://pubmed.ncbi.nlm.nih.gov/38014714/) | 2023 | Network meta-analysis | Cochrane Database Syst Rev | Pharmacological treatments in adult panic disorder |
| [35900217](https://pubmed.ncbi.nlm.nih.gov/35900217/) | 2023 | Guideline | World J Biol Psychiatry | WFSBP guidelines for anxiety, OCD and PTSD |
| [40788541](https://pubmed.ncbi.nlm.nih.gov/40788541/) | 2026 | Network meta-analysis | Eur Arch Psychiatry Clin Neurosci | Comparative efficacy and acceptability of anxiolytic drugs |
| [40946318](https://pubmed.ncbi.nlm.nih.gov/40946318/) | 2026 | Systematic review | Psychother Psychosom | Treatment options in treatment-resistant anxiety disorders |
| [6373161](https://pubmed.ncbi.nlm.nih.gov/6373161/) | 1984 | RCT | Curr Med Res Opin | Clomipramine vs diazepam in agoraphobia and social phobia |
| [22290995](https://pubmed.ncbi.nlm.nih.gov/22290995/) | 1993 | RCT | J Psychopharmacol | Clomipramine vs imipramine in panic/agoraphobia: psychomotor and cognitive effects |
| [10361962](https://pubmed.ncbi.nlm.nih.gov/10361962/) | 1999 | RCT | Eur Arch Psychiatry Clin Neurosci | Moclobemide vs clomipramine in panic disorder (n=135) |
| [2178909](https://pubmed.ncbi.nlm.nih.gov/2178909/) | 1990 | Review | Drugs | Pharmacology and therapeutic use in OCD and panic disorder |
| [10221358](https://pubmed.ncbi.nlm.nih.gov/10221358/) | 1999 | Cohort | J Psychopharmacol | Serum levels and clinical improvement in panic disorder; 97% panic-free at end of treatment |
| [18849910](https://pubmed.ncbi.nlm.nih.gov/18849910/) | 2008 | Review | CNS Spectrums | Medical treatment of OCD and anxiety; clomipramine effective but with more adverse events |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2565773 | APO Clomipramine | Tab 25 mg | Not available (licence cancelled) |

## Safety Considerations

- **Contraindications (from knowledge-graph disease associations, not label text):** epilepsy, manic bipolar affective disorder, liver disease, gallbladder disease, substance abuse/dependence, malignant hyperthermia, phaeochromocytoma and related adrenal or paraganglionic tumours, certain cancers (duodenum, small intestine, heart, adrenal gland, neuroblastoma), glaucoma, and hyperthyroidism/Graves disease/thyrotoxicosis.
- **Drug interactions:** the pharmacology data list interactions with the serotonin transporter, norepinephrine transporter, dopamine transporter, α1A-adrenoceptor, and Kir3.2/Kir3.4 channels. No clinically graded drug-drug interaction levels are available.
- **Class and literature signals:** tricyclic cardiac and anticholinergic effects, seizure risk (about 0.4% reported in a retrieved review), sexual dysfunction, rare haematological events such as pancytopenia (case report), and more adverse events than SSRIs. Clomipramine is metabolised via CYP2D6/CYP2C19, which supports genotype- or level-guided dosing.

Package insert warnings were not available, so please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Literature support in panic disorder and agoraphobia is consistent (RCTs, network meta-analyses, guidelines), but the registered trials are mostly OCD studies and give only indirect support for a broad "anxiety disorder" label. The New Zealand licence is cancelled, so any use would need a new regulatory pathway or specialist-supervised use.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (currently missing and blocking safety screening)
- Mechanism of action data (for example from DrugBank)
- Defining a specific anxiety subtype (such as panic disorder) and a trial or evidence review directed at it, given the first-line role of SSRIs
- A safety monitoring plan covering cardiac, seizure and anticholinergic risks, with genotype- or level-guided dosing
- Clarification of the regulatory status given the cancelled New Zealand licence

**Other predictions (summary):** Major depressive disorder and endogenous depression have extensive meta-analytic support, but these are established antidepressant uses rather than novel repurposing (Proceed with Guardrails). Agoraphobia (literature-based, Research Question), ADHD (weak, safety-limited, Research Question) and schizotypal personality disorder (indirect only, Research Question) need further study. Benign paroxysmal torticollis of infancy, schizoid, histrionic and paranoid personality disorders have no supporting evidence (Hold).
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


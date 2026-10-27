---
layout: default
title: Baclofen
parent: Model Prediction Only (L5)
nav_order: 56
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Baclofen: predicted new indication Attention deficit-hyperactivity disorder (evidence level L5). Approved use: Not stated in the licence records"
monitor_inn: "BACLOFEN"
predicted_indications: ["attention deficit-hyperactivity disorder", "nicotine dependence"]
---

# Baclofen
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

## Baclofen: From Spasticity to Attention Deficit-Hyperactivity Disorder
{: .text-alpha }

## One-Sentence Summary

Baclofen is a GABA-B receptor agonist that is widely known as a muscle relaxant, but no approved indication text is available in the New Zealand licence records compiled for this report.
The TxGNN model predicts it may be effective for **attention deficit-hyperactivity disorder (ADHD)**, but there are **no registered clinical trials** and none of the **9 publications** retrieved studies baclofen in ADHD.
A second prediction, **nicotine dependence**, has stronger support (3 Phase 2 trials and 20 publications) and is noted below as a more promising direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the licence records (the three licences are for oral tablet and intrathecal injection forms) |
| Predicted New Indication | Attention deficit-hyperactivity disorder |
| TxGNN Prediction Score | 99.32% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on known pharmacology, baclofen is a GABA-B receptor agonist. GABA-B activation can modulate dopaminergic and glutamatergic signalling in frontostriatal circuits involved in impulsivity and attention.

This link is plausible but unproven. The retrieved literature is dominated by Tourette syndrome and tic disorder reviews, off-label methylphenidate prescribing, and rodent studies on impulsivity, hippocampal EEG, social play and effort-based decision-making. None of these tests baclofen in ADHD directly. The very high model score therefore reflects network-based association rather than demonstrated clinical evidence.

For context, the second predicted indication, **nicotine dependence** (score 99.19%), has a clearer mechanistic rationale. GABA-B activation in the mesolimbic pathway reduces dopamine release in the ventral tegmental area and nucleus accumbens, dampening nicotine reward, craving and withdrawal. Rodent studies (PMID 18682277, 24553576) support this.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for ADHD.

For reference, the nicotine dependence prediction has three Phase 2 trials:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01821560](https://clinicaltrials.gov/study/NCT01821560) | Phase 2 | Completed | 44 | Baclofen effects on brain and behavioural responses to smoking cues (fMRI, genetics); mechanistic rather than a cessation efficacy trial |
| [NCT00257894](https://clinicaltrials.gov/study/NCT00257894) | Phase 2 | Terminated | 41 | Baclofen effect on smoking urge, withdrawal and reinforcement in moderate to heavy smokers |
| [NCT01228994](https://clinicaltrials.gov/study/NCT01228994) | Phase 2 | Terminated | 6 | Randomised trial of baclofen vs placebo for smoking abstinence; too few participants for interpretable results |

---

## Literature Evidence

None of the publications below tests baclofen in ADHD. They are the closest retrieved material.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35345730](https://pubmed.ncbi.nlm.nih.gov/35345730/) | 2022 | Review | Cureus | Behavioural, antipsychotic and alpha-agonist treatments for tics in Tourette syndrome, which often co-occurs with ADHD |
| [10342599](https://pubmed.ncbi.nlm.nih.gov/10342599/) | 1999 | Review | J Child Neurol | 450 patients with tics/Tourette treated with baclofen/botulinum toxin type A; tic severity rated with the Yale scale |
| [30122296](https://pubmed.ncbi.nlm.nih.gov/30122296/) | 2019 | Review | L'Encephale | Supervised off-label prescribing of methylphenidate in adult ADHD |
| [24295630](https://pubmed.ncbi.nlm.nih.gov/24295630/) | 2013 | Review | Int Rev Neurobiol | Emerging treatments for Tourette syndrome |
| [11393328](https://pubmed.ncbi.nlm.nih.gov/11393328/) | 2001 | Review | Paediatr Drugs | Clinical features and management of Tourette syndrome |
| [24062084](https://pubmed.ncbi.nlm.nih.gov/24062084/) | 2014 | Preclinical | Psychopharmacology | α2A-adrenergic stimulation in the ventral hippocampus reduces impulsive decision-making in rats (guanfacine) |
| [21300040](https://pubmed.ncbi.nlm.nih.gov/21300040/) | 2011 | Preclinical | Brain Res | EEG effects of neurotransmitter agonists in spontaneously hypertensive rats (ADHD model) |
| [24496320](https://pubmed.ncbi.nlm.nih.gov/24496320/) | 2014 | Preclinical | Neuropsychopharmacology | Anterior cingulate and amygdala roles in cognitive effort decision-making in rodents |

---

## New Zealand Market Information

All three licences are cancelled and none has approved indication text on record.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 403903 | Pacifen | Tab 10 mg | Not recorded (cancelled) |
| 2688417 | Sintetica Baclofen Intrathecal | Inj 2 mg per ml, 5 ml ampoule | Not recorded (cancelled) |
| 2162334 | Lioresal Intrathecal | Inj 0.05 mg per ml, 1 ml ampoule | Not recorded (cancelled) |

---

## Safety Considerations

- **Contraindication signals from the knowledge graph** (not label text): epilepsy, kidney disease, psychotic and schizophrenia-spectrum disorders, other mental disorders, stroke and cerebrovascular disorders, cerebral infarction, autonomic nervous system disease (including autonomic dysreflexia and Horner syndrome), pyoureter and disease of the bladder neck. These deserve particular attention in an ADHD population, where psychiatric comorbidity and seizure risk are relevant.
- **Drug Interactions** (100 records found; main ones listed):
  - Major: morphine, liposomal morphine
  - Moderate: dronabinol, nabilone, metoclopramide, opium, sibutramine, difenoxin, diphenoxylate
  - Further interactions of unknown severity were also recorded (e.g. metformin, omeprazole, pantoprazole, glimepiride).

Please refer to the package insert for full warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The ADHD prediction rests on model score alone (L5). No clinical trials exist, no retrieved publication studies baclofen in ADHD, and the drug has no valid New Zealand licence. By contrast, nicotine dependence (L2, three Phase 2 trials) is a better-supported research direction, though two of those trials were terminated early.

**To proceed, the following is needed:**
- Targeted literature search for baclofen in ADHD, in humans and in preclinical models
- Mechanism of action data to test the GABA-B and frontostriatal hypothesis
- Package insert warnings and contraindications from Medsafe, as safety screening cannot proceed without them
- For nicotine dependence: results and termination reasons for NCT00257894 and NCT01228994, and the outcomes of NCT01821560
- Clarification of the regulatory pathway, since all current New Zealand licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


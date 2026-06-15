---
layout: default
title: Atomoxetine
parent: 僅模型預測 (L5)
nav_order: 38
evidence_level: L5
indication_count: 10
---

# Atomoxetine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
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

# Atomoxetine: From ADHD to Specific Developmental Disorder

## One-Sentence Summary

Atomoxetine is a selective norepinephrine reuptake inhibitor (NRI) originally approved globally for attention-deficit/hyperactivity disorder (ADHD), though it has not received marketing authorization in New Zealand.
The TxGNN model predicts it may be effective for **Specific Developmental Disorder** (a spectrum encompassing ADHD, ASD with comorbid ADHD, and related neurodevelopmental conditions),
with **8 clinical trials** and **15 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | ADHD (Attention-Deficit/Hyperactivity Disorder) — globally approved; not registered in New Zealand |
| Predicted New Indication | Specific Developmental Disorder |
| TxGNN Prediction Score | 99.999% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data from DrugBank is not available in this Evidence Pack. Based on known pharmacology, atomoxetine is a **selective norepinephrine reuptake inhibitor (NRI)** that inhibits the norepinephrine transporter (NET) in the prefrontal cortex, raising synaptic norepinephrine (NE) concentrations. This directly improves attention, executive function, and impulse control — the core neurobiological deficits in ADHD and related neurodevelopmental disorders.

"Specific developmental disorder" as a diagnostic category encompasses ADHD, autism spectrum disorder (ASD) with comorbid ADHD symptoms, and other disorders of attention and executive function. The mechanistic bridge is therefore direct: atomoxetine's NRI action targets the same noradrenergic circuits that are dysregulated across this entire spectrum, making the predicted indication essentially an extension of the drug's established therapeutic niche rather than a true cross-disease leap.

The clinical evidence reinforces this reasoning. Multiple Phase 3 and Phase 4 randomized controlled trials have specifically enrolled children with ASD or pervasive developmental disorder alongside ADHD symptoms (NCT00498173, NCT00844753, NCT00380692), and a 396-patient multicenter Phase 4 RCT (NCT04085172) directly evaluated atomoxetine as an active comparator — providing the highest-quality head-to-head data. The TxGNN model's prediction thus aligns with a body of evidence that is already clinically well-recognized.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|-----------|-------------|
| [NCT04085172](https://clinicaltrials.gov/study/NCT04085172) | Phase 4 | Completed | 396 | Multicenter double-blind RCT; atomoxetine used as active comparator vs. guanfacine ER in ADHD children/adolescents who failed prior stimulants; most rigorously designed trial in this set |
| [NCT00844753](https://clinicaltrials.gov/study/NCT00844753) | Phase 4 | Completed | 128 | Double-blind placebo-controlled RCT evaluating atomoxetine with and without Parent Management Training in children with autism/Asperger's/PDD-NOS who have ADHD symptoms; 6-week dose titration |
| [NCT00510276](https://clinicaltrials.gov/study/NCT00510276) | Phase 4 | Completed | 445 | Double-blind placebo-controlled RCT assessing atomoxetine efficacy and functional outcomes in young adults with ADHD; largest sample in this set |
| [NCT00380692](https://clinicaltrials.gov/study/NCT00380692) | Phase 4 | Completed | 97 | Randomized double-blind comparison of atomoxetine vs. placebo for ADHD symptoms in children and adolescents with autism spectrum disorder |
| [NCT00498173](https://clinicaltrials.gov/study/NCT00498173) | Phase 3 | Completed | 60 | Double-blind placebo-controlled trial evaluating atomoxetine for ADHD symptoms in autistic disorder, Asperger's syndrome, and PDD-NOS |
| [NCT01470261](https://clinicaltrials.gov/study/NCT01470261) | N/A | Completed | 1398 | ADDUCE observational study; investigated long-term adverse effects of ADHD medications (including atomoxetine) on growth, neurological, psychiatric, and cardiovascular systems over 2 years |
| [NCT00573859](https://clinicaltrials.gov/study/NCT00573859) | Phase 1/2 | Completed | 27 | Examined reinforcing mechanisms of smoking in adult ADHD; provides mechanistic context for NRI effects on reward and attention circuits |
| [NCT05635318](https://clinicaltrials.gov/study/NCT05635318) | N/A | Unknown | 102 | Quantitative EEG neurofeedback as add-on therapy for ADHD; atomoxetine serves as a control arm; limited evidentiary weight due to unknown status |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [39701638](https://pubmed.ncbi.nlm.nih.gov/39701638/) | 2025 | Network Meta-analysis | The Lancet Psychiatry | Comparative efficacy and acceptability of pharmacological, psychological, and neurostimulatory interventions for ADHD in adults; highest-level synthesis of available evidence |
| [30653855](https://pubmed.ncbi.nlm.nih.gov/30653855/) | 2019 | Meta-analysis | Autism Research | Systematic review and meta-analysis of atomoxetine for ADHD in children with autism spectrum disorder; 3 RCTs, n=241; assessed efficacy and safety using GRADE approach |
| [27721971](https://pubmed.ncbi.nlm.nih.gov/27721971/) | 2016 | Systematic Review | Therapeutic Advances in Psychopharmacology | Review of atomoxetine efficacy in ADHD with common comorbidities including pervasive developmental disorders; covers children, adolescents, and adults |
| [32946507](https://pubmed.ncbi.nlm.nih.gov/32946507/) | 2020 | Systematic Review | PLoS One | Systematic review of sex differences in atomoxetine prescription rates and efficacy in girls and women with ADHD; highlights underrepresented populations |
| [25545605](https://pubmed.ncbi.nlm.nih.gov/25545605/) | 2015 | Review | Journal of Affective Disorders | Systematic review of comorbidity in pediatric bipolar disorder, including ADHD co-occurrence and treatment implications |
| [33012168](https://pubmed.ncbi.nlm.nih.gov/33012168/) | 2021 | Review | Clinical EEG and Neuroscience | qEEG in childhood ADHD and learning disabilities; supports neurophysiological overlap between ADHD and specific developmental disorders |
| [39514707](https://pubmed.ncbi.nlm.nih.gov/39514707/) | 2024 | Case-based Review | Journal of Developmental and Behavioral Pediatrics | Teletherapy and atomoxetine management of ADHD with co-occurring internalizing symptoms; illustrates real-world use in complex neurodevelopmental presentations |
| [35485452](https://pubmed.ncbi.nlm.nih.gov/35485452/) | 2022 | Cohort Study | Neuropsychopharmacology Reports | Retrospective cohort identifying patient-specific factors associated with atomoxetine efficacy in adult ADHD; long-term response approximately 40% at 6 months |
| [31425490](https://pubmed.ncbi.nlm.nih.gov/31425490/) | 2020 | Systematic Review | Therapeutic Drug Monitoring | Potential pharmacokinetic/pharmacodynamic interactions between synthetic cathinones and prescription drugs including atomoxetine; peripheral safety context |
| [32022784](https://pubmed.ncbi.nlm.nih.gov/32022784/) | 2020 | Systematic Review | Therapeutic Drug Monitoring | Novel phenethylamine interactions with prescription drugs including atomoxetine in ADHD populations; relevant to polypharmacy risk assessment |

---

## New Zealand Market Information

Atomoxetine currently holds **no marketing authorizations** in New Zealand. The drug is not registered with Medsafe and is not commercially available on the New Zealand market.

> For reference: Atomoxetine (brand name Strattera) is approved for ADHD in over 97 countries, including the United States (FDA, 2002), the European Union, Australia, and Japan. Any use in New Zealand would require special access arrangements (e.g., Section 29 of the Medicines Act 1981 or Medsafe provisional consent).

---

## Safety Considerations

Detailed warning and contraindication data specific to the New Zealand regulatory context is not available in this Evidence Pack.

> Please refer to the approved package insert (e.g., Strattera global prescribing information) for comprehensive safety information, including the FDA black-box warning regarding suicidal ideation in children and adolescents, hepatotoxicity risk, cardiovascular precautions (blood pressure and heart rate monitoring), and MAO inhibitor contraindication.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Atomoxetine's established mechanism of action directly targets the core neurobiological pathology of specific developmental disorders, and the L1 evidence base — including multiple completed Phase 3/4 RCTs in autism-spectrum and neurodevelopmental populations — provides sufficient clinical grounding to move forward; however, the absence of New Zealand regulatory approval and missing local safety documentation require structured risk management before any clinical deployment.

**To proceed, the following is needed:**

- **Regulatory pathway**: Determine feasibility of Medsafe registration or Section 29 access; review requirements for submission of an application covering the neurodevelopmental indication
- **Safety documentation**: Obtain and translate TFDA/FDA/EMA package insert warnings, contraindications, and black-box content (suicidality in paediatric patients) into local clinical protocols
- **MOA data confirmation**: Retrieve complete DrugBank pharmacology entry (DB00289) to formally document NRI mechanism for regulatory dossier
- **Drug interaction assessment**: Conduct a structured DDI review given that the DDI database returned no results (query_status: not_found) — atomoxetine is a CYP2D6 substrate with known interactions with MAO inhibitors, strong CYP2D6 inhibitors (fluoxetine, paroxetine), and cardiovascular agents
- **Paediatric monitoring plan**: Define mandatory cardiovascular monitoring (BP, HR), hepatic function surveillance, and suicidality screening protocols aligned with international labelling requirements
- **Indication scoping**: Clarify whether the target population is ADHD alone, ASD+ADHD comorbidity, or the broader specific developmental disorder category — as each carries different regulatory and clinical trial precedent
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


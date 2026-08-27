---
layout: default
title: Levetiracetam
parent: 僅模型預測 (L5)
nav_order: 202
evidence_level: L5
indication_count: 10
---

# Levetiracetam
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

# Levetiracetam: From Epilepsy (Partial-Onset Seizures) to Visual Epilepsy

## One-Sentence Summary

Levetiracetam is a broad-spectrum antiseizure medication historically used for partial-onset seizures and myoclonic seizures in juvenile myoclonic epilepsy. The TxGNN model predicts it may also be effective for **Visual Epilepsy** (a photosensitive/reflex seizure subtype), with **9 clinical trials** and **20 publications** currently identified — though none of this evidence is specific to the visual/photosensitive seizure population, making the signal largely mechanistic rather than disease-proven.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Epilepsy — partial-onset seizures (monotherapy/adjunctive) and myoclonic seizures in juvenile myoclonic epilepsy (per literature evidence, e.g. PMID 21936590); no official New Zealand/regulatory label text is available since the drug is not currently marketed there |
| Predicted New Indication | Visual Epilepsy |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L3 (indirect — observational/review-level evidence, no disease-specific trial) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

A confirmed original mechanism-of-action record is not available in the regulatory data (flagged as a High-severity data gap, DG002). However, evidence embedded in this pack's literature consistently describes levetiracetam as binding to synaptic vesicle protein 2A (SV2A), modulating neurotransmitter release and suppressing abnormal, hypersynchronous neuronal firing — the pharmacological basis for its broad-spectrum antiseizure activity (see PMID 26690830, and the mechanistic rationale documented for the status epilepticus candidate in this same evidence pack).

Visual epilepsy (photosensitive/reflex epilepsy) is a subtype of idiopathic generalized epilepsy in which seizures are triggered by visual stimuli (e.g., flickering light). Because levetiracetam's mechanism is not stimulus-specific — it dampens excessive cortical excitability broadly rather than acting on a particular sensory trigger pathway — it is mechanistically plausible that the same broad-spectrum effect seen in partial-onset and generalized seizures would extend to reflex/photosensitive seizures.

That said, the supporting evidence for this specific indication is indirect: the associated trials and literature largely cover neonatal seizures, migraine prophylaxis, traumatic brain injury seizure prevention, and hippocampal hyperactivity in psychosis — general antiseizure contexts rather than visual/photosensitive epilepsy itself. It is worth noting that this same evidence pack independently supports levetiracetam in status epilepticus with much stronger (L1) evidence, which is already an established clinical use — a useful internal sanity check that the model's broader predictions are directionally consistent with known pharmacology, even where the visual-epilepsy-specific data remains thin.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03107507](https://clinicaltrials.gov/study/NCT03107507) | Phase 4 | Unknown | 40 | Evaluated levetiracetam among newer AEDs for control of neonatal seizures, citing a potentially better side-effect profile than phenobarbital. |
| [NCT00203216](https://clinicaltrials.gov/study/NCT00203216) | N/A | Completed | 31 | Open-label trial of levetiracetam for migraine prophylaxis with/without aura (visual disturbances) in adults. |
| [NCT04277936](https://clinicaltrials.gov/study/NCT04277936) | Phase 2 | Terminated | 1 | fMRI study testing whether levetiracetam reduces hippocampal hyperactivity during a visual scene processing task in psychosis; not disease-specific, terminated early. |
| [NCT07336992](https://clinicaltrials.gov/study/NCT07336992) | Phase 3 | Not yet recruiting | 580 | RCT evaluating prophylactic levetiracetam for functional outcome after intracerebral haemorrhage, targeting seizure prevention. |
| [NCT00855738](https://clinicaltrials.gov/study/NCT00855738) | Phase 4 | Completed | 111 | Observational (LICEO) study assessing new AEDs, including levetiracetam, as first bitherapy in focal epilepsy. |
| [NCT00105040](https://clinicaltrials.gov/study/NCT00105040) | Phase 2 | Completed | 87 | RCT assessing cognitive/neuropsychological safety of adjunctive levetiracetam in children with refractory partial-onset seizures. |
| [NCT04559529](https://clinicaltrials.gov/study/NCT04559529) | Phase 2 | Completed | 62 | fMRI study of levetiracetam's effect on hippocampal hyperactivity in psychotic disorders using a visual scene processing paradigm. |
| [NCT04573803](https://clinicaltrials.gov/study/NCT04573803) | Phase 3 | Not yet recruiting | 1649 | MAST trial comparing AED duration/choice (including levetiracetam) for seizure prevention after traumatic brain injury. |
| [NCT04833907](https://clinicaltrials.gov/study/NCT04833907) | Phase 1/2 | Enrolling by invitation | 24 | Gene therapy trial for Canavan disease; levetiracetam is contextual, not the primary intervention. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [32385134](https://pubmed.ncbi.nlm.nih.gov/32385134/) | 2020 | RCT | Pediatrics | Levetiracetam showed efficacy and safety for neonatal seizures compared with phenobarbital. |
| [30487494](https://pubmed.ncbi.nlm.nih.gov/30487494/) | 2018 | RCT | Mymensingh Med J | Compared levetiracetam vs phenobarbital monotherapy for seizure control/tolerability in childhood epilepsy. |
| [37378757](https://pubmed.ncbi.nlm.nih.gov/37378757/) | 2023 | Review (network meta-analysis) | J Neurol | Network meta-analysis of antiseizure medications, including levetiracetam, in idiopathic generalized epilepsies. |
| [40450767](https://pubmed.ncbi.nlm.nih.gov/40450767/) | 2025 | Systematic Review | Epilepsy Behav | Meta-analysis of levetiracetam for myoclonic seizures in idiopathic generalized epilepsy, including JME. |
| [34260837](https://pubmed.ncbi.nlm.nih.gov/34260837/) | 2021 | Review | N Engl J Med | Overview of initial adult seizure management and antiseizure medication selection. |
| [34286461](https://pubmed.ncbi.nlm.nih.gov/34286461/) | 2022 | Systematic Review/Meta-analysis | Neurocrit Care | Evaluated efficacy, dosing, and adverse events of levetiracetam seizure prophylaxis in neurocritical care (ICH, TBI, SAH). |
| [35963261](https://pubmed.ncbi.nlm.nih.gov/35963261/) | 2022 | Phase 3 RCT (PEACH) | Lancet Neurol | Placebo-controlled trial found prophylactic levetiracetam did not significantly reduce acute seizure risk after intracerebral haemorrhage. |
| [21936590](https://pubmed.ncbi.nlm.nih.gov/21936590/) | 2011 | Review | CNS Drugs | Establishes levetiracetam's approved indications: partial-onset seizures, myoclonic seizures in JME, and primary generalized tonic-clonic seizures. |
| [35976303](https://pubmed.ncbi.nlm.nih.gov/35976303/) | 2022 | Review | Arq Neuropsiquiatr | Review of status epilepticus diagnosis, monitoring, and treatment, including levetiracetam's role. |
| [30884401](https://pubmed.ncbi.nlm.nih.gov/30884401/) | 2019 | Systematic Review | Epilepsy Behav | Compared levetiracetam vs carbamazepine efficacy/tolerability in rolandic epilepsy in children. |

---

## New Zealand Market Information

Levetiracetam is not currently marketed in New Zealand — there are no product authorizations on file (0 licenses), so no market information table can be produced.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication (visual epilepsy) rests on L3, largely indirect evidence — the cited trials and literature address general antiseizure use (neonatal seizures, migraine, TBI, psychosis-related hippocampal activity) rather than photosensitive/reflex epilepsy specifically. Combined with the drug's current unmarketed status in New Zealand, there is not yet a sufficient basis to proceed.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently blocking safety evaluation (DG001)
- Confirmed mechanism-of-action documentation from DrugBank (DG002)
- Dedicated clinical evidence in a photosensitive/reflex (visual) epilepsy population, rather than general seizure cohorts
- New Zealand market authorization and regulatory pathway assessment, given the drug is not currently marketed there
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


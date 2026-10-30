---
layout: default
title: Lamotrigine
parent: Model Prediction Only (L5)
nav_order: 257
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-30
description: "Lamotrigine: predicted new indication Trigeminal nerve neoplasm (evidence level L5). Approved use: Epilepsy (partial epilepsy, Lennox-Gastaut syndrome)"
monitor_inn: "LAMOTRIGINE"
predicted_indications: ["trigeminal nerve neoplasm", "trigeminal neuralgia"]
---

# Lamotrigine
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

## Lamotrigine: From Epilepsy to Trigeminal Nerve Neoplasm
{: .text-alpha }

## One-Sentence Summary

Lamotrigine is an antiseizure medicine originally used for epilepsy, including partial epilepsy and Lennox-Gastaut syndrome.
The TxGNN model's (deep learning) top-ranked prediction is **trigeminal nerve neoplasm**, but its score may reflect similarities the model learned between related diseases, not biological evidence: there are **0 clinical trials** and only **2 publications**, both about trigeminal neuralgia rather than a tumour.
The best-supported direction in the data compiled for this report is the second-ranked prediction, **trigeminal neuralgia**, with **2 completed small clinical trials** and **18 publications**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Epilepsy (partial epilepsy, Lennox-Gastaut syndrome); no approved indication text is available from the New Zealand licences |
| Predicted New Indication | Trigeminal nerve neoplasm (rank 1); trigeminal neuralgia (rank 2) is the better-supported alternative |
| TxGNN Prediction Score | 0.9997 (trigeminal nerve neoplasm); 0.9989 (trigeminal neuralgia) |
| Evidence Level | L5 for trigeminal nerve neoplasm; L2 for trigeminal neuralgia |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (5 of 5 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 5 (valid: 5) |
| Recommended Decision | Hold for trigeminal nerve neoplasm; Proceed with Guardrails for trigeminal neuralgia |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known information, lamotrigine is an antiseizure drug whose efficacy in epilepsy is established. Its anticonvulsant effect is generally attributed to blockade of voltage-gated sodium channels and reduced glutamate release, and this could plausibly apply to other conditions driven by abnormal neuronal firing.

**Trigeminal nerve neoplasm (rank 1).** The high score from the TxGNN model (deep learning) most likely reflects similarities the model learned between related diseases, such as trigeminal neuralgia, a condition for which lamotrigine is used, and is not biological evidence. The two retrieved papers (a review and a Gamma Knife case report on a cavernous malformation) concern trigeminal neuralgia, not a tumour. There is no evidence that lamotrigine treats the neoplasm itself. At most, it might relieve secondary neuropathic pain.

**Trigeminal neuralgia (rank 2).** Sodium channel blockade plausibly dampens the ectopic, paroxysmal firing of demyelinated trigeminal afferents. Guidelines such as the 2019 European Academy of Neurology guideline position lamotrigine as an add-on or alternative when carbamazepine or oxcarbazepine is not tolerated or is ineffective. This is mechanistically coherent with its antiseizure action, and the trial data below point the same way.


---

## Clinical Trial Evidence

No clinical trials are registered for trigeminal nerve neoplasm. The trials below are for the better-supported prediction, trigeminal neuralgia.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00913107](https://clinicaltrials.gov/study/NCT00913107) | Phase 2/3 | Completed | 21 | Lamotrigine versus carbamazepine in trigeminal neuralgia; efficacy and safety comparison. Very small sample |
| [NCT00203229](https://clinicaltrials.gov/study/NCT00203229) | Not specified | Completed | 20 | Double-blind, placebo-controlled add-on study of lamotrigine in trigeminal neuralgia (tic douloureux). Small sample |

---

## Literature Evidence

For the top-ranked prediction (trigeminal nerve neoplasm), the two publications retrieved are about trigeminal neuralgia: a 2007 review (PMID 17997704) and a 2018 Gamma Knife case report (PMID 30650431). The table below shows the most relevant publications for trigeminal neuralgia and for startle epilepsy.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [30860637](https://pubmed.ncbi.nlm.nih.gov/30860637/) | 2019 | Guideline | Eur J Neurol | European Academy of Neurology guideline on managing trigeminal neuralgia |
| [37892981](https://pubmed.ncbi.nlm.nih.gov/37892981/) | 2023 | Systematic Review | Biomedicines | Umbrella review of drugs used for trigeminal neuralgia, their efficacy and side effects |
| [34108244](https://pubmed.ncbi.nlm.nih.gov/34108244/) | 2021 | Review | Pract Neurol | Practical guide to diagnosis and medical and surgical treatment of trigeminal neuralgia |
| [31908187](https://pubmed.ncbi.nlm.nih.gov/31908187/) | 2020 | Review | Mol Pain | Overview from pathophysiology to pharmacological treatments |
| [38870050](https://pubmed.ncbi.nlm.nih.gov/38870050/) | 2024 | Review | Expert Rev Neurother | Carbamazepine and oxcarbazepine are effective but limited by adverse effects; newer anticonvulsants are possible adjuncts |
| [38246671](https://pubmed.ncbi.nlm.nih.gov/38246671/) | 2024 | Review | No Shinkei Geka | Lamotrigine is used off-label in Japan as an alternative to carbamazepine |
| [10512780](https://pubmed.ncbi.nlm.nih.gov/10512780/) | 1999 | Case series | Seizure | Adjunctive lamotrigine in four patients with startle-induced seizures (startle epilepsy) |
| [21896426](https://pubmed.ncbi.nlm.nih.gov/21896426/) | 2011 | Case series | Epileptic Disord | Add-on lamotrigine in six patients with startle-induced seizures; seizure control improved greatly in three |
| [26876275](https://pubmed.ncbi.nlm.nih.gov/26876275/) | 2017 | Preclinical | Epilepsy Behav | Acute and chronic lamotrigine effects in a genetic audiogenic seizure hamster model |
| [8734483](https://pubmed.ncbi.nlm.nih.gov/8734483/) | 1996 | Preclinical | Neuropharmacology | Lamotrigine antagonised audiogenic seizures in DBA/2 mice and potentiated other anticonvulsants |


---

## New Zealand Market Information

All five licences are valid, and no approved indication text is available.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2040034 | Lamictal | Tab dispersible 2 mg | Valid |
| 2056143 | Lamictal | Tab dispersible 5 mg | Valid |
| 2553376 | Logem | Tab dispersible 25 mg | Valid |
| 2553384 | Logem | Tab dispersible 50 mg | Valid |
| 2553392 | Logem | Tab dispersible 100 mg | Valid |

---

## Safety Considerations

- **Contraindication-related conditions (knowledge graph):** The TxGNN knowledge graph links lamotrigine to the following conditions as contraindications. This is not label text and should be confirmed against the product information: anxiety disorder, neurotic disorder, dysthymic disorder, liver disease, gallbladder disease, thrombocytopenia, neutropenia, agranulocytosis, anaemia (including macrocytic anaemia), pancytopenia, aplastic anaemia (including idiopathic) and non-infectious meningitis.
- **Rash risk:** Slow dose titration is needed because of the risk of serious rash, including Stevens-Johnson syndrome.
- **Drug Interactions:** 100 interactions were found. Moderate-level interactions include morphine (including liposomal), opium, dronabinol, nabilone, metoclopramide and orlistat. Metformin is rated Minor. Several others are of unknown severity (for example calcitriol, glimepiride, doxycycline, omeprazole, pantoprazole, lansoprazole, vancomycin). Interaction management is important when lamotrigine is added to existing regimens.

Please refer to the package insert for complete safety information; label warnings were not available in the data compiled for this report.

---

## Conclusion and Next Steps

**Decision: Hold for trigeminal nerve neoplasm (rank 1); Proceed with Guardrails for trigeminal neuralgia (rank 2)**

**Rationale:**
- The top-ranked prediction, trigeminal nerve neoplasm, has no trial evidence, and its score from the TxGNN model (deep learning) may reflect similarities the model learned between related diseases, such as trigeminal neuralgia, not biological evidence (L5), so it should not be pursued.
- Trigeminal neuralgia has two completed but very small trials (n=20 and n=21), consistent guideline support as an add-on or alternative to carbamazepine/oxcarbazepine, and a plausible mechanism (L2). There is no confirmatory Phase 3 evidence.

**To proceed, the following is needed:**
- Redefine the target indication from trigeminal nerve neoplasm to trigeminal neuralgia in any further evaluation.
- Larger, adequately powered controlled trials of lamotrigine in trigeminal neuralgia, ideally Phase 3.
- Prospective evaluation of startle-induced seizures if that direction is pursued.
- Detailed mechanism of action data and the package insert warnings and contraindications.
- Regulatory pathway clarification, noting that the drug is marketed in New Zealand with 5 valid licences out of 5.
- A titration and rash-monitoring plan, plus a DDI management plan, especially for patients taking carbamazepine, valproate or opioids.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


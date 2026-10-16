---
layout: default
title: Morphine
parent: Moderate Evidence (L3-L4)
nav_order: 311
evidence_level: L4
indication_count: 10
last_modified_at: 2026-10-15
description: "Morphine: predicted new indication Myofascial pain syndrome (evidence level L4). Approved use: Diarrheal disease"
monitor_inn: "Morphine"
predicted_indications: ["myofascial pain syndrome", "manic bipolar affective disorder", "Tourette syndrome", "restless legs syndrome", "attention deficit hyperactivity disorder, inattentive type", "migraine disorder", "trichotillomania", "methemoglobinemia", "migraine with brainstem aura", "specific developmental disorder"]
---

# Morphine
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **10** 
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

# Morphine: From Diarrheal Disease to Myofascial Pain Syndrome

## One-Sentence Summary

Morphine is an opioid analgesic that acts on opioid receptors. In the data compiled for this report its original indication is listed as diarrheal disease.
The TxGNN model's top-ranked prediction is **myofascial pain syndrome**, but **no clinical trials** and only **indirect literature** support it.
The best-supported candidate among the ten predictions is **restless legs syndrome** (rank 4), which is covered in the supplementary section below.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Diarrheal disease (from the knowledge graph; no approved indication text available from New Zealand licences) |
| Predicted New Indication | Myofascial pain syndrome |
| TxGNN Prediction Score | 99.75% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Morphine is known to be an agonist at the mu, delta and kappa opioid receptors, and its general analgesic activity gives a plausible link to a painful muscle condition such as myofascial pain syndrome.

The link to the original indication is weaker. Diarrhoea control relies on opioid effects on gut motility, which is a different mechanism from pain relief in muscle tissue. The high TxGNN score most likely reflects generic pain-related associations in the knowledge graph rather than specific evidence.

The retrieved literature is largely indirect (temporomandibular joint dysfunction, post-thoracotomy pain, failed back surgery, nerve blocks, botulinum toxin). Opioids are not a guideline-supported treatment for myofascial pain. One 2026 randomized trial (PMID 41664327) used morphine only as an adjuvant in a local-infiltration combination.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for myofascial pain syndrome.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [41664327](https://pubmed.ncbi.nlm.nih.gov/41664327/) | 2026 | RCT | Asian Spine J | Double-blind trial of dexmedetomidine and morphine versus plain ropivacaine for myofascial infiltration in spinal fusion; morphine used only as an adjuvant |
| [35066974](https://pubmed.ncbi.nlm.nih.gov/35066974/) | 2022 | Retrospective cohort | Pain Pract | Structured stretching programme for myofascial pain and its effect on opioid use in "legacy pain" patients |
| [21419546](https://pubmed.ncbi.nlm.nih.gov/21419546/) | 2011 | Review | J Oral Maxillofac Surg | Long-term opioid use in temporomandibular joint dysfunction can be neither supported nor refuted by current evidence |
| [39793344](https://pubmed.ncbi.nlm.nih.gov/39793344/) | 2025 | Clinical study | Eur J Obstet Gynecol Reprod Biol | Pudendal nerve block for pain after botulinum toxin injection in myofascial pelvic pain; not morphine-specific |
| [21691691](https://pubmed.ncbi.nlm.nih.gov/21691691/) | 2011 | Case series | Rev Assoc Med Bras | Therapeutic approaches in 56 patients with failed back surgery pain syndrome |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 754293 | RA-Morph | Oral liquid 2 mg per ml | Not stated (cancelled) |
| 754285 | RA-Morph | Oral liquid 1 mg per ml | Not stated (cancelled) |
| 754307 | RA-Morph | Oral liquid 5 mg per ml | Not stated (cancelled) |
| 754315 | RA-Morph | Oral liquid 10 mg per ml | Not stated (cancelled) |

All four authorizations are cancelled, so there is currently no valid single-ingredient or combination product in this dataset.

---

## Safety Considerations

- **Contraindications flagged in the knowledge graph** (not label text): hypertension, orthostatic hypotension, long QT syndrome and torsades de pointes, liver failure and liver disease, kidney disease, epilepsy, chronic obstructive pulmonary disease, pancreatitis, gallbladder disease, gastric ulcer, hypothyroidism, psychotic and other mental disorders, anxiety disorder, and potassium deficiency.
- **Pharmacological targets**: morphine acts on the delta (OPRD1), kappa (OPRK1) and mu (OPRM1) opioid receptors. No drug-drug interaction entries with levels were returned.
- **Class concerns noted in the evidence**: dependence, abuse liability, respiratory depression, opioid-induced hyperalgesia and medication-overuse headache.

Please refer to the package insert for authoritative warnings and contraindications.

---

## Other Predicted Indications (Supplementary)

| Rank | Indication | Score | Evidence Level | Assessment |
|---|---|---|---|---|
| 4 | Restless legs syndrome | 99.47% | L3 | Strongest evidence of the ten (see below) |
| 2 | Manic bipolar affective disorder | 99.64% | L5 | No direct clinical evidence; dependence risk in psychiatric patients |
| 3 | Tourette syndrome | 99.48% | L5 | Only speculative opioid links |
| 5 | ADHD, inattentive type | 99.34% | L5 | No trials or literature |
| 6 | Migraine disorder | 99.32% | L4 | Guidelines discourage opioids; a morphine challenge study found no benefit over placebo |
| 7 | Trichotillomania | 99.29% | L5 | No evidence; opioid antagonists, not agonists, are the studied agents |
| 8 | Methemoglobinemia | 99.15% | L5 | No plausible mechanism |
| 9 | Migraine with brainstem aura | 99.15% | L5 | Preclinical only; a spinal morphine neurological deficit case report is a safety signal |
| 10 | Specific developmental disorder | 99.06% | L5 | Literature unrelated to therapeutic use |

**Restless legs syndrome in more detail:**
- **Trial:** [NCT01476124](https://clinicaltrials.gov/study/NCT01476124) is a completed Phase 1 crossover study (18 healthy subjects) of the pharmacokinetics and tolerability of morphine with gabapentin enacarbil. It has no RLS efficacy data.
- **Registry and cohort data:** the National RLS Opioid Registry reports on long-term safety, dose stability and efficacy ([32918469](https://pubmed.ncbi.nlm.nih.gov/32918469/), [36697248](https://pubmed.ncbi.nlm.nih.gov/36697248/), [41543123](https://pubmed.ncbi.nlm.nih.gov/41543123/)). These are class-level rather than morphine-specific.
- **Morphine-specific reports:** [39106456](https://pubmed.ncbi.nlm.nih.gov/39106456/) (long-term intrathecal low-dose morphine without tolerance), [31883387](https://pubmed.ncbi.nlm.nih.gov/31883387/) (intrathecal pump series), [31133964](https://pubmed.ncbi.nlm.nih.gov/31133964/) (case report, morphine and fentanyl).
- **Reviews:** [27964861](https://pubmed.ncbi.nlm.nih.gov/27964861/) and [27310338](https://pubmed.ncbi.nlm.nih.gov/27310338/) discuss opioids in RLS.
- **Concerns:** opioid-induced augmentation, dependence and respiratory depression, with most evidence observational.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction, myofascial pain syndrome, has no registered trials and only indirect literature, and opioids are not a recommended treatment for it. All New Zealand licences are cancelled and no label safety data is available. Restless legs syndrome is the only candidate with meaningful supporting evidence (L3), and it is best treated as a research question rather than a ready repurposing candidate.

**To proceed, the following is needed:**
- Mechanism of action data for morphine
- Medsafe package insert warnings and contraindications, which are required before safety screening
- For restless legs syndrome: morphine-specific controlled efficacy data and a dependence and augmentation risk assessment
- A route and formulation review, since no valid New Zealand product is currently marketed and intrathecal use would need separate consideration
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


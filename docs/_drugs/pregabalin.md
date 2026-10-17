---
layout: default
title: Pregabalin
parent: Model Prediction Only (L5)
nav_order: 380
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-15
description: "Pregabalin: predicted new indication Tendinitis (evidence level L5). Approved use: Anxiety disorder, fibromyalgia, myofascial pain syndrome…"
monitor_inn: "PREGABALIN"
predicted_indications: ["tendinitis", "myositis fibrosa", "idiopathic granulomatous myositis", "inclusion body myositis", "migraine disorder", "migraine with brainstem aura"]
---

# Pregabalin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

# Pregabalin: From Neuropathic Pain and Anxiety to Tendinitis

## One-Sentence Summary

Pregabalin is originally used for postherpetic neuralgia, fibromyalgia, myofascial pain syndrome and anxiety disorder.
The TxGNN model's top-ranked prediction is **tendinitis**, but no clinical trials and no direct literature support it (**0 clinical trials**; **6 retrieved publications**, none showing that pregabalin treats tendinitis).
The best-supported alternative among the predictions is **migraine disorder** (2 registered trials, several randomized trials in children), which is a research question rather than an established use.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Anxiety disorder, fibromyalgia, myofascial pain syndrome, postherpetic neuralgia (from the knowledge graph; approved indication text is not available for the New Zealand licences) |
| Predicted New Indication | Tendinitis |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L4 (for tendinitis; mechanistic rationale only, no direct studies) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 8 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the data compiled for this report. In general, pregabalin binds the alpha-2-delta subunit of voltage-gated calcium channels and reduces release of excitatory neurotransmitters. This may relieve neuropathic and nociceptive pain.

The original indications (fibromyalgia, myofascial pain syndrome) are musculoskeletal and neuropathic pain conditions. Tendinitis is a painful musculoskeletal condition, so the model likely links them through shared pain-related features. This is a computational inference only. The retrieved literature does not show that pregabalin treats tendinitis. Two perioperative studies concern postoperative pain after shoulder (rotator cuff) surgery, which is not tendinitis treatment. The remaining papers are case reports, a commentary, or unrelated work.

Other predictions follow the same pattern:
- **Myositis fibrosa** and **idiopathic granulomatous myositis** (score 99.71%): no trials and no literature (L5). Pregabalin has no known effect on the underlying inflammation, so any benefit would be symptomatic.
- **Inclusion body myositis** (99.52%): L5. The one retrieved paper is an unrelated case report.
- **Migraine with brainstem aura** (99.43%): L4. Animal studies show pregabalin inhibits cortical spreading depression, the basis of migraine aura, but there are no clinical trials in this subtype.
- **Migraine disorder** (99.47%): L2, the strongest of the predictions (see below).

## Clinical Trial Evidence

Currently no related clinical trials are registered for tendinitis.

For the better-supported prediction, migraine disorder:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00447369](https://clinicaltrials.gov/study/NCT00447369) | Phase 3 | Withdrawn | 70 (planned) | Pregabalin vs sodium valproate for migraine prevention (randomized, blinded crossover); withdrawn, so no efficacy data |
| [NCT02747940](https://clinicaltrials.gov/study/NCT02747940) | Phase 4 | Completed | 200 | Brain imaging signatures of chronic pain (chronic migraine and fibromyalgia); not designed to test pregabalin efficacy in migraine |

## Literature Evidence

For tendinitis, the retrieved publications do not show efficacy:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34052386](https://pubmed.ncbi.nlm.nih.gov/34052386/) | 2022 | RCT | Arthroscopy | Perioperative pregabalin gave postoperative pain scores equivalent to interscalene block after rotator cuff repair (surgical analgesia, not tendinitis) |
| [32839073](https://pubmed.ncbi.nlm.nih.gov/32839073/) | 2021 | Retrospective cohort | J Orthop Sci | Analgesic and opioid-sparing effect of pregabalin after rotator cuff repair, with conflicting results in prior studies (not tendinitis) |
| [41017607](https://pubmed.ncbi.nlm.nih.gov/41017607/) | 2025 | Case report | Praxis | Fluoroquinolone-associated disability after ciprofloxacin (tendinopathy as a side effect; not pregabalin treatment) |
| [37051935](https://pubmed.ncbi.nlm.nih.gov/37051935/) | 2023 | Case report | Pain Pract | Posterior femoral cutaneous nerve impingement after a marathon (hamstring tendonitis); not a pregabalin study |

For migraine disorder (selected):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37637787](https://pubmed.ncbi.nlm.nih.gov/37637787/) | 2023 | RCT | Iran J Child Neurol | Pregabalin vs sodium valproate for pediatric migraine prophylaxis |
| [26024701](https://pubmed.ncbi.nlm.nih.gov/26024701/) | 2015 | RCT | Acta Med Iran | Pregabalin vs propranolol for prophylaxis of childhood migraine (99 children) |
| [39388181](https://pubmed.ncbi.nlm.nih.gov/39388181/) | 2024 | Network meta-analysis | JAMA Netw Open | Preventive medications in pediatric migraine, including pregabalin |
| [23797675](https://pubmed.ncbi.nlm.nih.gov/23797675/) | 2013 | Systematic review | Cochrane Database Syst Rev | Gabapentin or pregabalin for prophylaxis of episodic migraine in adults |
| [21479703](https://pubmed.ncbi.nlm.nih.gov/21479703/) | 2011 | Follow-up study | J Headache Pain | Efficacy and tolerability of pregabalin as migraine preventive over 3 months |
| [28223480](https://pubmed.ncbi.nlm.nih.gov/28223480/) | 2017 | Preclinical | PNAS | Pregabalin inhibits cortical spreading depression in mice |

## New Zealand Market Information

All eight licences are cancelled, so pregabalin currently has no valid marketed product in New Zealand. Approved indication text is not available for these licences.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2534096 | Pregabalin Pfizer | Cap 150 mg | Cancelled |
| 2534118 | Pregabalin Pfizer | Cap 300 mg | Cancelled |
| 2704536 | Lyrica | Cap 300 mg | Cancelled |
| 2704501 | Lyrica | Cap 75 mg | Cancelled |
| 2534088 | Pregabalin Pfizer | Cap 75 mg | Cancelled |

Three further cancelled licences (Lyrica 150 mg and 25 mg, Pregabalin Pfizer 25 mg) are not listed here.

## Safety Considerations

**Contraindication signals from the knowledge graph (not label text):** anxiety disorder, neurotic disorder, dysthymic disorder, thrombocytopenia, angioedema (including hereditary and ACE-inhibitor-type forms), myopathy and other muscle diseases (including rhabdomyolysis and familial periodic paralysis), nodular fasciitis, impaired renal function, and atrioventricular block. The graph's contraindication for anxiety disorder conflicts with its own indication edge for the same condition, so these edges should be verified against the label. The muscle-disease signals are particularly relevant to the predicted myositis indications.

**Drug interactions:** 100 interactions were found. Major interactions include:
- Morphine (including liposomal morphine): additive central nervous system and respiratory depression.

Moderate interactions include pioglitazone, rosiglitazone, troglitazone, difenoxin, diphenoxylate, dronabinol, nabilone, metoclopramide, opium and sibutramine.

**Other signals from the literature:** sexual dysfunction (PMID 30001248), sedation, and apnea risk when combined with opioids (PMID 28734941).

Package insert warnings and contraindications were not available and should be reviewed before any further step.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction (tendinitis) rests on a computational score alone. No trials exist, and the literature does not show that pregabalin treats tendinitis. Pregabalin also has no currently valid New Zealand licence, and label safety data is missing. Migraine disorder (L2) is the only prediction with some clinical support and could be considered separately as a research question.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, to enable safety screening
- Detailed mechanism-of-action data from DrugBank
- For tendinitis: controlled clinical studies directly testing pregabalin
- For migraine: adult, placebo-controlled confirmatory trials
- Clarification of the regulatory pathway given that all New Zealand licences are cancelled
- Review of the knowledge-graph contraindications (especially myopathy, renal impairment, angioedema) against the label
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


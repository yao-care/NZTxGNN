---
layout: default
title: Midazolam
parent: 僅模型預測 (L5)
nav_order: 226
evidence_level: L5
indication_count: 1
---

# Midazolam
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Midazolam: From Procedural Sedation to Insomnia

## One-Sentence Summary

Midazolam is a short-acting benzodiazepine most widely used for procedural sedation, anesthesia induction, and premedication (this Evidence Pack does not carry a formal Taiwan/NZ license text for the original indication).
The TxGNN model predicts it may also be effective for **Insomnia**, a repurposing hypothesis reinforced by **4 direct randomized controlled trials from the 1980s–1990s** and **32 clinical trial records** (mostly indirect, using midazolam as a sedation comparator) currently in the evidence base.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in this Evidence Pack (no license records); midazolam is generally used for procedural sedation / anesthesia induction |
| Predicted New Indication | Insomnia |
| TxGNN Prediction Score | 99.74% |
| Evidence Level | L2 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed formal MOA documentation is currently a data gap (DG002). Based on established pharmacology, midazolam is a short-acting benzodiazepine that acts as a positive allosteric modulator at the α subunit of the GABA-A receptor, enhancing inhibitory GABAergic transmission. This produces sedative, anxiolytic, muscle-relaxant, and hypnotic effects — the same mechanism shared by benzodiazepines already approved for insomnia (e.g., triazolam, temazepam, flurazepam), making this a clear class-effect prediction rather than a novel mechanistic leap.

Procedural sedation and insomnia are mechanistically linked through the same GABAergic hypnotic pathway; the difference is dosing/duration of use rather than target biology. Historically, oral midazolam formulations (e.g., Dormicum) were used in some European markets for short-term insomnia, which is consistent with the TxGNN prediction. The "Not Marketed" status and MOA data gap in this Evidence Pack appear to reflect missing regulatory records rather than a lack of mechanistic plausibility.

---

## Clinical Trial Evidence

Note: most of the 32 retrieved trials use midazolam only as a sedation/anesthesia comparator (e.g., vs. dexmedetomidine or remimazolam) rather than as a direct insomnia treatment, and several were graded low relevance ("C") during triage. The table below lists the trials most directly connected to midazolam and sleep/insomnia outcomes.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02142595](https://clinicaltrials.gov/study/NCT02142595) | Phase 4 | Completed | 111 | IV midazolam vs. dexmedetomidine compared for postoperative sleep quality after TURP under spinal anesthesia (graded most relevant, "B") |
| [NCT07336095](https://clinicaltrials.gov/study/NCT07336095) | Phase 3 | Not yet recruiting | 195 | Oral midazolam vs. oral melatonin as premedication in children, comparing sleep-inducing/anxiolytic effect |
| [NCT06407518](https://clinicaltrials.gov/study/NCT06407518) | NA | Recruiting | 280 | Preoperative oral midazolam solution tested for effect on postoperative pain in patients with sleep disturbance/anxiety before colorectal cancer surgery |
| [NCT01966315](https://clinicaltrials.gov/study/NCT01966315) | N/A | Terminated | 5 | Midazolam vs. dexmedetomidine compared via 24-hour polysomnography for ICU sleep quality/quantity and delirium incidence |
| [NCT00744380](https://clinicaltrials.gov/study/NCT00744380) | NA | Completed | 23 | Dexmedetomidine vs. added midazolam sedation compared for facilitating ICU extubation |
| [NCT00826553](https://clinicaltrials.gov/study/NCT00826553) | Phase 1 | Terminated | 6 | Polysomnographic comparison of α2 agonist (dexmedetomidine) vs. GABA agonist (midazolam-class) sedatives on sleep stages |
| [NCT04082767](https://clinicaltrials.gov/study/NCT04082767) | Phase 3 | Unknown | 120 | Dexmedetomidine vs. midazolam sedation efficacy in critically ill ventilated children |
| [NCT05606315](https://clinicaltrials.gov/study/NCT05606315) | Phase 4 | Unknown | 285 | Remimazolam (related benzodiazepine) vs. standard sedation in ICU mechanically ventilated patients |
| [NCT06498869](https://clinicaltrials.gov/study/NCT06498869) | NA | Completed | 178 | Colonoscopy sedation (midazolam + propofol ± ketamine) with Pittsburgh Sleep Quality Index assessment |
| [NCT06041711](https://clinicaltrials.gov/study/NCT06041711) | NA | Completed | 66 | General vs. regional anesthesia (sedation typically including midazolam) compared for perioperative sleep quality in hip arthroplasty |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [6138072](https://pubmed.ncbi.nlm.nih.gov/6138072/) | 1983 | RCT | British Journal of Clinical Pharmacology | Double-blind trial: midazolam 15 mg was an effective hypnotic in insomnia secondary to neuromuscular disease, better tolerated than comparator (no hangover effect) |
| [6120704](https://pubmed.ncbi.nlm.nih.gov/6120704/) | 1981 | RCT (dose-finding) | Arzneimittel-Forschung | Multi-center pilot study establishing optimal oral dose (10–30 mg) of midazolam for mild-to-moderate insomnia |
| [2121802](https://pubmed.ncbi.nlm.nih.gov/2121802/) | 1990 | RCT | Journal of Clinical Psychopharmacology | 14-day randomized, double-blind, multicenter study of midazolam vs. flurazepam on sleep, performance, and plasma levels in chronic insomniacs |
| [2229461](https://pubmed.ncbi.nlm.nih.gov/2229461/) | 1990 | RCT (executive summary) | Journal of Clinical Psychopharmacology | Companion multicenter summary of the 14-day flurazepam vs. midazolam chronic insomnia study |
| [2883820](https://pubmed.ncbi.nlm.nih.gov/2883820/) | 1986 | Review | Acta Psychiatrica Scandinavica Supplementum | Reviews clinical use of hypnotics, including benzodiazepines, across insomnia subtypes |
| [17988972](https://pubmed.ncbi.nlm.nih.gov/17988972/) | 2007 | Review | Orvosi Hetilap | General review of insomnia pathophysiology (hyperarousal, cerebral hypoperfusion), background context rather than midazolam-specific |
| [36615100](https://pubmed.ncbi.nlm.nih.gov/36615100/) | 2022 | RCT (different drug, contextual) | Journal of Clinical Medicine | Pilot study of lemborexant (not midazolam) for insomnia in high delirium-risk patients, relevant as it discusses benzodiazepine-related delirium risk in this population |

---

## New Zealand Market Information

Midazolam currently has no market authorization records in this Evidence Pack — market status is "Not Marketed" with 0 registered licenses.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic rationale is strong (established benzodiazepine class effect) and is supported by four direct RCTs from the 1980s–1990s specifically testing midazolam in insomnia. However, safety data required for an initial S1 review — TFDA warnings, contraindications (DG001, Blocking), and DDI records — are currently unavailable, and the drug is not marketed in New Zealand (0 authorizations), so a full evaluation cannot proceed at this time.

**To proceed, the following is needed:**
- Obtain the official package insert/regulatory safety data (warnings, contraindications) to resolve DG001
- Confirm formal MOA documentation to resolve DG002
- Complete DDI data acquisition (current query status: not found)
- Evaluate the NZ registration pathway, since midazolam is currently unmarketed there
- Consider whether the largely decades-old evidence base warrants a modern RCT or systematic review before advancing further
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


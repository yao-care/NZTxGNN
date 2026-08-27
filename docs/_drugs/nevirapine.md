---
layout: default
title: Nevirapine
parent: 僅模型預測 (L5)
nav_order: 240
evidence_level: L5
indication_count: 3
---

# Nevirapine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

# Nevirapine: From HIV-1 Infection to Feline Acquired Immunodeficiency Syndrome

## One-Sentence Summary

Nevirapine is a first-generation non-nucleoside reverse transcriptase inhibitor (NNRTI), originally developed for the treatment of **HIV-1 infection**. The TxGNN model predicts it may be effective for **Feline Acquired Immunodeficiency Syndrome (FIV in cats)**, but this direction is currently supported by only **0 clinical trials** and **1 publication** — and that single publication is a structural comparison study, not a demonstration of efficacy.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HIV-1 infection (based on established drug classification; not directly recorded in this evidence pack — see MOA gap below) |
| Predicted New Indication | Feline Acquired Immunodeficiency Syndrome (FIV) |
| TxGNN Prediction Score | 99.85% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data for nevirapine is not available in this evidence pack (flagged internally as a High-severity data gap). Based on established pharmacological knowledge, nevirapine is a first-generation NNRTI: it binds an allosteric hydrophobic pocket on the HIV-1 reverse transcriptase (RT) enzyme (involving residues such as Y181, K103, Y188) and has well-proven efficacy against HIV-1 infection in humans.

Feline immunodeficiency virus (FIV), the cause of feline acquired immunodeficiency syndrome, belongs to the same *Lentivirus* genus as HIV-1 — both cause a progressive, retrovirus-driven immunodeficiency syndrome. This shared lineage is the most likely basis for the TxGNN model's high similarity score.

However, the mechanistic case is weaker than it first appears. The single supporting publication (PMID 38031646) is explicitly a **biochemical and structural comparison** of NNRTIs (nevirapine, efavirenz, rilpivirine) against feline versus human RT — a study design typically used to test whether cross-species inhibition exists, not to confirm it. Amino acid sequence homology between FIV RT and HIV-1 RT is known to be low, and the NNRTI binding-pocket architecture differs meaningfully between the two enzymes. No in vitro potency data (IC50/EC50) or in vivo efficacy data are provided to show that nevirapine actually inhibits FIV RT. The mechanistic link should therefore be treated as an untested hypothesis rather than an established pharmacological rationale, which is consistent with the "Hold" recommendation already assigned at this stage.

It is also worth noting explicitly that FIV is a **veterinary indication affecting cats**, not a human disease — pursuing this direction would follow a veterinary drug development pathway rather than a human clinical one.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38031646](https://pubmed.ncbi.nlm.nih.gov/38031646/) | 2023 | Structural/Biochemical Comparison | Journal of Veterinary Science | Compared NNRTIs (nevirapine, efavirenz, rilpivirine) against feline vs. human immunodeficiency virus reverse transcriptase; notes no effective FIV treatment currently exists and evaluates structural basis for potential cross-species NNRTI activity — does not report confirmed antiviral efficacy against FIV |

---

## New Zealand Market Information

Nevirapine is not currently marketed in New Zealand, and no product authorizations are on record.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic evidence for this prediction is weak and partly contradictory — the only supporting publication is a structural comparison study, not an efficacy study, and known differences between FIV and HIV-1 reverse transcriptase argue against straightforward cross-species activity. With zero clinical trials, a single non-efficacy literature reference, and a blocking gap in safety/label data, this candidate has not progressed past the S0 screening stage.

**To proceed, the following is needed:**
- TFDA/manufacturer package insert warnings and contraindications (currently a Blocking data gap — without it, the candidate cannot enter the S1 safety review stage)
- Complete mechanism of action / structural binding data for nevirapine (currently a High-severity data gap)
- In vitro FIV RT inhibition assay data (IC50/EC50) to directly test the cross-species hypothesis raised in PMID 38031646
- If in vitro signal is positive, feline pharmacokinetic and toxicity data before any further (veterinary) clinical consideration
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


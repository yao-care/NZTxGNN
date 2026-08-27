---
layout: default
title: Mefenamic Acid
parent: 僅模型預測 (L5)
nav_order: 214
evidence_level: L5
indication_count: 8
---

# Mefenamic Acid
{: .fs-9 }

證據等級: **L5** | 預測適應症: **8** 個
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

Using no additional skill — this is a direct content-generation task fully specified by the prompt template already in the conversation; I'll follow it directly.

# Mefenamic Acid: From Pain/Inflammatory Conditions to Rheumatoid Arthritis

## One-Sentence Summary

> Mefenamic acid is a fenamate-class NSAID whose original regulatory indication data could not be retrieved from New Zealand sources (unmarketed product, no license records).
> The TxGNN model predicts it may be effective for **Rheumatoid Arthritis**,
> a use already explored in historical clinical literature, with **3 randomized controlled trials** and **20 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from regulatory records (analgesic/anti-inflammatory NSAID by known drug class) |
| Predicted New Indication | Rheumatoid Arthritis |
| TxGNN Prediction Score | 99.73% |
| Evidence Level | L2 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the structured evidence pack. Based on known pharmacology, mefenamic acid is a fenamate-class NSAID that non-selectively inhibits COX-1/COX-2, reducing prostaglandin synthesis and producing direct analgesic and anti-inflammatory effects. This mechanism is a well-established pharmacological fact for the fenamate class, not a speculative inference.

Rheumatoid arthritis (RA) is a chronic inflammatory joint disease in which prostaglandin-mediated inflammation drives pain and joint damage — the same pathway mefenamic acid targets. Because of this direct mechanistic overlap, mefenamic acid's anti-inflammatory/analgesic activity is plausibly applicable to RA symptom management.

Notably, this is not a purely novel repurposing signal: mefenamic acid was already studied head-to-head against ibuprofen, phenylbutazone, sulindac, flurbiprofen, and aspirin in RA patients during the 1960s–1970s, indicating the drug class had established clinical use in RA symptomatic treatment historically, even though current regulatory records (TFDA/NZ) show no active license or approved indication text for this specific use.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [373989](https://pubmed.ncbi.nlm.nih.gov/373989/) | 1979 | RCT | Current Medical Research and Opinion | Double-blind crossover in 24 RA patients: mefenamic acid, flurbiprofen and sulindac all significantly superior to placebo on pain score, joint tenderness, and morning stiffness |
| [330287](https://pubmed.ncbi.nlm.nih.gov/330287/) | 1977 | RCT | The Journal of International Medical Research | Randomized double-blind within-patient study (n=40): mefenamic acid and ibuprofen showed similar analgesic/anti-inflammatory effect; similar side-effect profile |
| [796645](https://pubmed.ncbi.nlm.nih.gov/796645/) | 1976 | RCT | The Medical Journal of Australia | Double-blind crossover trial: mefenamic acid (1500 mg/day) compared favourably with ibuprofen (1200 mg/day); side effects mild, mostly gastrointestinal |
| [4294443](https://pubmed.ncbi.nlm.nih.gov/4294443/) | 1967 | Cohort/Case series | Annals of the Rheumatic Diseases | Early clinical study specifically titled "Mefenamic acid in rheumatoid arthritis" |
| [306128](https://pubmed.ncbi.nlm.nih.gov/306128/) | 1978 | Review | Scottish Medical Journal | Reviews the clinical place of mefenamic acid in RA treatment |
| [10439](https://pubmed.ncbi.nlm.nih.gov/10439/) | 1976 | Comparative study | The Journal of Rheumatology | Evaluation of 10 antirheumatic drugs (including mefenamic acid) in 684 RA patients using daily pain charts and withdrawal/satisfaction metrics |
| [5920657](https://pubmed.ncbi.nlm.nih.gov/5920657/) | 1966 | Comparative study | British Medical Journal | Mefenamic acid and flufenamic acid compared with aspirin and phenylbutazone in RA |
| [6039589](https://pubmed.ncbi.nlm.nih.gov/6039589/) | 1967 | Comparative study | Annals of the Rheumatic Diseases | Outpatient RA drug assessment comparing mefenamic/flufenamic acids with phenylbutazone and aspirin |
| [4890710](https://pubmed.ncbi.nlm.nih.gov/4890710/) | 1967 | Double-blind study | Reumatismo | Clinical and biohumoral double-blind evaluation of mefenamic acid therapy in RA (preliminary observations) |
| [20668](https://pubmed.ncbi.nlm.nih.gov/20668/) | 1977 | Review | Seminars in Arthritis and Rheumatism | General review of anti-inflammatory drugs including fenamates |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Three randomized controlled trials (1976–1979) and a body of historical literature directly support mefenamic acid's analgesic/anti-inflammatory efficacy in RA patients, and its COX-inhibition mechanism is directly relevant to RA's inflammatory pathology. However, the drug is currently unmarketed in New Zealand and key safety/regulatory data (package insert warnings, contraindications, MOA, DDI) are unavailable, so this cannot proceed without safety gating.

**To proceed, the following is needed:**
- TFDA/NZ package insert warnings and contraindications (currently blocking data gap, DG001)
- Verified mechanism of action documentation from DrugBank or equivalent source (DG002)
- Drug-drug interaction profile (current query returned "not_found")
- New Zealand regulatory pathway assessment, since the product currently has zero active licenses
- Modern RA-specific trial data, since existing RCT evidence is >45 years old and predates current RA standard-of-care (DMARDs/biologics) comparators
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


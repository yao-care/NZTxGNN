---
layout: default
title: Pregabalin
parent: 僅模型預測 (L5)
nav_order: 287
evidence_level: L5
indication_count: 6
---

# Pregabalin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
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

Using the report template supplied in the task to produce the evaluation report from the given Evidence Pack.

# Pregabalin: From Neuropathic Pain to Tendinitis

## One-Sentence Summary

Pregabalin (DrugBank DB00230) is a gabapentinoid classically used for neuropathic pain, epilepsy (adjunctive), fibromyalgia, and generalized anxiety disorder. The TxGNN model predicts it may be effective for **Tendinitis**, but this direction is currently supported only by **0 clinical trials** and **6 publications**, none of which directly study tendon pathology.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not captured in this evidence pack (no NZ license/indication text available); pregabalin is globally established for neuropathic pain, epilepsy (adjunct), fibromyalgia, and generalized anxiety disorder |
| Predicted New Indication | Tendinitis |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L4 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in this evidence pack (flagged as a High-severity data gap). Based on well-established pharmacology, pregabalin binds the α2δ-1 subunit of voltage-gated calcium channels, reducing presynaptic release of excitatory neurotransmitters (glutamate, substance P), which underlies its clinical use in neuropathic pain and as an analgesic/anticonvulsant adjunct.

The literature retrieved for tendinitis consists mainly of RCTs evaluating pregabalin as a perioperative, opioid-sparing analgesic adjunct after arthroscopic rotator cuff repair — not studies of tendon inflammation, degeneration, or repair biology. The evidence pack's own mechanistic assessment is explicit on this point: current literature reflects symptomatic pain-control extrapolation rather than any disease-specific effect on tendon pathology, and there is no evidence pregabalin alters tendinopathy mechanisms.

Given the absence of a specific mechanistic rationale and of any tendinitis-focused trial, the high TxGNN score most likely reflects graph-structural proximity between "pain/nerve" and "musculoskeletal" nodes in the knowledge graph rather than a validated pharmacological hypothesis.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [32839073](https://pubmed.ncbi.nlm.nih.gov/32839073/) | 2021 | RCT (perioperative analgesia) | Journal of Orthopaedic Science | Retrospective cohort on analgesic efficacy and opioid-sparing effect of pregabalin after arthroscopic rotator cuff repair |
| [34052386](https://pubmed.ncbi.nlm.nih.gov/34052386/) | 2022 | RCT (perioperative analgesia) | Arthroscopy | Perioperative oral pregabalin produced pain scores comparable to interscalene brachial plexus block after rotator cuff repair |
| [37051935](https://pubmed.ncbi.nlm.nih.gov/37051935/) | 2023 | Case report | Pain Practice | Posterior femoral cutaneous nerve impingement from hamstring tendonitis post-marathon; not a pregabalin efficacy study |
| [41017607](https://pubmed.ncbi.nlm.nih.gov/41017607/) | 2025 | Case report | Praxis | Fluoroquinolone-associated tendinopathy/disability case; pregabalin not the study drug |
| [40818536](https://pubmed.ncbi.nlm.nih.gov/40818536/) | 2025 | Editorial commentary | Arthroscopy | Commentary on piriformis syndrome diagnosis and surgical management; not related to pregabalin |
| [39703364](https://pubmed.ncbi.nlm.nih.gov/39703364/) | 2024 | Preclinical (animal extract study) | Advances in Pharmacological and Pharmaceutical Sciences | Plant extract (not pregabalin) attenuates vincristine-induced peripheral neuropathy in rats |

---

## New Zealand Market Information

Pregabalin currently has no market authorization on record in this evidence pack (0 licenses); market status is "Not Marketed."

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence level is L4 with zero clinical trials and no literature specific to tendon inflammation or repair — the retrieved studies address perioperative pain control after rotator cuff surgery, not tendinitis treatment itself, so the TxGNN signal cannot currently be distinguished from graph-structural noise.

**To proceed, the following is needed:**
- TFDA/NZ regulatory package insert data (warnings, contraindications) — currently a Blocking data gap
- Confirmed mechanism of action (DrugBank query) — currently a High-severity data gap
- A tendinitis- or tendinopathy-specific preclinical or clinical study evaluating pregabalin's effect on inflammatory/repair pathways, not just pain scores
- If pursued, a dedicated trial design (not opioid-sparing surrogate outcomes)

---

## Appendix: Other TxGNN-Predicted Indications in This Evidence Pack

This evidence pack (`TW-DB00230-multi`) scored six candidate indications for pregabalin. Evidence strength varies considerably and does not track TxGNN rank order — notably, **migraine disorder** (ranked 5th by score) has substantially stronger clinical evidence than the top-ranked tendinitis prediction:

| Disease | TxGNN Score | Evidence Level | Decision Stage | Recommendation |
|---|---|---|---|---|
| Tendinitis | 99.71% | L4 | S0 | Hold |
| Myositis fibrosa | 99.71% | L5 | S0 | Hold |
| Idiopathic granulomatous myositis | 99.71% | L5 | S0 | Hold |
| Inclusion body myositis | 99.52% | L5 | S0 | Hold |
| **Migraine disorder** | 99.47% | **L2** | **S2** | **Research Question** |
| Migraine with brainstem aura | 99.43% | L4 | S0 | Hold |

Migraine disorder is backed by a Cochrane systematic review (PMID 23797674/23797675), a 2024 JAMA Network Open network meta-analysis, and multiple pediatric RCTs (PMID 37637787, 26024701), plus a mechanistic rationale (α2δ-1-mediated inhibition of cortical spreading depression) directly relevant to migraine pathophysiology — unlike the tendinitis and myositis predictions, whose supporting literature is largely incidental or graph-structural. **If this candidate pool is being triaged for further work, migraine disorder — not tendinitis — is the stronger near-term candidate** and warrants a separate evaluation report at decision stage S2.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Tyrosine
parent: 僅模型預測 (L5)
nav_order: 355
evidence_level: L5
indication_count: 10
---

# Tyrosine
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

Using the report template directly (this is a template-driven deliverable, not an open-ended coding/debugging task — no other skill fits better).

One judgment call worth flagging up front: the Evidence Pack ranks 10 TxGNN-predicted indications for Tyrosine, but `predicted_indications[0]` ("cauda equina syndrome") is explicitly annotated in the pack's own rationale as having **no treatment-mechanism link** — the only supporting literature is an unrelated case report. Several other top-ranked entries (hyperthyroidism, hyperthyroxinemia, TRβ resistance, neovascular glaucoma) are flagged by the pack itself as artifacts of embedding proximity or literal name confusion with "tyrosine kinase inhibitor" drugs. The only entry that reaches decision stage S1 with a coherent (if unresolved) mechanism is rank 5, **Postural Orthostatic Tachycardia Syndrome (POTS)**. I built the report around that entry rather than mechanically taking rank 1, since reporting a rationale the evidence pack itself refutes would be misleading. This is noted explicitly below.

---

# Tyrosine: From Nutritional Amino Acid Supplement to Postural Orthostatic Tachycardia Syndrome (Research Signal)

## One-Sentence Summary

Tyrosine is a non-essential amino acid and precursor for catecholamine (dopamine/norepinephrine/epinephrine) synthesis; it is not currently marketed as an approved medicine in New Zealand and has no on-file original therapeutic indication.
Among 10 TxGNN-predicted indications in this evidence pack, only **Postural Orthostatic Tachycardia Syndrome (POTS)** reaches a research-grade evidence tier (L4), supported by **1 clinical trial** and **4 publications**; the remaining 9 predictions (including the top TxGNN score, cauda equina syndrome) are explicitly flagged in the source data as lacking any credible mechanistic link.
Overall, the evidence is not sufficient to support active repurposing at this time.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not established — Tyrosine is not currently marketed as an approved medicine in New Zealand; no original indication is on file |
| Predicted New Indication | Postural Orthostatic Tachycardia Syndrome (POTS) |
| TxGNN Prediction Score | 99.46% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data for Tyrosine is not available in this evidence pack (Data Gap). Based on known pharmacology, tyrosine is the direct amino-acid substrate for tyrosine hydroxylase, the rate-limiting enzyme in catecholamine biosynthesis (dopamine → norepinephrine → epinephrine). This gives a plausible biochemical rationale for a role in POTS, a syndrome whose pathophysiology includes a "hypoadrenergic" subtype driven by insufficient norepinephrine synthesis — in that subtype, supplementing the biosynthetic precursor is mechanistically coherent.

However, the literature captured alongside this prediction cuts the other way for at least one subtype: PMID 39063020 reports benefit from **alpha-methyl-p-tyrosine (AMPT/metyrosine)**, a tyrosine hydroxylase *inhibitor* that lowers catecholamine synthesis, used successfully in a hyperadrenergic-pattern fatigue syndrome. That is the pharmacologic opposite of tyrosine supplementation. This means the direction of benefit is subtype-dependent, and the current evidence pack cannot distinguish which POTS patients would benefit from more tyrosine versus less catecholamine synthesis overall.

By contrast, most of the other 9 TxGNN-ranked predictions in this pack (cauda equina syndrome, obsolete neurogenic bladder, angle-closure/neovascular/traumatic glaucoma, hyperthyroidism, hyperthyroxinemia, TRβ resistance) are explicitly annotated as lacking a real mechanistic bridge — several are driven by literal name overlap between "tyrosine" and unrelated "tyrosine kinase inhibitor" drugs (e.g., axitinib, TKIs in the hyperthyroidism and neovascular glaucoma trial evidence), and others (hyperthyroidism, hyperthyroxinemia, TRβ resistance) involve supplying a hormone precursor to treat a state of hormone *excess* — a direction that does not make pharmacological sense. Tyrosine is a food-grade GRAS amino acid with a generally favorable safety background, which is the main reason POTS is framed as a research question rather than dismissed outright.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00580619](https://clinicaltrials.gov/study/NCT00580619) | Phase 1 | Completed | 170 | Studied the autonomic nervous system in chronic fatigue syndrome and the POTS subset, testing the hypothesis that sympathetic activation drives cardiovascular/inflammatory abnormalities; did not test tyrosine as an intervention (relevance grade B — related population/mechanism, not a direct tyrosine trial) |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31412221](https://pubmed.ncbi.nlm.nih.gov/31412221/) | 2020 | Review | Annual Review of Medicine | Describes the three major POTS mechanisms — partial autonomic neuropathy, hypovolemia, and hyperadrenergic state — relevant to identifying which subtype tyrosine supplementation could target |
| [15710782](https://pubmed.ncbi.nlm.nih.gov/15710782/) | 2005 | Cohort | Hypertension | Characterizes hyperadrenergic POTS associated with mast cell activation disorders |
| [12403667](https://pubmed.ncbi.nlm.nih.gov/12403667/) | 2002 | Cohort | Circulation | Documents cardiac sympathetic dysautonomia in POTS and related orthostatic intolerance syndromes |
| [39063020](https://pubmed.ncbi.nlm.nih.gov/39063020/) | 2024 | Case Report | International Journal of Molecular Sciences | Reports symptomatic improvement in stress-related chronic fatigue syndrome using alpha-methyl-p-tyrosine (AMPT), a tyrosine hydroxylase **inhibitor** — evidence for lowering, not raising, tyrosine-pathway activity in a hyperadrenergic-pattern patient |

---

## New Zealand Market Information

Currently not marketed in New Zealand — no product licenses are on file.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Across the 10 TxGNN-predicted indications for Tyrosine, only POTS reaches a research-grade evidence tier, and even there the direction of mechanistic benefit is subtype-dependent and unresolved (supplementation vs. inhibition of the same pathway). No clinical trial has directly tested tyrosine in POTS. Combined with the complete absence of regulatory safety data (a Blocking-severity data gap) and the drug's unmarketed status in New Zealand, the evidence does not support proceeding into active repurposing evaluation.

**To proceed, the following is needed:**
- TFDA/regulatory package insert warnings and contraindications (Blocking data gap — currently missing, blocks any S1 safety screen)
- Detailed mechanism of action / pharmacokinetic documentation (High-severity data gap)
- A tyrosine-specific interventional trial restricted to confirmed hypoadrenergic-subtype POTS patients
- Drug interaction (DDI) data — current query returned no results
- Clarification of which POTS subtype(s) tyrosine supplementation is hypothesized to benefit, given literature evidence points toward pathway *inhibition* being beneficial in hyperadrenergic presentations
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


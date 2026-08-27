---
layout: default
title: Sumatriptan
parent: 僅模型預測 (L5)
nav_order: 327
evidence_level: L5
indication_count: 1
---

# Sumatriptan
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

# Sumatriptan: From Migraine to Migraine with Brainstem Aura

## One-Sentence Summary

> Sumatriptan is a triptan-class 5-HT1B/1D receptor agonist originally used for the acute treatment of migraine and cluster headache.
> The TxGNN model predicts it may also be effective for **Migraine with Brainstem Aura**,
> though this is currently supported only by mechanistic literature (**0 disease-specific clinical trials**, **18 related publications**) and carries a known safety caveat that requires careful review.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Acute treatment of migraine (with or without aura) and cluster headache — based on internationally established use; not derived from NZ licensing data, as the drug is not currently marketed in New Zealand |
| Predicted New Indication | Migraine with Brainstem Aura |
| TxGNN Prediction Score | 99.74% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Sumatriptan is a selective 5-HT1B/1D receptor agonist. It terminates migraine attacks by constricting dilated intracranial meningeal blood vessels and inhibiting release of vasoactive neuropeptides (e.g., CGRP) from perivascular trigeminal axons in the dura mater, following activation of the trigeminovascular system. This mechanism is pharmacologically plausible across the migraine spectrum, including subtypes with aura, since the underlying trigeminovascular pathophysiology is shared.

However, "migraine with brainstem aura" (formerly known as basilar-type migraine) is a special case. In most product labels and clinical guidelines, this subtype is listed as a caution or relative contraindication for triptan-class drugs, because vasoconstriction in the theoretical vertebrobasilar/brainstem vascular territory could precipitate ischemic events. This concern is reinforced by literature evidence (PMID 25841032), which found sumatriptan's efficacy is *lower* in migraine with aura compared to migraine without aura.

In short, the TxGNN score of 99.74% reflects strong drug–disease category association driven by shared migraine pathophysiology — it is not a safety endorsement. This is a textbook example of a mechanistically reasonable prediction that carries a pre-existing, well-documented safety concern requiring manual clinical review before any further development.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25841032](https://pubmed.ncbi.nlm.nih.gov/25841032/) | 2015 | Cohort | Neurology | Sumatriptan efficacy is reduced in migraine with aura vs. without aura — directly relevant to the safety/efficacy profile in aura subtypes |
| [1313746](https://pubmed.ncbi.nlm.nih.gov/1313746/) | 1992 | RCT | Cephalalgia | Double-blind, placebo-controlled trial of oral sumatriptan 200mg specifically in classical migraine (migraine WITH aura) |
| [23657930](https://pubmed.ncbi.nlm.nih.gov/23657930/) | 2014 | RCT | Phytotherapy Research | Double-blind RCT comparing ginger powder vs. sumatriptan in acute migraine; sumatriptan used as the established active comparator |
| [33567890](https://pubmed.ncbi.nlm.nih.gov/33567890/) | 2021 | RCT | Cephalalgia | Early sumatriptan treatment prevented PACAP38-induced migraine attacks, supporting timing-dependent 5-HT1 agonist efficacy |
| [21469920](https://pubmed.ncbi.nlm.nih.gov/21469920/) | 2011 | Review | Expert Review of Neurotherapeutics | Review of needle-free subcutaneous sumatriptan, approved for acute migraine with or without aura and cluster headache |
| [8536293](https://pubmed.ncbi.nlm.nih.gov/8536293/) | 1995 | Review | Cephalalgia | Critical review of sumatriptan's 5-HT1 receptor mechanism, cranial vasoconstriction, and clinical experience in migraine/cluster headache |
| [25600718](https://pubmed.ncbi.nlm.nih.gov/25600718/) | 2015 | Review | Headache | American Headache Society evidence assessment of acute migraine pharmacotherapies, including triptans |
| [38307660](https://pubmed.ncbi.nlm.nih.gov/38307660/) | 2024 | Review | Handbook of Clinical Neurology | Review of status migrainosus, a recognized complication of migraine with or without aura |
| [31135819](https://pubmed.ncbi.nlm.nih.gov/31135819/) | 2019 | Mechanism study | JAMA Neurology | PET imaging shows triptans bind central 5-HT1B receptors during a migraine attack, clarifying CNS penetration and mechanism |
| [39391443](https://pubmed.ncbi.nlm.nih.gov/39391443/) | 2024 | Case report | Cureus | Case report of migraine with aura accompanied by myoclonus, illustrating atypical aura presentations |

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Despite a very high TxGNN association score (99.74%), migraine with brainstem aura is conventionally flagged as a caution/relative contraindication for triptans due to theoretical vertebrobasilar vasoconstriction risk, and existing literature shows reduced sumatriptan efficacy in migraine-with-aura populations. No clinical trials target this specific niche indication, and the drug is not currently marketed in New Zealand.

**To proceed, the following is needed:**
- TFDA/NZ package insert warnings and contraindications (Blocking data gap, DG001)
- Confirmed mechanism of action documentation from DrugBank (DG002)
- A dedicated vascular safety review of triptan use specifically in migraine with brainstem aura
- Drug interaction (DDI) data (currently not found)
- Confirmation of New Zealand market/licensing pathway, given the drug is presently unmarketed
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


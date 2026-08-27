---
layout: default
title: Ticagrelor
parent: 僅模型預測 (L5)
nav_order: 340
evidence_level: L5
indication_count: 10
---

# Ticagrelor
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

# Ticagrelor: From Acute Coronary Syndrome to Intracranial Arteriosclerosis

## One-Sentence Summary

Ticagrelor is a reversible P2Y12 receptor antagonist originally used for acute coronary syndrome (ACS) and ischemic cardiovascular event prevention as part of dual antiplatelet therapy.
The TxGNN model predicts it may also be effective for **Intracranial Arteriosclerosis**,
with **10 clinical trials** and **3 publications** currently supporting this direction, though the pivotal Phase 3 trial has not yet reported results.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not registered in New Zealand; per known pharmacology and confirmed by this evidence pack's own rationale for the "ischemic disease" candidate, ticagrelor's native indication is ACS / ischemic cardiovascular event prevention (dual antiplatelet therapy) |
| Predicted New Indication | Intracranial Arteriosclerosis |
| TxGNN Prediction Score | 99.97% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

**Note:** Among the 10 TxGNN-ranked candidates in this evidence pack, rank 4 ("ischemic disease") is itself flagged by the evidence rationale as ticagrelor's *native* approved indication rather than a repurposing hypothesis — this acts as an internal validity check, since a credible model should rediscover known indications alongside novel ones.

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data (DrugBank `original_moa`) is currently a data gap. Based on the available repurposing rationale, ticagrelor is a reversible P2Y12 receptor antagonist that directly inhibits ADP-induced platelet activation and aggregation — the core pharmacology behind its proven efficacy in ACS, post-PCI, prior myocardial infarction, and ischemic stroke/TIA prevention.

Intracranial arteriosclerosis (ICAS) involves ischemic events driven by plaque rupture at intracranial arterial sites, followed by platelet activation and thrombus formation — mechanistically parallel to coronary atherosclerosis. P2Y12 inhibition is therefore a logical extrapolation from ticagrelor's established antiplatelet role.

However, the evidence pack's own rationale flags an important caveat: intracranial vascular anatomy (including perforating arteries) and elevated hemorrhagic risk mean this mechanism cannot be directly extrapolated from coronary data without dedicated confirmatory trials. This is reflected in the L2 evidence level — supportive but not yet definitive.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT05047172](https://clinicaltrials.gov/study/NCT05047172) | Phase 3 | Active, not recruiting | 1683 | CAPTIVA trial — determines whether rivaroxaban, ticagrelor, or both are superior to clopidogrel for lowering 1-year rate of ischemic stroke, intracerebral hemorrhage, or vascular death in intracranial vascular atherostenosis; the pivotal trial, not yet reported |
| [NCT02605447](https://clinicaltrials.gov/study/NCT02605447) | Phase 4 | Completed | 2009 | EVOLVE Short DAPT — assessed safety of 3-month DAPT in high bleeding-risk PCI patients with the SYNERGY stent system |
| [NCT04948749](https://clinicaltrials.gov/study/NCT04948749) | N/A | Recruiting | 792 | DREAM-PRIDE — evaluates drug-eluting stent plus aggressive medical treatment (incl. antiplatelet) vs. medical treatment alone for symptomatic intracranial atherosclerotic disease |
| [NCT06714526](https://clinicaltrials.gov/study/NCT06714526) | N/A | Recruiting | 100 | Pilot RCT comparing genotype-guided P2Y12 inhibitor selection vs. conventional clopidogrel in symptomatic ICAS |
| [NCT01813435](https://clinicaltrials.gov/study/NCT01813435) | Phase 3 | Completed | 15991 | GLOBAL LEADERS — large general PCI population comparing ticagrelor+aspirin vs. standard DAPT strategy; indirect safety database support |
| [NCT01732822](https://clinicaltrials.gov/study/NCT01732822) | Phase 3 | Completed | 13885 | EUCLID — ticagrelor vs. clopidogrel in peripheral artery disease; low direct relevance to ICAS |
| [NCT06857045](https://clinicaltrials.gov/study/NCT06857045) | N/A | Withdrawn | 0 | 3- vs 6-month DAPT after NOVA intracranial sirolimus-eluting stent implantation |
| [NCT06058130](https://clinicaltrials.gov/study/NCT06058130) | N/A | Unknown | 2171 | Anticoagulation vs. anticoagulation + antiplatelet in acute ischemic stroke with concomitant AF and extracranial/intracranial artery stenosis |
| [NCT07164859](https://clinicaltrials.gov/study/NCT07164859) | Phase 3 | Not yet recruiting | 1700 | SOLOPCI — very short DAPT followed by P2Y12 monotherapy in older PCI patients (general, not ICAS-specific) |
| [NCT03620760](https://clinicaltrials.gov/study/NCT03620760) | Phase 4 | Unknown | 2036 | Low-dose vs. standard-dose ticagrelor after drug-eluting stent for unstable angina (general PCI population) |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39862061](https://pubmed.ncbi.nlm.nih.gov/39862061/) | 2025 | RCT | International Journal of Stroke | Design and early progress of the CAPTIVA trial, testing dual antithrombotic combinations (incl. ticagrelor) vs. clopidogrel+aspirin for symptomatic intracranial atherosclerotic stenosis |
| [39658130](https://pubmed.ncbi.nlm.nih.gov/39658130/) | 2025 | Cohort | Journal of Neurointerventional Surgery | Reports experience with lower-dose ticagrelor (60 mg BID) plus aspirin vs. standard aspirin/clopidogrel for intracranial stenting |
| [38252758](https://pubmed.ncbi.nlm.nih.gov/38252758/) | 2024 | Review | Stroke | Focused update on intracranial atherosclerosis, summarizing current knowledge gaps and treatment context |

---

## New Zealand Market Information

Ticagrelor is currently not marketed in New Zealand — 0 authorizations on file.

---

## Safety Considerations

Please refer to the package insert for safety information. No structured warnings, contraindications, or drug-interaction data are currently available in this evidence pack (TFDA package insert and DDI queries are data gaps).

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic rationale is plausible and one Phase 3 trial (CAPTIVA, NCT05047172) is directly testing ticagrelor in this indication, but it remains active with results not expected until 2027 — evidence level L2 / decision stage S2 ("Research Question") is not yet sufficient to proceed, particularly given the elevated intracranial hemorrhage risk noted in the rationale.

**To proceed, the following is needed:**
- CAPTIVA (NCT05047172) primary results comparing ticagrelor/rivaroxaban vs. clopidogrel for ischemic stroke, hemorrhage, and vascular death
- TFDA/local regulatory package insert data (warnings, contraindications) — currently a Blocking data gap (DG001)
- Detailed mechanism of action data from DrugBank — currently a High-severity data gap (DG002)
- Drug-drug interaction data (current DDI query returned not_found)
- New Zealand market entry status if this repurposing pathway is pursued further
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


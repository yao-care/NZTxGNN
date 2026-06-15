---
layout: default
title: Acetylcysteine
parent: 僅模型預測 (L5)
nav_order: 14
evidence_level: L5
indication_count: 10
---

# Acetylcysteine
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

# Acetylcysteine: From Mucolytic Agent to Thrombotic Disease

## One-Sentence Summary

Acetylcysteine (NAC) is a well-established mucolytic agent and antidote for acetaminophen overdose, with a long history of clinical use in respiratory conditions worldwide.
The TxGNN model predicts it may be effective for **Thrombotic Disease** — in particular thrombotic thrombocytopenic purpura (TTP) and transplantation-associated thrombotic microangiopathy (TA-TMA) —
with **9 clinical trials** and **20 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Mucolytic agent for respiratory conditions; antidote for acetaminophen overdose |
| Predicted New Indication | Thrombotic Disease (TTP / TA-TMA) |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L1 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Acetylcysteine carries a free thiol (–SH) group that enables direct thiol-disulfide exchange reactions. In the context of thrombotic disease, this chemistry is central: NAC can cleave the inter-subunit disulfide bonds within ultra-large von Willebrand factor (ULVWF) multimers — the oversized protein chains that drive platelet aggregation and microvascular thrombosis when ADAMTS13 protease activity is deficient, as occurs in TTP. A landmark 2011 study in the *Journal of Clinical Investigation* (PMID 21266777) demonstrated that NAC reduces ULVWF size and platelet-binding activity in both human plasma and murine models, providing direct mechanistic evidence for this hypothesis.

Beyond VWF modification, NAC acts as a glutathione (GSH) precursor, replenishing cellular antioxidant reserves and reducing oxidative stress on the vascular endothelium. This dual mechanism — mechanical disruption of pathological VWF multimers combined with endothelial cytoprotection — makes NAC biologically plausible as an adjunct treatment for thrombotic microangiopathies. The two clinical subtypes with the strongest supporting evidence are acquired TTP (ADAMTS13 autoantibody-mediated) and TA-TMA (a severe, often fatal complication within 100 days of hematopoietic stem cell transplantation for which plasma exchange achieves less than 10% response and complement inhibitors such as eculizumab are prohibitively expensive).

The mechanistic link to acetylcysteine's original mucolytic role is direct and coherent: both indications exploit the same free thiol chemistry. Just as NAC cleaves disulfide bonds in mucin glycoproteins to reduce sputum viscosity in airways, it cleaves disulfide bonds within ULVWF multimer subunits to interrupt the thrombotic cascade. This structural analogy is what makes the TxGNN prediction mechanistically credible rather than merely statistical.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03252925](https://clinicaltrials.gov/study/NCT03252925) | Phase 3 | Completed | 170 | Prospective trial evaluating NAC safety and efficacy in TA-TMA post-HSCT — the largest completed trial with a direct TA-TMA primary endpoint; provides the highest-grade clinical evidence in this indication |
| [NCT05907486](https://clinicaltrials.gov/study/NCT05907486) | Phase 3 | Unknown | 260 | NAC for prevention of thrombotic events after allogeneic HSCT — largest Phase 3 preventive trial (n=260); final results pending status confirmation |
| [NCT07279610](https://clinicaltrials.gov/study/NCT07279610) | Phase 2/3 | Active (not recruiting) | 44 | Multicenter prospective single-arm trial of NAC in TA-TMA — framed as accessible alternative to eculizumab given current treatment response rates below 10% for plasma exchange |
| [NCT03636932](https://clinicaltrials.gov/study/NCT03636932) | Phase 2 | Completed | 40 | RENACTIF trial: randomized, double-blind, placebo-controlled crossover study of NAC in chronic kidney disease — targets the thrombotic phenotype driven by uremic toxin-induced oxidative stress and elevated tissue factor; mechanism directly implicates ULVWF cleavage |
| [NCT04368598](https://clinicaltrials.gov/study/NCT04368598) | Phase 2 | Unknown | 44 | Single-arm study of NAC + high-dose dexamethasone in newly diagnosed immune thrombocytopenia (ITP) — indirect relevance; NAC addresses oxidative stress component of platelet destruction |
| [NCT03460808](https://clinicaltrials.gov/study/NCT03460808) | Phase 1/2 | Unknown | 200 | Atorvastatin + acetylcysteine + danazol vs. danazol alone in steroid-resistant/relapsed ITP — large multicentre study; NAC contribution difficult to isolate from combination regimen |
| [NCT01808521](https://clinicaltrials.gov/study/NCT01808521) | Early Phase 1 | Completed | 3 | Pilot IV NAC study in suspected TTP receiving plasma exchange — assessed whether NAC enhances ADAMTS13-mediated VWF cleavage and prevents platelet/VWF string propagation; exploratory only due to very small sample |
| [NCT05551624](https://clinicaltrials.gov/study/NCT05551624) | Early Phase 1 | Completed | 15 | Atorvastatin + NAC in steroid-resistant/relapsed ITP — small exploratory study assessing platelet count as surrogate endpoint; insufficient for efficacy conclusions |
| [NCT06518044](https://clinicaltrials.gov/study/NCT06518044) | Phase 2 | Not yet recruiting | 30 | NAC for hematopoietic recovery in severe aplastic anemia after haploidentical transplantation — mechanistic overlap with thrombotic disease is limited; not yet initiated |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [41977015](https://pubmed.ncbi.nlm.nih.gov/41977015/) | 2026 | Systematic Review | Journal of Clinical Medicine | NAC therapy in TTP: systematic review and critical appraisal — highest-tier synthesis of all available NAC evidence in ADAMTS13-deficiency thrombosis; most current overview available |
| [35940529](https://pubmed.ncbi.nlm.nih.gov/35940529/) | 2022 | RCT | Transplantation and Cellular Therapy | Prospective open-label randomized placebo-controlled trial of NAC prophylaxis for TA-TMA post-HSCT — direct randomized evidence for preventive efficacy in the transplant setting |
| [37311880](https://pubmed.ncbi.nlm.nih.gov/37311880/) | 2023 | Retrospective Cohort | Annals of Hematology | NAC treatment associated with reduced in-hospital mortality in acquired TTP — real-world cohort data supporting a survival benefit, though causal interpretation requires cautious framing |
| [21266777](https://pubmed.ncbi.nlm.nih.gov/21266777/) | 2011 | Mechanistic Study | Journal of Clinical Investigation | NAC reduces ULVWF multimer size and platelet-binding activity in human plasma and murine models — foundational evidence establishing the disulfide-cleavage mechanism underpinning the entire repurposing rationale |
| [28011677](https://pubmed.ncbi.nlm.nih.gov/28011677/) | 2017 | Animal Study | Blood | NAC in preclinical mouse and baboon TTP models — demonstrates VWF multimer reduction and disruption of platelet/VWF strings, supporting translational potential from bench to bedside |
| [32243196](https://pubmed.ncbi.nlm.nih.gov/32243196/) | 2020 | Review | Expert Review of Hematology | Repurposed drugs and novel agents in TTP treatment — positions NAC alongside rituximab, bortezomib, and caplacizumab as an emerging therapeutic option for refractory/relapsed disease |
| [33540569](https://pubmed.ncbi.nlm.nih.gov/33540569/) | 2021 | Review | Journal of Clinical Medicine | TTP pathophysiology, diagnosis, and management — comprehensive review contextualising ADAMTS13 biology and current treatment landscape for readers needing disease background |
| [28416507](https://pubmed.ncbi.nlm.nih.gov/28416507/) | 2017 | Review | Blood | Thrombotic thrombocytopenic purpura — authoritative review in a top haematology journal covering ADAMTS13 deficiency mechanisms and clinical management milestones |
| [39737637](https://pubmed.ncbi.nlm.nih.gov/39737637/) | 2025 | Case Report | Journal of Pediatric Hematology/Oncology | Plasma exchange + NAC in congenital TTP presenting with acute renal failure — novel ADAMTS13 mutation identified; NAC used as a clinical adjunct, illustrating real-world application in the rare congenital subtype |
| [28645643](https://pubmed.ncbi.nlm.nih.gov/28645643/) | 2017 | Review | Transfusion Clinique et Biologique | TTP management with plasma exchange, rituximab, and emerging agents — frames NAC within a multimodal treatment strategy for disease that remains refractory or relapses in up to 40% of patients |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
At least one completed Phase 3 trial (NCT03252925, n=170) and a 2026 systematic review provide direct clinical evidence supporting NAC in TA-TMA, while the mechanistic basis — ULVWF disulfide bond cleavage — is established in both preclinical and clinical work. However, acetylcysteine holds zero current authorisations in New Zealand, and structured safety data (package insert warnings, contraindications, drug interactions) are not available in this dataset, creating a blocking gap before formal regulatory steps can begin.

**To proceed, the following is needed:**
- Retrieve the full package insert (TFDA or equivalent) to extract contraindications, key warnings, and known drug interactions before safety profiling can be completed
- Confirm the outcome status of NCT05907486 (Phase 3, n=260) and await results from NCT07279610 (Phase 2/3, currently active)
- Narrow the target indication: TTP, TA-TMA post-HSCT, and CKD-associated thrombotic phenotype each have distinct patient populations, treatment pathways, and regulatory requirements — a single repurposing dossier covering all three is unlikely to be feasible
- Define required route and dose for the New Zealand context (IV formulation for TTP/TA-TMA vs. alternative routes for CKD thromboprophylaxis)
- Initiate a regulatory pre-submission consultation with Medsafe regarding the repurposing pathway given the absence of any existing New Zealand authorisations for this molecule
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Budesonide
parent: 僅模型預測 (L5)
nav_order: 54
evidence_level: L5
indication_count: 10
---

# Budesonide
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

# Budesonide: From Asthma to Atopic Eczema

## One-Sentence Summary

Budesonide is a synthetic inhaled corticosteroid primarily approved for asthma and inflammatory conditions of the respiratory tract and gastrointestinal mucosa.
The TxGNN model predicts it may be effective for **Atopic Eczema**,
with **2 related clinical trials** and **20 publications** currently identified to support this research direction.
However, clinical evidence remains at preclinical level, and a critical safety concern — elevated contact sensitisation rates to budesonide in patients with atopic skin barrier disruption — must be addressed before clinical advancement.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Asthma and inflammatory mucosal conditions (inhaled/topical corticosteroid) |
| Predicted New Indication | Atopic Eczema |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed |
| Number of Authorisations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not formally available in this evidence pack. Based on known pharmacology, budesonide is a potent synthetic glucocorticoid receptor (GR) agonist. It suppresses inflammation primarily by inhibiting NF-κB signalling, downregulating pro-inflammatory cytokines (IL-4, IL-5, IL-13), and reducing eosinophil recruitment to affected tissue. These effects are well-established across multiple mucosal sites — from the airways in asthma to the intestinal wall in Crohn's disease.

Atopic eczema is driven by Th2-skewed inflammation characterised by elevated IL-4 and IL-13 signalling, which disrupts skin barrier proteins (filaggrin, claudins) and sustains chronic pruritic lesions. This inflammatory pathway directly overlaps with budesonide's known anti-inflammatory targets, providing a strong biological rationale for the TxGNN model's prediction. The mechanistic link is reinforced by a 2025 mechanistic review (PMID 40284499) confirming budesonide's broad mucosal anti-inflammatory activity, explicitly covering dermatological and gastrointestinal applications beyond the respiratory tract.

The key challenge lies in drug delivery. Conventional topical corticosteroids penetrate atopic skin poorly and carry systemic absorption risks, particularly in children. A 2024 preclinical study (PMID 38275852) specifically addressed this by formulating budesonide-loaded Eudragit L100 pH-sensitive nanoparticles into hydrogels, exploiting the characteristic pH shift in atopic lesions to achieve targeted local drug release. This novel delivery platform directly addresses the main barrier to topical budesonide use in AD and represents a promising investigational path. However, an important safety paradox exists: patients with atopic dermatitis have significantly elevated rates of contact hypersensitivity to budesonide itself — a signal that must be characterised before topical application can be safely pursued.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01028560](https://clinicaltrials.gov/study/NCT01028560) | Phase 1/2 | Completed | 58 | Allergy immunotherapy in atopic wheezing children (18 months–3 years) at high risk for asthma; participants carry atopic phenotypes including eczema and food allergy, providing an indirect Th2-driven atopic cohort relevant to understanding immunological response in this population |
| [NCT04680117](https://clinicaltrials.gov/study/NCT04680117) | N/A | Unknown | 150 | Characterisation of severe paediatric asthma endotypes through immune, metabolomic and microbiota profiling; includes atopic phenotyping but primary focus is airway disease, not eczema treatment — limited direct relevance |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|---------|
| [38275852](https://pubmed.ncbi.nlm.nih.gov/38275852/) | 2024 | Formulation/Preclinical | Gels | Budesonide-loaded Eudragit L100 pH-sensitive nanoparticles in hydrogel form for paediatric AD local therapy; exploits atopic lesion pH shift to achieve targeted release — proof-of-concept for overcoming topical delivery barrier |
| [9496795](https://pubmed.ncbi.nlm.nih.gov/9496795/) | 1998 | Clinical Study | Pediatric Dermatology | Knemometry study in 14 AD children (aged 5–12) treated with topical budesonide across 6-week open trial; assessed short-term lower-leg growth impact — key paediatric safety benchmark |
| [8864369](https://pubmed.ncbi.nlm.nih.gov/8864369/) | 1996 | Clinical Study | Dermatology | IGF axis, bone and collagen turnover in AD children receiving topical glucocorticosteroids; demonstrates systemic absorption via percutaneous route with growth-suppressive implications — critical for paediatric safety profiling |
| [21062310](https://pubmed.ncbi.nlm.nih.gov/21062310/) | 2010 | Animal RCT | J Vet Pharmacol Ther | Randomised, blinded, placebo-controlled crossover study of 0.025% budesonide leave-on conditioner (Barazone) in 29 dogs with AD; significantly reduced skin lesions and pruritus — closest available controlled evidence of topical budesonide efficacy in AD |
| [19875223](https://pubmed.ncbi.nlm.nih.gov/19875223/) | 2010 | Clinical Cohort | Allergologia et Immunopathologia | Differential budesonide response in atopic versus non-atopic infants with recurrent wheezing; atopic phenotype showed distinct corticosteroid responsiveness — indirect support for budesonide activity in Th2-driven disease |
| [33931866](https://pubmed.ncbi.nlm.nih.gov/33931866/) | 2021 | Epidemiological | Contact Dermatitis | Italian SIDAPA patch test series (2018–2019); budesonide is the European standard marker for corticosteroid hypersensitivity — a declining allergy trend observed over 20 years, with implications for topical use safety in AD |
| [35133669](https://pubmed.ncbi.nlm.nih.gov/35133669/) | 2022 | Cross-sectional | Contact Dermatitis | Asian dermatology centre data on contact sensitisation rates in AD vs. non-AD patients; challenges the traditional assumption that AD reduces sensitisation risk — directly relevant to budesonide topical safety in this population |
| [30053491](https://pubmed.ncbi.nlm.nih.gov/30053491/) | 2018 | Cross-sectional | J Am Acad Dermatol | Contact dermatitis to topical medications including corticosteroids in adults with AD; skin barrier disruption in AD significantly increases sensitisation risk — a key safety signal for the proposed indication |
| [24603519](https://pubmed.ncbi.nlm.nih.gov/24603519/) | 2014 | Cross-sectional | Dermatitis | Contact hypersensitivity to the corticosteroid patch-test series in adolescents and adults with AD; budesonide allergy rates documented in this specific population — quantifies the treatment paradox risk |
| [19571596](https://pubmed.ncbi.nlm.nih.gov/19571596/) | 2009 | Review | Neuroimmunomodulation | Intranasal corticosteroids and HPA axis suppression; AD frequently co-exists with rhinitis and asthma, so cumulative corticosteroid exposure from multiple routes is a real clinical concern requiring monitoring |

---

## New Zealand Market Information

Budesonide is currently not registered or marketed in New Zealand. No product authorisations are on file. Clinical use data, approved indications, and local prescribing information are not available from the New Zealand regulatory database.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** Two data gaps were identified that limit safety assessment. TFDA (Taiwan) package insert warnings and contraindications (DG001, Blocking severity) could not be retrieved, preventing formal safety screening. DrugBank MOA and interaction data (DG002, High severity) are also missing. These gaps must be resolved before safety evaluation can proceed.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN model's 99.96% prediction score for budesonide in atopic eczema is mechanistically credible — the Th2/IL-4/IL-13 inflammatory axis targeted by budesonide is central to AD pathology. However, clinical evidence is currently at L4 (preclinical/mechanism level only), with no completed human trials directly evaluating budesonide as an AD treatment. Critically, patients with atopic dermatitis have documented elevated contact sensitisation rates to budesonide due to impaired skin barrier function, creating a safety paradox that must be characterised before any topical clinical programme is initiated.

**To proceed, the following is needed:**
- Retrieve full package insert safety data (warnings, contraindications) — DG001 is currently blocking formal safety evaluation
- Obtain MOA data from DrugBank — DG002 limits mechanistic analysis depth
- Conduct a systematic review of budesonide contact sensitisation rates specifically in AD patients, to quantify the treatment paradox risk
- Initiate a Phase 1 human skin safety and pharmacokinetics study using the novel nanoparticle hydrogel formulation described in PMID 38275852, to establish whether targeted delivery can reduce sensitisation risk
- Commission a regulatory pathway review with Medsafe (New Zealand), given that budesonide currently has no New Zealand product authorisations and a new indication would require a full dossier submission
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


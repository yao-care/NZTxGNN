---
layout: default
title: Albendazole
parent: High Evidence (L1-L2)
nav_order: 24
evidence_level: L2
indication_count: 3
---

# Albendazole
{: .fs-9 }

Evidence Level: **L2** | Predicted Indications: **3** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Albendazole: From Cystic Echinococcosis to Alveolar Echinococcosis

## One-Sentence Summary

Albendazole is a benzimidazole anthelmintic, originally used to treat cystic echinococcosis and cysticercosis.
The TxGNN model predicts it may be effective for **alveolar echinococcosis**,
with **3 clinical trials** (1 completed Phase 2 trial directly on point) and **20 publications** (mostly reviews) currently supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Cystic echinococcosis, cysticercosis (from the knowledge graph; no approved-indication text in the New Zealand licence record) |
| Predicted New Indication | Alveolar echinococcosis |
| TxGNN Prediction Score | 99.97% |
| Evidence Level | L2 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on known pharmacology, albendazole is a benzimidazole anthelmintic that binds parasite beta-tubulin, inhibiting microtubule polymerization and glucose uptake.

Alveolar echinococcosis is caused by *Echinococcus multilocularis*, a close relative of *E. granulosus*, which causes cystic echinococcosis (an established albendazole indication). Because both parasites share the same genus and target, the mechanism is expected to carry over.

Albendazole is already the guideline-recommended parasitostatic therapy for alveolar echinococcosis (WHO-IWGE expert consensus). This is therefore better described as an established extended use than as novel repurposing. Albendazole is generally parasitostatic rather than parasiticidal, so long-term or lifelong therapy is often needed when complete resection is not possible.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT07182305](https://clinicaltrials.gov/study/NCT07182305) | Phase 2 | Completed | 194 | Albendazole in early-stage alveolar echinococcosis found by ultrasound screening in Kyrgyzstan; directly relevant. Randomization and control arm are not stated and should be checked |
| [NCT02876146](https://clinicaltrials.gov/study/NCT02876146) | NA | Completed | 50 | Follow-up study of parasite viability and markers in albendazole-treated hepatic alveolar echinococcosis; supports treatment monitoring and withdrawal decisions, not efficacy |
| [NCT06483880](https://clinicaltrials.gov/study/NCT06483880) | NA | Unknown | 24 | Randomized trial of adjuvant albendazole vs placebo after pulmonary hydatid cyst resection; this is cystic disease, so only indirectly relevant |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [19931502](https://pubmed.ncbi.nlm.nih.gov/19931502/) | 2010 | Review (expert consensus) | Acta Trop | WHO-IWGE consensus on diagnosis, treatment and follow-up of cystic and alveolar echinococcosis |
| [30760475](https://pubmed.ncbi.nlm.nih.gov/30760475/) | 2019 | Review | Clin Microbiol Rev | Advances in echinococcosis diagnosis, genetics and treatment |
| [34161992](https://pubmed.ncbi.nlm.nih.gov/34161992/) | 2021 | Review | Semin Liver Dis | Overview of hepatic alveolar echinococcosis |
| [39254012](https://pubmed.ncbi.nlm.nih.gov/39254012/) | 2024 | Review | Tidsskr Nor Laegeforen | Surgery combined with prolonged antiparasitic therapy for alveolar echinococcosis |
| [40093668](https://pubmed.ncbi.nlm.nih.gov/40093668/) | 2025 | Review | World J Gastroenterol | Current management of liver echinococcosis; surgery is the cornerstone |
| [25526545](https://pubmed.ncbi.nlm.nih.gov/25526545/) | 2014 | Review | Parasite | Albendazole and mebendazole and prospects for new options |
| [34808118](https://pubmed.ncbi.nlm.nih.gov/34808118/) | 2022 | Review | Acta Trop | No non-surgical option yet replaces albendazole or mebendazole |
| [10980173](https://pubmed.ncbi.nlm.nih.gov/10980173/) | 2000 | Observational study | J Antimicrob Chemother | 35 patients on mebendazole or albendazole, followed a mean of 39 months |
| [38501660](https://pubmed.ncbi.nlm.nih.gov/38501660/) | 2024 | Preclinical | Antimicrob Agents Chemother | Solubilizing formulations of albendazole in a rat model of hepatic alveolar echinococcosis |
| [39508157](https://pubmed.ncbi.nlm.nih.gov/39508157/) | 2024 | Review | Parasitology | Albendazole is parasitostatic and can cause severe adverse effects; pyronaridine proposed as an alternative |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2429977 (cancelled) | Eskazole | Tab 400 mg | Not recorded |

## Safety Considerations

- **Contraindications / risk conditions (from the knowledge graph, not label text)**: liver disease, gallbladder disease, neutropenia, agranulocytosis, anaemia (including macrocytic anaemia), pancytopenia, aplastic anaemia.
- **Drug Interactions**: 96 interactions were identified. Moderate-level examples include aprepitant, cobicistat, deferasirox, fostamatinib, alemtuzumab, amprenavir, anagrelide, apalutamide, bendamustine, bexarotene, binimetinib, boceprevir and bosentan. Minor-level examples include cimetidine, dexamethasone, lidocaine and armodafinil.

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
A completed Phase 2 trial (n=194) directly addresses albendazole in alveolar echinococcosis, and albendazole is already guideline-recommended for this disease. However, the evidence is largely reviews, albendazole is only parasitostatic, and the only New Zealand licence is cancelled.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Confirmation of the design and results of NCT07182305 (randomization, control arm, outcomes)
- Detailed mechanism of action data from DrugBank
- A route to supply in New Zealand, since no valid licence exists
- A monitoring plan for long-term therapy: blood counts and liver function, given the haematological and hepatic risks listed above
- Review of the interaction list against patients' concurrent medicines

**Other predictions (not recommended at this time):**
- *Coenurosis* (L4, Research Question): the supporting trials and literature concern neurocysticercosis, not coenurosis; the high score likely reflects proximity to cysticercosis in the knowledge graph.
- *Cestode infectious disease* (L4, Research Question): a broad parent category that already contains the approved indications, so it is not a distinct new indication.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Zidovudine
parent: 僅模型預測 (L5)
nav_order: 364
evidence_level: L5
indication_count: 6
---

# Zidovudine
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

Using the drug-repurposing evaluation report template supplied in the system prompt, here is the report generated from the Evidence Pack.

Before the report, one editorial note on how I handled a structural mismatch in this specific pack: `predicted_indications` contains **6** ranked diseases, and array position `[0]` ("feline acquired immunodeficiency syndrome") is a veterinary/animal-model finding, not a human indication — its own `repurposing_rationale` explicitly says so. Rather than silently forcing a human-drug report around a cat disease, I followed the template's literal extraction rules for the required fields (so the Quick Overview reflects `predicted_indications[0]` as instructed), but added a supplementary comparison table so the reviewer can see all 6 predictions and their wildly different evidence quality (2 non-human models, 2 zero-evidence/likely false positives, 2 with real L1 human trial evidence that actually just restate zidovudine's original approved use). This is flagged transparently in the report rather than guessed away.

---

# Zidovudine: From HIV/AIDS Treatment to Feline Acquired Immunodeficiency Syndrome (Veterinary Model)

## One-Sentence Summary

> Zidovudine (AZT) is the original nucleoside reverse transcriptase inhibitor (NRTI), established since 1987 as an antiretroviral for HIV infection/AIDS.
> The top-ranked TxGNN prediction in this evidence pack is **Feline Acquired Immunodeficiency Syndrome** — a veterinary, cross-species model finding (score **99.96%**) — supported only by **20 animal-study publications** and **0 human clinical trials**.
> This pack additionally contains 5 other predictions, including two (AIDS-related complex, congenital HIV) with strong human trial evidence, but these largely restate zidovudine's already-established indication rather than a genuinely new repurposing target — see "Other Predicted Indications" below.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HIV infection / AIDS (antiretroviral therapy) — inferred from repurposing-rationale text in this pack; not separately listed in the regulatory record (New Zealand: unmarketed, 0 licenses on file) |
| Predicted New Indication | Feline Acquired Immunodeficiency Syndrome (veterinary/animal-model indication) |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, a structured mechanism-of-action (MOA) record is not available for this drug in the evidence pack (flagged as a High-severity data gap, DG002). Based on the mechanistic descriptions embedded in this pack's own repurposing rationale, zidovudine is a thymidine nucleoside analogue: it is phosphorylated intracellularly to its active triphosphate form, which competitively inhibits HIV-1 reverse transcriptase and causes premature viral DNA chain termination, blocking retroviral replication.

Feline immunodeficiency virus (FIV) and feline leukemia virus (FeLV) are both retroviruses (lentivirus/oncovirus) whose reverse transcriptase is structurally homologous to HIV-1's. The theoretical rationale is that a nucleoside analogue built to block HIV-1 RT should, in principle, cross-inhibit these related retroviral enzymes in cats.

However, this is explicitly a **cross-species animal model/veterinary indication**, not a human clinical indication, and the evidence pack itself flags that it does not fit the target population of a human drug-repurposing pipeline. It should be labeled clearly as a veterinary research question rather than a candidate for human regulatory advancement.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [2178336](https://pubmed.ncbi.nlm.nih.gov/2178336/) | 1990 | Prospective Animal Trial | Antimicrobial Agents and Chemotherapy | AZT + interferon-alpha 2b in presymptomatic FeLV-induced immunodeficiency (FAIDS); combination showed antiviral benefit over AZT alone |
| [2164083](https://pubmed.ncbi.nlm.nih.gov/2164083/) | 1990 | Prospective Animal Trial | J Acquired Immune Deficiency Syndromes | AZT + IFN-α + IL-2 prophylaxis for FeLV-FAIDS; AZT inhibited FeLV replication in vitro, ~25–30% added effect with IFN-α |
| [8381867](https://pubmed.ncbi.nlm.nih.gov/8381867/) | 1993 | Prospective Animal Trial | J Acquired Immune Deficiency Syndromes | Prophylactic AZT (30 mg/kg/day) prevented early viremia and lymphocyte decline in FIV-inoculated cats, but did not prevent primary infection |
| [2475068](https://pubmed.ncbi.nlm.nih.gov/2475068/) | 1989 | Review/Model Description | Antimicrobial Agents and Chemotherapy | Establishes FIV as a reverse-transcriptase-homologous model justifying AZT-based chemotherapy research for AIDS |
| [18550661](https://pubmed.ncbi.nlm.nih.gov/18550661/) | 2008 | Genetic/Phylogenetic Analysis | Journal of Virology | Phylogenetic analysis of FIV gag/pol/env genes in AZT-treated vs. treatment-naïve cats in Brazil |
| [7688949](https://pubmed.ncbi.nlm.nih.gov/7688949/) | 1993 | Cohort (feline) | Archives of Virology | AZT and cyclosporine both lowered plasma FIV titers at 2 weeks post-infection; effect not sustained |
| [7618256](https://pubmed.ncbi.nlm.nih.gov/7618256/) | 1995 | Animal Study (SCID mouse model) | Veterinary Immunology and Immunopathology | AZT reduced proviral burden and enhanced humoral immune response in SCID-feline mouse FIV model |
| [9226004](https://pubmed.ncbi.nlm.nih.gov/9226004/) | 1997 | Preclinical/Drug Delivery Study | Journal of Leukocyte Biology | Erythrocyte-based targeted delivery system for phosphorylated nucleoside analogues (AZT-class) to macrophages, an HIV reservoir cell |
| [15047505](https://pubmed.ncbi.nlm.nih.gov/15047505/) | 2004 | Animal Study | Antimicrobial Agents and Chemotherapy | Topical AZT-derivative spermicide (WHI-07) prevented vaginal/rectal FIV transmission in cats |
| [8399067](https://pubmed.ncbi.nlm.nih.gov/8399067/) | 1993 | Animal Study | J Immunotherapy | AZT combined with adoptive lymphocyte transfer and IFN-α reversed FeLV infection in cats |

---

## Other Predicted Indications in This Evidence Pack

This pack scored 6 diseases for zidovudine. Because the evidence quality and clinical relevance vary enormously between them, they are summarized here for full transparency rather than omitted:

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Decision Stage | Recommendation | Note |
|------|----------------------|-------------|-----------------|-----------------|------------------|------|
| 1 | Feline Acquired Immunodeficiency Syndrome | 99.96% | L3 | S1 | Research Question | Veterinary/animal model only (see main body above) |
| 2 | Simian Immunodeficiency Virus Infection | 99.96% | L3 | S1 | Research Question | Non-human primate model; validates HIV-RT mechanism, not a human indication |
| 3 | Neurodevelopmental disorder with ataxic gait, absent speech, decreased cortical white matter | 99.96% | L5 | S0 | Hold | 0 trials, 0 literature; purely theoretical LINE-1/interferon-pathway hypothesis |
| 4 | Obsolete familial combined hyperlipidemia | 99.62% | L5 | S0 | Hold | 0 trials, 0 literature; mechanistically contradictory — NRTIs are known to *cause* dyslipidemia, not treat it; disease term is itself flagged "obsolete" in the ontology — likely a false-positive/data-noise prediction |
| 5 | AIDS Related Complex | 99.19% | L1 | S3 | Proceed with Guardrails | 50 clinical trials, 20 publications, including the pivotal 1987 Fischl et al. placebo-controlled RCT (NEJM, PMID 3299089) that established AZT as the first approved anti-HIV drug — this **is** zidovudine's original indication, not a new one |
| 6 | Congenital Human Immunodeficiency Virus | 99.19% | L1 | S3 | Proceed with Guardrails | 33 clinical trials, 20 publications, anchored by PACTG 076-era studies (e.g. NCT00386230) establishing AZT for prevention of mother-to-child HIV transmission — an established, guideline-endorsed use since 1994, not a novel repurposing target |

**Interpretation:** none of the 6 predictions in this pack represent a genuinely new, actionable human repurposing indication for zidovudine. Ranks 1–2 are non-human models; ranks 3–4 have no supporting evidence at all and rank 4 is likely a database/ontology artifact; ranks 5–6 have strong evidence but simply rediscover the drug's own original approved indication.

---

## New Zealand Market Information

Zidovudine currently has no marketing authorization on file in New Zealand (0 licenses; market status: Not Marketed). No product, dosage form, or approved-indication records are available to summarize.

---

## Safety Considerations

Structured safety data (key warnings, contraindications, drug–drug interactions) could not be retrieved for this evidence pack. This is recorded as a **Blocking**-severity data gap (DG001 — TFDA/Medsafe package insert warnings/contraindications not yet extracted), which by definition prevents this candidate from completing the S1 safety pre-assessment stage.

Please refer to the package insert for safety information once retrieved.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked predicted indication in this pack (feline acquired immunodeficiency syndrome) is a veterinary cross-species model finding with no human clinical trial support (L3, Research Question stage) — it does not represent an actionable human repurposing candidate. The other predictions in this pack are similarly non-actionable: two are non-human primate models, two have zero supporting evidence (one likely a false positive), and the two with genuine L1 human trial evidence (AIDS-related complex, congenital HIV) merely restate zidovudine's own original, already-approved antiretroviral indication rather than constituting a new use.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data (warnings, contraindications) — currently a Blocking gap (DG001)
- Structured mechanism-of-action documentation (DG002)
- A pipeline-level review to exclude non-human (veterinary/primate model) predictions from the human drug-repurposing evaluation queue, or route them to a separate veterinary-use track
- Re-scoring/filtering to prevent "re-discovery" of a drug's own original indication (ranks 5–6) from being reported as a novel repurposing signal
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


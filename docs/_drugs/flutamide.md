---
layout: default
title: Flutamide
parent: Model Prediction Only (L5)
nav_order: 210
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Flutamide: predicted new indication Prostate cancer/brain cancer susceptibility (evidence level L5). Approved use: Prostate cancer"
monitor_inn: "FLUTAMIDE"
predicted_indications: ["fibroma of prostate", "Brenner tumor", "benign reproductive system neoplasm", "prostate leiomyoma", "benign prostate phyllodes tumor", "HIV infectious disease", "feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection"]
---

# Flutamide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Flutamide: From Prostate Cancer to Prostate Cancer/Brain Cancer Susceptibility
{: .text-alpha }

## One-Sentence Summary

Flutamide is a nonsteroidal androgen receptor antagonist, originally used to treat prostate cancer.
The TxGNN model's top-ranked prediction is **prostate cancer/brain cancer susceptibility**, a genetic susceptibility category rather than a treatable disease, and it has **no clinical trials and no publications** supporting it.
The more informative signal comes from the broader prediction "male reproductive organ cancer", which has **40 registered trials** and **20 publications**, but this largely overlaps with flutamide's original indication.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Prostate cancer (from the knowledge graph; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Prostate cancer/brain cancer susceptibility |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L5 (for the top-ranked prediction) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, flutamide is a nonsteroidal androgen receptor antagonist. It blocks the effect of androgens on target tissues and has long been used in prostate cancer, often together with LHRH agonists.

The top-ranked prediction, "prostate cancer/brain cancer susceptibility", describes a genetic susceptibility category, not a disease that can be treated. Its very high score most likely reflects the strong link between flutamide and prostate cancer in the knowledge graph. No trial or publication supports it as a distinct repurposing indication.

Other predictions in the top 10 fall into three groups:
- **Prostate-related tissue disorders** (fibroma of prostate, prostate leiomyoma, benign prostate phyllodes tumor): androgen blockade could plausibly act on hormone-dependent prostatic tissue, but there is no clinical or literature evidence.
- **Unrelated diseases** (Brenner tumor, HIV infectious disease, feline acquired immunodeficiency syndrome, simian immunodeficiency virus infection): there is no plausible mechanistic link, and these appear to be knowledge-graph artefacts.
- **Broader categories** ("benign reproductive system neoplasm" and "male reproductive organ cancer"): these are the only predictions with any evidence. They are summarised below because they are the most informative for decision-making.

---

## Clinical Trial Evidence

There are no clinical trials registered for the top-ranked prediction (prostate cancer/brain cancer susceptibility).

The broader category "male reproductive organ cancer" (rank 6) has 40 registered trials. The table lists the most relevant ones. Many of these are androgen-deprivation trials in prostate cancer in which flutamide's specific role is not confirmed in the data compiled for this report.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00006214](https://clinicaltrials.gov/study/NCT00006214) | Phase 2 | Completed | 63 | Flutamide as chemoprevention in men with prostatic intraepithelial neoplasia (directly tests the drug) |
| [NCT06601205](https://clinicaltrials.gov/study/NCT06601205) | Phase 2/3 | Completed | 125 | Pre-surgical window-of-opportunity trial: finasteride vs low-dose flutamide vs placebo in prostate cancer |
| [NCT02918968](https://clinicaltrials.gov/study/NCT02918968) | Phase 4 | Completed | 206 | Enzalutamide vs flutamide (with ADT) in castration-resistant prostate cancer after failure of bicalutamide |
| [NCT00450463](https://clinicaltrials.gov/study/NCT00450463) | Phase 2 | Completed | 64 | PROSTVAC vaccine plus flutamide vs flutamide alone in non-metastatic androgen-insensitive prostate cancer |
| [NCT00002881](https://clinicaltrials.gov/study/NCT00002881) | Phase 3 | Completed | N/A | Orchiectomy/LHRH analogue + flutamide with or without suramin and hydrocortisone in metastatic prostate cancer |
| [NCT00003323](https://clinicaltrials.gov/study/NCT00003323) | Phase 2 | Completed | 101 | Flutamide plus finasteride (potency-sparing hormonal therapy) for rising PSA after radiation or prostatectomy |
| [NCT00003645](https://clinicaltrials.gov/study/NCT00003645) | Phase 3 | Terminated | 64 | Adjuvant leuprolide plus flutamide vs no further therapy in high-risk surgically treated prostate cancer |
| [NCT00104741](https://clinicaltrials.gov/study/NCT00104741) | Phase 3 | Completed | 378 | Triptorelin and flutamide with radiotherapy in intermediate-prognosis localized prostate cancer |
| [NCT00002597](https://clinicaltrials.gov/study/NCT00002597) | Phase 3 | Completed | 2028 | Radiation therapy with or without antiandrogen therapy (flutamide, goserelin, leuprolide) in locally confined prostate cancer |
| [NCT03507608](https://clinicaltrials.gov/study/NCT03507608) | Early Phase 1 | Completed | 25 | Single pulsed-dose flutamide to create DNA double-strand breaks in prostate cancer patients on androgen suppression and brachytherapy |

---

## Literature Evidence

There is no literature for the top-ranked prediction. The table below draws on the broader categories "male reproductive organ cancer" and "benign reproductive system neoplasm".

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8252497](https://pubmed.ncbi.nlm.nih.gov/8252497/) | 1993 | Review | Cancer | Mechanism of action and pure antiandrogenic properties of flutamide; maximal prostate inhibition when combined with an LHRH agonist |
| [3071951](https://pubmed.ncbi.nlm.nih.gov/3071951/) | 1988 | Review | Am J Clin Oncol | Rationale for combining a nonsteroidal antiandrogen with LHRH agonists in prostate cancer |
| [1722793](https://pubmed.ncbi.nlm.nih.gov/1722793/) | 1991 | Clinical study | J Androl | Prostate volume, PSA and testosterone response to flutamide in men with benign prostatic hyperplasia (double-blind randomized trial setting) |
| [1989458](https://pubmed.ncbi.nlm.nih.gov/1989458/) | 1991 | Clinical/histopathology | Am J Surg Pathol | LHRH agonist plus flutamide caused marked atrophy of benign prostate glands in 23 prostatectomy specimens |
| [24950779](https://pubmed.ncbi.nlm.nih.gov/24950779/) | 2014 | Preclinical and Phase 2 | Cancer Prev Res | Flutamide and tissue biomarkers in women at high risk for ovarian cancer |
| [8146068](https://pubmed.ncbi.nlm.nih.gov/8146068/) | 1994 | Preclinical | The Prostate | Early flutamide exposure delayed tumour appearance and slowed growth in a rat prostatic tumour model |
| [16540588](https://pubmed.ncbi.nlm.nih.gov/16540588/) | 2006 | In vitro | Drug Metab Dispos | Flutamide metabolism across species; toxic metabolites proposed as a cause of hepatic dysfunction |
| [10209490](https://pubmed.ncbi.nlm.nih.gov/10209490/) | 1998 | Histopathology | J Pathol | Long-term combined androgen blockade (LHRH agonist plus flutamide) leads to marked regression of the prostate |
| [7526970](https://pubmed.ncbi.nlm.nih.gov/7526970/) | 1994 | Histopathology | Cancer | Morphologic changes after preoperative estramustine or flutamide in localized prostate cancer |
| [20541672](https://pubmed.ncbi.nlm.nih.gov/20541672/) | 2010 | Review | Prog Brain Res | Hormonal therapy of prostate cancer and the case for optimal androgen blockade |

---

## New Zealand Market Information

Both licences are cancelled; none is currently valid. Approved indication text is not available for either product.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2612232 | Prostacur | Tab 250 mg | Not available (cancelled) |
| 259985 | Flutamin | Tab 250 mg | Not available (cancelled) |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted/hormonal therapy (nonsteroidal antiandrogen), not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Liver function tests (hepatotoxicity is a known concern with flutamide); please also refer to the package insert |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Contraindications (knowledge graph, not label text)**: The knowledge graph links flutamide to contraindications in liver failure, liver disease, gallbladder disease, methemoglobinemia, sulfhemoglobinemia, hyperbilirubinemia (including hereditary forms and Gilbert syndrome), G6PD deficiency, glycogen storage disease, cystinuria, galactosemia, pyruvate dehydrogenase E3 deficiency, and methemoglobinemia due to methemoglobin reductase deficiency.
- **Hepatotoxicity**: The literature notes that some flutamide-treated patients developed severe hepatic dysfunction, with toxic metabolites proposed as the cause (PMID 16540588). Liver monitoring would be essential in any use.
- **Drug Interactions**: 100 interactions were identified. The two **Major** interactions in the listed results are **dolasetron** and **cisapride**. Most others are Moderate, including famotidine, loperamide, clarithromycin, levofloxacin, ondansetron, palonosetron, naltrexone, obeticholic acid, and several laxatives/bowel-preparation agents (bisacodyl, picosulfuric acid, polyethylene glycol, sodium sulfate, lactitol, lactulose, castor oil, glycerin). Metronidazole is Minor.

Please also refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction, prostate cancer/brain cancer susceptibility, is a genetic susceptibility category, not a treatable disease, and has no trial or literature support (L5). The apparent evidence for broader categories mostly reflects flutamide's original prostate cancer use, not a new indication. In addition, flutamide carries hepatotoxicity concerns, has no valid New Zealand licence, and has been largely superseded by newer antiandrogens.

**To proceed, the following is needed:**
- A better-defined candidate indication. The most plausible are benign prostatic conditions (supported by small, older human BPH studies, PMIDs 1722793 and 1989458) and prostatic intraepithelial neoplasia chemoprevention (NCT00006214).
- Confirmation of flutamide's specific role in each prostate cancer trial before relying on the grading, since many trials matched through the broad condition category.
- Medsafe package insert warnings and contraindications (currently a blocking data gap).
- Mechanism of action data from DrugBank.
- A hepatotoxicity monitoring and risk-benefit plan, and an assessment of whether any regulatory pathway exists given that both licences are cancelled.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


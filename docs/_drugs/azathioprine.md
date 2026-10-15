---
layout: default
title: Azathioprine
parent: Model Prediction Only (L5)
nav_order: 54
evidence_level: L5
indication_count: 10
---

# Azathioprine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Azathioprine: From Rheumatoid Arthritis to Inflammatory Bowel Disease

## One-Sentence Summary

Azathioprine is an immunosuppressant originally used for rheumatoid arthritis.
The TxGNN model's top-ranked prediction is a rare developmental syndrome with no supporting evidence, so this report focuses on the best-supported prediction, **inflammatory bowel disease (IBD)**, which is backed by **25 clinical trials** and **20 publications**. This is an already established clinical use rather than a novel finding.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Rheumatoid arthritis (from the knowledge graph; no approved-indication text is available in the New Zealand licence data) |
| Predicted New Indication | Top-ranked by TxGNN: colobomatous microphthalmia-rhizomelic dysplasia syndrome (99.99%, Evidence L5, Hold). Best-supported: inflammatory bowel disease (99.52%, Evidence L1) |
| TxGNN Prediction Score | 99.99% (top-ranked); 99.52% (IBD) |
| Evidence Level | L1 (IBD and ulcerative colitis); L5 for the top-ranked prediction |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Top-ranked prediction: Hold. IBD / ulcerative colitis: Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

**Top-ranked prediction.** The highest-scoring prediction (colobomatous microphthalmia-rhizomelic dysplasia syndrome) is a rare developmental skeletal and ocular malformation. There is no immune-mediated pathology that azathioprine could address, and no trials or literature support it. The very high score is a knowledge-graph artefact and should not be acted on. The same applies to brachydactyly-syndactyly syndrome and acromesomelic dysplasia, Hunter-Thompson type. WHIM syndrome is mechanistically concerning, because azathioprine adds immunosuppression and myelosuppression to an already neutropenic, infection-prone condition.

**IBD, the best-supported prediction.** Detailed mechanism of action data is not available in the data compiled for this report. From the known pharmacology of thiopurines, azathioprine is converted to 6-mercaptopurine and then to thioguanine nucleotides. These inhibit purine synthesis and induce T-cell apoptosis, which dampens the aberrant mucosal immune response seen in IBD.

Rheumatoid arthritis and IBD are both chronic immune-mediated inflammatory diseases, so an immunomodulator effective in one is plausibly useful in the other. Azathioprine is a guideline-recognised immunomodulator for maintaining remission in Crohn's disease and ulcerative colitis, used alone or with anti-TNF agents. It is therefore not a novel repurposing finding.

---

## Clinical Trial Evidence

The table lists the most relevant trials for IBD and ulcerative colitis. Many more were retrieved; these are the ones with the most direct azathioprine involvement.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00537316](https://clinicaltrials.gov/study/NCT00537316) | Phase 3 | Terminated | 242 | Infliximab alone or with azathioprine vs azathioprine alone in moderate to severe ulcerative colitis |
| [NCT02177071](https://clinicaltrials.gov/study/NCT02177071) | Phase 4 | Completed | 211 | Infliximab-antimetabolite combination vs monotherapy in Crohn's patients in steroid-free remission |
| [NCT02247258](https://clinicaltrials.gov/study/NCT02247258) | Phase 2 | Terminated | 63 | Immediate vs endoscopy-directed azathioprine to prevent postoperative ileal Crohn's recurrence |
| [NCT00976690](https://clinicaltrials.gov/study/NCT00976690) | Phase 3 | Completed | 83 | Azathioprine vs mesalazine to prevent postoperative Crohn's recurrence |
| [NCT02425852](https://clinicaltrials.gov/study/NCT02425852) | Phase 4 | Completed | 65 | Early infliximab plus azathioprine vs steroids plus azathioprine in acute severe colitis |
| [NCT03101800](https://clinicaltrials.gov/study/NCT03101800) | Phase 3 | Unknown | 84 | Low-dose azathioprine plus allopurinol vs azathioprine alone in ulcerative colitis |
| [NCT00098111](https://clinicaltrials.gov/study/NCT00098111) | Phase 3 | Terminated | 31 | Dose-ranging study of azathioprine (Imuran) in active Crohn's disease |
| [NCT01564823](https://clinicaltrials.gov/study/NCT01564823) | Phase 3 | Completed | 86 | Adalimumab vs azathioprine to prevent postoperative Crohn's recurrence |
| [NCT07235904](https://clinicaltrials.gov/study/NCT07235904) | Phase 4 | Recruiting | 300 | Mirikizumab top-down therapy vs azathioprine standard of care in new ulcerative colitis |
| [NCT00521950](https://clinicaltrials.gov/study/NCT00521950) | N/A | Completed | 853 | TPMT genotype screening before thiopurine use in IBD (supports safety guardrails) |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39586616](https://pubmed.ncbi.nlm.nih.gov/39586616/) | 2025 | RCT | Gut | Top-down infliximab plus azathioprine vs azathioprine alone in acute severe ulcerative colitis (ACTIVE trial) |
| [40013523](https://pubmed.ncbi.nlm.nih.gov/40013523/) | 2025 | Cochrane systematic review | Cochrane Database Syst Rev | Azathioprine and 6-mercaptopurine for maintenance of remission in ulcerative colitis (update) |
| [27192092](https://pubmed.ncbi.nlm.nih.gov/27192092/) | 2016 | Cochrane systematic review | Cochrane Database Syst Rev | Earlier Cochrane update on thiopurines for ulcerative colitis maintenance |
| [19392869](https://pubmed.ncbi.nlm.nih.gov/19392869/) | 2009 | Meta-analysis | Aliment Pharmacol Ther | Efficacy of azathioprine and mercaptopurine in ulcerative colitis |
| [29293971](https://pubmed.ncbi.nlm.nih.gov/29293971/) | 2018 | Review | J Crohns Colitis | Clinical review of thiopurine treatment in IBD |
| [19072367](https://pubmed.ncbi.nlm.nih.gov/19072367/) | 2008 | Review | Expert Rev Gastroenterol Hepatol | Molecular insights and clinical implications of azathioprine in IBD |
| [15177535](https://pubmed.ncbi.nlm.nih.gov/15177535/) | 2004 | Review | Gastroenterol Clin North Am | Critical review of efficacy and toxicity of azathioprine and 6-MP in IBD |
| [16048561](https://pubmed.ncbi.nlm.nih.gov/16048561/) | 2005 | Review | J Gastroenterol Hepatol | Pharmacogenetics and metabolite monitoring of thiopurines |
| [37586320](https://pubmed.ncbi.nlm.nih.gov/37586320/) | 2023 | Preclinical | Cell Rep Med | Gut commensal bacteria reduce 6-MP bioavailability and promote azathioprine failure |
| [32799195](https://pubmed.ncbi.nlm.nih.gov/32799195/) | 2020 | Review | Digestion | Malignancy risk in IBD |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 390003 | Azamun | Tab 50 mg | Not available (cancelled) |
| 2465345 | Azamun | Tab 25 mg | Not available (cancelled) |

Both authorizations are cancelled, so there is currently no valid marketed azathioprine product in the New Zealand data compiled for this report. Availability should be checked against other products or routes of supply.

---

## Safety Considerations

- **Contraindication-related conditions (knowledge graph, not label text)**: pancreatitis, thrombocytopenia, anaemia (including macrocytic anaemia), impaired renal function, infectious diseases (including fungal infection and dermatophytosis), lymphoma (including primary CNS lymphoma, lymphosarcoma and hepatosplenic T-cell lymphoma), hepatic vein thrombosis and Budd-Chiari syndrome.
- **Drug Interactions**: 100 interactions were identified. The main ones are:
  - Moderate: mesalazine, balsalazide, olsalazine and sulfasalazine.
  - Minor: zinc acetate, zinc gluconate and zinc sulfate.
  - Unrated: includes allopurinol-type interactions noted in the trial data (dose reduction required), as well as aspirin, amphotericin B, metformin, omeprazole and pantoprazole.
- **Guardrails for IBD use**: TPMT/NUDT15 genotyping, metabolite monitoring, blood count and liver surveillance, and monitoring for infection and malignancy (lymphoma, non-melanoma skin cancer).

Please refer to the package insert for label warnings, as these were not available in the data compiled.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails (IBD and ulcerative colitis); Hold (all other predictions, including the top-ranked rare syndromes)**

**Rationale:**
- IBD and ulcerative colitis are supported by multiple Phase 3/4 trials, Cochrane reviews and a 2025 RCT, giving L1 evidence, although this is an established use rather than new repurposing.
- All other predictions, including the three highest-scoring ones, have no clinical or literature support and often lack a plausible mechanism.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (the data compiled lacked them, which blocks formal safety screening)
- Detailed mechanism of action data
- Confirmation of supply, given that both New Zealand authorizations are cancelled
- A TPMT/NUDT15 testing and haematological/liver monitoring plan
- Review of interaction management, particularly with allopurinol and aminosalicylates
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


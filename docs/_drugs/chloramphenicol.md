---
layout: default
title: Chloramphenicol
parent: 僅模型預測 (L5)
nav_order: 72
evidence_level: L5
indication_count: 9
---

# Chloramphenicol
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
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

# Chloramphenicol: From Broad-Spectrum Bacterial Infections to Conjunctivitis

## One-Sentence Summary

Chloramphenicol is a broad-spectrum bacteriostatic antibiotic introduced into clinical practice in 1948, historically used for serious systemic bacterial infections including typhoid fever, meningitis, and cholera, though not currently registered in New Zealand.
The TxGNN model predicts it may be effective for **Conjunctivitis**,
with **0 registered clinical trials** and **19 publications** — including multiple RCTs — currently supporting this direction.

Note: Chloramphenicol ophthalmic preparations are already widely used for conjunctivitis in the UK and several other countries; TxGNN's high-confidence prediction (L1) reflects an established but formally unregistered use in New Zealand.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not registered in New Zealand; historically used for broad-spectrum bacterial infections (typhoid, meningitis, cholera) |
| Predicted New Indication | Conjunctivitis |
| TxGNN Prediction Score | 99.66% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Chloramphenicol exerts its antibacterial effect by binding to the **50S ribosomal subunit**, inhibiting peptidyl transferase activity and thereby blocking bacterial protein synthesis. This mechanism is bacteriostatic against a broad range of organisms, including the most common causative pathogens of bacterial conjunctivitis — *Staphylococcus aureus*, *Haemophilus influenzae*, and *Streptococcus pneumoniae*.

When administered as **ophthalmic eye drops or ointment**, systemic absorption is negligible, which effectively bypasses the primary toxicity concern associated with systemic chloramphenicol use — namely, idiosyncratic aplastic anaemia and dose-dependent myelosuppression. This favourable topical safety profile has made ocular chloramphenicol preparations a standard first-line treatment for bacterial conjunctivitis in the United Kingdom for decades.

It is therefore unsurprising that the TxGNN model ranks conjunctivitis at position 1 with near-perfect confidence: the prediction is consistent with extensive published clinical trial data and international prescribing practice. The practical question for New Zealand is not whether chloramphenicol works for conjunctivitis, but whether the residual aplastic anaemia signal (estimated risk ~1 in 224,000 courses based on pharmacovigilance literature) is acceptable relative to alternative first-line agents such as fusidic acid or moxifloxacin.

---

## Clinical Trial Evidence

No clinical trials for Chloramphenicol + Conjunctivitis are currently registered on ClinicalTrials.gov or ICTRP. The evidence base is entirely literature-derived, including multiple published RCTs (see Literature Evidence below).

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [38511104](https://pubmed.ncbi.nlm.nih.gov/38511104/) | 2024 | RCT (head-to-head) | Current Therapeutic Research | Moxifloxacin vs chloramphenicol for bacterial eye infections; chloramphenicol established as comparator standard for conjunctivitis |
| [32959365](https://pubmed.ncbi.nlm.nih.gov/32959365/) | 2020 | Cochrane Systematic Review | Cochrane Database of Systematic Reviews | Interventions for preventing ophthalmia neonatorum; antibiotic/antiseptic prophylaxis including chloramphenicol assessed for neonatal conjunctivitis prevention |
| [16378567](https://pubmed.ncbi.nlm.nih.gov/16378567/) | 2005 | Cochrane SR Update | Br J General Practice | Updated meta-analysis of topical antibiotics vs placebo for acute bacterial conjunctivitis; supports topical antibiotic efficacy in primary care |
| [17947266](https://pubmed.ncbi.nlm.nih.gov/17947266/) | 2007 | RCT (equivalency) | British Journal of Ophthalmology | 2.5% povidone-iodine vs ophthalmic chloramphenicol for preventing neonatal conjunctivitis in trachoma-endemic region; direct efficacy comparison |
| [8333258](https://pubmed.ncbi.nlm.nih.gov/8333258/) | 1993 | RCT | Acta Ophthalmologica | Fusidic acid twice daily vs chloramphenicol 6× daily in acute conjunctivitis (38 GPs, Norway); no significant difference in treatment response |
| [3554881](https://pubmed.ncbi.nlm.nih.gov/3554881/) | 1987 | RCT (single-blind) | Acta Ophthalmologica | Fusidic acid 1% vs chloramphenicol 0.5% for acute purulent conjunctivitis; clinical success 84% vs 81%; more stinging reported with chloramphenicol (14% vs 5%) |
| [3300139](https://pubmed.ncbi.nlm.nih.gov/3300139/) | 1987 | RCT (open-label) | Acta Ophthalmologica | Fusidic acid vs chloramphenicol vs framycetin in bacterial conjunctivitis (Tanzania); fusidic acid superior (93% vs 48% vs 74%), attributed to lower resistance rates |
| [8800624](https://pubmed.ncbi.nlm.nih.gov/8800624/) | 1996 | Pharmacovigilance Review | Drug Safety | Ocular chloramphenicol and aplastic anaemia link remains controversial; widely used in UK for conjunctivitis, rarely prescribed in US; individual country policies vary |
| [19680306](https://pubmed.ncbi.nlm.nih.gov/19680306/) | 2009 | Clinical Review | New Zealand Medical Journal | Management recommendations for New Zealand practitioners on acute infective conjunctivitis; discusses antibiotic choice including chloramphenicol |
| [6188739](https://pubmed.ncbi.nlm.nih.gov/6188739/) | 1983 | RCT (double-blind multicentre) | J Antimicrobial Chemotherapy | Trimethoprim-polymyxin B vs chloramphenicol ophthalmic solution in presumptive bacterial conjunctivitis (n=230); all preparations effective, few adverse events |

---

## New Zealand Market Information

Chloramphenicol has **no Medsafe-approved products** currently registered in New Zealand (0 authorisations as of data cutoff 2026-06-07). It is therefore not legally available as a marketed medicine in New Zealand in any dosage form.

For reference, the drug is available in the UK (Chloramphenicol 0.5% eye drops and 1% eye ointment, available OTC) and Australia. Any introduction to New Zealand would require Medsafe registration or Section 29 supply under the Medicines Act 1981.

---

## Safety Considerations

Formal safety data (warnings, contraindications, drug interactions) was not retrievable from New Zealand regulatory sources for this drug. The following key safety considerations are drawn from published literature:

- **Aplastic Anaemia Risk (Topical Use)**: A rare but serious idiosyncratic reaction has been associated with topical ocular chloramphenicol. The estimated risk is approximately 1 in 224,000 treatment courses based on pharmacovigilance analyses. This risk, while controversial, remains the principal reason for restricted use in many jurisdictions.
- **Bone Marrow Suppression (Systemic Use)**: Dose-dependent myelosuppression is well-established with systemic administration. This is substantially mitigated by the ophthalmic route.
- **Grey Baby Syndrome**: Relevant only for systemic use in neonates; not applicable to topical ophthalmic use.

Please refer to the product package insert for complete prescribing safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Chloramphenicol's efficacy for bacterial conjunctivitis is supported by multiple published RCTs (including Cochrane reviews), and the drug has decades of clinical use in UK ophthalmic practice. The TxGNN L1 prediction reflects genuine, mature evidence rather than a speculative extrapolation. The primary barrier to New Zealand use is regulatory registration and the low-probability aplastic anaemia signal requiring informed patient consent.

**To proceed, the following is needed:**

- **Medsafe registration pathway assessment**: Determine whether a full Section 20 consent application or a Section 29 unregistered supply pathway is appropriate for ophthalmic chloramphenicol formulations
- **Comparative effectiveness analysis against registered alternatives**: Quantify benefit-risk versus currently registered NZ options (e.g., fusidic acid, tobramycin, ciprofloxacin eye drops) in the NZ primary care context
- **Aplastic anaemia risk communication plan**: Develop patient information materials and prescriber guidance addressing the idiosyncratic bone marrow risk, consistent with UK MHRA and Australian TGA precedent
- **MOA data retrieval from DrugBank**: Complete the data gap (DG002) for formal mechanistic documentation in the evidence pack
- **Pharmacovigilance monitoring plan**: Establish adverse event reporting protocol if access is granted under Section 29 or compassionate supply
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


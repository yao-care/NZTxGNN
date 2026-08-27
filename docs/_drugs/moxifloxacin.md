---
layout: default
title: Moxifloxacin
parent: 僅模型預測 (L5)
nav_order: 234
evidence_level: L5
indication_count: 10
---

# Moxifloxacin
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

# Moxifloxacin: From Bacterial Infections to Bubonic Plague

## One-Sentence Summary

> Moxifloxacin is a fourth-generation fluoroquinolone antibiotic; specific original-indication licensing data is not yet available for this market, though its class-level use for common bacterial infections is well established.
> Of the **10 TxGNN-predicted indications** screened for this drug, only **Bubonic Plague** reached a substantive evidence stage, supported by **6 publications** (animal/in vitro PK-PD efficacy studies) and **no completed clinical trials** — human RCTs are not feasible for this indication.
> The remaining 9 predictions (e.g. hyperamylasemia, monoclonal gammopathy, congenital hematological disorder) show no mechanistic or evidentiary support and are classified as low-confidence screening noise.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | No approved-indication data on file (drug not yet marketed here); pharmacologically a broad-spectrum 4th-generation fluoroquinolone antibiotic |
| Predicted New Indication | Bubonic Plague |
| TxGNN Prediction Score | 99.41% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known pharmacology, moxifloxacin belongs to the fourth-generation fluoroquinolone class, inhibiting bacterial DNA gyrase and topoisomerase IV to block DNA replication. Its efficacy against a broad range of gram-positive and gram-negative bacteria is well established in clinical use.

*Yersinia pestis*, the causative agent of bubonic plague, is a gram-negative bacillus that falls squarely within the antibacterial spectrum fluoroquinolones are known to cover. This is therefore not a novel off-target repurposing signal in the way TxGNN predictions usually work — it is a direct extension of moxifloxacin's existing antimicrobial mechanism to an additional susceptible pathogen.

Because human RCTs for plague are neither ethical nor practically feasible (the disease is rare and rapidly lethal untreated), the standard evidentiary pathway for this indication is the FDA "Animal Rule" — efficacy demonstrated through animal models and in vitro pharmacokinetic/pharmacodynamic (PK/PD) studies, which is exactly the evidence base found here (multiple mouse-model and in vitro PK/PD studies against *Y. pestis*).

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [21115791](https://pubmed.ncbi.nlm.nih.gov/21115791/) | 2011 | In vitro PK/PD model | Antimicrobial Agents and Chemotherapy | Derived a moxifloxacin dosing regimen via in vitro PK/PD model that optimizes killing of *Y. pestis* and prevents emergence of resistance |
| [20052916](https://pubmed.ncbi.nlm.nih.gov/20052916/) | 2009 | Animal efficacy (comparative) | Antibiotiki i khimioterapiia | Moxifloxacin ED50 5.5–14.0 mg/kg against FI+/FI- *Y. pestis* strains in a mouse infection model, comparable to levofloxacin |
| [15555886](https://pubmed.ncbi.nlm.nih.gov/15555886/) | 2004 | Animal efficacy | International Journal of Antimicrobial Agents | Moxifloxacin gave full protection up to 6h (systemic) and 30h (aerosol) post-challenge in a mouse plague model, comparable to ciprofloxacin |
| [21486959](https://pubmed.ncbi.nlm.nih.gov/21486959/) | 2011 | In vitro comparative efficacy | Antimicrobial Agents and Chemotherapy | Compared candidate antibiotics, including moxifloxacin, against *Y. pestis* in an in vitro PK/PD model relative to streptomycin gold standard |
| [29623187](https://pubmed.ncbi.nlm.nih.gov/29623187/) | 2018 | Case report (adverse event) | Therapeutic Advances in Drug Safety | Case of moxifloxacin-induced tinnitus in an older adult; notes FDA-recommended fluoroquinolone use is limited to specific infections including plague |
| [26210091](https://pubmed.ncbi.nlm.nih.gov/26210091/) | 2015 | Case report (related pathogen) | Ticks and Tick-borne Diseases | Case of *Francisella tularensis* (a related CDC Category A biothreat pathogen) infection in China; tangential relevance to biothreat antibiotic planning |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Other TxGNN-Predicted Indications (Screened, Not Pursued)

Of the 10 candidates in this evidence pack, 9 were assessed as low-confidence screening noise (Evidence Level L4–L5, decision stage S0–S1, recommendation Hold or Research Question), with either no clinical/literature evidence at all, or evidence limited to incidental antibiotic use during infections in patients who happen to have the predicted disease (e.g. *monoclonal gammopathy*, *congenital hematological disorder*) rather than any treatment effect on the disease itself:

hyperamylasemia, polyclonal hyperviscosity syndrome, congenital analbuminemia, blood group incompatibility, premalignant hematological system disease, monoclonal gammopathy, hematological disease associated with acquired peripheral neuropathy, congenital hematological disorder, hematopoietic and lymphoid system neoplasm.

These should not be advanced without new, disease-specific mechanistic or trial evidence.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails** (for Bubonic Plague only; all other predicted indications: **Hold**)

**Rationale:**
Bubonic plague is the only candidate with a coherent mechanistic basis (established fluoroquinolone activity against *Y. pestis*) and multiple independent animal/in vitro PK-PD studies confirming efficacy — the accepted evidence standard for this indication under the FDA Animal Rule. However, this is a direct antimicrobial-spectrum extension rather than a genuine drug-repurposing discovery, and local regulatory/safety documentation is currently missing.

**To proceed, the following is needed:**
- TFDA/local package insert with warnings and contraindications (currently blocking — DG001)
- Formal mechanism-of-action documentation (currently missing — DG002)
- Confirmation of local regulatory pathway equivalent to the FDA Animal Rule for biothreat indications
- Drug-drug interaction data (current query returned no results)
- A biopreparedness-specific use case and stakeholder (public health authority) engagement plan, since this is not a conventional commercial indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


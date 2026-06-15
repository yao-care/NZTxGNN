---
layout: default
title: Alectinib
parent: 僅模型預測 (L5)
nav_order: 20
evidence_level: L5
indication_count: 10
---

# Alectinib
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

# Alectinib: From ALK-Positive Non-Small Cell Lung Cancer to ALK-Driven Rare Lung Tumors

## One-Sentence Summary

Alectinib is a second-generation, highly selective ALK/RET tyrosine kinase inhibitor, with robust evidence supporting its use in ALK-positive non-small cell lung cancer (NSCLC) — approved in the United States, EU, and Japan, but not yet registered in New Zealand.
The TxGNN model predicts it may be applicable to **ALK-driven rare lung tumors** (including neuroendocrine carcinomas), supported by **2 clinical trials** (including an active Phase 2/3 UK basket trial) and **multiple case reports** documenting responses in molecularly-defined rare cancers.
For the core indication of ALK-positive NSCLC, evidence reaches **Level 1** from multiple completed Phase 3 RCTs — one of the strongest evidence bases in contemporary thoracic oncology.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | ALK-Positive Non-Small Cell Lung Cancer (approved in US/EU/Japan; **not registered in New Zealand**) |
| Predicted New Indication | ALK-Driven Rare Lung Tumors (Neuroendocrine Carcinoma) |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L1 (ALK+ NSCLC overall); L3 (rare ALK+ lung tumors specifically) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why Is This Prediction Reasonable?

While detailed mechanism of action data from DrugBank is pending retrieval (data gap DG002), alectinib is well established in the scientific literature as a second-generation, ATP-competitive inhibitor of anaplastic lymphoma kinase (ALK) and RET tyrosine kinases. It binds with high selectivity to the ALK kinase domain, blocking downstream proliferation signals through the RAS/MAPK, JAK/STAT3, and PI3K/AKT pathways. Compared to the first-generation ALK inhibitor crizotinib, alectinib demonstrates markedly superior central nervous system penetration and retains activity against multiple acquired resistance mutations (including L1196M, G1269A, and F1174L), which is why multiple Phase 3 trials have demonstrated its superiority.

The primary oncogenic driver targeted by alectinib — the EML4-ALK fusion gene — drives tumor growth through constitutive ALK signaling and is present in approximately 3–5% of all NSCLC cases globally. The mechanistic rationale for extending this to rare ALK-positive lung tumors is straightforward: wherever an ALK fusion acts as the dominant oncogenic driver, targeted ALK inhibition should produce clinical responses regardless of histological subtype. This "oncogene-agnostic" principle has been directly validated by multiple case reports demonstrating objective responses to alectinib in ALK-rearranged large cell neuroendocrine carcinoma (LCNEC), atypical pulmonary carcinoids, and combined neuroendocrine-adenocarcinoma tumors.

The DETERMINE basket trial (NCT05770037) directly operationalizes this hypothesis — a Phase 2/3 UK platform trial actively recruiting adults, pediatric, and TYA patients with ALK-positive rare cancers across tumor histologies. Although an earlier dedicated Phase 2 trial (NCT04644315) was terminated due to recruitment challenges (only 1 patient enrolled), DETERMINE's pooled rare-population design overcomes this limitation. From a New Zealand perspective, the most urgent finding is that alectinib is unregistered despite Level 1 evidence in ALK+ NSCLC, representing both an unmet clinical need and a concrete regulatory opportunity.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT05770037](https://clinicaltrials.gov/study/NCT05770037) | Phase 2/3 | Recruiting | 30 | DETERMINE basket trial (UK): evaluates alectinib in adult, pediatric, and TYA patients with ALK-positive rare cancers across tumor types including neuroendocrine malignancies; alectinib arm designed to assess efficacy in molecularly-defined rare cancers beyond NSCLC; completion expected October 2029 |
| [NCT04644315](https://clinicaltrials.gov/study/NCT04644315) | Phase 2 | Terminated | 1 | Home-based, open-label study of alectinib in locally advanced or metastatic ALK-positive solid tumors other than lung cancer; terminated early due to insufficient enrollment, illustrating the recruitment challenge in this rare population |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|---------|
| [38598794](https://pubmed.ncbi.nlm.nih.gov/38598794/) | 2024 | Phase 3 RCT | N Engl J Med | ALINA trial: adjuvant alectinib vs platinum-based chemotherapy in resected ALK+ NSCLC; significant improvement in disease-free survival, establishing adjuvant use |
| [28586279](https://pubmed.ncbi.nlm.nih.gov/28586279/) | 2017 | Phase 3 RCT | N Engl J Med | ALEX trial: alectinib vs crizotinib in untreated advanced ALK+ NSCLC; alectinib superior in PFS and CNS activity |
| [28501140](https://pubmed.ncbi.nlm.nih.gov/28501140/) | 2017 | Phase 3 RCT | Lancet | J-ALEX trial: alectinib vs crizotinib in Japanese patients; median PFS 34.1 vs 10.2 months favouring alectinib |
| [30981696](https://pubmed.ncbi.nlm.nih.gov/30981696/) | 2019 | Phase 3 RCT | Lancet Respir Med | ALESIA trial: alectinib vs crizotinib in Asian ALK+ NSCLC; consistent PFS benefit in Asian populations confirms generalisability |
| [38331773](https://pubmed.ncbi.nlm.nih.gov/38331773/) | 2024 | Systematic Review / Network Meta-analysis | BMC Cancer | Comparative efficacy of all ALK inhibitors for global and Asian patients; alectinib identified as an optimal first-line option |
| [29151522](https://pubmed.ncbi.nlm.nih.gov/29151522/) | 2018 | Case Report | Internal Medicine (Tokyo) | ALK-rearranged large cell neuroendocrine carcinoma (LCNEC) with hepatic and bone metastases; significant partial response to alectinib after failure of cytotoxic chemotherapy |
| [34994612](https://pubmed.ncbi.nlm.nih.gov/34994612/) | 2021 | Case Report | JCO Precision Oncology | Metastatic ALK-fusion-positive LCNEC; partial response to alectinib documented, supporting ALK-agnostic treatment approach |
| [37031440](https://pubmed.ncbi.nlm.nih.gov/37031440/) | 2023 | Case Report | Orvosi Hetilap | Mixed large cell neuroendocrine carcinoma of the lung with ALK fusion; alectinib used as targeted therapy alternative to cytostatics with favourable initial response |
| [36690569](https://pubmed.ncbi.nlm.nih.gov/36690569/) | 2023 | Case Report | Clin Lung Cancer | ALK-positive neuroendocrine lung tumour; favorable response to alectinib, adding to the growing body of ALK+ neuroendocrine case evidence |
| [37561984](https://pubmed.ncbi.nlm.nih.gov/37561984/) | 2023 | Review | JCO Precision Oncology | ALK inhibitors (including alectinib and crizotinib) in adult-onset neuroblastoma; ALK somatic mutations enriched in adult disease; review supports ALK-inhibitor rationale across ALK-driven tumour types |

---

## New Zealand Market Information

Alectinib currently has **no Medsafe-approved products in New Zealand** (0 licenses as of 2026-06-06). The drug is approved in the United States (FDA, brand name Alecensa®), the European Union (EMA), Japan (PMDA), and numerous other jurisdictions for ALK-positive NSCLC in both first-line metastatic and adjuvant settings. New Zealand patients currently lack registered access to this standard-of-care agent.

---

## Cytotoxicity

Alectinib is an antineoplastic agent used in the targeted treatment of ALK-positive lung cancer.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Targeted therapy — Second-generation ALK/RET tyrosine kinase inhibitor (non-conventional cytotoxic) |
| Myelosuppression Risk | Low to Moderate — anaemia is the most common haematologic adverse event (including rare haemolytic anaemia); neutropenia and thrombocytopenia are less frequent than with conventional cytotoxics |
| Emetogenicity Classification | Low |
| Monitoring Items | Full blood count with differential; liver function tests (ALT/AST — hepatotoxicity monitoring); creatine phosphokinase (CPK — myalgia/rhabdomyolysis risk); fasting lipid panel (hypertriglyceridaemia); blood glucose; ECG (sinus bradycardia); body weight (clinically significant weight gain in ~10% of patients) |
| Handling Protection | Follow institutional cytotoxic drug handling protocols; oral capsule formulation reduces compounding exposure compared to IV agents, but standard cytotoxic precautions apply for handling and disposal |

---

## Safety Considerations

Based on published literature (New Zealand package insert data not available due to absence of Medsafe registration):

- **Metabolic toxicity**: Life-threatening hypertriglyceridaemia-induced pancreatitis has been reported; fasting lipid monitoring is warranted, particularly in patients with pre-existing dyslipidaemia
- **Weight gain**: Clinically significant weight gain observed in approximately 10% of patients on long-term alectinib; aetiology involves metabolic effects and potential promotion of adipogenesis; baseline weight and longitudinal monitoring recommended
- **Bradycardia**: Sinus bradycardia is documented; QTc prolongation risk appears low based on intensive electrocardiographic monitoring in pivotal Phase 2 studies
- **Haematologic**: Haemolytic anaemia has been reported as a rare but serious adverse event; monitoring of CBC and haemolysis markers (LDH, bilirubin, reticulocyte count) is advisable
- **Dermatologic**: Erythema multiforme and DRESS (drug reaction with eosinophilia and systemic symptoms) syndrome have been reported; most cases were managed with dose modification or corticosteroids, with successful rechallenge in some cases

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Alectinib has Level 1 evidence from at least four Phase 3 RCTs (ALEX, J-ALEX, ALESIA, ALINA) confirming superiority over crizotinib in ALK-positive NSCLC across global, Asian, and adjuvant populations. Despite this, alectinib holds zero Medsafe registrations in New Zealand — a clear and actionable gap between global standard of care and New Zealand clinical access. The novel repurposing direction toward ALK-driven rare lung tumors (neuroendocrine carcinomas) is mechanistically sound and supported by an active Phase 2/3 basket trial and accumulating case evidence, but requires more prospective data before regulatory consideration.

**To proceed, the following is needed:**

- **Immediate priority**: Submit Medsafe registration application for ALK-positive NSCLC based on existing global Phase 3 data (ALEX/J-ALEX/ALESIA/ALINA trials); expedited pathway may be applicable given overseas approvals
- Retrieve mechanism of action data from DrugBank API to complete DG002 data gap
- Develop New Zealand-specific safety monitoring protocol covering hepatic, cardiac (bradycardia), metabolic (hypertriglyceridaemia, weight gain), and haematologic surveillance
- Confirm ALK testing capacity and companion diagnostic availability (e.g., Ventana D5F3 IHC or FISH) across New Zealand oncology centres
- Conduct pharmacoeconomic evaluation for PHARMAC funding consideration, including cost-effectiveness against lorlatinib (third-generation ALK inhibitor) and reference to Australian PBS listing if available
- For rare tumour repurposing: await interim data from the DETERMINE basket trial (NCT05770037; expected 2025–2026) before initiating a New Zealand regulatory pathway for this novel indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


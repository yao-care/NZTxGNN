---
layout: default
title: Entecavir
parent: Model Prediction Only (L5)
nav_order: 179
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Entecavir: predicted new indication Chronic hepatitis C virus infection (evidence level L5). Approved use: Chronic hepatitis B virus infection"
monitor_inn: "ENTECAVIR"
predicted_indications: ["chronic hepatitis C virus infection", "HIV infectious disease", "hepatitis C virus infection", "feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter", "hepatitis, viral, animal", "hepatitis E virus infection", "hepatitis A virus infection"]
---

# Entecavir
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

## Entecavir: From Chronic Hepatitis B to Chronic Hepatitis C Virus Infection
{: .text-alpha }

## One-Sentence Summary

Entecavir is an oral nucleoside analogue originally used to treat chronic hepatitis B virus (HBV) infection.
The TxGNN model predicts it may be useful for **chronic hepatitis C virus (HCV) infection**, supported by **2 clinical trials** and **20 publications**.
That evidence concerns managing HBV in patients co-infected with HCV, not treating HCV itself.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Chronic hepatitis B virus infection |
| Predicted New Indication | Chronic hepatitis C virus infection |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L3 (observational and review-level evidence only; no completed Phase 3 RCTs for this indication) |
| New Zealand Market Status | Cancelled only (no currently valid authorization) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Entecavir is a guanosine nucleoside analogue. Inside cells it is converted to its active triphosphate form, which blocks the HBV polymerase at the priming, reverse transcription and DNA synthesis steps. Detailed mechanism of action data beyond this were not available for this report.

The link to hepatitis C is **indirect**. HCV is an RNA virus that replicates through a different enzyme (the NS5B RNA-dependent RNA polymerase), and entecavir has no known direct activity against it. The relevant signal comes from patients infected with both viruses. When direct-acting antivirals (DAAs) clear HCV, dormant HBV can reactivate. Entecavir suppresses HBV replication and can prevent this reactivation.

The TxGNN prediction therefore most likely reflects a co-infection management role rather than a true anti-HCV effect. The model's top-ranked indications also include on-label hepatitis B, which is a confirmation of the existing use rather than repurposing.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02555943](https://clinicaltrials.gov/study/NCT02555943) | Phase 2/3 | Completed | 23 | Prospective study of HBV reactivation (incidence, morbidity, risk factors) during DAA treatment in HCV/HBV co-infected patients. Small, and it tests DAAs rather than entecavir as an HCV therapy. |
| [NCT04405011](https://clinicaltrials.gov/study/NCT04405011) | Not applicable | Unknown | 60 | Three-arm randomized open-label study of whether prophylactic nucleos(t)ide analogue at the start of DAA therapy prevents HBV reactivation, and whether 24 weeks is better than 12 weeks. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34864158](https://pubmed.ncbi.nlm.nih.gov/34864158/) | 2022 | RCT | Clin Gastroenterol Hepatol | Randomized trial titled "Entecavir Prevents HBV Reactivation During Direct Acting Antivirals for HCV/HBV Dual Infection". It addresses the unresolved question of how to prevent HBV reactivation during DAA therapy. |
| [36146665](https://pubmed.ncbi.nlm.nih.gov/36146665/) | 2022 | Cohort | Viruses | 66 anti-HCV-positive chronic hepatitis B patients on nucleos(t)ide analogue therapy, studied for HCV reactivation and viral load changes. |
| [24773464](https://pubmed.ncbi.nlm.nih.gov/24773464/) | 2014 | Review | Expert Opin Pharmacother | HBV/HCV co-infection carries a high risk of cirrhosis and hepatocellular carcinoma and warrants effective treatment. |
| [35327336](https://pubmed.ncbi.nlm.nih.gov/35327336/) | 2022 | Review | Biomedicines | Overview of therapy for chronic hepatitis B, C and D. Long-term nucleoside/nucleotide analogue therapy has markedly changed the course of hepatitis B liver disease. |
| [25027705](https://pubmed.ncbi.nlm.nih.gov/25027705/) | 2014 | Review | Minerva Gastroenterol Dietol | Antiviral drugs for hepatitis B and C (including entecavir) and their effects on kidney function. |
| [16937041](https://pubmed.ncbi.nlm.nih.gov/16937041/) | 2006 | Review | Wien Med Wochenschr | Current treatment and future prospects in chronic hepatitis B and C. |
| [35108715](https://pubmed.ncbi.nlm.nih.gov/35108715/) | 2022 | Not classified | Dig Dis | Incidence and clinical course of HBV reactivation in patients treated with DAAs for hepatitis C. |
| [28230928](https://pubmed.ncbi.nlm.nih.gov/28230928/) | 2017 | Not classified | J Gastroenterol Hepatol | Risk of HBV reactivation when chronic hepatitis C patients with HBV infection receive DAAs. |
| [32173307](https://pubmed.ncbi.nlm.nih.gov/32173307/) | 2020 | Not classified | Clin Res Hepatol Gastroenterol | Management of viral hepatitis B and C in children. |
| [24868325](https://pubmed.ncbi.nlm.nih.gov/24868325/) | 2014 | Not classified | World J Hepatol | Entecavir and tenofovir in patients with hepatitis B and C before and after liver and kidney transplantation. Doses need adjustment to creatinine clearance. |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2654431 (cancelled) | Entecavir (Rex) | Tab 0.5 mg | Not stated |

---

## Safety Considerations

- **Disease-related cautions (knowledge-graph contraindication links, not label text)**: impaired renal function, fatty liver disease, non-alcoholic steatohepatitis, and lactic acidosis.
- **Drug Interactions**: 44 interactions were identified, of which the 20 listed are all rated Moderate. The main ones are:
  - Acid suppressants: famotidine, ranitidine, cimetidine.
  - Metformin.
  - Amphotericin B and its lipid formulations.
  - Aminoglycosides and other nephrotoxic anti-infectives: kanamycin, neomycin, vancomycin, polymyxin B.
  - Levofloxacin and amoxicillin.
  - Aminosalicylates: mesalazine, balsalazide, olsalazine, sulfasalazine.
  - Orlistat.
- **Literature notes**: nucleoside analogue class effects include lactic acidosis. A case report describes entecavir-associated thrombocytopenia.

Please refer to the package insert for full warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Entecavir has no direct anti-HCV mechanism, and the supporting trials and papers address HBV reactivation prevention in co-infected patients rather than HCV treatment. The only RCT identified shows a role in HBV prophylaxis during HCV therapy, which is better framed as a co-infection management use than as a new HCV indication. The single New Zealand authorization is also cancelled, so there is no currently marketed product.

**To proceed, the following is needed:**
- Clarify whether the goal is HCV treatment (not supported by the mechanism) or HBV prophylaxis in HCV/HBV co-infection (better supported).
- Review the full text and results of the entecavir prophylaxis RCT (PMID 34864158) and the nucleoside analogue prophylaxis trial (NCT04405011).
- Retrieve the Medsafe package insert for warnings and contraindications.
- Obtain detailed mechanism of action data.
- Assess the regulatory pathway given the cancelled New Zealand authorization.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Ursodeoxycholic Acid
parent: Model Prediction Only (L5)
nav_order: 472
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Ursodeoxycholic Acid: predicted new indication Homozygous familial hypercholesterolemia (evidence level L5). Approved use: Cholelithiasis, cholecystolithiasis, choledocholithiasis…"
---

# Ursodeoxycholic Acid
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

# Ursodeoxycholic Acid: From Cholelithiasis to Homozygous Familial Hypercholesterolemia

## One-Sentence Summary

Ursodeoxycholic acid (UDCA) is a bile acid originally used to treat gallstone disease (cholelithiasis, cholecystolithiasis, choledocholithiasis) and primary biliary cholangitis.
The TxGNN model predicts it may be effective for **homozygous familial hypercholesterolemia (HoFH)**,
but there are currently **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Cholelithiasis, cholecystolithiasis, choledocholithiasis, primary biliary cholangitis, Reynolds syndrome (no approved indication text is available from the New Zealand licence record) |
| Predicted New Indication | Homozygous familial hypercholesterolemia |
| TxGNN Prediction Score | 99.86% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorization) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Based on known information, UDCA is a hydrophilic bile acid whose efficacy in gallstone disease and primary biliary cholangitis is established. Mechanistically, it may be applicable to disorders of cholesterol handling.

The proposed link runs through bile acid and cholesterol metabolism. UDCA is reported to reduce intestinal cholesterol absorption, suppress hepatic cholesterol synthesis and biliary cholesterol secretion, and modulate the bile acid pool (for example, through FXR-related signaling). This is why a model built on drug-disease network relationships would connect it with a lipid disorder.

However, this link is plausible but unverified, and there is a significant biological concern. HoFH is caused mainly by loss-of-function variants in *LDLR* (or *APOB*, *PCSK9*, *LDLRAP1*), leading to near-absent LDL receptor activity. Bile acid-mediated cholesterol lowering depends largely on upregulating LDL receptors, so a meaningful LDL-C-lowering effect in HoFH is doubtful. The high model score should therefore be read as a hypothesis only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2397528 (cancelled) | Ursosan | Cap 250 mg | Not recorded |

## Safety Considerations

- **Drug Interactions**: The interaction screening returned 100 potential interacting drugs in total; the first 20 listed are summarized here. Three are rated **Minor**: cyclosporine, aluminum hydroxide, and magaldrate. The remaining 17 have an unclassified severity (for example cyanocobalamin, calcitriol, folic acid, pravastatin, ramipril, lorazepam, tramadol, fluconazole, oseltamivir, erythromycin, sildenafil, pantoprazole, citalopram, moxifloxacin, nelfinavir, glimepiride, and indinavir). Notably, aluminum-containing antacids are commonly reported to bind bile acids and may reduce absorption, and cyclosporine absorption can be affected, so these warrant attention if the drug is used.

Package insert warnings and contraindications were not available for this report; please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score (99.86%), with no clinical trials or publications. The mechanism is also biologically questionable in HoFH, where LDL receptor function is largely absent. In addition, the only New Zealand authorization has been cancelled.

**To proceed, the following is needed:**
- Mechanism of action data for UDCA, and a critical assessment of whether any LDL-receptor-independent effect could matter in HoFH
- Preclinical or early clinical evidence of lipid lowering in HoFH or receptor-deficient models
- Package insert warnings and contraindications (currently blocking for safety screening)
- Clarification of the regulatory status and available formulations in New Zealand, since the sole licence is cancelled
- Review of the interaction profile, especially with bile acid-binding agents and cyclosporine
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


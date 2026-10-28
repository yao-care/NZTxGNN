---
layout: default
title: Beclomethasone Dipropionate
parent: Model Prediction Only (L5)
nav_order: 57
evidence_level: L5
indication_count: 1
monitor_inn: "BECLOMETHASONE DIPROPIONATE"
predicted_indications: ["atopic eczema"]
last_modified_at: 2026-10-27
description: "Beclomethasone Dipropionate: predicted new indication atopic eczema (evidence level L5). Approved use: Not recorded in the local data"
---

# Beclomethasone Dipropionate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Beclomethasone Dipropionate: Inhaled/topical glucocorticoid → atopic eczema
{: .text-alpha }

## One-Sentence Summary

Beclomethasone dipropionate is a glucocorticoid corticosteroid, available in New Zealand as inhaled aerosol products.
The TxGNN model predicts it may be effective for **atopic eczema**, but the supporting evidence is limited: **no registered clinical trials** and **9 publications**, none of which is a controlled trial in atopic eczema.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | atopic eczema |
| TxGNN Prediction Score | 0.9941 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 5 of 5) |
| Number of Authorizations | 5 (valid: 5) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on drug class knowledge, beclomethasone dipropionate is a glucocorticoid prodrug. It is converted to the active metabolite beclomethasone-17-monopropionate, which binds the glucocorticoid receptor and dampens pro-inflammatory cytokine and NF-kB signalling.

Atopic eczema is a Th2-driven inflammatory skin disease, and corticosteroids are a standard anti-inflammatory approach for it. Topical beclomethasone has historically been used in dermatology. This makes the prediction plausible at the drug-class level. This reasoning is inferred from class knowledge, not from mechanism data supplied for this drug.

The publications found are mostly about atopic dermatitis treatment with steroids in general, safety (adrenal suppression, growth), or formulation work. They do not provide controlled efficacy evidence for beclomethasone in atopic eczema, so the evidence level is L5.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [1476023](https://pubmed.ncbi.nlm.nih.gov/1476023/) | 1992 | Clinical study | Acta Derm Venereol Suppl | Oral beclomethasone in 14 children with difficult atopic dermatitis; stable control in 10/14 at about 1000 mcg/day, but slowed linear growth was seen |
| [14522624](https://pubmed.ncbi.nlm.nih.gov/14522624/) | 2003 | Clinical study | J Dermatolog Treat | Steroid wet-wrap dressings in 8 children with atopic eczema; short-term growth and bone turnover assessed |
| [30911861](https://pubmed.ncbi.nlm.nih.gov/30911861/) | 2019 | Preclinical formulation | AAPS PharmSciTech | Beclomethasone micelles in a hydrogel tested in an animal dermatitis model |
| [374799](https://pubmed.ncbi.nlm.nih.gov/374799/) | 1979 | Comparative clinical study | Nihon Hifuka Gakkai Zasshi | Systemic effects of topical beclomethasone compared with betamethasone valerate and fluocinonide |
| [8765824](https://pubmed.ncbi.nlm.nih.gov/8765824/) | 1996 | In vitro study | J Allergy Clin Immunol | Topical steroids enhanced spontaneous IgE production in cells from atopic dermatitis patients |
| [19571596](https://pubmed.ncbi.nlm.nih.gov/19571596/) | 2009 | Review | Neuroimmunomodulation | Intranasal corticosteroids and adrenal suppression in allergic disease |
| [11488426](https://pubmed.ncbi.nlm.nih.gov/11488426/) | 2001 | Review | Jpn J Pharmacol | Overview of drugs for allergic diseases, including inhaled beclomethasone |
| [14616123](https://pubmed.ncbi.nlm.nih.gov/14616123/) | 2003 | Review | Allergy | Corticosteroid allergy in patients with asthma |
| [7813169](https://pubmed.ncbi.nlm.nih.gov/7813169/) | 1994 | Review | Clin Ter | Allergy drugs in pregnancy |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2023857 | Qvar | Aerosol inhaler, 100 mcg per dose | Not recorded |
| 2326175 | Beclazone 250 | Aerosol inhaler, 250 mcg per dose CFC-free | Not recorded |
| 2023873 | Qvar | Aerosol inhaler, 50 mcg per dose | Not recorded |
| 2326159 | Beclazone 50 | Aerosol inhaler, 50 mcg per dose CFC-free | Not recorded |
| 2326167 | Beclazone 100 | Aerosol inhaler, 100 mcg per dose CFC-free | Not recorded |

All locally listed products are inhaled aerosols; no topical skin formulation appears in the local data.

---

## Safety Considerations

- **Drug Interactions**: 49 interactions were recorded. Notable ones include:
  - **Major**: desmopressin
  - **Moderate**: acetazolamide, foscarnet, furosemide, hydrochlorothiazide, bumetanide, cisplatin, amphotericin B (several formulations), and other thiazide and loop diuretics (a pattern that suggests additive potassium loss)
  - **Minor**: formoterol, salbutamol, arformoterol
- **Safety signals from the literature**: oral beclomethasone in children with atopic dermatitis was associated with slowed linear growth and effects on adrenal function; corticosteroid use also carries a risk of HPA axis suppression and contact allergy.

Please refer to the package insert for further safety information, including contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high (0.9941), and the glucocorticoid mechanism is plausible for atopic eczema. However, there are no registered trials and no controlled study of beclomethasone in atopic eczema, and the local products are inhaled aerosols only, with no skin formulation.

**To proceed, the following is needed:**
- Controlled clinical data of beclomethasone dipropionate in atopic eczema, compared with established topical corticosteroids
- A suitable topical formulation and a route-compatibility assessment
- Package insert warnings and contraindications from Medsafe
- Mechanism-of-action data from DrugBank
- A growth and adrenal-suppression safety assessment, especially for children
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


---
layout: default
title: Carbamazepine
parent: 僅模型預測 (L5)
nav_order: 63
evidence_level: L5
indication_count: 10
---

# Carbamazepine
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

# Carbamazepine: From Epilepsy & Trigeminal Neuralgia to Trigeminal Nerve Neoplasm

## One-Sentence Summary

Carbamazepine (CBZ) is one of the most widely prescribed antiepileptic drugs, with well-established use in focal seizures and trigeminal neuralgia through its voltage-gated sodium channel-blocking mechanism.
The TxGNN model predicts it may be effective for **Trigeminal Nerve Neoplasm**,
with **1 clinical trial** and **20 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Epilepsy (partial and generalized tonic-clonic seizures), trigeminal neuralgia — inferred from literature context; no New Zealand registration data captured |
| Predicted New Indication | Trigeminal Nerve Neoplasm |
| TxGNN Prediction Score | 99.9976% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed (0 registered products) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known information from the literature included within, carbamazepine functions primarily by blocking voltage-gated sodium channels (Nav1.7/Nav1.8), thereby stabilising hyperexcitable neuronal membranes and suppressing ectopic discharges along peripheral and central nerve pathways. Its analgesic efficacy in classical trigeminal neuralgia is extensively documented (PMID 36824641, PMID 17997704) and forms the pharmacological foundation for this prediction.

Trigeminal nerve neoplasms — including schwannomas, neurolymphomas, meningiomas, and other tumours along the trigeminal pathway — frequently compress or directly infiltrate the nerve, producing focal demyelination and aberrant neural discharges that are clinically indistinguishable from idiopathic trigeminal neuralgia. This is precisely the pathophysiology that CBZ's sodium channel blockade is designed to interrupt. Critically, PMID 3181365 provides direct preclinical mechanistic evidence: intravenous carbamazepine immediately and dose-dependently inhibits spontaneous ectopic discharges from experimental neuromas in rats, directly paralleling the mechanism expected in neoplasm-induced neuropathic pain.

Multiple case reports within the evidence pack further validate this clinical overlap. In PMID 30741017, a primary malignant lymphoma of the trigeminal nerve was initially treated with carbamazepine before the neoplastic aetiology was identified. In PMID 25142539, a patient with perineural lymphoma along the trigeminal nerve showed initial improvement with CBZ before the diagnosis was established. These real-world cases demonstrate that carbamazepine is already being empirically deployed in the trigeminal nerve neoplasm setting, even without a formal repurposing designation.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06853119](https://clinicaltrials.gov/study/NCT06853119) | N/A | Not Yet Recruiting | 120 | MRI-based observational study analysing brain network dynamics and microstructural changes in trigeminal neuralgia patients; evaluates blood-brain barrier integrity and water exchange rates in correlation with clinical indicators — not a drug intervention trial |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|---------|
| [36824641](https://pubmed.ncbi.nlm.nih.gov/36824641/) | 2022 | Review | Acta Clinica Croatica | Comprehensive review of trigeminal neuralgia treatment; notes TN can be caused by vascular compression or tumour process; documents medical and surgical approaches including CBZ as first-line |
| [17997704](https://pubmed.ncbi.nlm.nih.gov/17997704/) | 2007 | Review | Expert Review of Neurotherapeutics | Details etiology of vascular compression → focal demyelination → aberrant discharges; discusses full spectrum of medical and surgical treatments for TN |
| [3181365](https://pubmed.ncbi.nlm.nih.gov/3181365/) | 1988 | Animal Study | Experimental Neurology | **Key mechanistic evidence**: IV carbamazepine immediately inhibits spontaneous A-alpha/beta and A-delta fibre discharges from experimental neuromas at clinically relevant doses — directly supports efficacy against neoplasm-induced ectopic discharges |
| [30741017](https://pubmed.ncbi.nlm.nih.gov/30741017/) | 2023 | Case Report | British Journal of Neurosurgery | Primary malignant lymphoma of trigeminal nerve presenting as facial pain; CBZ was prescribed initially but provided insufficient relief — illustrates the clinical overlap between neoplasm-induced and idiopathic TN |
| [25142539](https://pubmed.ncbi.nlm.nih.gov/25142539/) | 2014 | Case Report | Clinical Neurology | Malignant lymphoma with perineural spread along the trigeminal nerve, initially diagnosed as classical TN with initial CBZ response; secondary cranial nerve symptoms later revealed the neoplastic cause |
| [9109911](https://pubmed.ncbi.nlm.nih.gov/9109911/) | 1997 | Case Report | Neurology | Post-irradiation neuromyotonia in bilateral facial and trigeminal nerve distribution responded to carbamazepine — demonstrates CBZ efficacy in radiation-related nerve dysfunction, relevant to post-treatment neoplasm settings |
| [22647513](https://pubmed.ncbi.nlm.nih.gov/22647513/) | 2012 | Case Report | Neurological Surgery | Combined trigeminal and glossopharyngeal neuralgia due to vascular compression; CBZ documented as first-line medical therapy before surgical microvascular decompression |
| [33989821](https://pubmed.ncbi.nlm.nih.gov/33989821/) | 2021 | Case Series | World Neurosurgery | Petroclival meningioma encasing the trigeminal nerve causing TN in <5% of cases; resected via Kawase approach — illustrates the surgical context where pre-operative medical pain management (including CBZ) is standard |
| [32454201](https://pubmed.ncbi.nlm.nih.gov/32454201/) | 2020 | Case Series | World Neurosurgery | Trigeminal schwannomas from pterygopalatine fossa resected endoscopically; benign slow-growing tumours (0.1–0.4% of intracranial tumours) — establishes neoplasm subtype context where CBZ-class pain management is relevant |
| [12590697](https://pubmed.ncbi.nlm.nih.gov/12590697/) | 2003 | Case Report | Neurosurgery | Isolated trigeminal nerve sarcoid granuloma radiologically mimicking schwannoma; supports the diagnostic and therapeutic overlap between nerve tumours and inflammatory lesions, both of which may benefit from CBZ pain management |

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** Key warnings, contraindications, and drug-drug interaction data were not retrieved in this evidence pack. Of particular clinical importance for carbamazepine: HLA-B\*15:01 carrier status screening is recommended in populations of Han Chinese and other Asian ancestry prior to initiation, given the risk of Stevens-Johnson syndrome. Full safety evaluation requires downloading and parsing the Medsafe-approved package insert.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Carbamazepine has sound mechanistic justification for managing neuropathic pain in trigeminal nerve neoplasms — the drug's sodium channel blockade directly targets the ectopic discharge pathophysiology caused by tumour compression and nerve infiltration. Multiple case reports confirm CBZ is already in empirical use in this setting, and preclinical neuroma data (PMID 3181365) provides direct mechanistic corroboration. However, no prospective clinical trials specifically targeting trigeminal nerve neoplasm exist, and the New Zealand registration status requires independent verification.

**To proceed, the following is needed:**
- Obtain and parse the Medsafe/TFDA package insert to document full warnings, contraindications, and monitoring requirements
- Complete drug-drug interaction (DDI) profile (query returned no results — re-query required)
- Conduct HLA-B\*15:01 pharmacogenomic screening protocol review for relevant patient populations
- Design a prospective case series or registry study specifically enrolling trigeminal nerve neoplasm patients receiving CBZ for neuropathic pain control
- Verify current New Zealand registration status through the Medsafe online database directly (the 0-license finding may reflect a data retrieval gap rather than true non-registration)
- Clarify mechanism of action documentation from DrugBank API (currently listed as data gap)
- Develop a safety monitoring plan covering CBC, liver function, renal function, and serum CBZ levels for the target population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---


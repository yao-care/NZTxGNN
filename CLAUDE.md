# NZTxGNN - Claude CLI 使用指引

紐西蘭站（Pharmac 給付清單藥品的老藥新用預測）。網站：https://nztxgnn.yao.care/ 。
站群共用的維運流程在母目錄 txgnn 的 CLAUDE.md；本站自己的規則寫在這裡。

- 本專案結果僅供研究參考，不構成醫療建議；老藥新用候選需經過臨床驗證才能應用；所有網站頁面需包含 YMYL 免責聲明。

---

## 藥物頁查核機制（2026-11-12 起）

用戶 2026-11-12 決定：30 站各有自己的查核機制，核心流程比照 TwTxGNN（Tw 的 CLAUDE.md「藥物頁人工查核紀錄」「藥物頁許可證表與張數」兩節）。
本站照 UsTxGNN 的試做版建立，規則與程式都在本 repo，頁面文字用英文、依據是紐西蘭 Pharmac 給付清單（Pharmaceutical Schedule）。

| 環節 | 本站檔案 | 說明 |
|------|----------|------|
| 查核紀錄 | `docs/_data/drug_reviews.json` | 單一真實來源，欄位同 Tw（correct／annotate／rereview／rereview_result／header_level／description_level、history、status） |
| 套用 | `scripts/apply_drug_reviews.py` | 套到 `docs/_drugs/*.md`：查核框 `<!-- review:begin id -->`、每頁「## Review Log」表；可重複執行；`--check` 只檢查、`--touch` 連 last_modified_at 一起改 |
| 頁首等級 | `src/nztxgnn/regulatory/page_format.py` | 頁首＝Quick Overview「Evidence Level」格的**第一個**等級（主預測）。2026-11-12 實測本站現有頁面全部是這個規則（第一個等級跟格內最高等級不同的頁也照第一個），照實寫成程式；Tw 是整格取最高，本站不同 |
| 撤回預測 | `src/nztxgnn/regulatory/withdrawn.py` | verdict=withdraw 的預測不進證據監測（`scripts/extract_drug_list.py`）、搜尋索引與 FHIR（母目錄 `.claude/rebuild_search_index.py`、`.claude/sync_fhir_levels.py` 會讀本站這支） |
| 許可證 | `scripts/sync_license_tables.py`（程式表前後有 `<!-- licenses:begin/end -->` 標記）、`snapshots/nz_licenses.json.gz`、`scripts/build_license_snapshot.py`、`src/nztxgnn/regulatory/licenses.py` | 快照存事實層與本藥號碼、程式表列；gate 依快照驗市場段落的號碼屬於本藥、狀態列與張數列＝事實層、表格列狀態不跟事實相反 |
| 刪改轉紀錄 | `scripts/reviews_from_edits.py` | 依 git 前後版本或依成分重建刪句後（`scripts/license_rebuild_run.py` 會自動跑）把正文刪改轉成 correct 紀錄 |
| 待查清單 | `docs/_data/drug_reviews_uncertain.json`、`scripts/check_original_indications.py` | 原適應症交叉驗證；本站證據包的許可證紀錄目前沒有適應症文字時，可比對 0 頁（程式照實回報） |
| gate | `scripts/check_seo_docs.py` 第 17–20 項 | 紀錄都已套用、許可證＝快照、頁首等級、撤回預測沒有對外發布 |

原則（同 Tw）：基本事實照主管機關資料更正；模型預測原文不刪不改寫，只加註；證據等級與決策只經重審（rereview → rereview_result）變更。

改頁之後要跑：

```bash
python3 scripts/apply_drug_reviews.py          # 重套查核紀錄（sync_notes_to_docs.py、license_rebuild_run.py、每週排程結尾都會自動跑）
python3 scripts/build_license_snapshot.py --ws <工作目錄>   # 證據包或事實層變了才要（排程會自動跑）
python3 scripts/check_seo_docs.py              # 必須 exit 0
```

2026-10-31～11-06 許可證修正時直接刪掉的矛盾子句（10-31 58ced9c7、11-06 80993f23／56ff65d7／827800db／e6c733d5／8445a9f3），2026-11-12 由 scripts/reviews_from_edits.py 依 git 前後版本轉入（530 筆、320 頁；其中 9 筆因報告在 11 月經 AI 重產、原句已不在頁面，標 resolved_by_regeneration）。
`scripts/sync_notes_to_docs.py` 依 notes 重寫整頁時會重套紀錄，notes 裡的舊錯句會被紀錄再更正一次。
2026-11-12 正文許可狀態對事實層掃描：tetracycline 在 11 月 AI 重產時照證據包（全標 Cancelled only）寫「cancelled-only in New Zealand」，Pharmac 清單 2 筆都是現行列載；以 `nz-lic-20261112-tetracycline-1` 刪掉該句。

## 作用機轉段的資料來源與查核機制（2026-11-19）

seo-ops 轉交：搜「<藥名> mechanism of action／moa」落在本站藥物頁的排名 30–99，因為頁面寫「Detailed mechanism of action data is not available in the data compiled for this report」。
2026-11-19 量測：公開藥物頁 461 頁有 409 頁在「Why is This Prediction Reasonable?」段這樣寫（證據包 drugbank.source＝vocab_only、mechanism_of_action＝None，AI 照實寫「沒有資料」；另 3 處在「Conclusion and Next Steps」，是待辦或已有查核框的句子，不動）。
作用機轉是藥物本身的基本藥理事實、不是模型預測，所以照查核機制補：每段都節錄官方仿單或資料庫原文、附可點的出處與取用日期，不自己寫。

- 來源評估（2026-11-19，雲端實測）：
  - Medsafe Data Sheet（紐西蘭主管機關仿單，最適合）：Data Sheet 查詢 /DbSearch/InfoSearch 是 Blazor Server 互動頁，沒有可下載的清單或 API，舊的 /profs/Datasheet/<字母>/<名>.pdf 網址已不通（回 Page not found），雲端無法依藥名逐一取得 PDF，這次沒用；之後能取得時應優先換成 Medsafe。
  - 證據包／data/ 的 DrugBank 欄位：本站證據包只有 vocab（drugbank.mechanism_of_action 全是 None），而且 DrugBank 文字授權不允許轉載，沒用。
  - **美國 FDA 核准仿單（採用）**：openFDA drug label API（https://api.fda.gov/drug/label.json，雲端可連、免金鑰每日 1,000 次；FDA 仿單是美國政府公開資料），連結到 NLM DailyMed 同一份 SPL（setid）。PLR 格式用「12.1 Mechanism of Action」，抗感染藥 12.1 只有一句時接 12.4 Microbiology 的「Mechanism of Action」小標，舊格式用「Clinical Pharmacology」段（沒有小標時從第一句講作用、不講藥動學的句子起，到第一句藥動學句為止）。
  - ChEMBL drug mechanism 表（CC BY-SA 3.0，結構化：作用機轉＋標靶）：美國沒有仿單時的備援；ChEMBL API 從雲端很慢（每次 5 秒～3 分鐘）。
  - NCBI／PubChem：沒有逐藥的作用機轉欄（PubChem 的 Pharmacology 段多半轉自 DrugBank），沒用。
- `scripts/build_mechanism_reviews.py --cache <目錄>`：掃公開頁「不可得」那句 → 依 front matter monitor_inn（NZ 的 INN 寫法另對 USAN，表在 US_NAME，例 clomifene→clomiphene、benzatropine→benztropine、rifampicin→rifampin）查 openFDA，只用單一成分、主成分名（去鹽類；酯類要相同，fluticasone furoate≠propionate）跟本頁相同的處方藥仿單；
  排序：全身給藥（外用仿單常寫「X 治療痤瘡的機轉不明」）→ 節錄夠長 → 有 12.1 → 有品牌名（多半是原廠）→ 版本新。美國只有複方仿單的（glecaprevir、sacubitril、tezacaftor…）只節錄 12.1 裡講本藥成分的句子並寫明是複方。
  節錄照原文 2～4 句（拿掉「[see …]」交互參照與段落編號，前幾句只講生理背景時以「[…]」接上第一句講到本藥的句子），不改寫。紀錄寫進 `docs/_data/drug_reviews.json`（action＝mechanism，id `nz-moa-<日期>-<藥>-1`，sources 附 url、accessed、excerpt、label_version、section、set_id）；美國仿單與 ChEMBL 都沒有的，列進 `docs/_data/drug_reviews_uncertain.json`（source＝mechanism of action，附原因）。已有紀錄的頁不重抓。`--no-chembl` 只查仿單（ChEMBL 慢時先跑），`--list` 只列仍寫「不可得」的頁。
- `scripts/apply_drug_reviews.py` 的 mechanism：只把「不可得」那句（句子後半是 AI 推論的，連接詞後的部分保留、首字母大寫）換成「According to the … section of the US FDA-approved prescribing information for <藥>: "…" ([DailyMed: …](…), label version …; accessed …)」，不加框；Review Log 列原句與出處。模型預測與其他正文不動；「To proceed」清單裡的「Detailed mechanism of action data」待辦條目不改。
- gate 第 21 項（`scripts/check_seo_docs.py`，函式在 build_mechanism_reviews.gate_errors）：公開頁仍寫「不可得」、既沒有 mechanism 紀錄也沒列待查的，擋下；有紀錄沒套上的由第 17 項擋。
- 注意：節錄的是美國仿單，劑型與適應症可能跟紐西蘭產品不同（例 calcitriol、clobetasol 取到外用仿單）；文字上已寫明「US FDA-approved prescribing information」。sync_notes_to_docs.py 整頁重寫後會重套紀錄（claim 是頁面原句）；報告重產、原句改寫後要再跑 build_mechanism_reviews.py。

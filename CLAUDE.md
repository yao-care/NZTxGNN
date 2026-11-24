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
- 2026-11-20 補做（照各站同日推出的版本更新本站 build_mechanism_reviews.py：偵測改成正文章節裡同時有作用機轉／資料／不可得字樣的短句，原 regex 漏掉「is not currently available」「The compiled data lack …」等寫法；同頁不只一句時每句各一筆紀錄（-1、-2…）；同頁別筆紀錄的更正文字與錨點句不當成「不可得」句；新增 `--fda-bulk <目錄>` 改讀 openFDA 全量下載檔，不吃 API 配額；選仿單規則不變）：
- 2026-11-20 試跑母目錄「交付前驗證」規則時補做：偵測改成原條件不動、另加新增寫法（L2，比中的再用 NOT 排除以缺資料為前提的模型推論），字樣用獨立、更寬的偵測全量掃出的候選句逐句標註校準（A 沒有漏、B 沒有誤中）；偵測漏抓的「不可得」句補 3 筆紀錄；節錄後面接著 AI 說明「上面描述依據」的句子（會讀成美國仿單的出處）12 筆改成那句在節錄前（原文不改，只換順序）；同頁重複引用同一段節錄的 1 筆改成指向上方節錄的一句（附同一個來源連結）；以上同 id 重發，前版存 history。程式另加 reorder_src_tail、同頁已引用同一段節錄時改指向上方（產生器 txgnn/.claude/moa_rollout_20261119/fix_20261120/）。
  ChEMBL 恢復可連，待查的 71 頁重跑，27 頁補上紀錄（ChEMBL 26、仿單 1），44 頁仍查不到（待查清單更新原因、標已查 ChEMBL；補上的 27 筆標 resolved）；新偵測另找到 33 頁漏掉的句子，30 頁補上仿單節錄、3 頁列待查。合計 395 筆紀錄、47 頁待查。gate 第 21 項改成「仍寫不可得、又沒列待查」就擋（不再只看有沒有紀錄）。
- 注意：節錄的是美國仿單，劑型與適應症可能跟紐西蘭產品不同（例 calcitriol、clobetasol 取到外用仿單）；文字上已寫明「US FDA-approved prescribing information」。sync_notes_to_docs.py 整頁重寫後會重套紀錄（claim 是頁面原句）；報告重產、原句改寫後要再跑 build_mechanism_reviews.py。

## 正文 Pharmac 品項張數由程式改寫（2026-11-24）

用戶 2026-11-24 決定（各站同日「許可證張數與狀態不再每週過期」）：每週許可證排程只改概覽表狀態列／張數列與程式表，正文寫死的張數會跟著 Pharmac 清單過期、讓 checker 擋頁。
本站市場段落的表 2026-11-06 起已是程式產生（`scripts/sync_license_tables.py`，`<!-- licenses:begin/end -->`），這次只做正文張數。
- `scripts/rewrite_license_counts.py`＋`src/nztxgnn/regulatory/count_prose.py`（照 AuTxGNN 移植，同為給付清單、英文句型）：依快照事實層改寫「three Pharmac Schedule items」「the 2 listings」「N of M …」這類句型明確的品項張數；
  有保留、講證據包或查詢、相對數、劑型／廠商／單方複方子集、fully subsidised 子集、Medsafe 或其他國家口徑、表格顯示筆數、partial 頁不動；同一行有判斷不了的整行不動；0／1 單複數、「only／all」前提句不改，`--list`／`--uncertain` 列待查。
  句子含 indication 等字樣（NOT_LICENSE）不當成張數句。不寫查核框（同 Au／Kr／My／Tw）；查核紀錄 replacement／anchor 所在的句子不改。
- 掛鉤：`apply_drug_reviews.py`（非 --check）結尾先重排程式表（`sync_license_tables.run`，沒有 data/bundles 的頁略過）再改寫張數；`build_license_snapshot.py` 寫完快照後同樣兩步（`--no-rewrite` 不跑；
  `license_rebuild_run.py` 轉紀錄前那次建快照用 `--no-rewrite`，免得改寫被當成刪改轉成紀錄）；母目錄排程在 checker 前跑 build_license_snapshot。gate 第 22 項擋「再跑一次會變」（2026-11-24 以假句實測會擋、改寫後通過）。
- 2026-11-24 全量量測（公開頁 461 頁，排除 noindex 7 頁、轉址頁）：正文張數句由程式改寫 0 句（11-06～11-12 刪句後正文剩的張數句都跟事實一致）、判斷不了 0 句；
  另寫的獨立寬偵測（同句有數字＋Pharmac／item／listing／licence 字樣）正文 29 句：多是 DDI「only entry」「one known use」等誤中，張數句（fluoxetine、framycetin、phenol、tenoxicam「two New Zealand licences」、polymyxin_b「Both products」）都等於事實；
  再掃「the only／one … product」：erythromycin「The only New Zealand product is an intravenous injection」（實為 3 個品項、都是靜脈注射）以 `nz-lic-20261124-erythromycin-1` 更正；
  hydrocortisone_acetate「The only local product is a rectal foam」（acetate 子集口徑）列待查。單方／複方說法（atomoxetine、dantrolene、metoprolol、montelukast、valsartan、glecaprevir）逐句對證據包成分欄都成立。
- 查核紀錄說明裡寫死的張數（`reviews_from_edits.figures()` 產生的「The Pharmaceutical Schedule lists N listings for this drug, N current; …」，510 筆、查核框顯示 509 處）同 id 重發成不寫數字的
  「The Quick Overview and the table under New Zealand Market Information show the current Pharmac Schedule figures.」，前版存 history；`figures()` 也改成這句，新紀錄不再寫死張數。resolved_by_regeneration 的 9 筆不顯示、不動。
  checked 日期保留原值（更正內容沒變），查核紀錄表註明 revised 2026-11-24；309 頁 last_modified_at 依「正文有變」改 2026-11-24。
- checker 全量前後都是 0；gate exit 0。

## 說明頁與證據等級頁的在地資料（2026-11-19）

用戶 2026-11-19 決定的第一批改版（母目錄 `.claude/gen_local_pages_20261119.py` 產生，不逐站手改）：說明頁（about／guide／sources／downloads／methodology，自有結構站只有證據等級頁與隱私頁）加本站資料區塊（`<!-- local-facts -->`）、證據等級頁的事實段與藥名表（`<!-- tier-facts -->`、`<!-- tier-table -->`）、隱私頁的適用法律／權利／著作權與資料來源段（`<!-- privacy-law -->`）；en／es／de 樣板站的共用長段落縮成一段（`<!-- local-condensed -->`）。
資料檔 `docs/_data/local_facts.json`、`docs/_data/evidence_tier_facts.json` 由 `scripts/local_facts.py`（＝母目錄 `.claude/local_facts_lib.py`）依藥物頁、許可證快照、查核紀錄、FHIR 算；`apply_drug_reviews.py`、許可證快照程式結尾會自動 `--write`，`check_seo_docs.py` 跑 `--check` 擋過期。跨站欄位（cross：本站獨有預測、30 站中位數）與頁面區塊只由母目錄產生器更新；藥物頁改等級、noindex 或預測後要在母目錄重跑。

## 藥物頁 description 句尾在地化（2026-11-19）

用戶 2026-11-19 決定（「藥物頁簡述在地化」，SEO 工具判定 30 站藥物頁近似重複，原因之一是 description 句尾都是同一句「原適應症：本地資料未記載」）：公開藥物頁（排除 noindex、轉址頁）front matter description 句尾的「Approved use: Not recorded in the local data」換成本站許可證快照事實層（`snapshots/*_licenses.json.gz`，經 `src/*/regulatory/licenses.py` 的 snapshot()）的品項數句子，資料名稱照本站口徑：Pharmac Schedule 品項（只寫筆數、不寫有效）。藥名、預測新適應症、等級字樣不動（母目錄 sync_description_levels.py 照舊對得到）；已有原適應症原文的頁不動。
例：abacavir「Approved use: Not recorded in the local data」→「In New Zealand: 2 Pharmac Schedule items」；2026-11-19 改了 368 頁。
由母目錄 `.claude/drug_meta_description.py --apply --site NZTxGNN` 產生（站上複本 `tools/txgnn/.claude/drug_meta_description.py --apply --root .`），改動記在 `docs/_data/description_local_log.json`（pages＝目前句尾與原字樣、entries＝每次改動前後整行，可還原）。快照數字變了重跑會換新數字；`apply_drug_reviews.py`、`build_license_snapshot.py` 結尾會自動 --apply，`check_seo_docs.py` 跑 --check 擋「還是未記載句尾」或「數字跟快照不符」。只改 front matter，lastmod_fix 不算正文改動、last_modified_at 不動。
同日修：`apply_drug_reviews.py`、`build_license_snapshot.py` 結尾重算 local_facts 的 atexit 掛鉤在結束時取不到 `__file__`（NameError，之前其實沒跑成），改成先算好路徑。

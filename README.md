# 彩券開獎爬蟲（GitHub Actions 排程版）

多來源交叉比對彩券開獎號碼，確認後自動寫入 repo 根目錄的 `data.json`，
給彩券系統 App 讀取顯示。改造自原本手動在 Windows 上跑的版本，現在改用
**GitHub Actions** 排程，不需要電腦開機也會自動執行。

## 這次改動了什麼

1. `lottery_scraper.py` 新增 `STATE_DIR` 環境變數支援：本機手動執行不設定就跟以前
   一樣把 `lottery.db` / `status.json` / `source_health.json` 存在腳本旁邊；
   GitHub Actions 執行時會設成 `state/`，執行完再把這個資料夾 commit 回 repo，
   讓「今天是否已抓過」「來源健康度」這些狀態能跨執行留存（見交接文件方案一）。
2. **新增「Google新聞搜尋」來源**：四種彩券各多一個用 Google 新聞 RSS 搜尋
   開獎報導的備援來源（不用申請 API key），跟其他來源一樣參與交叉比對，
   萬一原本 6-7 個查詢網站同時掛掉/改版，這個可以當額外一票。
   ⚠️ 新聞措辭比查詢網站的固定表格更不穩定，只涵蓋「開出／號碼：」這類常見
   句型，抓不到就跟其他來源失敗時一樣被略過，不影響整體運作，但建議正式
   上線後留意 log，看這個來源實際命中率如何，必要時再調整正則。
3. 新增 `.github/workflows/scrape-*.yml` 四個排程檔，各自在對應台灣時間
   （換算成 UTC）觸發，帶對應的 `--game` 參數執行，執行完自動 `git commit` +
   `push` 把 `state/` 資料夾寫回去。
4. 寫入 `data.json` 用的是 Actions 內建自動核發的 `GITHUB_TOKEN`
   （範圍僅限這個 repo），不需要用到你原本申請的 Personal Access Token。
   Workflow 已設定 `permissions: contents: write` 讓它可以寫入。

## 部署步驟

1. 把這個資料夾整個上傳 / push 到 `redwinds542688/Lottery-database` 這個 repo
   的 `main` 分支（跟現有的 `data.json`、`data/lottery-db.json` 放在一起就好，
   不會互相影響）。
2. 到 repo 的 **Settings → Actions → General → Workflow permissions**，確認選的是
   **「Read and write permissions」**（否則 workflow 內建的 `GITHUB_TOKEN`
   沒有寫入權限，`data.json` 跟 `state/` 都會寫入失敗）。
3. 不需要另外設定 Secrets——這個 workflow 完全用 Actions 自動核發的
   `GITHUB_TOKEN`，你原本的 Personal Access Token 用不到了（除非你想手動在
   本機跑 `lottery_scraper.py`，那時候才需要自己在環境變數設 `GITHUB_TOKEN`）。
4. push 上去之後，到 repo 的 **Actions** 分頁應該會看到四個 workflow
   （今彩539／大樂透／香港六合彩／加州天天樂）。可以先手動點
   **Run workflow**（`workflow_dispatch`）測試一次，確認能正常抓資料、
   寫入 `data.json`，也確認 `state/` 資料夾有被 commit 更新。

## 排程時間對照表

| 彩券 | 台灣時間 | UTC（cron） |
|---|---|---|
| 加州天天樂 | 11:10 | 03:10 → `10 3 * * *` |
| 今彩539 | 20:50 | 12:50 → `50 12 * * *` |
| 大樂透 | 21:10 | 13:10 → `10 13 * * *` |
| 香港六合彩 | 21:50 | 13:50 → `50 13 * * *` |

> 注意：GitHub Actions 的排程時間不是絕對準時，官方說明是「可能會延遲，
> 尤其在整點附近」，通常誤差在幾分鐘內，不影響程式邏輯（程式本身就會
> 每 5 分鐘重試、最多重試 30 分鐘）。

## 已知限制 / 之後可以再優化的地方

- `state/` 資料夾用 git commit 方式持久化，理論上如果兩個 workflow
  在極短時間內同時 push 可能會 race（目前四個排程時間都間隔數小時，
  機率極低），workflow 裡已經加了 `git pull --rebase` 降低衝突機率，
  真的衝突的話該次 job 會失敗，重新手動觸發即可，不影響已經確認並
  寫入 `data.json` 的資料。
- 如果之後想更簡化，「今天是否已抓過」這個檢查也可以改成直接讀
  GitHub 上 `data.json` 裡最新一筆的 `draw_date`／`checked_at` 來判斷，
  就可以不用依賴本機 SQLite、少一份要持久化的狀態檔案
  （`source_health.json` 因為要記錄「連續失敗天數」，還是需要跨執行留存）。

# -*- coding: utf-8 -*-
"""
每日開獎號碼爬蟲（多來源交叉比對版）

規則：
  - 每種彩券設定 3 個查詢來源
  - 從指定時間開始，每隔固定分鐘數重新抓取一次
  - 只要有 >= 2 個來源號碼完全一致，視為「確認無誤」，寫入 SQLite 資料庫
  - 若超過重試上限仍無法達成一致，記錄警告並停止（不寫入資料庫）

用法：
    python lottery_scraper.py --game 539
    python lottery_scraper.py --game marksix
    python lottery_scraper.py --game fantasy5
    python lottery_scraper.py --game lotto649

建議透過 cron / 工作排程器在對應時間各自啟動一次：
    加州天天樂 每天 11:10 啟動
    今彩539   每天 20:50 啟動
    香港六合彩 每天 21:50 啟動
    大樂透    每天 21:10 啟動

註：大樂透開獎號碼含 6 個正選號碼 + 1 個特別號，本程式交叉比對時
    只比對 6 個正選號碼（特別號解析較不穩定，暫不納入比對條件）。
"""

import argparse
import base64
import datetime
import email.utils
import json
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# STATE_DIR 可用環境變數覆寫（GitHub Actions 用這個把狀態檔案指到
# repo 裡會被 commit 回去的資料夾，讓 lottery.db / status.json /
# source_health.json 能跨執行留存；本機手動執行不設定就跟以前一樣存在腳本旁邊）
STATE_DIR = os.environ.get("STATE_DIR") or BASE_DIR
os.makedirs(STATE_DIR, exist_ok=True)
DB_PATH = os.path.join(STATE_DIR, "lottery.db")
STATUS_PATH = os.path.join(STATE_DIR, "status.json")
SOURCE_HEALTH_PATH = os.path.join(STATE_DIR, "source_health.json")
SOURCE_DISABLE_DAYS = 30  # 同一個來源連續失敗超過這麼多天，自動停用

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

# 2026-08-27 起改成「單次嘗試」設計：每次執行只抓一輪就結束，不在程式內部
# 重試等待（原本的 RETRY_INTERVAL_SECONDS / MAX_RETRY_HOURS 迴圈重試機制已移除）。
# 重試改交給 GitHub Actions 排程「每小時觸發一次」負責，這一輪沒抓到，
# 下個整點排程會自動再抓一次，直到某次成功為止。
#
# 後來（同一天稍晚）又加了一個例外：如果今天是該彩券「常見的開獎日」
# （見 GAME_CONFIG 的 typical_draw_weekdays），本輪會在這裡設定的時間
# 上限內反覆重試（而不是只抓 1 次），提高抓到的機會；不是常見開獎日的
# 話，才維持原本的單次嘗試。
DRAW_DAY_SEARCH_MINUTES = 5          # 開獎日當天，本輪最多搜尋幾分鐘
DRAW_DAY_RETRY_INTERVAL_SECONDS = 50  # 開獎日當天，搜尋期間每隔幾秒重試一次
REQUIRED_AGREEING_SOURCES = 2     # 備援路徑用：找不到「單一來源日期+差異」確認時，至少要幾個來源一致才算數

# GitHub Actions 執行環境的系統時區預設是 UTC，不是台灣時間。如果直接呼叫
# Python 內建的 datetime.date.today()，在台灣時間凌晨 0 點到早上 8 點之間
# 執行（例如手動觸發測試）時，會誤判成「昨天」，導致日期新鮮度比對出錯。
# 所以全部「今天日期」的判斷都要透過下面這個 taiwan_today() 函式取得，
# 不要再直接呼叫 datetime.date.today()。用固定 +8 小時位移計算日期，
# 不依賴系統時區設定，也不需要額外安裝 tzdata。
TAIWAN_TZ = datetime.timezone(datetime.timedelta(hours=8))


def taiwan_today():
    """回傳台灣目前的日期（UTC+8），不受執行環境系統時區影響。"""
    return datetime.datetime.now(TAIWAN_TZ).date()


def taiwan_now_str():
    """回傳台灣目前的日期時間字串（YYYY-MM-DD HH:MM:SS），不受執行環境
    系統時區影響。GitHub Actions 系統時間預設是 UTC，如果直接用
    datetime.datetime.now()，寫進 checked_at / log 時間戳記 / status.json
    的 updated_at 都會是 UTC 時間，跟台灣時間差 8 小時，容易誤導看資料
    的人。所以這些地方全部改用這個函式取得時間字串。"""
    return datetime.datetime.now(TAIWAN_TZ).strftime("%Y-%m-%d %H:%M:%S")


# --- 雲端儲存設定（2026-08-25 由 Railway 改成 GitHub） ---------------------
# GITHUB_REPO 格式："帳號名稱/repo名稱"，例如 "redwi/lottery-data"
# GITHUB_TOKEN 是有該 repo 讀寫權限的 Personal Access Token（設定方式見 HANDOFF）
# GITHUB_BRANCH / GITHUB_DATA_PATH 通常不用改，有特殊需求才需要設定
GITHUB_REPO = os.environ.get("GITHUB_REPO", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
GITHUB_DATA_PATH = os.environ.get("GITHUB_DATA_PATH", "data.json")

GITHUB_API_BASE = "https://api.github.com"
GITHUB_MAX_RECORDS_PER_GAME = 1000  # 避免 data.json 無限長大，每種彩券只保留最新這麼多筆
GITHUB_UPLOAD_MAX_RETRIES = 3       # 遇到別的請求同時改檔案（409衝突）時的重試次數


def _github_headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url():
    return f"{GITHUB_API_BASE}/repos/{GITHUB_REPO}/contents/{GITHUB_DATA_PATH}"


def _github_fetch_data():
    """讀取 GitHub repo 上目前的 data.json，回傳 (data_dict, sha)。
    檔案還不存在的話回傳 ({}, None)。"""
    resp = requests.get(
        _github_contents_url(),
        headers=_github_headers(),
        params={"ref": GITHUB_BRANCH},
        timeout=15,
    )
    if resp.status_code == 404:
        return {}, None
    resp.raise_for_status()
    payload = resp.json()
    raw = base64.b64decode(payload["content"])
    try:
        data = json.loads(raw.decode("utf-8")) if raw.strip() else {}
    except json.JSONDecodeError:
        data = {}
    return data, payload["sha"]


def _github_put_data(data, sha, commit_message):
    """把整份 data_dict 寫回 GitHub repo。sha=None 代表檔案還不存在（新建）。"""
    body = {
        "message": commit_message,
        "content": base64.b64encode(
            json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        ).decode("ascii"),
        "branch": GITHUB_BRANCH,
    }
    if sha:
        body["sha"] = sha
    resp = requests.put(
        _github_contents_url(),
        headers=_github_headers(),
        json=body,
        timeout=20,
    )
    return resp


def upload_to_cloud(game, period, draw_date, numbers, special, sources):
    """把確認過的號碼寫進 GitHub repo 裡的 data.json，讓手機也能讀到
    （直接開 raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}/{GITHUB_DATA_PATH}
    就能看到最新資料，不用另外架伺服器）。"""
    if not GITHUB_REPO or not GITHUB_TOKEN:
        return  # 沒設定 GITHUB_REPO / GITHUB_TOKEN 就跳過，只存本機 DB

    record = {
        "period": period,
        "draw_date": draw_date,
        "numbers": " ".join(str(n) for n in numbers),
        "special_number": str(special) if special is not None else "",
        "agreeing_sources": ", ".join(sources),
        "checked_at": taiwan_now_str(),
    }

    for attempt in range(1, GITHUB_UPLOAD_MAX_RETRIES + 1):
        try:
            data, sha = _github_fetch_data()
            game_records = data.setdefault(game, [])

            # 跟 Railway 版本一樣的去重規則：同一期別+同一組號碼就不重複加入
            # 去重規則（2026-08-27 修正）：只比對「號碼是否一樣」，不再要求
            # 期別也要一致。原本要求 period 也要相同，但早期版本的資料
            # 沒有存 period（空字串），跟後來版本抓到的同一期資料（有真正
            # 期別）拿去比對時，因為 period 對不起來（"" != "11979"），
            # 就算號碼完全相同也不會被判定成重複，導致同一期開獎被重複
            # 寫入兩筆、只是日期標示方式不同（可對照 fantasy5 實際發生的
            # 案例：MON/AUG 24 跟 2026-08-25 其實是同一期）。
            # 同一種彩券在合理時間範圍內開出完全相同的一組號碼，機率低到
            # 可以放心當作「同一期」處理，不需要 period 再加一層限制。
            is_duplicate = any(
                r.get("numbers") == record["numbers"] for r in game_records
            )
            if is_duplicate:
                log(f"GitHub 上已經有相同資料，略過上傳：{game} {numbers}")
                return

            game_records.insert(0, record)  # 新的放最前面
            data[game] = game_records[:GITHUB_MAX_RECORDS_PER_GAME]

            resp = _github_put_data(
                data, sha, commit_message=f"更新 {game} 開獎資料 {draw_date}"
            )
            if resp.status_code in (200, 201):
                log(f"已同步上傳到 GitHub：{game} {numbers}")
                return
            if resp.status_code == 409 and attempt < GITHUB_UPLOAD_MAX_RETRIES:
                log(f"GitHub 上傳衝突（可能同時有別的更新），第 {attempt} 次重試...")
                continue
            log(f"GitHub 上傳失敗（{resp.status_code}）：{resp.text}")
            return
        except Exception as e:
            log(f"GitHub 上傳失敗：{e}")
            return


def log(msg):
    now = taiwan_now_str()
    print(f"[{now}] {msg}")


def normalize_numbers(raw_list):
    """把任意格式的號碼字串轉成排序後的整數 tuple，方便比對。"""
    nums = [int(n) for n in raw_list if str(n).strip().isdigit()]
    return tuple(sorted(nums))


_MONTH_NAME_TO_NUM = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def normalize_draw_date(raw):
    """把各來源、各種格式的開獎日期字串，統一轉換成 datetime.date 物件。
    抓不到就回傳 None（代表這個來源沒提供可辨識的日期，呼叫端要當成
    「無法驗證」處理，不是「日期錯誤」）。

    做法：依序嘗試各種已知格式的規則，只要抓到「年、月、日」三個數字，
    就統一組成 datetime.date 回傳；每種格式的分隔符號、順序不同沒關係，
    只要能抓出這三個數字就算成功辨識。

    支援格式範例：
    - 2026-08-25、2026-08-25T00:00:00（ISO，含時間也可以）
    - 2026/08/25
    - 2026年8月25日（西元年）
    - 115年8月25日（民國年，2-3 碼＋「年」，自動 +1911 換算成西元年；
      奧索樂透網、大樂透民間來源偶爾會用這種格式）
    - TUE/AUG 25, 2026、AUG 25, 2026、August 25, 2026（月份名在前，
      加州彩券官網等英文來源）
    - 25 August 2026（日期在前、月份名在後，lottery.hk 香港來源）
    - 8/25/2026（美式 M/D/YYYY，年份放最後，加州天天樂的美國來源）
      注意：這個格式本身無法區分「月/日」還是「日/月」，這裡固定當成
      美式 M/D/YYYY 解讀，因為目前唯一會用到這個格式的都是美國網站；
      如果未來有台灣/香港來源也用「數字/數字/年份放最後」這種格式、
      但其實是日/月順序，會被解析錯誤，屆時要另外處理。
    """
    if not raw:
        return None
    s = str(raw).strip()

    # 格式一：西元年在前，用 -／年 分隔（後面接時間或中文「日」都不影響辨識）
    m = re.search(r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})", s)
    if m:
        try:
            return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None

    # 格式二：民國年在前，例如 "115年8月25日"（2-3 碼數字＋「年」，換算 +1911）
    m = re.search(r"(?<!\d)(\d{2,3})年(\d{1,2})月(\d{1,2})日", s)
    if m:
        try:
            return datetime.date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None

    # 格式三：英文月份名稱在前（縮寫或全名皆可），例如 "TUE/AUG 25, 2026"
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", s)
    if m:
        month = _MONTH_NAME_TO_NUM.get(m.group(1)[:3].upper())
        if month:
            try:
                return datetime.date(int(m.group(3)), month, int(m.group(2)))
            except ValueError:
                return None

    # 格式四：日期在前、英文月份名稱在後，例如 "25 August 2026"（lottery.hk）
    m = re.search(r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})", s)
    if m:
        month = _MONTH_NAME_TO_NUM.get(m.group(2)[:3].upper())
        if month:
            try:
                return datetime.date(int(m.group(3)), month, int(m.group(1)))
            except ValueError:
                return None

    # 格式五：美式 M/D/YYYY（年份放最後，純數字），例如 "8/25/2026"
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        try:
            return datetime.date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            return None

    return None


# ---------------------------------------------------------------------------
# 共用工具：pilio.idv.tw / lotto-8.com（同一個「樂透雲」後端資料庫，
# 但網域不同、伺服器不同，仍當作獨立來源）與 twlottery.in 這幾個
# 台灣本地網站共用的表格/文字解析邏輯
# ---------------------------------------------------------------------------
_WEEKDAY_MAP = {
    "一": 0, "二": 1, "三": 2, "四": 3, "五": 4, "六": 5, "日": 6,
    "MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6,
}


def _resolve_ambiguous_date(field_a, field_b, weekday_text, year):
    """欄位是兩個數字（順序可能是月/日或日/月，不同網站不一定），
    用星期幾反推正確組合，避免猜錯月日順序。"""
    weekday_key = weekday_text.strip().upper()
    target = _WEEKDAY_MAP.get(weekday_key)
    if target is None:
        return None
    candidates = []
    for month, day in [(field_a, field_b), (field_b, field_a)]:
        try:
            d = datetime.date(year, int(month), int(day))
            if d.weekday() == target:
                candidates.append(d)
        except ValueError:
            continue
    if len(candidates) == 1:
        return candidates[0].strftime("%Y-%m-%d")
    return None


def fetch_pilio_latest(url, num_count, has_special):
    """
    抓 pilio.idv.tw / lotto-8.com 這類「樂透雲」系列網站，取最新一期（頁面第一筆）。
    回傳 (period, draw_date, numbers_tuple, special)
    """
    resp = requests.get(url, headers=HEADERS, timeout=15, verify=False)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    current_year = taiwan_today().year
    date_field_re = re.compile(r"(\d{1,2})/(\d{1,2})\s+(\d{2})\(([^)]+)\)")

    for tr in soup.find_all("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(tds) < 2:
            continue
        m = date_field_re.search(tds[0])
        if not m:
            continue
        a, b, yy, weekday_text = m.groups()
        year = 2000 + int(yy)
        iso_date = _resolve_ambiguous_date(a, b, weekday_text, year)
        if not iso_date:
            continue

        nums = re.findall(r"\d{1,2}", tds[1])
        special = None
        if has_special and len(tds) >= 3:
            special_nums = re.findall(r"\d{1,2}", tds[2])
            if special_nums:
                special = int(special_nums[0])

        if len(nums) >= num_count:
            return "", iso_date, normalize_numbers(nums[:num_count]), special

    raise ValueError("解析失敗，網站結構可能已改版（pilio/lotto-8系列）")


def fetch_twlottery_latest(url, num_count, has_special):
    """
    抓 twlottery.in 年度歷史頁面（例如 /lotteryHK/list/2026），取最新一期（頁面第一筆）。
    回傳 (period, draw_date, numbers_tuple, special)
    """
    resp = requests.get(url, headers=HEADERS, timeout=15, verify=False)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    date_re = re.compile(r"^(\d{4})\.(\d{2})\.(\d{2})\s*\([一二三四五六日]\)$")

    def is_period_token(s):
        return s == "期" or "｜" in s or (s.isdigit() and len(s) >= 3)

    def is_number_token(s):
        return s.isdigit() and len(s) <= 2

    total_needed = num_count + 1 if has_special else num_count
    i = 0
    while i < len(lines):
        m = date_re.match(lines[i])
        if m:
            iso_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            j = i + 1
            while j < len(lines) and is_period_token(lines[j]):
                j += 1
            numbers = []
            while j < len(lines) and is_number_token(lines[j]) and len(numbers) < total_needed:
                numbers.append(lines[j])
                j += 1
            if len(numbers) >= num_count:
                special = int(numbers[num_count]) if has_special and len(numbers) > num_count else None
                return "", iso_date, normalize_numbers(numbers[:num_count]), special
            i = j
        else:
            i += 1

    raise ValueError("解析失敗，網站結構可能已改版（twlottery.in）")


# ---------------------------------------------------------------------------
# 新聞來源（備援）：用 Google 新聞 RSS 搜尋開獎報導
# 2026-08-25 新增：查詢網站若同時掛掉/改版時的額外獨立來源，不用申請 API key。
# 注意：新聞措辭比查詢網站的固定表格更不穩定，這裡只涵蓋常見的「開出／號碼：」
# 句型，抓不到就 raise ValueError，跟其他來源一樣會被 try_cross_check 當成失敗處理，
# 不影響其餘來源正常運作。
# ---------------------------------------------------------------------------
def _search_news_for_numbers(query, num_count, has_special=False):
    """搜尋 Google 新聞 RSS，回傳 (period, draw_date, numbers, special)。
    今彩539 / 加州天天樂用 num_count=5, has_special=False；
    大樂透 / 香港六合彩用 num_count=6, has_special=True。"""
    url = (
        "https://news.google.com/rss/search"
        f"?q={requests.utils.quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    )
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    items = root.findall(".//item")
    if not items:
        raise ValueError("新聞搜尋沒有結果")

    date_re = re.compile(r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})")
    total_needed = num_count + 1 if has_special else num_count
    # 常見報導句型：「...開出 3、9、19、23、26」「號碼：03 09 19 23 26」
    numbers_re = re.compile(
        r"(?:開出|號碼[:：]?)\s*((?:\d{1,2}\D+){" + str(total_needed - 1) + r"}\d{1,2})"
    )

    for item in items[:10]:  # 只看最新前 10 篇，避免抓到過期報導
        title = item.findtext("title") or ""
        desc = item.findtext("description") or ""
        text = re.sub(r"<[^>]+>", " ", f"{title} {desc}")  # description 可能包 html 標籤

        m_num = numbers_re.search(text)
        if not m_num:
            continue
        all_numbers = re.findall(r"\d{1,2}", m_num.group(1))[:total_needed]
        if len(all_numbers) < num_count:
            continue

        m_date = date_re.search(text)
        if m_date:
            y, mo, d = m_date.groups()
            draw_date = f"{y}-{int(mo):02d}-{int(d):02d}"
        else:
            # 新聞常只寫「今天」沒有完整日期，退而求其次用發稿時間當開獎日；
            # 如果連發稿時間都解析不出來，就留空字串，讓 normalize_draw_date
            # 回傳 None（代表「無法驗證日期」），不要冒充今天的日期 ——
            # 冒充今天會讓這個來源永遠通過日期新鮮度檢查，等於形同虛設。
            try:
                pub_dt = email.utils.parsedate_to_datetime(item.findtext("pubDate"))
                draw_date = pub_dt.strftime("%Y-%m-%d")
            except Exception:
                draw_date = ""

        special = None
        if has_special and len(all_numbers) >= total_needed:
            special = int(all_numbers[num_count])

        return "", draw_date, normalize_numbers(all_numbers[:num_count]), special

    raise ValueError("新聞內文找不到符合格式的號碼，報導寫法可能跟預期的句型不同")


def source_539_news():
    """今彩539：Google 新聞搜尋當備援來源"""
    return _search_news_for_numbers("今彩539 開獎", num_count=5, has_special=False)


def source_lotto649_news():
    """大樂透：Google 新聞搜尋當備援來源"""
    return _search_news_for_numbers("大樂透 開獎", num_count=6, has_special=True)


def source_marksix_news():
    """香港六合彩：Google 新聞搜尋當備援來源"""
    return _search_news_for_numbers("六合彩 開獎", num_count=6, has_special=True)


def source_fantasy5_news():
    """加州天天樂：Google 新聞搜尋當備援來源（英文關鍵字比較容易搜到美國媒體）"""
    return _search_news_for_numbers("California Fantasy 5 winning numbers", num_count=5, has_special=False)


WEEKDAY_ZH = ["一", "二", "三", "四", "五", "六", "日"]


def today_weekday_zh():
    return WEEKDAY_ZH[taiwan_today().weekday()]


# ---------------------------------------------------------------------------
# 來源健康度追蹤：同一個來源連續失敗超過 SOURCE_DISABLE_DAYS 天就自動停用
# ---------------------------------------------------------------------------
def _load_source_health():
    if not os.path.exists(SOURCE_HEALTH_PATH):
        return {}
    try:
        with open(SOURCE_HEALTH_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_source_health(data):
    try:
        tmp_path = SOURCE_HEALTH_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, SOURCE_HEALTH_PATH)
    except Exception as e:
        log(f"寫入 source_health.json 失敗：{e}")


def is_source_disabled(game_key, source_name):
    data = _load_source_health()
    entry = data.get(game_key, {}).get(source_name)
    return bool(entry and entry.get("disabled"))


def record_source_result(game_key, source_name, success):
    """記錄這次抓取成功/失敗，如果連續失敗超過 SOURCE_DISABLE_DAYS 天就自動停用該來源"""
    data = _load_source_health()
    game_data = data.setdefault(game_key, {})
    entry = game_data.setdefault(source_name, {
        "first_fail_date": None,
        "disabled": False,
        "last_success_date": None,
    })

    today_str = taiwan_today().strftime("%Y-%m-%d")

    if success:
        entry["last_success_date"] = today_str
        entry["first_fail_date"] = None
        if entry.get("disabled"):
            log(f"  提醒：來源「{source_name}」原本已停用，這次又抓成功了，"
                f"若確定恢復正常可以手動把 source_health.json 裡的 disabled 改回 false")
    else:
        if not entry.get("first_fail_date"):
            entry["first_fail_date"] = today_str
        else:
            first_fail = datetime.datetime.strptime(entry["first_fail_date"], "%Y-%m-%d").date()
            days_failing = (taiwan_today() - first_fail).days
            if days_failing >= SOURCE_DISABLE_DAYS and not entry.get("disabled"):
                entry["disabled"] = True
                log(f"  ⚠️ 來源「{source_name}」已連續失敗 {days_failing} 天（超過 {SOURCE_DISABLE_DAYS} 天），"
                    f"已自動停用，之後不會再嘗試抓取此來源，建議找一個新網站來源替換它")

    data[game_key] = game_data
    _save_source_health(data)


def update_status(game_key, **fields):
    """更新 status.json，供螢幕下方常駐顯示小工具讀取目前狀態。"""
    try:
        if os.path.exists(STATUS_PATH):
            with open(STATUS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {}
    except Exception:
        data = {}

    entry = data.get(game_key, {})
    entry.update(fields)
    entry["updated_at"] = taiwan_now_str()
    data[game_key] = entry

    try:
        tmp_path = STATUS_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, STATUS_PATH)  # 原子寫入，避免顯示端讀到寫一半的檔案
    except Exception as e:
        log(f"寫入 status.json 失敗：{e}")


# ---------------------------------------------------------------------------
# 資料庫
# ---------------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS draws (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game TEXT NOT NULL,
            period TEXT,
            draw_date TEXT,
            numbers TEXT NOT NULL,
            special_number TEXT,
            agreeing_sources TEXT NOT NULL,
            checked_at TEXT NOT NULL,
            UNIQUE(game, period, numbers)
        )
        """
    )
    # 相容舊版資料庫（如果之前建過沒有 special_number 欄位的表，補上去）
    cols = [row[1] for row in conn.execute("PRAGMA table_info(draws)").fetchall()]
    if "special_number" not in cols:
        conn.execute("ALTER TABLE draws ADD COLUMN special_number TEXT")
    conn.commit()
    return conn


def save_confirmed(conn, game, period, draw_date, numbers, special, sources):
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO draws
                (game, period, draw_date, numbers, special_number, agreeing_sources, checked_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                game,
                period,
                draw_date,
                " ".join(str(n) for n in numbers),
                str(special) if special is not None else "",
                ", ".join(sources),
                taiwan_now_str(),
            ),
        )
        conn.commit()
        log(f"已確認並存入本機資料庫：{game} {numbers}"
            f"{'（特別號：' + str(special) + '）' if special is not None else ''}"
            f"（來源：{', '.join(sources)}）")
    except Exception as e:
        log(f"寫入本機資料庫失敗：{e}")

    upload_to_cloud(game, period, draw_date, numbers, special, sources)


# ---------------------------------------------------------------------------
# 今彩539 三個來源
# ---------------------------------------------------------------------------
def source_539_official():
    """台彩官方 API
    2026-08-25 修正：原本的網址 .../DailyCashResult 已經失效（實測回應 404），
    正確路徑要帶 month 參數，欄位也不是 dailyCashResult/drawNumberAppear，
    已對照官方目前實際格式（content.daily539Res / drawNumberSize）修正。"""
    today = taiwan_today()
    url = (
        "https://api.taiwanlottery.com/TLCAPIWeB/Lottery/Daily539Result"
        f"?period&month={today.year}-{today.month:02d}&pageSize=31"
    )
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("content", {}).get("daily539Res") or []
    if not results:
        raise ValueError("空資料")
    latest = results[0]
    period = str(latest.get("period"))
    draw_date = latest.get("lotteryDate")
    numbers = latest.get("drawNumberSize")
    return period, draw_date, normalize_numbers(numbers), None


def source_539_i539tw():
    """i539.tw 民間查詢網（同步台彩開獎）"""
    url = "https://i539.tw/"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{4}/\d{2}/\d{2}).{0,20}?(\d{2}\D+\d{2}\D+\d{2}\D+\d{2}\D+\d{2})", text)
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    numbers = re.findall(r"\d{2}", m.group(2))
    return "", draw_date, normalize_numbers(numbers), None


def source_539_auzonet():
    """奧索樂透網"""
    url = "https://lotto.auzonet.com/daily539"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{2,3}年\d{1,2}月\d{1,2}日|\d{4}-\d{2}-\d{2}).{0,30}?(\d{1,2}\D+\d{1,2}\D+\d{1,2}\D+\d{1,2}\D+\d{1,2})", text)
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    numbers = re.findall(r"\d{1,2}", m.group(2))[:5]
    return "", draw_date, normalize_numbers(numbers), None


def source_539_pilio():
    return fetch_pilio_latest("https://www.pilio.idv.tw/lto539/list.asp", 5, False)


def source_539_lotto8():
    return fetch_pilio_latest("https://www.lotto-8.com/listlto539.asp", 5, False)


def source_539_arclink():
    """lotto2.arclink.com.tw 民間查詢網"""
    url = "https://lotto2.arclink.com.tw/539/"
    resp = requests.get(url, headers=HEADERS, timeout=15, verify=False)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2}).{0,30}?(\d{1,2}\D+\d{1,2}\D+\d{1,2}\D+\d{1,2}\D+\d{1,2})", text)
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    numbers = re.findall(r"\d{1,2}", m.group(2))[:5]
    return "", draw_date, normalize_numbers(numbers), None


def source_539_twlottery():
    current_year = taiwan_today().year
    return fetch_twlottery_latest(f"https://twlottery.in/lottery539/list/{current_year}", 5, False)


SOURCES_539 = [
    ("台彩官方API", source_539_official),
    ("i539.tw", source_539_i539tw),
    ("奧索樂透網", source_539_auzonet),
    ("pilio.idv.tw", source_539_pilio),
    ("lotto-8.com", source_539_lotto8),
    ("arclink.com.tw", source_539_arclink),
    ("twlottery.in", source_539_twlottery),
    ("Google新聞搜尋", source_539_news),
]


# ---------------------------------------------------------------------------
# 香港六合彩 三個來源
# ---------------------------------------------------------------------------
def source_marksix_hkjc():
    """香港賽馬會 getJSON 端點"""
    today = taiwan_today()
    start = today - datetime.timedelta(days=10)
    url = "https://bet.hkjc.com/marksix/getJSON.aspx"
    params = {"sd": start.strftime("%Y%m%d"), "ed": today.strftime("%Y%m%d"), "sb": 0}
    resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    text = resp.text
    dates = re.findall(r'"date":"(.*?)"', text)
    nums = re.findall(r'"no":"(.*?)"', text)
    draws = re.findall(r'"draw":"(.*?)"', text)
    if not nums:
        raise ValueError("空資料，端點可能已失效")
    all_numbers = re.split(r"[,+]", nums[-1])
    main_numbers = normalize_numbers(all_numbers[:6])
    special = int(all_numbers[6]) if len(all_numbers) >= 7 and all_numbers[6].strip().isdigit() else None
    return (draws[-1] if draws else ""), (dates[-1] if dates else ""), main_numbers, special


def source_marksix_lotteryhk():
    """lottery.hk 民間查詢網"""
    url = "https://lottery.hk/en/mark-six/results/"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{4}-\d{2}-\d{2}|\d{1,2}\s\w+\s\d{4}).{0,60}?((?:\d{1,2}\D+){5,6}\d{1,2})", text)
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    all_numbers = re.findall(r"\d{1,2}", m.group(2))[:7]
    special = int(all_numbers[6]) if len(all_numbers) >= 7 else None
    return "", draw_date, normalize_numbers(all_numbers[:6]), special


def source_marksix_marksixlotterynumbers():
    """marksixlotterynumbers.hk 民間查詢網"""
    url = "https://www.marksixlotterynumbers.hk/"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{4}年\d{1,2}月\d{1,2}日).{0,60}?((?:\d{1,2}\D+){5,6}\d{1,2})", text)
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    all_numbers = re.findall(r"\d{1,2}", m.group(2))[:7]
    special = int(all_numbers[6]) if len(all_numbers) >= 7 else None
    return "", draw_date, normalize_numbers(all_numbers[:6]), special


def source_marksix_pilio():
    return fetch_pilio_latest("https://www.pilio.idv.tw/ltohk/list.asp", 6, True)


def source_marksix_lotto8():
    return fetch_pilio_latest("https://www.lotto-8.com/listltohkbbk.asp", 6, True)


def source_marksix_twlottery():
    current_year = taiwan_today().year
    return fetch_twlottery_latest(f"https://twlottery.in/lotteryHK/list/{current_year}", 6, True)


def source_marksix_9800():
    """9800.com.tw 六合彩開獎查詢頁
    2026-08-25 修正：原本網址 http://www.9800.com.tw/lotto649/ 其實是台灣大樂透頁面（設錯了！），
    正確的香港六合彩頁面是 http://www.9800.com.tw/lotto6/，表格格式也不同，改用專屬解析邏輯。"""
    url = "http://www.9800.com.tw/lotto6/"
    resp = requests.get(url, headers=HEADERS, timeout=15, verify=False)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(
        r"(\d{6})\s+(\d{4}-\d{2}-\d{2})\s+((?:\d{1,2}\s+){5}\d{1,2})\s*\+\s*(\d{1,2})",
        text,
    )
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版（9800.com.tw）")
    period, draw_date, nums_str, special = m.groups()
    numbers = re.findall(r"\d{1,2}", nums_str)
    return period, draw_date, normalize_numbers(numbers), int(special)


SOURCES_MARKSIX = [
    ("HKJC官方", source_marksix_hkjc),
    ("lottery.hk", source_marksix_lotteryhk),
    ("marksixlotterynumbers.hk", source_marksix_marksixlotterynumbers),
    ("pilio.idv.tw", source_marksix_pilio),
    ("lotto-8.com", source_marksix_lotto8),
    ("twlottery.in", source_marksix_twlottery),
    ("9800.com.tw", source_marksix_9800),
    ("Google新聞搜尋", source_marksix_news),
]


# ---------------------------------------------------------------------------
# 加州天天樂 (Fantasy 5) 三個來源
# ---------------------------------------------------------------------------
def source_fantasy5_official():
    """加州彩券官網"""
    url = "https://www.calottery.com/en/draw-games/fantasy-5"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n", strip=True)
    draw_match = re.search(r"Draw #(\d+)", text)
    date_match = re.search(r"([A-Z]{3}/[A-Z]{3}\s\d{1,2},\s\d{4})", text)
    if not draw_match:
        raise ValueError("解析失敗，網站結構可能已改版")
    idx = text.find(draw_match.group(0))
    window = text[idx: idx + 200]
    numbers = re.findall(r"\b\d{1,2}\b", window)[:5]
    return draw_match.group(1), (date_match.group(1) if date_match else ""), normalize_numbers(numbers), None


def source_fantasy5_lotteryusa():
    """lotteryusa.com 民間查詢網"""
    url = "https://www.lotteryusa.com/california/fantasy-5/"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"Latest numbers(.{0,120}?)Est\. jackpot", text)
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    chunk = m.group(1)
    digits = re.sub(r"[^\d]", "", chunk.split(",", 1)[-1] if "," in chunk else chunk)
    numbers = [digits[i:i + 2] for i in range(0, min(len(digits), 10), 2)]
    if len(numbers) < 5:
        raise ValueError("號碼解析數量不足，需人工確認格式")
    return "", "", normalize_numbers(numbers), None


def source_fantasy5_lotterynet():
    """lottery.net 民間查詢網"""
    url = "https://www.lottery.net/california/fantasy-5/numbers"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4}).{0,60}?((?:\d{1,2}\D+){4}\d{1,2})", text)
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    numbers = re.findall(r"\d{1,2}", m.group(2))[:5]
    return "", draw_date, normalize_numbers(numbers), None


def source_fantasy5_lotto8():
    return fetch_pilio_latest("https://www.lotto-8.com/USA/listltoFT5.asp", 5, False)


def source_fantasy5_twlottery():
    current_year = taiwan_today().year
    return fetch_twlottery_latest(f"https://twlottery.in/lotteryCA5/list/{current_year}", 5, False)


def source_fantasy5_pilio():
    """pilio.idv.tw 美國 Fantasy5 頁面（若網址結構跟 HK/539 系列一致）"""
    return fetch_pilio_latest("https://www.pilio.idv.tw/USA/listFT5.asp", 5, False)


def source_fantasy5_lotteryextreme():
    """lotteryextreme.com 民間查詢網"""
    url = "https://www.lotteryextreme.com/california/fantasy5-results"
    resp = requests.get(url, headers=HEADERS, timeout=15, verify=False)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4}).{0,40}?((?:\d{1,2}\D+){4}\d{1,2})", text)
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    numbers = re.findall(r"\d{1,2}", m.group(2))[:5]
    return "", draw_date, normalize_numbers(numbers), None


SOURCES_FANTASY5 = [
    ("加州彩券官網", source_fantasy5_official),
    ("lotteryusa.com", source_fantasy5_lotteryusa),
    ("lottery.net", source_fantasy5_lotterynet),
    ("lotto-8.com", source_fantasy5_lotto8),
    ("twlottery.in", source_fantasy5_twlottery),
    ("pilio.idv.tw", source_fantasy5_pilio),
    ("lotteryextreme.com", source_fantasy5_lotteryextreme),
    ("Google新聞搜尋", source_fantasy5_news),
]


# ---------------------------------------------------------------------------
# 大樂透 三個來源
# ---------------------------------------------------------------------------
def source_lotto649_official():
    """台彩官方 API
    2026-08-25 修正：原本網址沒帶 month 參數，官方API會回傳 totalSize=0（永遠查不到資料）。
    另外原本用 drawNumberAppear 當「6個正選號碼」，但這個欄位實際上是7個數字
    （6正選+1特別號，且是開獎順序未排序），會把特別號也一起誤判成正選號碼去跟其他來源比對；
    specialNumber 這個欄位官方API也根本不存在，特別號永遠抓不到。
    已改用 drawNumberSize，並比照 backfill.py 的作法把前6碼當正選、第7碼當特別號分開處理。"""
    today = taiwan_today()
    url = (
        "https://api.taiwanlottery.com/TLCAPIWeB/Lottery/Lotto649Result"
        f"?period&month={today.year}-{today.month:02d}&pageSize=31"
    )
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    content = data.get("content", {})
    results = content.get("lotto649Res") or []
    if not results:
        raise ValueError("空資料")
    latest = results[0]
    period = str(latest.get("period"))
    draw_date = latest.get("lotteryDate")
    all_numbers = latest.get("drawNumberSize") or []
    main_numbers = all_numbers[:6]
    special_raw = all_numbers[6] if len(all_numbers) >= 7 else None
    special = int(special_raw) if special_raw is not None and str(special_raw).strip().isdigit() else None
    return period, draw_date, normalize_numbers(main_numbers), special


def source_lotto649_i539tw():
    """i539.tw 民間查詢網（同步收錄大樂透）"""
    url = "https://i539.tw/lotto649"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(
        r"(\d{4}/\d{2}/\d{2}).{0,30}?((?:\d{1,2}\D+){6}\d{1,2})", text
    )
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    all_numbers = re.findall(r"\d{1,2}", m.group(2))[:7]
    special = int(all_numbers[6]) if len(all_numbers) >= 7 else None
    return "", draw_date, normalize_numbers(all_numbers[:6]), special


def source_lotto649_auzonet():
    """奧索樂透網"""
    url = "https://lotto.auzonet.com/lotto649"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(
        r"(\d{2,3}年\d{1,2}月\d{1,2}日|\d{4}-\d{2}-\d{2}).{0,30}?((?:\d{1,2}\D+){6}\d{1,2})",
        text,
    )
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    all_numbers = re.findall(r"\d{1,2}", m.group(2))[:7]
    special = int(all_numbers[6]) if len(all_numbers) >= 7 else None
    return "", draw_date, normalize_numbers(all_numbers[:6]), special


def source_lotto649_pilio():
    return fetch_pilio_latest("https://www.pilio.idv.tw/ltobig/list.asp", 6, True)


def source_lotto649_lotto8():
    return fetch_pilio_latest("https://www.lotto-8.com/Taiwan/listltobig.asp", 6, True)


def source_lotto649_arclink():
    """lotto2.arclink.com.tw 民間查詢網"""
    url = "https://lotto2.arclink.com.tw/lotto/"
    resp = requests.get(url, headers=HEADERS, timeout=15, verify=False)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2}).{0,40}?((?:\d{1,2}\D+){5,6}\d{1,2})", text)
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    all_numbers = re.findall(r"\d{1,2}", m.group(2))[:7]
    special = int(all_numbers[6]) if len(all_numbers) >= 7 else None
    return "", draw_date, normalize_numbers(all_numbers[:6]), special


def source_lotto649_9800():
    """9800.com.tw 大樂透開獎查詢頁"""
    url = "http://www.9800.com.tw/html/a1/"
    resp = requests.get(url, headers=HEADERS, timeout=15, verify=False)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2}).{0,40}?((?:\d{1,2}\D+){5,6}\d{1,2})", text)
    if not m:
        raise ValueError("解析失敗，網站結構可能已改版")
    draw_date = m.group(1)
    all_numbers = re.findall(r"\d{1,2}", m.group(2))[:7]
    special = int(all_numbers[6]) if len(all_numbers) >= 7 else None
    return "", draw_date, normalize_numbers(all_numbers[:6]), special


SOURCES_LOTTO649 = [
    ("台彩官方API", source_lotto649_official),
    ("i539.tw", source_lotto649_i539tw),
    ("奧索樂透網", source_lotto649_auzonet),
    ("pilio.idv.tw", source_lotto649_pilio),
    ("lotto-8.com", source_lotto649_lotto8),
    ("arclink.com.tw", source_lotto649_arclink),
    ("9800.com.tw", source_lotto649_9800),
    ("Google新聞搜尋", source_lotto649_news),
]


GAME_CONFIG = {
    # typical_draw_weekdays：這個彩券「正常情況下」的開獎星期幾，用 Python
    # date.weekday() 的編號（週一=0 ... 週日=6）。用來決定當天要不要多花
    # 時間搜尋（見下方 run_until_confirmed 的說明）：
    #   - 是這個彩券常見開獎日 → 本輪最多搜尋 DRAW_DAY_SEARCH_MINUTES 分鐘，
    #     每隔一小段時間重試一次，提高抓到的機會。
    #   - 不是常見開獎日（含過年加開這種例外情況）→ 維持單次嘗試就好，
    #     跟原本沒改過的行為一樣，不會變得更差，只是不會額外多花力氣。
    #   - 設成 None 代表「每天都當作可能的開獎日」，固定都用比較久的搜尋
    #     （適用開獎日不固定、或本來就每天開獎的彩券）。
    "539": {"name": "今彩539", "sources": SOURCES_539, "typical_draw_weekdays": {0, 1, 2, 3, 4, 5}},
    # 香港六合彩：開獎日是週二、週四固定，加上週六或週日擇一（依當週賽馬
    # 日而定，無法單純用星期幾判斷），沒辦法安全縮小成固定的星期集合，
    # 所以設成 None，每天都用比較久的搜尋，寧可多花一點資源也不要漏抓。
    "marksix": {"name": "香港六合彩", "sources": SOURCES_MARKSIX, "typical_draw_weekdays": None},
    # 加州天天樂：不同來源對「開獎日期」的標示慣例不一樣，不能整個彩券
    # 套用同一個時差校正——
    #   - 「加州彩券官網」等美國網站標示的是美國加州當地日期，開獎在
    #     美西晚間，換算到台灣已經是隔天，需要 +1 天校正。
    #   - lotto-8.com / twlottery.in / pilio.idv.tw 這類台灣的樂透雲鏡像
    #     站，或是過去用過的 sc888.net，標示的可能已經是「亞洲這邊看到
    #     結果的日期」（本身就已經是校正過的），如果再 +1 天會校正過頭、
    #     反而對不起來。這類尚未實際驗證過慣例的來源，一律不校正（0）。
    # 所以改成「source_date_offset_days」逐一設定每個來源要不要校正，
    # 沒列出來的來源預設不校正。
    # 天天樂每天都開獎，typical_draw_weekdays 也設 None（每天都當開獎日）。
    "fantasy5": {
        "name": "加州天天樂",
        "sources": SOURCES_FANTASY5,
        "typical_draw_weekdays": None,
        "source_date_offset_days": {
            "加州彩券官網": 1,
            "lottery.net": 1,
            "lotteryextreme.com": 1,
        },
    },
    # 大樂透平常只有週二（1）、週五（4）開獎
    "lotto649": {"name": "大樂透", "sources": SOURCES_LOTTO649, "typical_draw_weekdays": {1, 4}},
}


# ---------------------------------------------------------------------------
# 交叉比對邏輯
# ---------------------------------------------------------------------------
def try_cross_check(game_key, conn):
    """呼叫該遊戲所有「未被停用」的來源，回傳 (period, draw_date, numbers, special, agreeing_source_names) 或 None。

    確認規則（2026-08-26 調整，把「至少 2 個來源一致」放寬成主要用單一來源驗證）：
    - 主要路徑：只要有 1 個來源同時符合
        1) 開獎日期可辨識，且等於今天
        2) 抓到的號碼跟資料庫裡「最新一筆」（不限日期）不同
      就視為最新一期已確認，不用等其他來源一致。
    - 備援路徑：如果沒有任何單一來源同時通過上述兩項（例如今天還沒開獎、
      或抓到的日期都無法辨識），才退回舊邏輯：只要有 REQUIRED_AGREEING_SOURCES
      個以上來源號碼彼此一致，也視為確認（一樣會做日期新鮮度檢查，避免多個
      來源剛好都回傳同一份過期快取）。
    """
    cfg = GAME_CONFIG[game_key]
    fetched = []
    for name, fn in cfg["sources"]:
        if is_source_disabled(game_key, name):
            continue  # 已因連續失敗超過一個月被自動停用，跳過不再嘗試
        try:
            period, draw_date, numbers, special = fn()
            if numbers:
                fetched.append((name, period, draw_date, numbers, special))
                log(f"  {cfg['name']} - {name}：{numbers}"
                    f"{'（特別號 ' + str(special) + '）' if special is not None else ''}")
                record_source_result(game_key, name, success=True)
            else:
                record_source_result(game_key, name, success=False)
        except Exception as e:
            log(f"  {cfg['name']} - {name} 抓取失敗：{e}")
            record_source_result(game_key, name, success=False)

    active_count = sum(1 for name, _ in cfg["sources"] if not is_source_disabled(game_key, name))
    if active_count < len(cfg["sources"]):
        disabled_count = len(cfg["sources"]) - active_count
        log(f"  （目前有 {disabled_count} 個來源已因長期失敗被停用，剩 {active_count} 個來源在使用）")

    if not fetched:
        log("  本輪所有來源都抓取失敗，無法比對")
        return None

    from collections import Counter
    from datetime import timedelta

    today = taiwan_today()
    source_offsets = cfg.get("source_date_offset_days", {})
    latest_numbers = get_latest_numbers(conn, cfg["name"])

    # 主要路徑：單一來源即可確認 —— 日期是今天，且號碼跟資料庫最新一期不同
    for name, period, draw_date, numbers, special in fetched:
        offset_days = source_offsets.get(name, 0)
        parsed_date = normalize_draw_date(draw_date)
        if parsed_date is not None and offset_days:
            parsed_date = parsed_date + timedelta(days=offset_days)
        if parsed_date != today:
            continue  # 日期無法辨識，或（校正時差後）不是今天，跳過這個來源
        if latest_numbers is not None and numbers == latest_numbers:
            continue  # 跟資料庫最新一期一樣，可能是舊資料快取，跳過這個來源
        log(f"  {cfg['name']} - {name}：日期驗證為今天（{today}）"
            f"{f'（此來源日期已 +{offset_days} 天校正時差）' if offset_days else ''}，"
            f"且號碼與資料庫最新一期不同，判定為最新資料，單一來源即可確認")
        return period, draw_date, numbers, special, [name]

    # 備援路徑：沒有任何單一來源同時通過「日期＋差異」驗證時，退回舊邏輯——
    # REQUIRED_AGREEING_SOURCES 個以上來源號碼彼此一致，也視為確認
    counter = Counter(item[3] for item in fetched)
    best_numbers, count = counter.most_common(1)[0]
    if count < REQUIRED_AGREEING_SOURCES:
        log("  沒有來源同時通過「日期＋資料差異」驗證，也沒有足夠來源號碼一致，本輪無法確認")
        return None

    agreeing = [item for item in fetched if item[3] == best_numbers]
    period = next((p for _, p, _, _, _ in agreeing if p), "")
    draw_date = next((d for _, _, d, _, _ in agreeing if d), "")

    # 日期新鮮度檢查：避免多個來源剛好都回傳「同一份過期快取」被誤判為交叉確認成功。
    # 每個來源各自套用自己的時差校正（同一組一致來源裡，可能有的來源要
    # +1 天、有的不用），再看有沒有任一個校正後對得上今天。
    # 只有在至少一個一致來源提供了「可辨識」的日期時才檢查；若都無法辨識日期，
    # 代表這些來源本來就不提供日期資訊，不因此擋下確認（維持原本行為，避免卡死）。
    parsed_dates = []
    for src_name, _, d, _, _ in agreeing:
        parsed = normalize_draw_date(d)
        if parsed is not None:
            offset_days = source_offsets.get(src_name, 0)
            if offset_days:
                parsed = parsed + timedelta(days=offset_days)
            parsed_dates.append(parsed)
    if parsed_dates and today not in parsed_dates:
        stale_list = ", ".join(sorted({d.strftime("%Y-%m-%d") for d in parsed_dates}))
        log(f"  {cfg['name']}：{len(agreeing)} 個來源號碼一致，"
            f"但開獎日期是 {stale_list}，跟今天（{today}）對不起來，"
            f"判定為舊資料快取，本輪不予確認")
        return None

    # 特別號：在號碼一致的來源中，取出現次數最多的特別號值（可能有來源解析不到，忽略 None）
    specials = [s for _, _, _, _, s in agreeing if s is not None]
    special = Counter(specials).most_common(1)[0][0] if specials else None
    agreeing_names = [name for name, _, _, _, _ in agreeing]
    return period, draw_date, best_numbers, special, agreeing_names


def get_latest_numbers(conn, game):
    """取得該遊戲資料庫裡「最新一筆」（不限日期）的號碼，回傳排序後的整數 tuple；
    沒有任何紀錄則回傳 None。供 try_cross_check 判斷「這次抓到的號碼是不是新的」。"""
    row = conn.execute(
        """
        SELECT numbers FROM draws
        WHERE game = ?
        ORDER BY id DESC LIMIT 1
        """,
        (game,),
    ).fetchone()
    if not row or not row[0]:
        return None
    return normalize_numbers(row[0].split())


def already_confirmed_today(conn, game):
    """檢查該遊戲今天是否已經有確認成功的紀錄，避免重複抓取。"""
    today = taiwan_today().strftime("%Y-%m-%d")
    row = conn.execute(
        """
        SELECT id, numbers, special_number, checked_at FROM draws
        WHERE game = ? AND date(checked_at) = ?
        ORDER BY id DESC LIMIT 1
        """,
        (game, today),
    ).fetchone()
    return row


def run_until_confirmed(game_key):
    """單次嘗試設計（2026-08-27 調整，同一天稍晚再加開獎日例外）：
    每次執行預設只抓一輪、不在程式內部重試等待。原本的「30 分鐘內每 5 分鐘
    重試」邏輯，改成交給外層 GitHub Actions 排程「每小時觸發一次」來負責
    重試——這一輪沒抓到就直接結束，下個整點排程會自動再抓一次。因為前面
    已經有「今天已經抓取並確認過」的檢查，一旦某次成功，同一天內其餘整點
    觸發都會在幾秒內直接跳過，不會浪費資源。

    例外：如果今天是這個彩券「常見的開獎日」（見 GAME_CONFIG 的
    typical_draw_weekdays），本輪不會只抓 1 次就放棄，而是在
    DRAW_DAY_SEARCH_MINUTES 分鐘內每隔 DRAW_DAY_RETRY_INTERVAL_SECONDS 秒
    重試一次，提高抓到的機會；不是常見開獎日（含過年加開這類例外情況）
    的話，維持原本的單次嘗試，不會比原本更差。"""
    cfg = GAME_CONFIG[game_key]
    conn = init_db()

    existing = already_confirmed_today(conn, cfg["name"])
    if existing:
        _, existing_numbers, existing_special, existing_checked_at = existing
        log(f"{cfg['name']} 今天已經抓取並確認過（號碼：{existing_numbers}，時間：{existing_checked_at}），"
            f"不再重複抓取，明天會自動再開始新一輪")
        update_status(
            game_key,
            fetching=False,
            date=taiwan_today().strftime("%Y-%m-%d"),
            weekday=today_weekday_zh(),
            numbers=[int(n) for n in existing_numbers.split()],
            special=int(existing_special) if existing_special else None,
        )
        conn.close()
        return True

    update_status(game_key, fetching=True)

    typical_weekdays = cfg.get("typical_draw_weekdays")
    is_draw_day = typical_weekdays is None or taiwan_today().weekday() in typical_weekdays

    if is_draw_day:
        log(f"今天可能是 {cfg['name']} 的開獎日，本輪最多搜尋 "
            f"{DRAW_DAY_SEARCH_MINUTES} 分鐘（每隔 {DRAW_DAY_RETRY_INTERVAL_SECONDS} 秒重試一次）")
        deadline = time.monotonic() + DRAW_DAY_SEARCH_MINUTES * 60
        attempt = 0
        result = None
        while True:
            attempt += 1
            log(f"第 {attempt} 次嘗試...")
            result = try_cross_check(game_key, conn)
            if result or time.monotonic() >= deadline:
                break
            time.sleep(DRAW_DAY_RETRY_INTERVAL_SECONDS)
    else:
        log(f"今天不是 {cfg['name']} 的常見開獎日，本輪只嘗試 1 次；"
            f"若未確認，下個整點排程會自動再抓一次")
        result = try_cross_check(game_key, conn)

    if result:
        period, draw_date, numbers, special, sources = result
        save_confirmed(conn, cfg["name"], period, draw_date, numbers, special, sources)
        update_status(
            game_key,
            fetching=False,
            date=taiwan_today().strftime("%Y-%m-%d"),
            weekday=today_weekday_zh(),
            numbers=list(numbers),
            special=special,
        )
        conn.close()
        return True

    log(f"{cfg['name']} 本輪未確認，今日暫不存入資料庫，下個整點排程會自動再抓一次")
    update_status(game_key, fetching=False)
    conn.close()
    return False


def main():
    parser = argparse.ArgumentParser(description="多來源交叉比對開獎號碼爬蟲")
    parser.add_argument("--game", required=True, choices=list(GAME_CONFIG.keys()), help="要抓取的彩券")
    parser.add_argument(
        "--check-confirmed-only",
        action="store_true",
        help="只檢查今天是否已經確認過，不抓取也不寫入任何資料。"
             "exit code 0 代表今天已確認，1 代表尚未確認。"
             "給「每日重新啟用排程」的 workflow 用，判斷要不要跳過某個彩券。",
    )
    args = parser.parse_args()

    if args.check_confirmed_only:
        cfg = GAME_CONFIG[args.game]
        conn = init_db()
        existing = already_confirmed_today(conn, cfg["name"])
        conn.close()
        sys.exit(0 if existing else 1)

    confirmed = run_until_confirmed(args.game)

    # 把「今天是否已確認」寫進 GitHub Actions 的 step output（$GITHUB_OUTPUT），
    # 讓 workflow yml 可以根據這個結果，決定要不要把「每小時觸發」的排程
    # 停用到明天（避免今天已經抓到了，還一直每小時空轉觸發）。
    # 本機直接執行（不是在 GitHub Actions 環境）時 GITHUB_OUTPUT 不存在，
    # 這段就直接跳過，不影響本機測試。
    github_output_path = os.environ.get("GITHUB_OUTPUT")
    if github_output_path:
        try:
            with open(github_output_path, "a", encoding="utf-8") as f:
                f.write(f"confirmed={'true' if confirmed else 'false'}\n")
        except Exception as e:
            log(f"寫入 GITHUB_OUTPUT 失敗（不影響爬取結果）：{e}")


if __name__ == "__main__":
    main()

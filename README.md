# <div align="center"> SearchParser </div>

## A. 介紹

本專案為一個 **搜尋與文章解析自動化工具**，可透過自建的 SearxNG 搜尋引擎取得指定關鍵字的新聞文章，並自動擷取正文與關鍵欄位。支援將解析結果儲存至 PostgreSQL 資料庫。

* 整合 [SearxNG](https://github.com/searxng/searxng) 作為搜尋後端
* 使用 `newspaper4k` 與 `cloudscraper` 做文章解析與防止跳轉封鎖
* 支援資料庫快取查詢與結果去重
* 模組化、可選擇是否儲存結果至資料庫
* 本工具已於多個繁體中文主題關鍵字下驗測共 **231 個 URL**，其中成功擷取內文者達 **189 筆**，成功率達 **82%**，展現好的解析率與穩定性。

## B. 特色

1. **搜尋整合**：透過本地 SearxNG 搜尋新聞內容，支援多引擎與語言
2. **內容解析**：自動擷取文章正文、標題、時間與來源網站
3. **資料儲存**：成功與失敗結果可儲存至 `parsed_articles` / `failed_articles` 表
4. **快取比對**：已解析文章不重複處理，加速效率
5. **多線程解析**：支援 ThreadPool 加速批次解析流程

## C. 依賴

* Python 3.8+
* `newspaper4k`
* `cloudscraper`
* `psycopg2-binary`（若需使用 PostgreSQL）

安裝方式：

```bash
pip install -r requirements.txt
```

## D. 環境準備

### 1. 啟動本地 SearxNG

請先啟動 `searxng/docker-compose.yaml`，該檔案與 `settings.yml` 已包含於本專案中：

```bash
cd searxng
docker-compose up -d
```

預設會啟動於 `http://localhost:8080`

## E. 用法

### 1. 建立 `SearchParser`

```python
from SearchParser.search_parser import SearchParser
from SearchParser.database.postgres_db.postgres_tools import PostgresHandler
from SearchParser.utils.logger import logger

your_postgres_handler = PostgresHandler(config_path="./config/private/database.ini", logger=logger)
parser = SearchParser(db_handler=your_postgres_handler)  # 或可不傳 db
```

### 2. 搜尋並解析文章

```python
result = parser.search_and_parse(
    query="碳權交易",
    min_parsed=5,
    max_attempts=30
)
```

回傳格式：

```python
{
  "query": "碳權交易",
  "success": [ {...}, ... ],  # 成功解析的文章
  "failed": [ {...}, ... ]   # 解析失敗或內容不足的
}
```

### 3. 資料庫結構（如啟用 DB）

```sql
CREATE TABLE parsed_articles (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE,
    query TEXT,
    title TEXT,
    snippet TEXT,
    engine TEXT,
    published TIMESTAMP,
    score FLOAT,
    text TEXT,
    error TEXT,
    inserted_at TIMESTAMP DEFAULT now()
);

CREATE TABLE failed_articles (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE,
    query TEXT,
    title TEXT,
    snippet TEXT,
    engine TEXT,
    published TIMESTAMP,
    score FLOAT,
    text TEXT,
    error TEXT,
    inserted_at TIMESTAMP DEFAULT now()
);

```

## F. 備註

* 預設會跳過 `wikinews.org` 的來源文章
* 若設有資料庫，會自動快取成功解析結果並避免重複處理
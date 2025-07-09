from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from parser import parse_article
from typing import Dict, List, Optional

import requests

from utils.logger import logger
from utils.text_utils import extract_date_from_metadata, parse_published_date


class SearchParser:
    def __init__(
        self,
        search_engine_url: str = "http://localhost:8080",
        db_handler=None,
        timeout: int = 10,
    ):
        self.search_engine_url = search_engine_url
        self.db = db_handler
        self.timeout = timeout
        
    def _fetch_results(
        self,
        query: str,
        language: Optional[str] = None,
        safesearch: int = 0,
        categories: str = "general",
        engines: Optional[str] = None,
        time_range: Optional[str] = None,
        max_results : int = 10
    ) -> List[Dict]:
        try:
            params = {
                "q": query,
                "format": "json",
                "safesearch": safesearch,
                "categories": categories,
            }
            if language:
                params["language"] = language
            if engines:
                params["engines"] = engines
            if time_range:
                params["time_range"] = time_range

            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "*/*",
                "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Connection": "keep-alive",
            }
            
            logger.info(f"[搜尋引擎] 開始查詢：{query}，最大筆數限制：{max_results}")

            response = requests.get(f"{self.search_engine_url}/search", params=params, timeout=self.timeout, headers=headers)
            response.raise_for_status()
            data = response.json()
            logger.info(f"共找到 {len(data['results'])} 筆搜尋結果")

            results = []
            skip_count = 0
            for r in data.get("results", []):
                if len(results) >= max_results:
                    break
                
                if "wikinews.org" in r.get("url", ""):
                    skip_count += 1
                    continue
                
                published = r.get("publishedDate") or extract_date_from_metadata(r.get("metadata", ""))
                results.append({
                    "title": r["title"],
                    "url": r["url"],
                    "snippet": r.get("content", ""),
                    "engine": r.get("engine"),
                    "published": parse_published_date(published),
                    "score": r["score"]
                })
            logger.info(f"[搜尋引擎] 成功保留 {len(results)} 篇，過濾掉 wikinews.org {skip_count} 篇")
            return results

        except requests.RequestException as e:
            logger.error(f"[SearxNG Error] 搜尋失敗: {e}")
            return []

    def search_and_parse(
            self,
            query: str,
            min_parsed: int = 5,
            max_attempts: int = 30,
            **kwargs
        ) -> List[Dict]:
        logger.info(f"[解析流程] 開始處理查詢：{query}，min_parsed={min_parsed}，max_attempts={max_attempts}")
        raw_results = self._fetch_results(query=query, max_results=max_attempts, **kwargs)

        parsed_results = []
        failed_results = []
        parse_attempts = 0
        batch_size = 5
        
        for i in range(0, len(raw_results), batch_size):
            if len(parsed_results) >= min_parsed or parse_attempts >= max_attempts:
                break
            batch = raw_results[i:i + batch_size]
            
            if self.db:
                batch_urls = [r['url'] for r in batch]
                existing_articles = self._get_existing_articles(batch_urls)
                
                logger.debug(f"[快取檢查] 資料庫已有 {len(existing_articles)} 篇")
                
                for r in batch:
                    if r['url'] in existing_articles:
                        logger.info(f"[快取命中] 使用 DB 資料：{r['url']}")
                        parsed_results.append(existing_articles[r['url']])
                        parse_attempts += 1
                        if len(parsed_results) >= min_parsed:
                            logger.info("已達成功上限（DB 快取），提前結束解析")
                            break
                batch = [r for r in batch if r["url"] not in existing_articles]
            
            if not batch:
                logger.debug("[解析流程] 本批次剩下皆為快取文章，略過解析")
                continue
            
            needed = min_parsed - len(parsed_results)
            if needed <= 0:
                break
            batch = batch[:needed]
                 
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                futures = {
                    executor.submit(parse_article, r["url"]): r
                    for r in batch
                }
                for future in as_completed(futures):
                    parse_attempts += 1
                    r = futures[future]
                    
                    try:
                        parsed = future.result()
                        text = parsed.get("text", "") if parsed else ""
                        error_message = parsed.get("error", "") if parsed else ""
                        text_length = len(text)

                        is_success = bool(text.strip()) and not error_message and text_length >= 50

                        if is_success:
                            logger.info(f"[解析成功] {r['url']}，長度={text_length}")
                            parsed_results.append({**r, **parsed})
                            if len(parsed_results) >= min_parsed:
                                logger.warning("已達成功上限，提前結束解析")
                                break
                        else:
                            failed_results.append({**r, **parsed})
                    except Exception as e:
                        logger.error(f"[解析失敗] {r['url']} → {e}")
            
            if parse_attempts >= max_attempts:
                logger.warning("達到最大嘗試數量")
                break

        logger.info(f"成功解析 {len(parsed_results)} 篇文章，失敗 {len(failed_results)} 篇，共嘗試 {parse_attempts} 篇")
        
        if self.db:
            self._write_results_to_db(query, parsed_results, failed_results)
        
        return {
            "query": query,
            "success": parsed_results,
            "failed": failed_results
        } 
        
    def _get_existing_articles(self, urls: List[str]) -> Dict[str, Dict]:
        if not urls or self.db is None:
            return {}

        placeholders = ','.join(['%s'] * len(urls))
        sql = f"""
            SELECT
                url,
                title,
                snippet,
                engine,
                published,
                score,
                text,
                error
            FROM parsed_articles
            WHERE url IN ({placeholders})
        """
        result = self.db._execute_sql(sql, urls)
        return {row["url"]: row for row in result["formatted_data"]}
    
    def _write_results_to_db(self, query: str, success: List[dict], failed: List[dict]):
        if self.db is None:
            logger.warning("未設定資料庫，無法寫入")
            return

        inserted_at = datetime.now()
        
        for r in success:
            r["query"] = query
            r["inserted_at"] = inserted_at

        for r in failed:
            r["query"] = query
            r["inserted_at"] = inserted_at

        if success:
            logger.info(f"[DB 寫入] 成功結果準備寫入 {len(success)} 篇")
            self.db.add_data("parsed_articles", success, on_conflict_do_nothing=True, unique_columns="url")

        if failed:
            logger.info(f"[DB 寫入] 失敗結果準備寫入 {len(failed)} 篇")
            self.db.add_data("failed_articles", failed, on_conflict_do_nothing=True, unique_columns="url")
        logger.info("[DB 寫入] 資料寫入完成")
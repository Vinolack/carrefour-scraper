import logging
import requests
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from src import configloader
from src.extractor import extract_product_details, extract_product_links

# 配置日志
logger = logging.getLogger('carrefour_scraper')
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    # 容器内路径
    log_file = "/app/logs/scraper.log"
    # 简单配置，确保文件存在
    try:
        open(log_file, 'a').close()
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except: pass
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(console_handler)

c = configloader.config()
MAX_WORKERS = int(c.get_key("max_concurrent_tasks") or 20)
api_config = c.get_key("api") or {}
# 默认回退值，防止启动报错，实际运行时应由配置文件或环境变量提供
CF_HOST = api_config.get("cf_host", "127.0.0.1")
CF_PORT = api_config.get("cf_port", 3000)
API_URL = f"http://{CF_HOST}:{CF_PORT}/cf-clearance-scraper"

def fetch_html_direct(url, max_retries=3):
    """调用 Node 服务获取 HTML"""
    payload = {"url": url, "mode": "source"}
    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, json=payload, timeout=(5, 60))
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and data.get('code') == 200:
                        return data.get('source') or data.get('data')
                    elif isinstance(data, str):
                        return data
                except: 
                    return response.text
            else:
                logger.error(f"API Error {response.status_code} for {url}")
        except Exception as e:
            logger.error(f"Fetch error {url}: {e}")
            time.sleep(1)
    return None

def process_store_page(url):
    """[Store阶段] 抓取店铺页，提取商品链接"""
    html = fetch_html_direct(url)
    if html:
        return extract_product_links(html)
    return []

def process_product_page(url):
    """[Product阶段] 抓取商品详情"""
    html = fetch_html_direct(url)
    if not html or len(html) < 100:
        return {"Product URL": url, "error": "Fetch failed or empty"}
    try:
        return extract_product_details(html, url)
    except Exception as e:
        logger.error(f"Extraction error {url}: {e}")
        return {"Product URL": url, "error": str(e)}

def run_batch_job(task_type: str, urls: list, pages: int, job_store: dict, job_id: str):
    """后台任务主逻辑"""
    
    target_product_urls = []
    
    # --- 阶段 1: 如果是店铺，先收集商品链接 ---
    if task_type == "store":
        job_store[job_id]["status"] = "scanning_pages"
        
        # 1. 生成所有分页链接
        store_pages_to_scrape = []
        for url in urls:
            for p in range(1, pages + 1):
                # 拼接分页参数
                sep = "&" if "?" in url else "?"
                p_url = f"{url}{sep}noRedirect=1&page={p}"
                store_pages_to_scrape.append(p_url)
        
        total_pages = len(store_pages_to_scrape)
        job_store[job_id]["progress"] = f"Scanning 0/{total_pages} store pages"
        logger.info(f"Task {job_id}: Scanning {total_pages} store pages...")

        found_links = set()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {executor.submit(process_store_page, u): u for u in store_pages_to_scrape}
            
            count = 0
            for future in as_completed(future_to_url):
                count += 1
                try:
                    links = future.result()
                    if links:
                        found_links.update(links)
                except Exception as e:
                    logger.error(f"Store page error: {e}")
                
                if count % 5 == 0 or count == total_pages:
                    job_store[job_id]["progress"] = f"Scanning {count}/{total_pages} pages (Found {len(found_links)} products)"

        target_product_urls = list(found_links)
        logger.info(f"Task {job_id}: Found {len(target_product_urls)} products.")
        
    else:
        # 如果是商品链接，直接使用
        target_product_urls = urls

    # --- 阶段 2: 抓取商品详情 ---
    if not target_product_urls:
        job_store[job_id]["status"] = "completed"
        job_store[job_id]["progress"] = "No products found"
        job_store[job_id]["completed_at"] = datetime.now().isoformat()
        return

    job_store[job_id]["status"] = "scraping_products"
    total_products = len(target_product_urls)
    results = []
    
    logger.info(f"Task {job_id}: Scraping {total_products} products details...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(process_product_page, u): u for u in target_product_urls}
        
        count = 0
        for future in as_completed(future_to_url):
            count += 1
            try:
                data = future.result()
                results.append(data)
            except Exception as e:
                logger.error(f"Product error: {e}")
            
            # 实时更新进度和结果
            if count % 2 == 0 or count == total_products:
                job_store[job_id]["progress"] = f"Scraping {count}/{total_products} products"
                job_store[job_id]["results"] = results
                job_store[job_id]["results_count"] = len(results)

    job_store[job_id]["status"] = "completed"
    job_store[job_id]["completed_at"] = datetime.now().isoformat()
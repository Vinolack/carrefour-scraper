import logging
import requests
import json
import time
import random
import string
import os
import math
from threading import Lock
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from src import configloader
from src.extractor import extract_product_details, extract_product_links

# 配置日志
logger = logging.getLogger('carrefour_scraper')
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    log_file = "/app/logs/scraper.log"
    try:
        if not os.path.exists(os.path.dirname(log_file)):
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
        open(log_file, 'a').close()
        
        # 修改日志级别为 INFO，确保记录运行状态
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        # 增加进程ID记录，方便调试多进程
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] [P:%(process)d] - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except: pass
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(console_handler)

c = configloader.config()

# --- 配置参数 ---
# 进程数
MAX_WORKERS = int(c.get_key("MAX_WORKERS") or 8)

# 进程内线程数限制：每个进程内部开启多少个线程
THREAD_LIMIT = int(os.getenv("THREAD_LIMIT", 10))

api_config = c.get_key("api") or {}
CF_HOST = api_config.get("cf_host", "127.0.0.1")
CF_PORT = api_config.get("cf_port", 3000)
API_URL = f"http://{CF_HOST}:{CF_PORT}/cf-clearance-scraper"

# 代理配置
PROXY_HOST = c.get_key('PROXY_HOST')
PROXY_PORT = c.get_key('PROXY_PORT')
PROXY_USER_BASE = c.get_key('PROXY_USER_BASE')
PROXY_PASS = c.get_key('PROXY_PASS')

# --- 进程级全局变量 ---
_PROXY_LOCK = Lock()
_PROXY_SESSION_STATE = {
    "session_id": None,
    "request_count": 0,
    "limit": 0,
    "current_config": None
}

def get_proxy_config():
    """
    获取当前进程的代理配置。
    线程安全地管理 Session 轮换。
    """
    if not PROXY_HOST:
        return None

    # 加锁防止多个线程同时触发轮换
    with _PROXY_LOCK:
        state = _PROXY_SESSION_STATE
        
        # 初始化或检查是否需要轮换
        if state["session_id"] is None or state["request_count"] >= state["limit"]:
            # 生成新的 Session ID
            session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
            full_username = f"{PROXY_USER_BASE}{session_id}"
            
            # 设置新的随机上限 (1000 ~ 1500 次)
            limit = random.randint(1000, 1500)
            
            new_config = {
                "host": PROXY_HOST,
                "port": PROXY_PORT,
                "username": full_username,
                "password": PROXY_PASS
            }
            
            # 更新状态
            state["session_id"] = session_id
            state["current_config"] = new_config
            state["limit"] = limit
            state["request_count"] = 0
            
            logger.info(f"Rotated Proxy Session: {session_id} (Limit: {limit})")

        state["request_count"] += 1
        return state["current_config"]

def fetch_html_direct(url, max_retries=3):
    """调用 Node 服务获取 HTML"""
    
    # 1. 获取代理配置 (线程安全)
    proxy_conf = get_proxy_config()
    
    # 2. 构造 Payload
    payload = {
        "url": url, 
        "mode": "source"
    }
    if proxy_conf:
        payload["proxy"] = proxy_conf
    
    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, json=payload, timeout=(10, 90))
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

# --- 单个任务处理函数 ---

def process_single_store_page(url):
    try:
        html = fetch_html_direct(url)
        if html:
            return extract_product_links(html)
    except Exception as e:
        logger.error(f"Error processing store page {url}: {e}")
    return []

def process_single_product_page(url):
    try:
        html = fetch_html_direct(url)
        if not html or len(html) < 100:
            return {"Product URL": url, "error": "Fetch failed or empty"}
        return extract_product_details(html, url)
    except Exception as e:
        logger.error(f"Extraction error {url}: {e}")
        return {"Product URL": url, "error": str(e)}

# --- 批量处理函数

def process_batch_store_pages(urls, thread_limit):
    """子进程：使用多线程处理一批店铺页面"""
    found_links = set()
    with ThreadPoolExecutor(max_workers=thread_limit) as executor:
        future_to_url = {executor.submit(process_single_store_page, u): u for u in urls}
        for future in as_completed(future_to_url):
            try:
                links = future.result()
                if links:
                    found_links.update(links)
            except: pass
    return list(found_links)

def process_batch_products(urls, thread_limit):
    """子进程：使用多线程处理一批商品页面"""
    results = []
    # 开启线程池，每个线程负责一个商品
    # thread_limit 实际上控制的是“同时处理多少个商品”
    with ThreadPoolExecutor(max_workers=thread_limit) as executor:
        future_to_url = {executor.submit(process_single_product_page, u): u for u in urls}
        for future in as_completed(future_to_url):
            try:
                data = future.result()
                results.append(data)
            except: pass
    return results

# --- 主任务逻辑 ---

def run_batch_job(task_type: str, urls: list, pages: int, job_store: dict, job_id: str):
    """后台任务主逻辑：负责切分任务并分发给进程池"""
    
    target_product_urls = []
    
    # --- 阶段 1: Store 模式 ---
    if task_type == "store":
        job_store[job_id]["status"] = "scanning_pages"
        
        # 1. 生成所有 URL
        all_store_urls = []
        for url in urls:
            for p in range(1, pages + 1):
                sep = "&" if "?" in url else "?"
                p_url = f"{url}{sep}noRedirect=1&page={p}"
                all_store_urls.append(p_url)
        
        total_pages = len(all_store_urls)
        
        # 2. 切分 Batch
        # 每个进程分到的任务量
        chunk_size = math.ceil(total_pages / MAX_WORKERS)
        batches = [all_store_urls[i:i + chunk_size] for i in range(0, total_pages, chunk_size)]
        
        logger.info(f"Task {job_id}: Scanning {total_pages} pages using {len(batches)} processes (Thread Limit: {THREAD_LIMIT})...")
        job_store[job_id]["progress"] = f"Scanning {total_pages} pages..."

        found_links = set()
        
        # 3. 提交给进程池
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交 Batch 任务
            futures = [executor.submit(process_batch_store_pages, batch, THREAD_LIMIT) for batch in batches]
            
            count = 0
            for future in as_completed(futures):
                try:
                    links = future.result()
                    if links:
                        found_links.update(links)
                except Exception as e:
                    logger.error(f"Batch store error: {e}")
                count += 1
                job_store[job_id]["progress"] = f"Scanning batch {count}/{len(batches)} (Total found: {len(found_links)})"

        target_product_urls = list(found_links)
        logger.info(f"Task {job_id}: Found {len(target_product_urls)} products.")
        
    else:
        target_product_urls = urls

    # --- 阶段 2: Product 模式 ---
    if not target_product_urls:
        job_store[job_id]["status"] = "completed"
        job_store[job_id]["progress"] = "No products found"
        job_store[job_id]["completed_at"] = datetime.now().isoformat()
        return

    job_store[job_id]["status"] = "scraping_products"
    total_products = len(target_product_urls)
    results = []
    
    # 1. 切分 Batch
    chunk_size = math.ceil(total_products / MAX_WORKERS)
    # 确保至少有1个
    if chunk_size < 1: chunk_size = 1
    
    batches = [target_product_urls[i:i + chunk_size] for i in range(0, total_products, chunk_size)]
    
    logger.info(f"Task {job_id}: Scraping {total_products} products using {len(batches)} processes (Thread Limit: {THREAD_LIMIT})...")
    
    # 2. 提交给进程池
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_batch_products, batch, THREAD_LIMIT) for batch in batches]
        
        count = 0
        for future in as_completed(futures):
            try:
                batch_results = future.result()
                results.extend(batch_results)
            except Exception as e:
                logger.error(f"Batch product error: {e}")
            
            count += 1
            # 更新进度
            current_count = len(results)
            job_store[job_id]["progress"] = f"Scraping products: {current_count}/{total_products}"
            # 注意：如果数据量非常大，频繁全量写入内存DB可能会有性能问题，建议分批处理或简化
            job_store[job_id]["results"] = results
            job_store[job_id]["results_count"] = current_count

    job_store[job_id]["status"] = "completed"
    job_store[job_id]["completed_at"] = datetime.now().isoformat()
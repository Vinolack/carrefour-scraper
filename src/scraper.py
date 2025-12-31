import logging
import requests
import json
import time
import random
import string
import os
import math
import threading
from threading import Lock
from datetime import datetime
from multiprocessing import Manager
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from src import configloader
from src.extractor import extract_product_details, extract_product_links

# --- 日志配置 ---
def setup_logging():
    log_file = "/app/logs/scraper.log"
    logger = logging.getLogger('carrefour_scraper')
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers: logger.handlers.clear()
    try:
        if not os.path.exists(os.path.dirname(log_file)):
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [P:%(process)d] - %(message)s'))
        logger.addHandler(file_handler)
    except: pass
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s'))
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()
c = configloader.config()

# --- 配置参数 ---
MAX_WORKERS = int(c.get_key("MAX_WORKERS") or 3)
THREAD_LIMIT = int(os.getenv("THREAD_LIMIT", 10))

api_config = c.get_key("api") or {}
CF_HOST = api_config.get("cf_host", "127.0.0.1")
CF_PORT = api_config.get("cf_port", 3000)
API_URL = f"http://{CF_HOST}:{CF_PORT}/cf-clearance-scraper"

PROXY_HOST = c.get_key('PROXY_HOST')
PROXY_PORT = c.get_key('PROXY_PORT')
PROXY_USER_BASE = c.get_key('PROXY_USER_BASE')
PROXY_PASS = c.get_key('PROXY_PASS')

# --- 进程级全局变量 ---
_PROXY_LOCK = Lock()
_PROXY_SESSION_STATE = {
    "session_id": None, "request_count": 0, "limit": 0, "current_config": None
}

def get_proxy_config(force_rotate=False):
    if not PROXY_HOST: return None
    with _PROXY_LOCK:
        state = _PROXY_SESSION_STATE
        if force_rotate or state["session_id"] is None or state["request_count"] >= state["limit"]:
            session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
            full_username = f"{PROXY_USER_BASE}{session_id}"
            limit = random.randint(1000, 1500)
            state["session_id"] = session_id
            state["current_config"] = {"host": PROXY_HOST, "port": PROXY_PORT, "username": full_username, "password": PROXY_PASS}
            state["limit"] = limit
            state["request_count"] = 0
            logger.info(f"Rotated Proxy: {session_id}")
        state["request_count"] += 1
        return state["current_config"]

def fetch_html_direct(url, max_retries=3):
    for attempt in range(max_retries):
        force_new_ip = (attempt > 0)
        proxy_conf = get_proxy_config(force_rotate=force_new_ip)
        payload = {"url": url, "mode": "source"}
        if proxy_conf: payload["proxy"] = proxy_conf
        try:
            response = requests.post(API_URL, json=payload, timeout=(15, 90))
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict) and data.get('code') == 200:
                        return data.get('source') or data.get('data')
                    elif isinstance(data, str): return data
                except: return response.text
            elif response.status_code in [403, 429, 502, 503, 504]:
                logger.warning(f"API Fail {response.status_code}. Rotating...")
            else:
                logger.error(f"API Error {response.status_code} for {url}")
        except Exception as e:
            logger.error(f"Conn Error: {e}. Rotating...")
        time.sleep(1 + attempt)
    return None

# --- Worker Functions ---
def init_worker():
    setup_logging()

def process_batch_products(urls, thread_limit, shared_counter, mode="full"):
    """
    shared_counter: multiprocessing.Manager.Value，用于实时计数
    mode: "full" or "price_check"
    """
    results = []
    errors = 0
    with ThreadPoolExecutor(max_workers=thread_limit) as executor:
        future_to_url = {executor.submit(process_single_product_page, u, mode): u for u in urls}
        for future in as_completed(future_to_url):
            try:
                data = future.result()
                if "error" in data and "Fetch failed" in str(data["error"]):
                    errors += 1
                results.append(data)
            except: 
                errors += 1
            finally:
                # 每处理完一个链接，立即增加计数器
                # Manager Value 是进程安全的
                shared_counter.value += 1
                
    return results, errors

def process_single_product_page(url, mode="full"):
    try:
        html = fetch_html_direct(url)
        if not html or len(html) < 100:
            return {"Product URL": url, "error": "Fetch failed or empty"}
        return extract_product_details(html, url, mode=mode)
    except Exception as e:
        return {"Product URL": url, "error": str(e)}

# --- Store Phase Functions (Unchanged logic, simpler signature) ---
def process_batch_store_pages(urls, thread_limit):
    found_links = set()
    errors = 0
    with ThreadPoolExecutor(max_workers=thread_limit) as executor:
        future_to_url = {executor.submit(process_single_store_page, u): u for u in urls}
        for future in as_completed(future_to_url):
            try:
                links = future.result()
                if links: found_links.update(links)
                else: errors += 1
            except: errors += 1
    return list(found_links), errors

def process_single_store_page(url):
    try:
        html = fetch_html_direct(url)
        if html: return extract_product_links(html)
    except: pass
    return []

# --- Progress Monitor ---
def monitor_progress(job_id, job_store, shared_counter, total, stop_event):
    """后台线程：每3秒更新一次数据库进度"""
    while not stop_event.is_set():
        current = shared_counter.value
        job_store[job_id]["processed"] = min(current, total)
        # 仅更新数字，不覆盖 progress 文本描述
        time.sleep(3)

# --- Main Logic ---

def run_batch_job(task_type: str, urls: list, pages: int, job_store: dict, job_id: str):
    
    target_product_urls = []
    
    # === Phase 1: Store Processing ===
    if task_type == "store":
        job_store[job_id]["status"] = "scanning_pages"
        all_store_urls = []
        for url in urls:
            for p in range(1, pages + 1):
                sep = "&" if "?" in url else "?"
                p_url = f"{url}{sep}noRedirect=1&page={p}"
                all_store_urls.append(p_url)
        
        total_pages = len(all_store_urls)
        job_store[job_id]["total"] = total_pages
        job_store[job_id]["progress"] = "Scanning store pages..."
        
        chunk_size = math.ceil(total_pages / MAX_WORKERS)
        if chunk_size < 1: chunk_size = 1
        batches = [all_store_urls[i:i + chunk_size] for i in range(0, total_pages, chunk_size)]
        
        logger.info(f"Task {job_id}: Scanning {total_pages} pages...")
        found_links = set()
        processed_count = 0
        total_errors = 0
        
        future_to_batch_size = {}

        # Store 阶段还是按 batch 更新，因为不像 Product 阶段那么耗时
        with ProcessPoolExecutor(max_workers=MAX_WORKERS, initializer=init_worker) as executor:
            for batch in batches:
                f = executor.submit(process_batch_store_pages, batch, THREAD_LIMIT)
                future_to_batch_size[f] = len(batch)
            
            for future in as_completed(future_to_batch_size):
                b_size = future_to_batch_size[future]
                try:
                    links, errs = future.result()
                    if links: found_links.update(links)
                    total_errors += errs
                    processed_count += b_size
                    job_store[job_id]["processed"] = processed_count
                except Exception as e:
                    logger.error(f"Store Batch Error: {e}")
        
        if total_errors >= total_pages and total_pages > 0:
            job_store[job_id]["status"] = "failed"
            job_store[job_id]["progress"] = "Failed to scan pages"
            job_store[job_id]["completed_at"] = datetime.now().isoformat()
            return
            
        target_product_urls = list(found_links)
        logger.info(f"Task {job_id}: Found {len(target_product_urls)} products.")
    
    else:
        # product or price_check
        target_product_urls = urls

    # === Phase 2: Product / Price Check Processing ===
    if not target_product_urls:
        job_store[job_id]["status"] = "completed"
        job_store[job_id]["progress"] = "No products found"
        job_store[job_id]["completed_at"] = datetime.now().isoformat()
        return

    job_store[job_id]["status"] = "scraping_products"
    total_products = len(target_product_urls)
    job_store[job_id]["total"] = total_products
    job_store[job_id]["processed"] = 0
    
    # 确定模式
    scrape_mode = "price_check" if task_type == "price_check" else "full"
    status_text = "Checking prices..." if scrape_mode == "price_check" else "Scraping details..."
    job_store[job_id]["progress"] = status_text
    
    chunk_size = math.ceil(total_products / MAX_WORKERS)
    if chunk_size < 1: chunk_size = 1
    batches = [target_product_urls[i:i + chunk_size] for i in range(0, total_products, chunk_size)]
    
    logger.info(f"Task {job_id}: Processing {total_products} items (Mode: {scrape_mode})...")

    final_results = []
    total_errors = 0
    
    # 使用 Manager 创建共享计数器
    with Manager() as manager:
        shared_counter = manager.Value('i', 0)
        stop_monitor = threading.Event()
        
        # 启动监控线程：每3s更新一次 job_store
        monitor = threading.Thread(
            target=monitor_progress, 
            args=(job_id, job_store, shared_counter, total_products, stop_monitor),
            daemon=True
        )
        monitor.start()

        try:
            with ProcessPoolExecutor(max_workers=MAX_WORKERS, initializer=init_worker) as executor:
                futures = []
                for batch in batches:
                    # 传入 shared_counter 和 scrape_mode
                    f = executor.submit(process_batch_products, batch, THREAD_LIMIT, shared_counter, scrape_mode)
                    futures.append(f)
                
                for future in as_completed(futures):
                    try:
                        batch_res, errs = future.result()
                        final_results.extend(batch_res)
                        total_errors += errs
                    except Exception as e:
                        logger.error(f"Product Batch Error: {e}")

        finally:
            # 停止监控线程并做最后一次更新
            stop_monitor.set()
            job_store[job_id]["processed"] = shared_counter.value

    # 失败判定
    fail_rate = total_errors / total_products if total_products > 0 else 0
    if fail_rate > 0.9 and total_products > 10:
        job_store[job_id]["status"] = "failed"
        job_store[job_id]["progress"] = "Too many errors"
    else:
        job_store[job_id]["status"] = "completed"
        job_store[job_id]["progress"] = "Completed"
        job_store[job_id]["results"] = final_results 
        job_store[job_id]["results_count"] = len(final_results)
    
    job_store[job_id]["completed_at"] = datetime.now().isoformat()
    logger.info(f"Task {job_id} Finished. Status: {job_store[job_id]['status']}")
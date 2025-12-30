import os
import sys
import time
import logging
import pandas as pd
import requests
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import configloader

from excel_reader import read_links_from_excel
from extractor import extract_product_links, extract_product_details

# 初始化配置
c = configloader.config()
MAX_WORKERS = c.get_key("max_concurrent_tasks") or 5 

# 获取 API 配置
api_config = c.get_key("api") or {}
CF_HOST = api_config.get("cf_host", "127.0.0.1")
CF_PORT = api_config.get("cf_port", 3000)
API_URL = f"http://{CF_HOST}:{CF_PORT}/cf-clearance-scraper"

def get_exe_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

# --- 日志系统配置 ---
logger = logging.getLogger('carrefour_scraper')
logger.setLevel(logging.DEBUG)
logger.propagate = False

log_file = os.path.normpath(os.path.join(get_exe_dir(), '..', 'scraper.log'))
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.ERROR) 
file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [File:%(filename)s:%(lineno)d] - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)


def fetch_html_direct(url, max_retries=3, base_delay=2.0):
    """
    直接使用 Python requests 调用 API，替代 Node.js 子进程。
    适配返回格式: {'source': '<html>...', 'code': 200}
    """
    payload = {
        "url": url,
        "mode": "source"
    }
    
    for attempt in range(1, max_retries + 1):
        try:
            # 设置较短的连接超时和合理的读取超时
            response = requests.post(API_URL, json=payload, timeout=(5, 60))
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    if isinstance(data, dict):
                        # 检查 code 是否为 200
                        if data.get('code') == 200:
                            # 优先获取 source 字段
                            if 'source' in data:
                                return data['source']
                            # 备用：有些接口可能用 data 字段
                            elif 'data' in data:
                                return data['data']
                        else:
                            logger.error(f"API Logic Error for {url} | Code: {data.get('code')} | Msg: {data}")
                            # 如果 code 不是 200，视为失败，进入重试
                            if attempt < max_retries:
                                time.sleep(base_delay * attempt)
                            continue

                    # 如果返回的是字符串
                    if isinstance(data, str):
                        return data
                        
                    # 兜底：如果解析不出结构，但 HTTP 200，尝试直接返回 response.text
                    logger.warning(f"Unexpected JSON format for {url}: {str(data)[:100]}")
                    return response.text

                except json.JSONDecodeError:
                    # 不是 JSON，直接返回文本
                    return response.text
            else:
                logger.error(f"API Error {response.status_code} for {url} | Response: {response.text[:200]}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url} (Attempt {attempt}): {e}")
        except Exception as e:
            logger.exception(f"Unexpected error fetching {url}: {e}")

        if attempt < max_retries:
            time.sleep(base_delay * attempt)

    return None

def process_store_page(url):
    logger.info(f"Processing store page: {url}")
    html = fetch_html_direct(url)
    if html:
        links = extract_product_links(html)
        logger.info(f"Page processed: {url} | Found {len(links)} links")
        return links
    else:
        return []

def process_product_page(url):
    logger.info(f"Scraping product: {url}")
    html = fetch_html_direct(url)
    
    if html:
        # 增加数据校验防止空 HTML 导致解析卡死
        if len(html) < 100:
            logger.error(f"HTML too short or invalid for {url}")
            return {"Product URL": url, "Title": "INVALID_HTML"}
            
        details = extract_product_details(html, url)
        if not details.get('Title'):
            logger.warning(f"Product scraped but Title missing: {url}")
        return details
    else:
        return {"Product URL": url, "Title": "FETCH_FAILED"}

def main():
    exe_folder = get_exe_dir()
    excel_file_path = os.path.join(exe_folder, "..", "input_links.xlsx")
    if not os.path.exists(excel_file_path):
        excel_file_path = "input_links.xlsx"

    logger.info(f"Reading store links from: {excel_file_path}")
    store_links = read_links_from_excel(excel_file_path)
    
    if not store_links:
        logger.error("No store links found in Excel.")
        return

    logger.info(f"Starting optimized concurrency with MAX_WORKERS={MAX_WORKERS}")

    # --- Phase 1: Collecting Product URLs ---
    all_product_urls = set()
    logger.info("--- Phase 1: Collecting Product URLs ---")
    
    direct_product_links = [l.strip() for l in store_links if "/p/" in l.strip()]
    store_page_links = [l.strip() for l in store_links if "/p/" not in l.strip()]
    
    for l in direct_product_links:
        all_product_urls.add(l)
    
    if store_page_links:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {}
            for url in store_page_links:
                future = executor.submit(process_store_page, url)
                future_to_url[future] = url
                time.sleep(0.2) 
            
            for future in as_completed(future_to_url):
                try:
                    links = future.result()
                    for l in links:
                        all_product_urls.add(l)
                except Exception as e:
                    logger.error(f"Exception in store page thread: {e}")

    unique_product_list = sorted(list(all_product_urls))
    total_products = len(unique_product_list)
    logger.info(f"Total unique products found: {total_products}")

    if not unique_product_list:
        logger.warning("No products found to scrape.")
        return

    # --- Phase 2: Scraping Product Details ---
    scraped_data = []
    logger.info("--- Phase 2: Scraping Product Details ---")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {}
        for i, prod_url in enumerate(unique_product_list):
            future = executor.submit(process_product_page, prod_url)
            future_to_url[future] = prod_url
            time.sleep(0.2) 

        completed_count = 0
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                scraped_data.append(data)
            except Exception as e:
                logger.error(f"Critical error processing product {url}: {e}")
                scraped_data.append({"Product URL": url, "Title": "CRITICAL_ERROR"})
            
            completed_count += 1
            if completed_count % 10 == 0:
                logger.info(f"Progress: {completed_count}/{total_products}")

    # --- Phase 3: Save ---
    if scraped_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_filename = f"carrefour_products_{timestamp}.xlsx"
        output_path = os.path.join(exe_folder, "..", output_filename)
        if not os.path.exists(os.path.dirname(output_path)):
             output_path = output_filename

        logger.info(f"Saving {len(scraped_data)} rows to {output_path}")
        
        try:
            df = pd.DataFrame(scraped_data)
            columns_order = [
                "Product URL", "EAN", "Brand", "Title", "Category", 
                "Price", "Shipping Cost", "Description", 
                "Image 1", "Image 2", "Image 3", "Image 4", "Image 5"
            ]
            for col in columns_order:
                if col not in df.columns: df[col] = ""
            df = df[columns_order]
            df.to_excel(output_path, index=False)
            logger.info("Successfully saved Excel file.")
        except Exception as e:
            logger.error(f"Failed to save Excel: {e}")
            try:
                csv_path = output_path.replace('.xlsx', '.csv')
                df.to_csv(csv_path, index=False)
                logger.info(f"Saved to CSV backup: {csv_path}")
            except Exception as csv_e:
                logger.error(f"Failed to save CSV backup: {csv_e}")
    else:
        logger.warning("No data extracted to save.")

if __name__ == "__main__":
    main()
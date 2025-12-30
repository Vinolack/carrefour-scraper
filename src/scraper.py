import subprocess
import os
import sys
import time
import logging
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import configloader

from excel_reader import read_links_from_excel
from extractor import extract_product_links, extract_product_details

# 初始化配置
c = configloader.config()
MAX_WORKERS = c.get_key("max_concurrent_tasks") or 5 

def get_exe_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

# --- 日志系统配置 (Robust Logging Setup) ---
logger = logging.getLogger('carrefour_scraper')
logger.setLevel(logging.DEBUG) # 基础级别设为 DEBUG，由 Handler 过滤
logger.propagate = False # 防止传播到 root logger 造成重复打印

# 1. 文件日志：只记录错误 (ERROR)
log_file = os.path.normpath(os.path.join(get_exe_dir(), '..', 'scraper.log'))
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.ERROR) 
file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [File:%(filename)s:%(lineno)d] - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# 2. 控制台日志：显示进度 (INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)


def fetch_html_via_node(url, max_retries=3, base_delay=2.0):
    for attempt in range(1, max_retries + 1):
        try:
            base_path = get_exe_dir()
            node_script = os.path.join(base_path, 'node', 'index.js')
            if not os.path.exists(node_script):
                node_script = os.path.join(base_path, 'src', 'node', 'index.js')
            if not os.path.exists(node_script):
                 node_script = 'src/node/index.js'

            result = subprocess.run(
                ['node', node_script, url],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=90 
            )
            
            stdout = result.stdout or ''
            stderr = result.stderr or ''
            
            if result.returncode != 0:
                logger.error(f"Node process failed for {url} | Stderr: {stderr.strip()}")
                if attempt < max_retries:
                    time.sleep(base_delay * attempt)
                continue
                
            if not stdout.strip():
                logger.error(f"Node returned empty HTML for {url}")
                if attempt < max_retries:
                    time.sleep(base_delay * attempt)
                continue

            return stdout

        except Exception as e:
            logger.exception(f"Subprocess critical error for {url}: {e}")
            time.sleep(base_delay * attempt)
    
    return None

def process_store_page(url):
    """并发任务：处理店铺/列表页"""
    logger.info(f"Processing store page: {url}")
    html = fetch_html_via_node(url)
    if html:
        links = extract_product_links(html)
        logger.info(f"Page processed: {url} | Found {len(links)} links")
        return links
    else:
        # fetch_html_via_node 已经记录了 error，这里只需 info 标记业务流程失败
        return []

def process_product_page(url):
    """并发任务：处理商品详情页"""
    logger.info(f"Scraping product: {url}")
    html = fetch_html_via_node(url)
    if html:
        details = extract_product_details(html, url)
        # 简单校验数据完整性，如果有严重缺失可以记录 Warning
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

    logger.info(f"Starting concurrency with MAX_WORKERS={MAX_WORKERS}")

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
                time.sleep(0.5) # 间隔
            
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
            time.sleep(0.5) # Python 端提交任务间隔

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
            if completed_count % 5 == 0:
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
import subprocess
import os
import sys
import time
import logging
import pandas as pd
from datetime import datetime

# 引入已存在的模块
from excel_reader import read_links_from_excel
from extractor import extract_product_links, extract_product_details

# 路径辅助函数
def get_exe_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

# Logger setup
logger = logging.getLogger('carrefour_scraper')
logger.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# 日志文件保存在项目根目录
log_file = os.path.normpath(os.path.join(get_exe_dir(), '..', 'scraper.log'))
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

def fetch_html_via_node(url, max_retries=3, base_delay=2.0):
    """
    调用 Node.js 脚本 (src/node/index.js) 获取页面渲染后的 HTML 源码
    """
    for attempt in range(1, max_retries + 1):
        logger.info(f"Fetching URL (Attempt {attempt}/{max_retries}): {url}")
        try:
            # 定位 node 脚本路径
            # 假设结构是 carrefour-scraper/src/node/index.js
            # 且当前脚本在 src/scraper.py
            base_path = get_exe_dir()
            node_script = os.path.join(base_path, 'node', 'index.js')
            
            # 如果是开发环境直接运行，可能需要调整路径
            if not os.path.exists(node_script):
                # 尝试相对于 src 的路径
                node_script = os.path.join(base_path, 'src', 'node', 'index.js')
            
            # 再做一个兜底检查，如果是从根目录运行
            if not os.path.exists(node_script):
                 node_script = 'src/node/index.js'

            # 调用 node
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
                logger.warning(f"Node process error: {stderr.strip()}")
                if attempt < max_retries:
                    time.sleep(base_delay * attempt)
                continue
                
            if not stdout.strip():
                logger.warning("Empty HTML returned from Node script.")
                if attempt < max_retries:
                    time.sleep(base_delay * attempt)
                continue

            return stdout

        except Exception as e:
            logger.exception(f"Subprocess call failed: {e}")
            time.sleep(base_delay * attempt)
    
    return None

def main():
    exe_folder = get_exe_dir()
    # 定位 input_links.xlsx
    excel_file_path = os.path.join(exe_folder, "..", "input_links.xlsx")
    if not os.path.exists(excel_file_path):
        excel_file_path = "input_links.xlsx" # 尝试当前目录

    logger.info(f"Reading store links from: {excel_file_path}")
    store_links = read_links_from_excel(excel_file_path)
    
    if not store_links:
        logger.error("No store links found in Excel. Please check input_links.xlsx.")
        return

    # --- 阶段 1: 收集商品链接 ---
    all_product_urls = set()
    logger.info("--- Phase 1: Collecting Product URLs from Store Pages ---")
    
    for link_item in store_links:
        # 这里的 link_item 可能是列表页链接，也可能是商品链接
        # 清理一下空白字符
        url_to_check = link_item.strip()
        
        # 判断是否直接为商品链接（包含 /p/）
        if "/p/" in url_to_check:
            logger.info(f"Detected direct product link: {url_to_check}")
            all_product_urls.add(url_to_check)
            continue # 跳过下面的店铺页抓取逻辑

        # 如果不是商品链接，则视为店铺/分类列表页
        html = fetch_html_via_node(url_to_check)
        if html:
            links = extract_product_links(html)
            logger.info(f"Found {len(links)} products on store page: {url_to_check}")
            for l in links:
                all_product_urls.add(l)
        else:
            logger.error(f"Failed to fetch store page: {url_to_check}")

    unique_product_list = sorted(list(all_product_urls))
    logger.info(f"Total unique products found: {len(unique_product_list)}")

    if not unique_product_list:
        logger.warning("No products found. Exiting.")
        return

    # --- 阶段 2: 抓取商品详情 ---
    scraped_data = []
    logger.info("--- Phase 2: Scraping Product Details ---")
    
    for i, prod_url in enumerate(unique_product_list):
        logger.info(f"[{i+1}/{len(unique_product_list)}] Processing: {prod_url}")
        
        prod_html = fetch_html_via_node(prod_url)
        
        if prod_html:
            details = extract_product_details(prod_html, prod_url)
            scraped_data.append(details)
            
            # 简单打印日志确认抓取情况
            title = details.get('Title', 'N/A')
            price = details.get('Price', 'N/A')
            logger.info(f"   -> Extracted: {title[:30]}... | Price: {price}")
        else:
            logger.error(f"   -> Failed to fetch HTML for {prod_url}")
            # 保留一条记录以便知道哪个失败了
            scraped_data.append({"Product URL": prod_url, "Title": "FETCH_FAILED"})

        # 避免请求过于频繁，稍微暂停
        time.sleep(1.5)

    # --- 阶段 3: 保存 Excel ---
    if scraped_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_filename = f"carrefour_products_{timestamp}.xlsx"
        # 保存到项目根目录
        output_path = os.path.join(exe_folder, "..", output_filename)
        if not os.path.exists(os.path.dirname(output_path)):
             output_path = output_filename

        logger.info(f"Saving {len(scraped_data)} rows to {output_path}")
        
        df = pd.DataFrame(scraped_data)
        
        # 设置列的顺序，符合阅读习惯
        columns_order = [
            "Product URL", "EAN", "Brand", "Title", "Category", 
            "Price", "Shipping Cost", "Description", 
            "Image 1", "Image 2", "Image 3", "Image 4", "Image 5"
        ]
        
        # 补全可能缺失的列，防止报错
        for col in columns_order:
            if col not in df.columns:
                df[col] = ""
                
        # 按照指定顺序输出
        df = df[columns_order]
        
        try:
            df.to_excel(output_path, index=False)
            logger.info("Successfully saved Excel file.")
        except Exception as e:
            logger.error(f"Failed to save Excel: {e}")
            # 如果Excel保存失败，尝试保存CSV作为备份
            csv_path = output_path.replace('.xlsx', '.csv')
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved to CSV backup: {csv_path}")
    else:
        logger.warning("No data extracted to save.")

if __name__ == "__main__":
    main()
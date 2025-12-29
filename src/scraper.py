import subprocess
import os
import re
import sys
import time
import logging

from excel_reader import read_links_from_excel
from extractor import extract_product_links

# 资源路径
def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def get_exe_dir():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

# Logger setup
logger = logging.getLogger('carrefour_scraper')
logger.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
log_file = os.path.normpath(os.path.join(get_exe_dir(), '..', 'scraper.log'))
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)
    
def scrape_store_links(links):
    product_links = []

    # Retry config
    MAX_RETRIES = 3
    BASE_DELAY = 2.0

    for link in links:
        logger.info(f"Scraping link: {link}")

        def fetch_and_extract(url, max_retries=MAX_RETRIES, base_delay=BASE_DELAY):
            for attempt in range(1, max_retries + 1):
                logger.info(f"Attempt {attempt}/{max_retries} for {url}")
                try:
                    result = subprocess.run(
                        ['node', os.path.join('src', 'node', 'index.js'), url],
                        capture_output=True,
                        text=True,
                        encoding='utf-8',
                        errors='replace',
                        timeout=60
                    )
                except Exception as e:
                    logger.exception(f"Subprocess call failed on attempt {attempt}: {e}")
                    if attempt < max_retries:
                        sleep_time = base_delay * (2 ** (attempt - 1))
                        logger.info(f"Sleeping {sleep_time}s before retry")
                        time.sleep(sleep_time)
                        continue
                    return []

                stderr = (result.stderr or '').strip()
                stdout = result.stdout or ''
                logger.debug(f"Subprocess returncode={result.returncode}; stdout_len={len(stdout)}; stderr_len={len(stderr)}")

                if result.returncode != 0:
                    logger.warning(f"Node process returned non-zero code: {result.returncode}; stderr: {stderr}")
                    if attempt < max_retries:
                        sleep_time = base_delay * (2 ** (attempt - 1))
                        logger.info(f"Sleeping {sleep_time}s before retry")
                        time.sleep(sleep_time)
                        continue
                    return []

                if not stdout.strip():
                    logger.warning(f"Empty HTML returned for {url}. stderr: {stderr}")
                    if attempt < max_retries:
                        sleep_time = base_delay * (2 ** (attempt - 1))
                        logger.info(f"Sleeping {sleep_time}s before retry")
                        time.sleep(sleep_time)
                        continue
                    return []

                # Extract links and decide whether to accept result
                extracted = extract_product_links(stdout)
                logger.info(f"Extracted {len(extracted)} product links from {url} on attempt {attempt}")
                if len(extracted) == 0 and attempt < max_retries:
                    logger.warning(f"0 links extracted, will retry")
                    sleep_time = base_delay * (2 ** (attempt - 1))
                    time.sleep(sleep_time)
                    continue

                return extracted

        try:
            extracted_links = fetch_and_extract(link)
            if extracted_links:
                product_links.extend(extracted_links)
            else:
                logger.warning(f"No links found for {link} after retries")
        except Exception as e:
            logger.exception(f"Unexpected error when scraping {link}: {e}")

    return product_links

def extract_product_links(html):
    # Use regex to find product links in the HTML source
    pattern = r'https://www\.carrefour\.fr/p/[^\s"]+'
    if not html:
        return []
    return re.findall(pattern, html)

def main():
    exe_folder = get_exe_dir()
    excel_file_path = os.path.join(exe_folder, "..", "input_links.xlsx")
    links = read_links_from_excel(excel_file_path)
    product_links = scrape_store_links(links)

    # Write the extracted product links to a temporary file
    with open('extracted_product_links.txt', 'w') as f:
        for link in product_links:
            f.write(link + '\n')
    print(f"Extracted {len(product_links)} product links.")

if __name__ == "__main__":
    main()
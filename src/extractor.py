import re
import json
import html
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit
import os
import requests
import logging
import time
import uuid
from typing import Optional
import configloader
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1. 获取主 Logger (必须与 scraper.py 中定义的名称一致)
logger = logging.getLogger('carrefour_scraper')

c = configloader.config()
IMAGE_HOST_UPLOAD_URL = c.get_key("IMAGE_HOST_UPLOAD_URL")
IMAGE_TOKEN = c.get_key("IMAGE_TOKEN")
MAX_RETRIES = 5

def extract_product_links(html_content):
    if not html_content:
        return []
    base_url = "https://www.carrefour.fr"
    found_links = set()
    regex_loose = r'(?:https?:\\?/\\?/[a-z0-9\.-]+)?(\\?/p\\?/[a-zA-Z0-9\-%_\.\?=&]+)'
    matches_loose = re.findall(regex_loose, html_content)
    for link in matches_loose:
        clean_link = link.replace('\\/', '/')
        found_links.add(clean_and_join(base_url, clean_link))
    return list(found_links)

def clean_and_join(base_url, link):
    link = link.strip()
    if not link.startswith('http'):
        if not link.startswith('/'):
            link = '/' + link
        full_url = urljoin(base_url, link)
    else:
        full_url = link
    return full_url

def remove_html_tags(text):
    if not text:
        return ""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text).strip()

# --- Robust Image Functions ---

def download_image(url: str, timeout: int = 30) -> Optional[str]:
    """下载图片到本地，包含详细的错误日志"""
    image_dir = "product_image"
    os.makedirs(image_dir, exist_ok=True)
    
    for attempt in range(MAX_RETRIES):
        try:
            # 增加 verify=False 以防证书问题，设置 strict headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=timeout)
            
            if response.status_code != 200:
                logger.error(f"[Download Fail] {url} | Status: {response.status_code}")
                # 非 200 状态码一般重试也没用，除非是 5xx
                if 500 <= response.status_code < 600:
                    time.sleep(1.5)
                    continue
                return None

            if not response.content:
                logger.error(f"[Download Fail] {url} | Empty content received")
                return None

            path = urlsplit(url).path
            ext = os.path.splitext(path)[1]
            if not ext: ext = '.jpg'
            
            filename = f"{uuid.uuid4()}{ext}"
            save_path = os.path.join(image_dir, filename)
            
            with open(save_path, 'wb') as f:
                f.write(response.content)
            
            return save_path

        except requests.exceptions.RequestException as e:
            # 只有最后一次重试失败才记录 ERROR，避免日志刷屏，但可以记录 debug
            if attempt == MAX_RETRIES - 1:
                logger.error(f"[Download Error] {url} after {MAX_RETRIES} retries: {e}")
                return None
            time.sleep(1.0 * (attempt + 1))
        except Exception as e:
            logger.error(f"[Download Exception] {url} : {e}")
            return None
            
    return None

def upload_to_image_host(file_path: str) -> Optional[str]:
    """上传图片到图床，包含响应内容分析日志"""
    if not os.path.exists(file_path):
        logger.error(f"[Upload Error] File not found: {file_path}")
        return None
    
    if os.path.getsize(file_path) == 0:
        logger.error(f"[Upload Error] Empty file: {file_path}")
        return None

    for attempt in range(MAX_RETRIES):
        try:
            with open(file_path, 'rb') as f:
                # 注意：files 需要重新打开，或者 seek(0)
                # 上面 with open 已经打开了，但在循环里每次都要重新读取流的位置
                # 这里每次循环都重新 open 比较安全
                pass

            with open(file_path, 'rb') as f:
                response = requests.post(
                    IMAGE_HOST_UPLOAD_URL,
                    files={'image': f},
                    data={'token': IMAGE_TOKEN},
                    timeout=15 * (attempt + 1)
                )
            
            if response.ok:
                try:
                    json_resp = response.json()
                    original_url = json_resp.get('url')
                    if original_url:
                        parts = list(urlsplit(original_url))
                        parts[1] = "gbcm-imagehost.vshare.dev"
                        return urlunsplit(parts)
                    else:
                        logger.error(f"[Upload Fail] JSON missing 'url' field. Resp: {json_resp}")
                except json.JSONDecodeError:
                    logger.error(f"[Upload Fail] Invalid JSON response. Body: {response.text[:100]}")
            else:
                # 记录非 200 的响应内容，帮助 Debug
                logger.error(f"[Upload Fail] Status: {response.status_code} | Body: {response.text[:200]}")

        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                logger.error(f"[Upload Error] Failed after {MAX_RETRIES} attempts: {e}")
            time.sleep(1.0 * (attempt + 1))
        except Exception as e:
            logger.error(f"[Upload Exception] {e}")
            break
            
    return None

def process_single_image(index, img_url):
    """
    辅助函数：处理单张图片的下载与上传，用于线程池
    返回 (index, final_url_or_None)
    """
    final_img = img_url.replace('p_FORMAT', 'p_1500x1500')
    uploaded_url = None
    local_path = None
    
    try:
        local_path = download_image(final_img)
        if local_path:
            uploaded_url = upload_to_image_host(local_path)
        else:
            # 下载失败已经在 download_image 中记录日志
            pass
            
    except Exception as e:
        logger.error(f"Image processing pipeline error for {final_img}: {e}")
    finally:
        # 清理本地文件
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except OSError as e:
                logger.warning(f"Failed to delete temp image {local_path}: {e}")
                
    return index, uploaded_url

def extract_product_details(html_content, product_url):
    data = {
        "Product URL": product_url,
        "Category": "", "Title": "", "Description": "",
        "Price": "", "Shipping Cost": "", "Brand": "", "EAN": "",
        "Image 1": "", "Image 2": "", "Image 3": "", "Image 4": "", "Image 5": ""
    }

    if not html_content:
        return data

    try:
        ean_match = re.search(r'-(\d+)$', product_url)
        ean = ean_match.group(1) if ean_match else None
        if ean: data['EAN'] = ean

        marker = "window.__INITIAL_STATE__="
        start_idx = html_content.find(marker)
        state_data = None
        
        if start_idx != -1:
            value_start = start_idx + len(marker)
            script_end_idx = html_content.find("</script>", value_start)
            if script_end_idx != -1:
                json_str = html_content[value_start:script_end_idx].strip()
                clean_json_str = json_str.replace('\\"', '"').replace('\\\\', '\\')
                if clean_json_str.endswith(';'): clean_json_str = clean_json_str[:-1]
                try:
                    state_data = json.loads(clean_json_str)
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON Decode Error for {product_url}: {e}")

        if state_data:
            try:
                products_map = state_data.get('vuex', {}).get('analytics', {}).get('indexedEntities', {}).get('product', {})
                if not ean or ean not in products_map:
                    if products_map:
                        ean = list(products_map.keys())[0]
                        data['EAN'] = ean
                
                product_info = products_map.get(ean, {})
                attributes = product_info.get('attributes', {})

                if attributes:
                    data['Title'] = attributes.get('title', '') or attributes.get('shortTitle', '')
                    data['Brand'] = attributes.get('brand', '')
                    
                    desc_obj = attributes.get('description', {})
                    raw_desc = desc_obj.get('long', '') or desc_obj.get('short', '')
                    data['Description'] = html.unescape(remove_html_tags(raw_desc))

                    categories = attributes.get('categories', [])
                    if categories:
                        sorted_cats = sorted(categories, key=lambda x: x.get('level', 0))
                        cat_names = [c.get('label', '') for c in sorted_cats]
                        data['Category'] = " / ".join(cat_names)

                    # --- 并发处理图片 (Concurrent Image Processing) ---
                    img_paths = attributes.get('images', {}).get('paths', [])
                    target_imgs = img_paths[:5]
                    
                    if target_imgs:
                        # 使用线程池并发下载和上传图片
                        # 建议 worker 数不宜过大，以免触发图床频率限制
                        with ThreadPoolExecutor(max_workers=5) as img_executor:
                            future_to_idx = {}
                            for i, img_url in enumerate(target_imgs):
                                future = img_executor.submit(process_single_image, i, img_url)
                                future_to_idx[future] = i
                            
                            for future in as_completed(future_to_idx):
                                try:
                                    idx, url = future.result()
                                    if url:
                                        data[f'Image {idx+1}'] = url
                                    else:
                                        # 如果 URL 为空，已经在 process_single_image 中记录了日志
                                        # 这里不需要额外操作
                                        pass
                                except Exception as exc:
                                    logger.error(f"Image thread exception for product {product_url}: {exc}")

                    # --- 价格和运费 ---
                    selected_offer_id = attributes.get('offerServiceId')
                    all_offers = attributes.get('offers', {})
                    offers_data = all_offers.get(ean, all_offers)
                    target_offer = None
                    
                    if selected_offer_id and selected_offer_id in offers_data:
                        target_offer = offers_data[selected_offer_id]
                    elif offers_data:
                        target_offer = offers_data[list(offers_data.keys())[0]]

                    if target_offer:
                        offer_attrs = target_offer.get('attributes', {})
                        final_price = None
                        
                        promotion = offer_attrs.get('promotion')
                        if promotion and isinstance(promotion, dict):
                            final_price = promotion.get('messageArgs', {}).get('discountedPrice')

                        if final_price is None:
                            final_price = offer_attrs.get('price', {}).get('price')
                        
                        if final_price is not None:
                            data['Price'] = f"{str(final_price).replace('.', ',')}€"

                        shipping_info = offer_attrs.get('marketplace', {}).get('shipping', {})
                        ship_cost = shipping_info.get('defaultShippingCharge')
                        is_free = shipping_info.get('freeShippingFlag')

                        if is_free is True:
                             data['Shipping Cost'] = "0,00€"
                        elif ship_cost is not None:
                            data['Shipping Cost'] = f"{str(ship_cost).replace('.', ',')}€"
                        else:
                            data['Shipping Cost'] = "See Site"

            except Exception as e:
                logger.error(f"Data Parsing Error for {product_url}: {e}")

    except Exception as e:
        logger.error(f"Extraction Error for {product_url}: {e}")

    return data
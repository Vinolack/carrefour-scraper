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
                if 500 <= response.status_code < 600:
                    time.sleep(1.0)
                    continue
                logger.error(f"[Download Fail] {url} | Status: {response.status_code}")
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
            if attempt == MAX_RETRIES - 1:
                logger.error(f"[Download Error] {url} after {MAX_RETRIES} retries: {e}")
                return None
            time.sleep(1.0 * (attempt + 1))
        except Exception as e:
            logger.error(f"[Download Exception] {url} : {e}")
            return None
            
    return None

def upload_to_image_host(file_path: str) -> Optional[str]:
    """上传图片到图床"""
    if not os.path.exists(file_path):
        return None
    
    if os.path.getsize(file_path) == 0:
        return None

    for attempt in range(MAX_RETRIES):
        try:
            with open(file_path, 'rb') as f:
                response = requests.post(
                    IMAGE_HOST_UPLOAD_URL,
                    files={'image': f},
                    data={'token': IMAGE_TOKEN},
                    timeout=20 * (attempt + 1)
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
                        logger.error(f"[Upload Fail] JSON missing 'url'. Resp: {json_resp}")
                except json.JSONDecodeError:
                    logger.error(f"[Upload Fail] Invalid JSON. Body: {response.text[:100]}")
            else:
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
    """辅助函数：处理单张图片的下载与上传，用于线程池"""
    final_img = img_url.replace('p_FORMAT', 'p_1500x1500')
    uploaded_url = None
    local_path = None
    
    try:
        local_path = download_image(final_img)
        if local_path:
            uploaded_url = upload_to_image_host(local_path)
    except Exception as e:
        logger.error(f"Image processing error for {final_img}: {e}")
    finally:
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except OSError:
                pass
    return index, uploaded_url

def extract_product_details(html_content, product_url):
    """
    从商品详情页HTML中提取详细信息。
    """
    data = {
        "Product URL": product_url,
        "Category": "", "Title": "", "Description": "",
        "Price": "", "Shipping Cost": "", "Brand": "", "EAN": "",
        "Image 1": "", "Image 2": "", "Image 3": "", "Image 4": "", "Image 5": ""
    }

    if not html_content:
        return data

    # 1. 尝试从 URL 提取 EAN
    try:
        ean_match = re.search(r'-(\d+)$', product_url)
        ean = ean_match.group(1) if ean_match else None
        if ean: data['EAN'] = ean
    except:
        pass

    state_data = None
    
    # 2. 尝试提取并解析 JSON 数据
    try:
        marker = "window.__INITIAL_STATE__="
        start_idx = html_content.find(marker)
        
        if start_idx != -1:
            value_start = start_idx + len(marker)
            script_end_idx = html_content.find("</script>", value_start)
            
            if script_end_idx != -1:
                json_str = html_content[value_start:script_end_idx].strip()
                # 去除末尾分号
                if json_str.endswith(';'):
                    json_str = json_str[:-1]
                
                # [关键修复] 直接加载，不要进行 replace 替换
                # 原来的 .replace('\\"', '"') 会破坏 JSON 结构，导致 Expecting ',' delimiter 错误
                try:
                    state_data = json.loads(json_str)
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON Decode Error for {product_url}: {e}")
    except Exception as e:
        logger.error(f"Error extracting JSON block for {product_url}: {e}")

    # 3. 解析 JSON 数据填充字段
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

                # --- 图片处理 ---
                img_paths = attributes.get('images', {}).get('paths', [])
                target_imgs = img_paths[:5]
                
                if target_imgs:
                    for i, img_url in enumerate(target_imgs):
                        try:
                            # 顺序调用，稳定第一
                            _, url = process_single_image(i, img_url)
                            if url:
                                data[f'Image {i+1}'] = url
                        except Exception as img_e:
                            logger.error(f"Error processing image {i} for {product_url}: {img_e}")

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

    # 4. [兜底策略] 如果 JSON 解析失败导致 Title 为空，尝试使用 Regex 从 HTML 直接提取
    if not data['Title']:
        logger.info(f"Fallback extraction (Regex) for {product_url}")
        
        # 提取标题: 通常在 <h1 class="...">Title</h1>
        try:
            title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html_content, re.IGNORECASE | re.DOTALL)
            if title_match:
                data['Title'] = remove_html_tags(title_match.group(1))
        except: pass

        # 提取价格: 查找类似 29,99€ 的模式
        if not data['Price']:
            try:
                # 匹配 'content="29.99"' 或者显示的文本 '29,99 €'
                # 优先找 meta price
                price_meta = re.search(r'itemprop="price"[^>]*content="([\d\.]+)"', html_content)
                if price_meta:
                    data['Price'] = f"{price_meta.group(1).replace('.', ',')}€"
                else:
                    # 尝试模糊匹配文本
                    price_text = re.search(r'(\d+,\d+)\s?€', html_content)
                    if price_text:
                        data['Price'] = f"{price_text.group(1)}€"
            except: pass

    return data
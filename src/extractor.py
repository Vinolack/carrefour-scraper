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
from src import configloader
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def format_price(val):
    """格式化价格：使用点号分隔小数，并加上€符号"""
    if val is None:
        return ""
    try:
        # 确保转为 float
        f_val = float(val)
        return f"{f_val:.2f}€"
    except (ValueError, TypeError):
        return str(val)

# --- Image Functions ---

def download_image(url: str, timeout: int = 30) -> Optional[str]:
    image_dir = "product_image"
    os.makedirs(image_dir, exist_ok=True)
    
    for attempt in range(MAX_RETRIES):
        try:
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

def extract_product_details(html_content, product_url, mode="full"):
    """
    mode="full": 提取所有信息（含图片），Price 为最低价。
    mode="price_check": 仅提取价格/卖家，Price 为页面真实价（BuyBox）。
    """
    data = {
        "Product URL": product_url,
        "Category": "", "Title": "", "Description": "",
        "Price": "", "Shipping Cost": "", "Brand": "", "EAN": "",
        "Seller": "",
        "more_seller1": "", "price1": "", "shipping1": "",
        "more_seller2": "", "price2": "", "shipping2": "",
        "more_seller3": "", "price3": "", "shipping3": "",
    }
    
    is_valid_parse = False
    # 仅 full 模式才预留图片字段
    if mode == "full":
        data.update({"Image 1": "", "Image 2": "", "Image 3": "", "Image 4": "", "Image 5": ""})

    if not html_content:
        return data

    # 1. 提取 EAN
    try:
        ean_match = re.search(r'-(\d+)$', product_url)
        ean = ean_match.group(1) if ean_match else None
        if ean: data['EAN'] = ean
    except: pass

    state_data = None
    try:
        marker = "window.__INITIAL_STATE__="
        start_idx = html_content.find(marker)
        if start_idx != -1:
            value_start = start_idx + len(marker)
            script_end_idx = html_content.find("</script>", value_start)
            if script_end_idx != -1:
                json_str = html_content[value_start:script_end_idx].strip()
                if json_str.endswith(';'): json_str = json_str[:-1]
                state_data = json.loads(json_str)
                is_valid_parse = True
    except Exception as e:
        logger.error(f"Error extracting JSON for {product_url}: {e}")

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
                # Basic Info
                data['Title'] = attributes.get('title', '') or attributes.get('shortTitle', '')
                
                # 仅 full 模式提取详细描述等
                if mode == "full":
                    data['Brand'] = attributes.get('brand', '')
                    desc_obj = attributes.get('description', {})
                    raw_desc = desc_obj.get('long', '') or desc_obj.get('short', '')
                    data['Description'] = html.unescape(remove_html_tags(raw_desc))
                    
                    categories = attributes.get('categories', [])
                    if categories:
                        sorted_cats = sorted(categories, key=lambda x: x.get('level', 0))
                        cat_names = [c.get('label', '') for c in sorted_cats]
                        data['Category'] = " / ".join(cat_names)

                # Images (仅 full 模式并行下载)
                if mode == "full":
                    img_paths = attributes.get('images', {}).get('paths', [])
                    target_imgs = img_paths[:5]
                    if target_imgs:
                        with ThreadPoolExecutor(max_workers=len(target_imgs)) as img_executor:
                            future_to_index = {
                                img_executor.submit(process_single_image, i, img_url): i 
                                for i, img_url in enumerate(target_imgs)
                            }
                            for future in as_completed(future_to_index):
                                idx = future_to_index[future]
                                try:
                                    _, url = future.result()
                                    if url: data[f'Image {idx+1}'] = url
                                except: pass

                # Offers Logic
                selected_offer_id = attributes.get('offerServiceId')
                offers_root = attributes.get('offers', {})
                raw_offers = offers_root.get(ean, {})
                parsed_offers = []
                offers_iter = raw_offers.values() if isinstance(raw_offers, dict) else raw_offers
                
                for offer in offers_iter:
                    try:
                        o_id = offer.get('id')
                        o_attrs = offer.get('attributes', {})
                        
                        price = None
                        promotion = o_attrs.get('promotion')
                        if promotion and isinstance(promotion, dict):
                            price = promotion.get('messageArgs', {}).get('discountedPrice')
                        if price is None:
                            price = o_attrs.get('price', {}).get('price')
                        if price is not None: price = float(price)
                        
                        marketplace = o_attrs.get('marketplace')
                        shipping_cost = 0.0
                        seller_name = "Carrefour"
                        
                        if marketplace:
                            shipping = marketplace.get('shipping', {})
                            is_free = shipping.get('freeShippingFlag')
                            if is_free is True:
                                shipping_cost = 0.0
                            else:
                                cost = shipping.get('defaultShippingCharge')
                                shipping_cost = float(cost) if cost is not None else 0.0
                            seller_name = marketplace.get('seller', 'Carrefour')
                        
                        if price is not None:
                            parsed_offers.append({
                                'id': o_id, 'seller': seller_name,
                                'price': price, 'shipping': shipping_cost
                            })
                    except: pass

                # Find Main Offer (BuyBox)
                main_offer = None
                if selected_offer_id:
                    for o in parsed_offers:
                        if o['id'] == selected_offer_id:
                            main_offer = o
                            break
                if not main_offer and parsed_offers:
                    main_offer = parsed_offers[0]
                
                # Fill Seller info
                if main_offer:
                    data['Seller'] = main_offer['seller']
                    data['Shipping Cost'] = format_price(main_offer['shipping']) if main_offer['shipping'] > 0 else "0.00€"

                # === Price Logic Distinction ===
                if parsed_offers:
                    min_price_val = min(o['price'] for o in parsed_offers)
                    
                    if mode == "price_check":
                        # price_check 模式：取页面真实价 (BuyBox)
                        if main_offer:
                            data['Price'] = format_price(main_offer['price'])
                        else:
                            data['Price'] = "None"
                    else:
                        # full 模式：取比价后的最低价
                        data['Price'] = format_price(min_price_val)

                # Competitors (Same for both modes, as requested)
                other_offers = [o for o in parsed_offers if o['id'] != selected_offer_id]
                other_offers.sort(key=lambda x: x['price'])
                for i in range(min(3, len(other_offers))):
                    offer = other_offers[i]
                    idx = i + 1
                    data[f'more_seller{idx}'] = offer['seller']
                    data[f'price{idx}'] = format_price(offer['price'])
                    data[f'shipping{idx}'] = format_price(offer['shipping']) if offer['shipping'] > 0 else "0.00€"

        except Exception as e:
            logger.error(f"Parsing error: {e}")

    # Fallback (Regex)
    if not data['Title']:
        try:
            m = re.search(r'<h1[^>]*>(.*?)</h1>', html_content, re.IGNORECASE|re.DOTALL)
            if m: data['Title'] = remove_html_tags(m.group(1))
        except: pass
    
    # Fallback Price
    if not data['Price']:
        try:
            m = re.search(r'itemprop="price"[^>]*content="([\d\.]+)"', html_content)
            if m: data['Price'] = format_price(m.group(1))
        except: pass

    # === 数据校验与过滤 ===

    # 1. 基础完整性校验 (适用于所有模式)
    # 必须解析出 JSON (state_data) 或者至少必须有价格，否则视为无效抓取
    if not state_data and not data['Price']:
        return {
            "Product URL": product_url,
            "error": "Critical: JSON parsing failed AND Price missing"
        }

    # 2. 模式特定处理
    if mode == "price_check":
        # 校验：price_check 模式必须包含 Seller 信息
        if not data['Price'] or not data['Seller']:
             return {
                "Product URL": product_url,
                "error": "Incomplete data for price_check: Missing Price or Seller"
            }
        
        # 过滤：只返回指定字段
        return {
            "Product URL": data["Product URL"],
            "Price": data["Price"],
            "Shipping Cost": data["Shipping Cost"],
            "Seller": data["Seller"]
        }
    
    # full 模式的校验
    if mode == "full":
        # full 模式通常需要更严格的校验，比如必须有 Title
        if not data['Title'] or not data['Price']:
            return {
                "Product URL": product_url,
                "error": "Incomplete data for full scan: Missing Title or Price"
            }

    return data
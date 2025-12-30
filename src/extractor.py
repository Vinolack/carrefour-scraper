import re
import json
import html
from urllib.parse import urljoin, urlparse

def extract_product_links(html_content):
    """
    从HTML内容中提取 Carrefour 商品链接。
    """
    if not html_content:
        return []
    
    base_url = "https://www.carrefour.fr"
    found_links = set()

    # ---------------------------------------------------------
    # 扫描所有包含 /p/ 的类似链接的字符串
    # ---------------------------------------------------------
    # 匹配 /p/ 开头，直到遇到 引号、空格、问号或 HTML 标签结束符
    # 适用于 JSON 中的 "\/p\/..." 或者 href="/p/..."
    regex_loose = r'(?:https?:\\?/\\?/[a-z0-9\.-]+)?(\\?/p\\?/[a-zA-Z0-9\-%_\.]+)'
    matches_loose = re.findall(regex_loose, html_content)
    for link in matches_loose:
        # 修复可能存在的转义斜杠 (例如 json 中的 \/)
        clean_link = link.replace('\\/', '/')
        found_links.add(clean_and_join(base_url, clean_link))

    return list(found_links)

def clean_and_join(base_url, link):
    """辅助函数：拼接 URL 并清理格式"""
    link = link.strip()
    
    # 如果是相对路径，拼接域名
    if not link.startswith('http'):
        # 确保 link 以 / 开头 (urljoin 需要)
        if not link.startswith('/'):
            link = '/' + link
        full_url = urljoin(base_url, link)
    else:
        full_url = link
    return full_url

def remove_html_tags(text):
    """移除字符串中的HTML标签"""
    if not text:
        return ""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text).strip()

def extract_product_details(html_content, product_url):
    """
    从商品详情页HTML中提取详细信息。
    返回包含所需字段的字典。
    """
    data = {
        "Product URL": product_url,
        "Category": "",
        "Title": "",
        "Description": "",
        "Price": "",
        "Shipping Cost": "",
        "Brand": "",
        "EAN": "",
        "Image 1": "", "Image 2": "", "Image 3": "", "Image 4": "", "Image 5": ""
    }

    if not html_content:
        return data

    try:
        # 1. 尝试从 URL 提取 EAN (通常在末尾)
        ean_match = re.search(r'-(\d+)$', product_url)
        ean = ean_match.group(1) if ean_match else None
        if ean:
            data['EAN'] = ean

        # 2. 提取 __INITIAL_STATE__ JSON 数据 (使用字符串定位法)
        marker = "window.__INITIAL_STATE__="
        start_idx = html_content.find(marker)
        
        state_data = None
        
        if start_idx != -1:
            # 确定截取范围
            value_start = start_idx + len(marker)
            # 寻找脚本结束标签
            script_end_idx = html_content.find("</script>", value_start)
            
            if script_end_idx != -1:
                json_str = html_content[value_start:script_end_idx].strip()
                clean_json_str = json_str.replace('\\"', '"').replace('\\\\', '\\')
                # 清理末尾可能存在的分号
                if clean_json_str.endswith(';'):
                    clean_json_str = clean_json_str[:-1]
                
                try:
                    state_data = json.loads(clean_json_str)
                except json.JSONDecodeError as e:
                    print(f"JSON Decode Error for {product_url}: {e}")
        else:
            print(f"Warning: '__INITIAL_STATE__' not found in HTML for {product_url}")

        # 3. 解析 JSON 数据
        if state_data:
            try:
                # 路径: vuex -> analytics -> indexedEntities -> product
                products_map = state_data.get('vuex', {}).get('analytics', {}).get('indexedEntities', {}).get('product', {})
                
                # 如果没有 EAN 或者 EAN 不在 keys 里，尝试取第一个 key
                if not ean or ean not in products_map:
                    if products_map:
                        ean = list(products_map.keys())[0]
                        data['EAN'] = ean
                
                product_info = products_map.get(ean, {})
                attributes = product_info.get('attributes', {})

                if not attributes:
                    print(f"Warning: No attributes found for EAN {ean}")
                    return data

                # --- 基础信息 ---
                data['Title'] = attributes.get('title', '') or attributes.get('shortTitle', '')
                data['Brand'] = attributes.get('brand', '')

                # --- 描述 ---
                desc_obj = attributes.get('description', {})
                raw_desc = desc_obj.get('long', '') or desc_obj.get('short', '')
                data['Description'] = html.unescape(remove_html_tags(raw_desc))

                # 提取类目
                # categories 是一个列表，我们将其拼接成路径
                categories = attributes.get('categories', [])
                if categories:
                    # 按 level 排序以防乱序
                    sorted_cats = sorted(categories, key=lambda x: x.get('level', 0))
                    cat_names = [c.get('label', '') for c in sorted_cats]
                    data['Category'] = " / ".join(cat_names)

                # --- 图片 ---
                img_paths = attributes.get('images', {}).get('paths', [])
                for i, img_url in enumerate(img_paths[:5]):
                    final_img = img_url.replace('p_FORMAT', 'p_1500x1500')
                    data[f'Image {i+1}'] = final_img

                # --- 价格和运费 ---
                selected_offer_id = attributes.get('offerServiceId')
                all_offers = attributes.get('offers', {})
                
                # 兼容 offers 结构: 可能包含 EAN 层级，也可能直接是 offer ID 层级
                offers_data = {}
                if ean in all_offers:
                    offers_data = all_offers[ean]
                else:
                    offers_data = all_offers

                target_offer = None
                
                # 定位 offer
                if selected_offer_id and selected_offer_id in offers_data:
                    target_offer = offers_data[selected_offer_id]
                elif offers_data:
                    first_key = list(offers_data.keys())[0]
                    target_offer = offers_data[first_key]

                if target_offer:
                    # 价格
                    offer_attrs = target_offer.get('attributes', {})

                    final_price = None
                    
                    # 1. 尝试从 promotion 中获取折扣价
                    # 数据结构示例: attributes -> promotion -> messageArgs -> discountedPrice
                    promotion = offer_attrs.get('promotion')
                    if promotion and isinstance(promotion, dict):
                        message_args = promotion.get('messageArgs', {})
                        final_price = message_args.get('discountedPrice')

                    # 2. 如果没有折扣价，从 price 对象中获取原价
                    # 数据结构示例: attributes -> price -> price
                    if final_price is None:
                        price_info = offer_attrs.get('price', {})
                        final_price = price_info.get('price')
                    
                    if final_price is not None:
                        data['Price'] = f"{str(final_price).replace('.', ',')}€"
                    else:
                         # 如果真的找不到价格，可以设为默认值或保持 None
                        pass

                    # 数据结构示例: attributes -> marketplace -> shipping
                    shipping_info = offer_attrs.get('marketplace', {}).get('shipping', {})
                    ship_cost = shipping_info.get('defaultShippingCharge')
                    is_free_shipping = shipping_info.get('freeShippingFlag')

                    if is_free_shipping is True:
                         data['Shipping Cost'] = "0,00€"
                    elif ship_cost is not None:
                        data['Shipping Cost'] = f"{str(ship_cost).replace('.', ',')}€"
                    else:
                        # 只有当既不是免运费，又没有具体金额时，才显示 See Site
                        data['Shipping Cost'] = "See Site"

            except Exception as e:
                print(f"Data Parsing Error: {e}")

    except Exception as e:
        print(f"Extraction Error: {e}")

    return data
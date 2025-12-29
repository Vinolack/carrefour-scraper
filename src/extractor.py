def extract_product_links(html_content):
    import re
    from urllib.parse import urljoin

    product_links = []
    base_url = "https://www.carrefour.fr"

    # (?:https://www\.carrefour\.fr)? 表示域名部分是可选的
    # /p/[^"\']+ 表示匹配 /p/ 开头直到遇到引号结束的字符串
    pattern = r'href=["\']((?:https://www\.carrefour\.fr)?/p/[^"\']+)["\']'
    
    matches = re.findall(pattern, html_content)

    for match in matches:
        # 1. 如果 match 是 "/p/xxx" (相对路径)，它会自动拼接 base_url 变成 "https://www.carrefour.fr/p/xxx"
        # 2. 如果 match 已经是 "https://..." (绝对路径)，它会直接使用 match，忽略 base_url
        full_url = urljoin(base_url, match)
        
        # 过滤掉非商品链接（双重保险）
        if "/p/" in full_url:
            product_links.append(full_url)

    # 使用 set 去重，防止同一个商品被多次提取
    return list(set(product_links))

def read_links_from_file(file_path):
    # Delegate to excel_reader which supports the new comma-separated format
    from .excel_reader import read_links_from_excel
    return read_links_from_excel(file_path)
import re
from urllib.parse import urljoin

def extract_product_links(html_content):
    """
    从HTML内容中提取 Carrefour 商品链接。
    采用多重策略以确保最大兼容性。
    """
    if not html_content:
        print("警告: extract_product_links 收到了空的内容")
        return []
    
    # 调试信息：确认接收到的内容长度
    print(f"DEBUG: 正在处理 HTML 内容，长度: {len(html_content)} 字符")

    base_url = "https://www.carrefour.fr"
    found_links = set()

    # ---------------------------------------------------------
    # 策略 1: 标准 href 匹配 (支持单引号、双引号，支持 href = "...")
    # ---------------------------------------------------------
    # 解释:
    # href\s*=\s* -> 匹配 href=，允许等号周围有空格
    # ["']         -> 匹配开头的引号 (单引号或双引号)
    # (.*?)        -> 捕获链接内容 (非贪婪)
    # ["']         -> 匹配结尾的引号
    regex_href = r'href\s*=\s*["\']([^"\']*/p/[^"\']+)["\']'
    
    matches_href = re.findall(regex_href, html_content, re.IGNORECASE)
    for link in matches_href:
        found_links.add(clean_and_join(base_url, link))

    # ---------------------------------------------------------
    # 策略 2: 兜底策略 - 扫描所有包含 /p/ 的类似链接的字符串
    # ---------------------------------------------------------
    # 如果策略 1 没找到，或者 HTML 格式非常乱 (例如 JSON 字符串中的转义引号)
    if len(found_links) == 0:
        print("DEBUG: 策略1未找到链接，尝试策略2 (JSON/宽泛搜索)...")
        # 匹配 /p/ 开头，直到遇到 引号、空格、问号或 HTML 标签结束符
        # 适用于 JSON 中的 "\/p\/..." 或者 href="/p/..."
        regex_loose = r'(?:https?:\\?/\\?/[a-z0-9\.-]+)?(\\?/p\\?/[a-zA-Z0-9\-%_\.]+)'
        
        matches_loose = re.findall(regex_loose, html_content)
        for link in matches_loose:
            # 修复可能存在的转义斜杠 (例如 json 中的 \/)
            clean_link = link.replace('\\/', '/')
            found_links.add(clean_and_join(base_url, clean_link))

    product_links = list(found_links)
    print(f"DEBUG: 提取结束，共找到 {len(product_links)} 个链接")
    
    return product_links

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

def read_links_from_file(file_path):
    from .excel_reader import read_links_from_excel
    return read_links_from_excel(file_path)
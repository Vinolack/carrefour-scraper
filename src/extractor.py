def extract_product_links(html_content):
    import re
    from urllib.parse import urljoin

    product_links = []
    base_url = "https://www.carrefour.fr"

    # Regular expression to find product links
    pattern = r'href=["\'](https://www\.carrefour\.fr/p/[^"\']+)["\']'
    matches = re.findall(pattern, html_content)

    for match in matches:
        full_url = urljoin(base_url, match)
        product_links.append(full_url)

    return product_links


def read_links_from_file(file_path):
    # Delegate to excel_reader which supports the new comma-separated format
    from .excel_reader import read_links_from_excel
    return read_links_from_excel(file_path)
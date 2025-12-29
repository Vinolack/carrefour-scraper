import pandas as pd
import json
import os

def _load_config():
    cfg_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'config.json')
    # cfg_path above goes up one extra level; normalize
    cfg_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', 'config.json'))
    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return { 'default_pages': 1 }

def read_links_from_excel(file_path):
    """
    Reads rows where each row can be either:
      - separate columns: 'Store Link' / 'Page Number' (or lowercase variants),
      - or a single cell with comma-separated values: "<store_link>,<page1>,<page2>,..."

    Returns a flat list of fully-expanded page URLs (with ?noRedirect=1&page=N appended when N>1).
    """
    cfg = _load_config()
    default_pages = int(cfg.get('default_pages', 1))

    try:
        df = pd.read_excel(file_path, dtype=str)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return []

    links = []

    # helper to expand pages
    def expand(link, pages):
        for p in pages:
            try:
                pi = int(str(p).strip())
            except Exception:
                continue
            if pi > 1:
                links.append(f"{link}?noRedirect=1&page={pi}")
            else:
                links.append(link)

    # check for column-based format
    cols = [c.lower() for c in df.columns]
    if 'store link' in cols or 'store_link' in cols:
        # column based
        for _, row in df.iterrows():
            # try multiple name variations
            store = None
            page_cell = None
            for name in ['Store Link', 'store_link', 'store link', 'Store_Link', 'store link']:
                if name in df.columns:
                    store = row.get(name)
                    break
            if store is None:
                # fallback to first column
                store = row.iloc[0]

            for pname in ['Page Number', 'page_number', 'page number', 'Page_Number']:
                if pname in df.columns:
                    page_cell = row.get(pname)
                    break

            store = str(store).strip()
            if page_cell is None or str(page_cell).strip() == '' or str(page_cell).strip().lower() == 'nan':
                pages = list(range(1, default_pages + 1))
            else:
                # allow comma-separated page numbers in cell
                parts = str(page_cell).split(',')
                pages = [p.strip() for p in parts if p.strip()]

            expand(store, pages)
    else:
        # row-based single cell with comma-separated values
        for _, row in df.iterrows():
            first = None
            # find first non-null cell
            for v in row.values:
                if v is not None and str(v).strip() != '' and str(v).strip().lower() != 'nan':
                    first = str(v).strip()
                    break
            if not first:
                continue
            parts = [p.strip() for p in first.split(',') if p.strip()]
            if not parts:
                continue
            store = parts[0]
            if len(parts) == 1:
                pages = list(range(1, default_pages + 1))
            else:
                pages = parts[1:]
            expand(store, pages)

    return links
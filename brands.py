# -*- coding: utf-8 -*-
"""
DISCOVERY ONLY: Find supplier URLs for each brand using googlesearch (no API key).
- Runs targeted Google queries per brand
- Filters for likely supplier domains (HVAC-ish)
- Dedupes to one URL per domain
- Saves Brand | Domain | URL | Query to CSV/Excel

Run:
  python discover_supplier_urls.py
"""

import time, random, csv
from typing import List, Dict
import pandas as pd
import tldextract

# --- googlesearch import (no API key needed) ---
try:
    from googlesearch import search
except Exception:
    raise SystemExit("Install googlesearch-python: pip install googlesearch-python (or: pip install google)")

# =======================
# CONFIG
# =======================
BRANDS = [
    "Carrier","Trane","Lennox","Daikin","Mitsubishi Electric","Goodman","Rheem","Ruud","York",
    "Bryant","American Standard","Bosch","LG","Fujitsu","Tempstar","Payne","ICP","Johnson Controls",
    "Emerson/Copeland","Danfoss","Honeywell","Siemens","Schneider","Aprilaire","Nu-Calgon",
    "Fieldpiece","Testo","Amana","Electrolux","Panasonic","Toshiba","Lloyd", "Buderus", "Arcoaire", 
    "Comfortmaker", "Day & Night", "Heil", "Alliance Air Products", "Daikin Applied", "Quietflex", "Fujitsu Halcyon", 
    "Gree", "Champion", "Coleman", "Luxaire", "Hitachi", "Johnson Control–Hitachi", "AirEase", "Armstrong Air", "Concord", 
    "Ducane", "Broan", "Frigidaire", "Gibson", "Intertherm", "Maytag", "Miller", "Reznor", "Sure Comfort", "WeatherKing", 
    "Samsung", "Toshiba-Carrier",
]

# How many results per query & how many query patterns per brand
RESULTS_PER_QUERY   = 10           # 10–20 is a good start
QUERIES_PER_BRAND   = 5            # number of patterns to run per brand
GOOGLE_PAUSE_SECS   = 3.5          # wait between requests to avoid captchas
MAX_DOMAINS_PER_BRAND = 50         # cap to keep results manageable

# Optional ZIPs to bias to US suppliers (will be mixed into queries at the end)
ZIP_SEEDS = ["10001","90001","60601","77002","33101","85001","80202","98101","19103","30303"]
USE_ZIPS  = True
ZIP_SAMPLES_PER_BRAND = 3          # how many ZIPs to add per brand

# Query patterns (we’ll take the first QUERIES_PER_BRAND)
QUERY_PATTERNS = [
    '{brand} HVAC distributor',
    '{brand} authorized dealer',
    '{brand} HVAC supplier',
    '{brand} refrigeration distributor',
    '{brand} "where to buy" HVAC',
    '{brand} "find a dealer" HVAC',
    '{brand} wholesale HVAC',
    '{brand} HVAC wholesaler',
    '{brand} sales rep',
    '{brand} representatives',
    '{brand} contractors',
    '{brand} parts distributor',
    '{brand} "find a dealer"',
    '{brand} "where to buy"',
    '{brand} "find a contractor"',
    '{brand} "find a distributor"',


]

# Filter: domains to skip (noise) & words that suggest supplier relevance
SKIP_DOMAIN_CONTAINS = [
    "facebook.com","twitter.com","linkedin.com","instagram.com","youtube.com",
    "indeed.com","glassdoor.com","ziprecruiter.com","wikipedia.org","amazon.com","ebay.com",
    "/careers","/jobs","/news","/blog","/press","/cookie","/privacy","/terms","/legal"
]
LIKELY_SUPPLIER_HINTS = [
    "hvac","heating","cooling","refrigeration","mechanical","supply","supplies","distributor",
    "wholesale","wholesaler","contractor","air-conditioning","ac-supply","parts","filters","thermostat"
]

OUT_CSV  = "supplier_urls_by_brand.csv"
OUT_XLSX = "supplier_urls_by_brand.xlsx"

# =======================
# HELPERS
# =======================
def likely_supplier_url(url: str) -> bool:
    u = url.lower()
    if any(s in u for s in SKIP_DOMAIN_CONTAINS):
        return False
    return True  # keep everything else for discovery stage

def dedupe_by_domain(urls: List[str]) -> List[str]:
    seen, out = set(), []
    for u in urls:
        dom = tldextract.extract(u).registered_domain
        if not dom or dom in seen:
            continue
        seen.add(dom)
        out.append(u)
    return out

def discover_for_brand(brand: str) -> List[Dict[str, str]]:
    # Build queries
    patterns = QUERY_PATTERNS[:QUERIES_PER_BRAND]
    queries = [p.format(brand=brand) for p in patterns]

    if USE_ZIPS and ZIP_SEEDS:
        zips = random.sample(ZIP_SEEDS, min(ZIP_SAMPLES_PER_BRAND, len(ZIP_SEEDS)))
        for z in zips:
            queries.append(f'{brand} HVAC distributor {z}')

    found: List[Dict[str, str]] = []
    raw_urls: List[str] = []

    # Run each query
    for q in queries:
        try:
            for url in search(q, stop=RESULTS_PER_QUERY, pause=GOOGLE_PAUSE_SECS):
                raw_urls.append(url)
        except Exception as e:
            print("Search error:", e)
            time.sleep(6)
        # Print raw results before filtering
        print(f"Query: {q} -> {len(raw_urls)} raw URLs so far")
        # Polite jitter between queries
        time.sleep(random.uniform(1.0, 2.0))

    # Filter & dedupe by domain
    raw_urls = [u for u in raw_urls if likely_supplier_url(u)]
    deduped = dedupe_by_domain(raw_urls)[:MAX_DOMAINS_PER_BRAND]

    # Build rows
    rows = []
    for u in deduped:
        rows.append({
            "Brand": brand,
            "Domain": tldextract.extract(u).registered_domain or "",
            "URL": u,
            "Query": ""  # (left empty; we could store the specific query if needed)
        })
    return rows

# =======================
# MAIN
# =======================
def main():
    all_rows: List[Dict[str, str]] = []
    for brand in BRANDS:
        print(f"[DISCOVER] {brand}")
        rows = discover_for_brand(brand)
        print(f"  -> {len(rows)} unique supplier domains")
        all_rows.extend(rows)

    # Save
    df = pd.DataFrame(all_rows, columns=["Brand","Domain","URL","Query"])
    # Sort for readability
    df = df.sort_values(["Brand","Domain"]).reset_index(drop=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    df.to_excel(OUT_XLSX, index=False)
    print(f"\nSaved {len(df)} rows")
    print(f"- {OUT_CSV}")
    print(f"- {OUT_XLSX}")

if __name__ == "__main__":
    main()

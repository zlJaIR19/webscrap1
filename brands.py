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

# --- Search backends ---
USE_DDG = True   # <<< set True to bypass googlesearch issues for now

# Tolerant wrapper around googlesearch (handles different signatures)
def gs_search(query: str, count: int, pause: float):
    results = []
    try:
        # some forks use num_results=
        for url in search(query, num_results=count, pause=pause, lang="en", tld="com", safe="off"):
            results.append(url)
        return results
    except TypeError:
        pass
    try:
        # others use num= (no stop=)
        for url in search(query, num=count, pause=pause, lang="en", tld="com"):
            results.append(url)
        return results
    except Exception as e:
        print("googlesearch error:", e)
        return results

# DuckDuckGo HTML search (no API key)
import httpx, urllib.parse
from bs4 import BeautifulSoup

def _unwrap_ddg_href(href: str) -> str | None:
    # DDG returns //duckduckgo.com/l/?uddg=<encoded>
    if "/l/?" in href and "uddg=" in href:
        q = urllib.parse.urlsplit(href).query
        params = urllib.parse.parse_qs(q)
        if "uddg" in params and params["uddg"]:
            return urllib.parse.unquote(params["uddg"][0])
        return None
    # or an absolute URL
    if href.startswith("http"):
        return href
    return None

def ddg_search(query: str, count: int = 20) -> list[str]:
    # hit the HTML endpoint directly and follow redirects (302)
    url = "https://html.duckduckgo.com/html/"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = httpx.get(url, params={"q": query}, headers=headers,
                      timeout=30, follow_redirects=True)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "lxml")
        out = []
        # result links live in a.result__a (HTML SERP)
        for a in soup.select("a.result__a"):
            href = a.get("href")
            if not href:
                continue
            real = _unwrap_ddg_href(href)
            if real:
                out.append(real)
                if len(out) >= count:
                    break
        return out
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            print(f"🚫 DDG blocked query: {query[:50]}...")
            return []
        else:
            print(f"❌ DDG HTTP error {e.response.status_code}: {query[:50]}...")
            return []
    except Exception as e:
        print(f"❌ DDG error: {e}")
        return []

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
RESULTS_PER_QUERY   = 5          # 10–20 is a good start
QUERIES_PER_BRAND   = 3            # number of patterns to run per brand
GOOGLE_PAUSE_SECS   = 3.5          # wait between requests to avoid captchas
MAX_DOMAINS_PER_BRAND = 50         # cap to keep results manageable

# Optional ZIPs to bias to US suppliers (will be mixed into queries at the end)
ZIP_SEEDS = ["10001","90001","60601","77002","33101","85001","80202","98101","19103","30303"]
USE_ZIPS  = False
ZIP_SAMPLES_PER_BRAND = 3          # how many ZIPs to add per brand

# Query patterns (we'll take the first QUERIES_PER_BRAND)
QUERY_PATTERNS = [
    '{brand} HVAC distributor',
    '{brand} HVAC supplier',
    '{brand} HVAC wholesaler',
    '{brand} authorized dealer',
    '{brand} "find a dealer"',
    '{brand} "where to buy"',
    '{brand} sales rep',
    '{brand} representatives',
    '{brand} parts distributor',
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
    bad = ["facebook.com","twitter.com","linkedin.com","instagram.com",
           "youtube.com","indeed.com","glassdoor.com","ziprecruiter.com",
           "wikipedia.org","amazon.com","ebay.com"]
    return not any(b in u for b in bad)

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
        urls = ddg_search(q, RESULTS_PER_QUERY) if USE_DDG else gs_search(q, RESULTS_PER_QUERY, GOOGLE_PAUSE_SECS)
        print(f"Query: {q} -> RAW +{len(urls)}", urls[:5])
        raw_urls.extend(urls)
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

def write_outputs(rows: List[Dict[str, str]]) -> None:
    df = pd.DataFrame(rows, columns=["Brand","Domain","URL","Query"])
    df = df.sort_values(["Brand","Domain"]).reset_index(drop=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    df.to_excel(OUT_XLSX, index=False)

# =======================
# MAIN
# =======================
def main():
    all_rows: List[Dict[str, str]] = []
    
    for i, brand in enumerate(BRANDS):
        print(f"[DISCOVER] {brand} ({i+1}/{len(BRANDS)})")
        try:
            rows = discover_for_brand(brand)
            print(f"  -> {len(rows)} unique supplier domains")
            all_rows.extend(rows)
            
            # Save progress every 5 brands to avoid losing data
            if (i + 1) % 5 == 0 or i == len(BRANDS) - 1:
                print(f" Saving progress... ({len(all_rows)} total rows)")
                write_outputs(all_rows)
                
        except KeyboardInterrupt:
            print(f"\n Interrupted! Saving {len(all_rows)} rows collected so far...")
            write_outputs(all_rows)
            break
        except Exception as e:
            print(f" Error with {brand}: {e}")
            continue

    print(f"\n Final save: {len(all_rows)} total rows")
    write_outputs(all_rows)

if __name__ == "__main__":
    main()

# best_hvac_scraper.py
# -*- coding: utf-8 -*-
"""
HVAC Supplier Scraper
- Input: Excel/CSV with column 'Website'
- Output: Excel/CSV with 10 fields filled
"""

import re, time, random
import pandas as pd
import httpx
from bs4 import BeautifulSoup
import phonenumbers
from urllib.parse import urljoin

# --------------------
# CONFIG
# --------------------
INPUT_FILE = "HVAC_Suppliers.xlsx"
OUTPUT_XLSX = "HVAC_Suppliers_Populated.xlsx"
OUTPUT_CSV  = "HVAC_Suppliers_Populated.csv"

FIELDS = [
    "Company Name", "Website", "Location", "Contact Person", "Role (Contact Person)",
    "Phone Number", "Email", "Brands Distributed", "Equipment Categories Offered",
    "Key Parts and Components Available"
]

BRANDS = [
    "Carrier","Trane","Lennox","Daikin","Mitsubishi Electric","Goodman","Rheem","Ruud","York",
    "Bryant","American Standard","Bosch","LG","Fujitsu","Tempstar","Payne","ICP","Johnson Controls",
    "Emerson/Copeland","Danfoss","Honeywell","Siemens","Schneider","Aprilaire","Nu-Calgon",
    "Fieldpiece","Testo","Amana","Electrolux","Panasonic","Toshiba","Lloyd","Buderus","Arcoaire",
    "Comfortmaker","Day & Night","Heil","Alliance Air Products","Daikin Applied","Quietflex","Fujitsu Halcyon",
    "Gree","Champion","Coleman","Luxaire","Hitachi","AirEase","Armstrong Air","Concord","Ducane","Broan",
    "Frigidaire","Gibson","Intertherm","Maytag","Miller","Reznor","Sure Comfort","WeatherKing","Samsung",
    "Toshiba-Carrier"
]

EQUIPMENT_KEYWORDS = [
    "air conditioner","heat pump","furnace","boiler","chiller","mini split","thermostat",
    "air handler","ventilation","humidifier","dehumidifier","controls","packaged unit"
]

PARTS_KEYWORDS = [
    # Core mechanical components
    "compressor", "coil", "evaporator coil", "condenser coil", "heat exchanger",
    "blower", "fan", "motor", "capacitor", "contactors", "relays",

    # Controls & electronics
    "thermostat", "control board", "circuit board", "defrost control", "ignition control",
    "relay", "sensor", "pressure switch", "limit switch", "contactor",

    # Refrigerant & flow components
    "refrigerant", "expansion valve", "txv", "metering device", "solenoid valve",
    "service valve", "suction line", "liquid line", "filter drier", "sight glass",

    # Airflow & filtration
    "filter", "air filter", "pleated filter", "belt", "pulley", "sheave",
    "fan blade", "wheel", "duct", "grille", "damper",

    # Heating-specific
    "burner", "igniter", "flame sensor", "pilot assembly", "gas valve",
    "oil nozzle", "heat strip", "sequencer",

    # Cooling-specific
    "condensate pump", "drain pan", "drain line", "float switch",

    # Misc
    "gasket", "seal", "insulation", "thermocouple", "transformer",
    "humidifier pad", "uv lamp", "lamp ballast"
]
# --------------------
# HELPERS
# --------------------
def fetch_html(url: str) -> str | None:
    # Skip invalid URLs
    if not url or not url.startswith(("http://", "https://")):
        return None
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; HVAC-Scraper/1.0)"}
        r = httpx.get(url, timeout=20, headers=headers, follow_redirects=True)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        print(f"Fetch failed: {url[:50]}... - {type(e).__name__}")
    return None

def extract_phone(text: str) -> str | None:
    for m in phonenumbers.PhoneNumberMatcher(text, "US"):
        return phonenumbers.format_number(m.number, phonenumbers.PhoneNumberFormat.NATIONAL)
    return None

def extract_email(text: str) -> str | None:
    m = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, re.I)
    return m.group(0) if m else None

def detect_brands(text: str, soup: BeautifulSoup) -> list[str]:
    found = set()
    lowtext = text.lower()
    for b in BRANDS:
        if b.lower() in lowtext:
            found.add(b)
    # check image alts
    for img in soup.select("img[alt]"):
        alt = img.get("alt", "").strip()
        if alt:
            for b in BRANDS:
                if b.lower() in alt.lower():
                    found.add(b)
    return sorted(found)

def detect_keywords(text: str, keywords: list[str]) -> list[str]:
    found = []
    lowtext = text.lower()
    for kw in keywords:
        if kw in lowtext:
            found.append(kw.capitalize())
    return sorted(set(found))

def try_subpages(base_url: str, soup: BeautifulSoup) -> list[str]:
    out = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        # Skip mailto, tel, javascript, and anchor links
        if href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        href_lower = href.lower()
        if any(k in href_lower for k in ["contact","about","brand","product","service","part","catalog"]):
            full_url = urljoin(base_url, href)
            # Only add valid http/https URLs
            if full_url.startswith(("http://", "https://")):
                out.append(full_url)
    return list(set(out))

# --------------------
# MAIN EXTRACTION
# --------------------
def extract_from_url(url: str) -> dict:
    url = str(url).strip()
    # Handle invalid/empty URLs
    if not url or url.lower() in ["nan", "none", ""]:
        return {k: None for k in FIELDS} | {"Website": url}
    if not url.startswith("http"):
        url = "https://" + url

    html = fetch_html(url)
    if not html: 
        return {k: None for k in FIELDS} | {"Website": url}

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    # company name
    company = None
    if soup.title:
        company = soup.title.get_text(strip=True).split("|")[0].strip()
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        company = h1.get_text(strip=True)

    phone = extract_phone(text)
    email = extract_email(text)
    location = None
    addr = soup.find("address")
    if addr:
        location = addr.get_text(" ", strip=True)

    brands = detect_brands(text, soup)
    categories = detect_keywords(text, EQUIPMENT_KEYWORDS)
    parts = detect_keywords(text, PARTS_KEYWORDS)

    # crawl subpages if missing info
    if not (phone and email and brands):
        for sub in try_subpages(url, soup)[:3]:
            sub_html = fetch_html(sub)
            if not sub_html: continue
            sub_soup = BeautifulSoup(sub_html, "lxml")
            sub_text = sub_soup.get_text(" ", strip=True)
            if not phone: phone = extract_phone(sub_text)
            if not email: email = extract_email(sub_text)
            if not location and sub_soup.find("address"):
                location = sub_soup.find("address").get_text(" ", strip=True)
            if not brands: brands = detect_brands(sub_text, sub_soup)
            if not categories: categories = detect_keywords(sub_text, EQUIPMENT_KEYWORDS)
            if not parts: parts = detect_keywords(sub_text, PARTS_KEYWORDS)

    return {
        "Company Name": company,
        "Website": url,
        "Location": location,
        "Contact Person": None,
        "Role (Contact Person)": None,
        "Phone Number": phone,
        "Email": email,
        "Brands Distributed": ", ".join(brands) if brands else None,
        "Equipment Categories Offered": ", ".join(categories) if categories else None,
        "Key Parts and Components Available": ", ".join(parts) if parts else None,
    }

# --------------------
# RUN
# --------------------
def main():
    import os
    df = pd.read_excel(INPUT_FILE)
    if "Website" not in df.columns:
        raise ValueError("Excel must have a 'Website' column")

    PROGRESS_FILE = "progress_backup.csv"
    
    # Resume from progress file if it exists
    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        progress_df = pd.read_csv(PROGRESS_FILE)
        rows = progress_df.to_dict('records')
        start_index = len(rows)
        print(f"📂 Resuming from row {start_index} (found {len(rows)} completed rows)")
    else:
        rows = []
        print(f"🚀 Starting fresh scrape of {len(df)} websites")
    
    try:
        for i, row in df.iterrows():
            # Skip already processed rows
            if i < start_index:
                continue
                
            site = str(row["Website"])
            print(f"[{i+1}/{len(df)}] Processing {site}...")
            rec = extract_from_url(site)
            rows.append(rec)
            
            # Save progress after EVERY row to avoid data loss
            pd.DataFrame(rows).to_csv(PROGRESS_FILE, index=False)
            
            # Show progress every 10 rows
            if (i + 1) % 10 == 0:
                print(f"  ✓ Progress saved ({i+1}/{len(df)} completed)")
            
            time.sleep(random.uniform(1.0, 2.0))  # polite delay
    
    except KeyboardInterrupt:
        print(f"\n⚠ Interrupted! Progress saved to {PROGRESS_FILE} ({len(rows)} rows)")
        print(f"  Run script again to resume from row {len(rows)+1}")
        return

    out_df = pd.DataFrame(rows)
    # Save CSV first (always works even if Excel is open)
    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✓ Saved CSV: {OUTPUT_CSV}")
    
    # Try to save Excel, but don't fail if file is locked
    try:
        out_df.to_excel(OUTPUT_XLSX, index=False)
        print(f"✓ Saved Excel: {OUTPUT_XLSX}")
        # Clean up progress file on success
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            print(f"✓ Cleaned up progress file")
    except PermissionError:
        print(f"⚠ Could not save {OUTPUT_XLSX} (file is open). CSV saved successfully.")
        print(f"  Close the Excel file and run: pd.read_csv('{OUTPUT_CSV}').to_excel('{OUTPUT_XLSX}', index=False)")

if __name__ == "__main__":
    main()

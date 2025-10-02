import pandas as pd

try:
    df = pd.read_excel('HVAC_Suppliers.xlsx')
    print("✓ File loaded successfully")
    print(f"\nColumns found: {list(df.columns)}")
    print(f"Number of rows: {len(df)}")
    print(f"\nFirst 5 rows:")
    print(df.head())
    
    # Check for required 'URL' column
    if 'URL' in df.columns:
        print("\n✓ Required 'URL' column is present")
        print(f"  - Non-empty URLs: {df['URL'].notna().sum()}")
        print(f"  - Sample URLs:")
        for url in df['URL'].dropna().head(3):
            print(f"    {url}")
    else:
        print("\n✗ MISSING required 'URL' column")
        print("  Script requires a column named 'URL'")
        
except Exception as e:
    print(f"Error reading file: {e}")
